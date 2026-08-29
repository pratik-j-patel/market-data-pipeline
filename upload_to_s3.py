#!/usr/bin/env python3
"""
Upload the daily price partitions to S3.

    python upload_to_s3.py --dry-run     # show what would change, touch nothing
    python upload_to_s3.py               # upload what has changed
    python upload_to_s3.py --quiet       # totals only

WHAT GOES UP
    data/date=YYYY-MM-DD/prices.jsonl  ->  s3://$S3_BUCKET/raw/prices/date=YYYY-MM-DD/prices.jsonl

    The local partition layout is preserved verbatim, because step 6 loads the
    whole prefix with a single Snowflake COPY INTO. Hive-style `date=` folders
    are also what Snowflake, Spark and Athena expect to read as a partition key.

    data/_raw/ is NOT uploaded. Those are unparsed API responses kept as local
    crash insurance; paying to store them twice buys nothing.

IDEMPOTENCY
    S3 returns an ETag per object. For a single-part upload the ETag IS the
    MD5 of the content, so comparing it to the local file's MD5 tells us
    whether the bytes already match -- no download, no guesswork. Unchanged
    files are skipped. Running this twice in a row uploads nothing the second
    time, which is the same property fetch_tickers.py has at the file layer.

    One LIST call returns up to 1000 keys with their ETags, so checking all
    ~500 partitions costs one request rather than 500 HEADs.

    Uploads are sequential. At ~11 KB per file the run is short enough that a
    thread pool would add failure modes for a few seconds of wall clock.
    Revisit if the partition count grows by an order of magnitude.

CREDENTIALS
    Read from .env into the environment by python-dotenv, then picked up by
    boto3's standard credential chain. No key is ever written in this file.
    Set or rotate them with scripts/set_aws_keys.sh.
"""

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"

BUCKET = os.getenv("S3_BUCKET")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# Everything the pipeline lands unmodified from the source goes under raw/.
# Later dbt layers get their own prefixes; keeping them separate from day one
# costs nothing and saves a migration.
DEST_PREFIX = "raw/prices"

CONTENT_TYPE = "application/x-ndjson"


def new_run_id() -> str:
    """UTC instant, matching the run ids fetch_tickers.py writes."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M")


def md5_hex(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def local_partitions() -> list:
    """Every data/date=*/prices.jsonl, sorted by date."""
    return sorted(DATA_DIR.glob("date=*/prices.jsonl"))


def key_for(path: Path) -> str:
    return f"{DEST_PREFIX}/{path.parent.name}/{path.name}"


def remote_etags(s3) -> dict:
    """{key: etag} for everything already under DEST_PREFIX."""
    etags = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=DEST_PREFIX + "/"):
        for obj in page.get("Contents", []):
            etags[obj["Key"]] = obj["ETag"].strip('"')
    return etags


def write_manifest(run_id, uploaded, skipped, failed, bytes_sent, dry_run) -> Path:
    manifest = {
        "run_id": run_id,
        "kind": "upload",
        "dry_run": dry_run,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "bucket": BUCKET,
        "prefix": DEST_PREFIX,
        "region": REGION,
        "files_uploaded": len(uploaded),
        "files_skipped_identical": skipped,
        "files_failed": failed,
        "bytes_uploaded": bytes_sent,
        "oldest_partition_uploaded": uploaded[0] if uploaded else None,
        "newest_partition_uploaded": uploaded[-1] if uploaded else None,
    }
    path = DATA_DIR / "_runs" / f"upload_{run_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2))
    return path


def die(observed: str, check: str) -> None:
    """State what was observed and what to check. Never a cause we cannot prove."""
    print(f"\nSTOPPED: {observed}", file=sys.stderr)
    print(f"Check:   {check}", file=sys.stderr)
    sys.exit(1)


def run(dry_run: bool, verbose: bool) -> int:
    if not BUCKET:
        die("S3_BUCKET is not set in the environment.",
            ".env should contain S3_BUCKET=... . Run scripts/set_aws_keys.sh.")

    files = local_partitions()
    if not files:
        die(f"No files matched {DATA_DIR}/date=*/prices.jsonl",
            "Run fetch_tickers.py first, or check you are in the project root.")

    s3 = boto3.client("s3", region_name=REGION)

    try:
        existing = remote_etags(s3)
    except NoCredentialsError:
        die("boto3 found no AWS credentials.",
            ".env should contain AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. "
            "Run scripts/set_aws_keys.sh.")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "?")
        die(f"S3 refused the list request on '{BUCKET}' with code {code}.",
            "The bucket name in .env, the region, and that the IAM policy grants "
            "s3:ListBucket on arn:aws:s3:::<bucket> (no trailing /*).")

    print(f"bucket:      s3://{BUCKET}/{DEST_PREFIX}/  ({REGION})")
    print(f"local files: {len(files)}")
    print(f"already in S3: {len(existing)}")
    if dry_run:
        print("mode:        DRY RUN -- nothing will be written\n")
    else:
        print()

    uploaded, failed, skipped, bytes_sent = [], [], 0, 0

    for path in files:
        key = key_for(path)
        digest = md5_hex(path)

        if existing.get(key) == digest:
            skipped += 1
            if verbose and skipped <= 3:
                print(f"  skip   {key}  (identical)")
            continue

        if dry_run:
            uploaded.append(path.parent.name)
            if verbose:
                print(f"  WOULD UPLOAD  {key}")
            continue

        try:
            s3.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=path.read_bytes(),
                ContentType=CONTENT_TYPE,
            )
            uploaded.append(path.parent.name)
            bytes_sent += path.stat().st_size
            if verbose:
                print(f"  put    {key}")
        except (ClientError, BotoCoreError) as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code", type(exc).__name__)
            failed.append({"key": key, "code": code})
            print(f"  FAILED {key}  ({code})", file=sys.stderr)

    if verbose and skipped > 3:
        print(f"  skip   ... and {skipped - 3} more identical files")

    run_id = new_run_id()
    manifest_path = write_manifest(run_id, uploaded, skipped, failed, bytes_sent, dry_run)

    verb = "would upload" if dry_run else "uploaded"
    print(f"\n{verb}:    {len(uploaded)}")
    print(f"skipped:     {skipped} (identical)")
    print(f"failed:      {len(failed)}")
    if not dry_run:
        print(f"bytes sent:  {bytes_sent:,}")
    print(f"manifest:    {manifest_path.relative_to(PROJECT_ROOT)}")

    if failed:
        print("\nSome objects did not upload. The run is INCOMPLETE.", file=sys.stderr)
        return 1
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload daily price partitions to S3.")
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would change without writing to S3")
    parser.add_argument("--quiet", action="store_true", help="totals only")
    args = parser.parse_args()
    sys.exit(run(dry_run=args.dry_run, verbose=not args.quiet))


if __name__ == "__main__":
    main()
