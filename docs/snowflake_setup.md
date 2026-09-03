# Loading the S3 data into Snowflake

Everything in this file is one-time setup. Once it is done, `sql/06_snowflake_raw_load.sql`
section 5 is the only statement that runs again.

The order below is forced by a chicken-and-egg in the way Snowflake authenticates
to S3, and following it out of order costs an evening. Snowflake reads the bucket
by assuming an IAM role in the AWS account — no keys are stored in Snowflake at
all — but the two values that lock that role to this one Snowflake account do not
exist until the integration has been created, and the integration cannot be
created until the role does. So the role gets a deliberately useless trust policy
first, and a real one after.

## What you need before starting

- The bucket name and region from `.env` (`S3_BUCKET`, `AWS_DEFAULT_REGION`).
- The 12-digit AWS account ID.
- Console access to AWS IAM.

Replace `<S3_BUCKET>` and `<AWS_ACCOUNT_ID>` in the SQL and JSON files as you go.
Neither value is committed to this repository, which is why they are placeholders.

## Trip 1 — AWS: create the role

1. IAM → Policies → Create policy → JSON. Paste `aws/snowflake-read-policy.json`
   with the bucket name filled in. Name it `snowflake-market-data-read`.

   The policy grants read and list only. Snowflake's published template also
   includes `PutObject` and `DeleteObject`; they are removed here because
   Snowflake never writes to this bucket, and a role that cannot write cannot be
   made to damage the data by a bug on either side. `GetObjectVersion` is kept
   even though bucket versioning is off — it is inert now and correct if
   versioning is ever turned on.

2. IAM → Roles → Create role. Trusted entity type **AWS account**, then
   **Another AWS account** — and enter *your own* account ID. Tick **Require
   external ID** and enter `0000`.

   "Another AWS account" while entering your own account ID looks wrong and is
   not. The **Require external ID** checkbox only appears on that branch; the
   **This account** branch offers "Require MFA" instead, and a role created there
   cannot be given an external ID from the console at all. Everything here is a
   placeholder anyway: AWS will not create a role without a principal, and the
   real principal is a Snowflake-side IAM user whose ARN does not exist until the
   storage integration does. Both values are overwritten in trip 2.

3. Attach it, and name the role `snowflake-market-data-read` as well. A policy
   and a role can share a name because they are different resource types and
   their ARNs differ (`:policy/...` versus `:role/...`), which is what every
   caller actually resolves. Leave **Set permissions boundary** untouched.
   Create the role, then copy the **Role ARN** from the summary page;
   `sql/06_snowflake_raw_load.sql` section 2 needs it.

4. Before creating, read the trust policy the console generated on the review
   page — it is the only place a wrong turn on the trusted-entity screen shows
   up before the role exists. It is correct when `Action` is `sts:AssumeRole`,
   the principal is your own account, and there is an `sts:ExternalId` condition
   of `0000`. The console renders the principal as the bare account ID
   (`"AWS": "888348805721"`) rather than the `arn:aws:iam::<id>:root` form used
   in `aws/snowflake-trust-policy.bootstrap.json`; IAM treats the two as the same
   principal, so either is fine. A missing `sts:ExternalId` is the failure to
   watch for — that means the **This account** branch was taken.

AWS moves these console paths regularly — the charge-type filter and the S3
bucket namespace option both moved during step 5. If a screen does not look like
the description, the labels above are the thing to search for, not the clicks.

## Snowflake: create the account

Sign up at <https://signup.snowflake.com>.

- Edition: **Standard**. Everything in this project runs on Standard, and it is
  the cheapest per credit once the trial ends.
- Cloud provider: **AWS**.
- Region: **must match `AWS_DEFAULT_REGION`**. A stage reading across regions pays
  AWS transfer on every load, forever, for no benefit.

The trial is 30 days or $400 of credits, whichever runs out first. At this data
volume the clock is the binding constraint, not the credits.

## Snowflake: sections 1 and 2

Run sections 1 and 2 of `sql/06_snowflake_raw_load.sql` as `ACCOUNTADMIN`.

`DESC INTEGRATION s3_market_data` prints a property table. Two rows matter:

- `STORAGE_AWS_IAM_USER_ARN` — an IAM user in Snowflake's own AWS account.
- `STORAGE_AWS_EXTERNAL_ID` — a value unique to this integration.

The external ID is what stops another Snowflake customer who learns your role ARN
from assuming it. The role trusts Snowflake's IAM user *and* that specific string.

## Trip 2 — AWS: finish the trust policy

IAM → Roles → `snowflake-market-data-read` → Trust relationships → Edit trust
policy. Replace the whole document with `aws/snowflake-trust-policy.json`,
substituting the two values from `DESC INTEGRATION`.

## Snowflake: sections 3 to 7

Run section 3. `LIST @prices_stage` is the first statement that actually uses the
role, so this is where a credential problem surfaces, before any table exists.

- **503 rows listed** — the stage works.
- **Zero rows, no error** — the stage authenticated but found nothing at that
  prefix. Check `URL` and `STORAGE_ALLOWED_LOCATIONS` against the real prefix.
- **An access-denied or assume-role error** — check the trust policy: both the
  IAM user ARN and the external ID have to match `DESC INTEGRATION` exactly, and
  IAM changes take a few seconds to take effect.

Then run sections 4 and 5. Section 6 should return the same figures the local
copy holds:

```
$ ls -d data/date=*/ | wc -l          # 503 partitions
$ cat data/date=*/prices.jsonl | wc -l # 12529 rows
```

`rows_loaded` and `distinct_keys` should be equal. If `distinct_keys` is lower,
the same file was loaded twice.

Finally, re-run section 5 unchanged. It should report zero files processed. That
is Snowflake's own load history, not logic in this repository — and it expires
after 64 days, after which re-copying an old file would duplicate its rows.

## Cost, after the trial

The trial converts to on-demand billing when a card is added; without one the
account stops. At roughly 5.5 MB of storage and a warehouse that auto-suspends
after 60 seconds, the running cost is small, but check Snowflake's current
on-demand rates rather than trusting a figure written here.
