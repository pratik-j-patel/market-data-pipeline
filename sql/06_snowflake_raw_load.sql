-- =============================================================================
-- Landing the S3 partitions in Snowflake.
--
-- Run as ACCOUNTADMIN, in a Snowflake account created on AWS in the same region
-- as the bucket. The region has to match: a stage that reads across regions pays
-- AWS transfer on every load, and the bill grows with the data, not with the
-- setup.
--
-- Two values are deliberately not in this file. The bucket name lives in .env,
-- because the invariant this repo holds is that no value in .env appears in a
-- committed file. The AWS account ID is out because this repository is public
-- and an account ID is a useful thing for a stranger not to have.
--
--   <S3_BUCKET>       the bucket holding raw/prices/
--   <AWS_ACCOUNT_ID>  12-digit AWS account number
--
-- Sections 2 and 4 are separated by a trip to the AWS console. That ordering is
-- forced, not stylistic -- see docs/snowflake_setup.md.
-- =============================================================================


-- ---------------------------------------------------------------- 1. Container
-- XSMALL with a 60-second auto-suspend. Snowflake bills for the seconds a
-- warehouse is running, not for the queries it answers, so an idle warehouse is
-- the only way a project this size spends real money.

USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS load_wh
    WAREHOUSE_SIZE       = XSMALL
    AUTO_SUSPEND         = 60
    AUTO_RESUME          = TRUE
    INITIALLY_SUSPENDED  = TRUE;

CREATE DATABASE IF NOT EXISTS market_data;
CREATE SCHEMA   IF NOT EXISTS market_data.raw;

USE WAREHOUSE load_wh;
USE DATABASE  market_data;
USE SCHEMA    raw;


-- ------------------------------------------------- 2. Storage integration (1/2)
-- A storage integration holds no credentials. Snowflake assumes an IAM role in
-- my account instead, which is why the role has to exist before this statement
-- runs, and why the role's trust policy cannot be finished until after it.
--
-- Prerequisite: the IAM role snowflake-market-data-read exists with the
-- permission policy in aws/snowflake-read-policy.json and the bootstrap trust
-- policy in aws/snowflake-trust-policy.bootstrap.json.

CREATE OR REPLACE STORAGE INTEGRATION s3_market_data
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/snowflake-market-data-read'
    STORAGE_ALLOWED_LOCATIONS = ('s3://<S3_BUCKET>/raw/prices/')
    ENABLED                   = TRUE;

-- Read back the two values that only exist once the integration does, and put
-- them in the role's real trust policy before going any further.
DESC INTEGRATION s3_market_data;

-- >>> STOP. Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from the
-- >>> output above into aws/snowflake-trust-policy.json, replace the role's
-- >>> trust policy in the AWS console, then continue.
--
-- >>> AND DO NOT RE-RUN THIS SECTION AFTERWARDS. CREATE OR REPLACE generates a
-- >>> NEW external ID, which no longer matches the trust policy in AWS, and every
-- >>> stage operation starts failing with an assume-role error that looks like a
-- >>> permissions problem and is not. If the integration ever does have to be
-- >>> recreated, DESC it again and update the trust policy in the same sitting.


-- ------------------------------------------------------- 3. File format (2/2)
-- The files are newline-delimited JSON: one complete object per line, not a
-- JSON array. TYPE = JSON reads that as one row per line, which is the whole
-- reason step 3 wrote .jsonl instead of .json.

CREATE OR REPLACE FILE FORMAT jsonl_format
    TYPE              = JSON
    STRIP_OUTER_ARRAY = FALSE;

CREATE OR REPLACE STAGE prices_stage
    STORAGE_INTEGRATION = s3_market_data
    URL                 = 's3://<S3_BUCKET>/raw/prices/'
    FILE_FORMAT         = jsonl_format;

-- The first statement that actually exercises the IAM role. If the trust policy
-- is wrong this fails here, before any table exists.
LIST @prices_stage;


-- ------------------------------------------------------------- 4. Raw table
-- One VARIANT column holding the line exactly as it was written. Nothing is
-- cast, renamed or dropped on the way in, so this table is a faithful copy of
-- what is in S3 and the staging model in step 7 does the real work.
--
-- That matters more than it looks. Five of the ten fields -- open, high, low,
-- close and volume -- serialize as JSON integers whenever a value lands on a
-- round number, and as floats otherwise. Any tool that infers a schema from a
-- sample will type some of them INTEGER and silently truncate the rest. VARIANT
-- keeps each value's own JSON type until step 7 casts every column by hand.
--
-- The three columns after the payload are file-level lineage: which S3 object a
-- row came from, which line of it, and when it arrived. They are what makes a
-- bad row traceable back to a specific line of a specific file, and they are the
-- raw material for the freshness tests in step 11.

CREATE TABLE IF NOT EXISTS raw_prices (
    payload          VARIANT       NOT NULL,
    source_file      VARCHAR       NOT NULL,
    file_row_number  NUMBER        NOT NULL,
    -- SYSDATE() is UTC and ignores the session timezone. CURRENT_TIMESTAMP()
    -- would record whatever timezone the loading session happened to be in,
    -- which is how a freshness check ends up off by four hours in November.
    -- Verified accepted as a column DEFAULT on Snowflake Standard, Sep 2 2026.
    loaded_at        TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
);


-- --------------------------------------------------------------- 5. The load
-- COPY INTO reads the whole raw/prices/ prefix in one statement. The date=
-- partition layout from step 3 was chosen for exactly this.
--
-- loaded_at is left out of the column list on purpose: a column omitted from a
-- COPY takes its table DEFAULT, which is the only way to get SYSDATE() per row.
--
-- ON_ERROR = ABORT_STATEMENT is the default, stated because it is a decision. A
-- half-loaded table is worse than an empty one; it looks like it worked.

COPY INTO raw_prices (payload, source_file, file_row_number)
FROM (
    SELECT
        $1,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @prices_stage
)
FILE_FORMAT = (FORMAT_NAME = jsonl_format)
ON_ERROR    = ABORT_STATEMENT;


-- ------------------------------------------------------------ 6. Verification
-- Row count, and how many distinct files it came from. Compare both against the
-- local numbers in docs/snowflake_setup.md rather than against a figure written
-- here, which would go stale the next time the backfill runs.

-- rows_loaded and distinct_keys must be equal: the grain is one row per ticker
-- per trading day, so any gap between them means the same file was loaded twice.

SELECT COUNT(*)                                 AS rows_loaded,
       COUNT(DISTINCT source_file)              AS files_loaded,
       COUNT(DISTINCT payload:ticker::string
                      || '|' ||
                      payload:trade_date::string) AS distinct_keys,
       COUNT(DISTINCT payload:ticker::string)   AS tickers,
       MIN(payload:trade_date::date)            AS first_day,
       MAX(payload:trade_date::date)            AS last_day
FROM raw_prices;

-- The reason the VARIANT column was worth it: every field is still its own JSON
-- type and casts cleanly, including the five that arrive as integers on
-- round-number days.
SELECT payload:ticker::string     AS ticker,
       payload:trade_date::date   AS trade_date,
       payload:open::float        AS open,
       payload:high::float        AS high,
       payload:low::float         AS low,
       payload:close::float       AS close,
       payload:volume::float      AS volume,
       payload:vwap::float        AS vwap,
       payload:transactions::int  AS transactions,
       source_file,
       file_row_number,
       loaded_at
FROM raw_prices
LIMIT 5;


-- ----------------------------------------------------------- 7. Idempotency
-- Re-run the COPY INTO in section 5. It should report zero files processed.
--
-- Snowflake keeps a per-table record of which files it has already loaded and
-- skips them, so a repeated load is a no-op without any logic of mine. This is
-- the fourth layer of the same property: the file writer merges on
-- (trade_date, ticker), the S3 uploader compares MD5 to ETag, and the warehouse
-- skips loaded files.
--
-- The limit is worth knowing and worth writing down: that load history expires
-- after 64 days. A file re-copied after that window duplicates its rows, which
-- is precisely what the uniqueness test in step 11 is there to catch.
