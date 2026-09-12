-- =============================================================================
-- 07 -- the identity the public dashboard connects as.
--
-- Everything before this file was built by one Snowflake user with ACCOUNTADMIN.
-- That is defensible while the only thing holding the credential is a laptop.
-- It stops being defensible the moment a key is pasted into a hosting provider's
-- configuration for an app anybody on the internet can open.
--
-- So the dashboard gets its own identity, and this file is what that identity is
-- allowed to do. It is deliberately short: a reader can check the blast radius of
-- the public app by reading twenty lines rather than trusting a sentence in a
-- README.
--
-- Run as ACCOUNTADMIN, once. Re-running is safe -- every statement is IF NOT
-- EXISTS or an idempotent GRANT.
-- =============================================================================

use role accountadmin;

-- ---------------------------------------------------------------- warehouse
-- A separate warehouse from load_wh, and the reason is accounting rather than
-- performance. Snowflake reports credits per warehouse, so a dedicated one makes
-- "what does the dashboard cost" a question with an answer instead of an
-- estimate. Running cost is unchanged: both are XSMALL and every wake bills a
-- 60-second minimum wherever it happens.
--
-- INITIALLY_SUSPENDED keeps CREATE WAREHOUSE from starting it -- and billing it
-- -- at the moment it is created.
create warehouse if not exists dash_wh
    warehouse_size      = XSMALL
    auto_suspend        = 60
    auto_resume         = TRUE
    initially_suspended = TRUE
    comment             = 'Dashboard only. Separate from load_wh so its credits are attributable.';

-- ---------------------------------------------------------------- role
create role if not exists dashboard_reader
    comment = 'Read-only on marts. Nothing else.';

-- USAGE on a database or schema is permission to SEE it, not to read from it.
-- Both are required before any SELECT can resolve, and granting them on
-- market_data.marts alone is what makes staging and raw invisible to this role
-- rather than merely unreadable.
grant usage  on warehouse dash_wh            to role dashboard_reader;
grant usage  on database  market_data        to role dashboard_reader;
grant usage  on schema    market_data.marts  to role dashboard_reader;

-- ALL covers what exists now; FUTURE covers what a later dbt run creates. Only
-- the first kind appears in SHOW GRANTS TO ROLE -- future grants are attached to
-- the schema, so SHOW FUTURE GRANTS IN SCHEMA market_data.marts is where they
-- are visible. Measured 2026-09-12: five rows and two rows respectively.
grant select on all    tables in schema market_data.marts to role dashboard_reader;
grant select on future tables in schema market_data.marts to role dashboard_reader;
grant select on all    views  in schema market_data.marts to role dashboard_reader;
grant select on future views  in schema market_data.marts to role dashboard_reader;

-- ---------------------------------------------------------------- user
-- TYPE = SERVICE is the part worth reading twice. Snowflake's service users
-- "cannot log in using a password" and "cannot log in using SAML SSO" -- not by
-- policy, but because the account type has nowhere to put a password. A
-- credential that does not exist cannot be phished, guessed, reused from another
-- site, or left in a screenshot.
--
-- The practical consequence: this user cannot be tested in Snowsight at all.
-- Verification has to come through the connector, which is how the dashboard
-- reaches Snowflake anyway, so the test exercises the real path.
create user if not exists dashboard_app
    type              = SERVICE
    default_role      = dashboard_reader
    default_warehouse = dash_wh
    comment           = 'Streamlit Cloud. Key pair only -- a SERVICE user cannot hold a password.';

grant role dashboard_reader to user dashboard_app;

-- The public half of the key pair is registered separately, by
-- scripts/set_dashboard_profile.py, which generates the pair and builds the
-- ALTER USER statement to the clipboard. It is not in this file because the
-- statement carries a key, and a key does not belong in a repository even when
-- it is only the public half -- keeping the habit intact is worth more than the
-- convenience of one pasted line.

-- ---------------------------------------------------------------- verify
-- show grants to role dashboard_reader;              -- 5 rows, none in staging or raw
-- show future grants in schema market_data.marts;    -- 2 rows
-- desc user dashboard_app;                           -- RSA_PUBLIC_KEY_FP after registration
