-- =============================================================================
-- The fact table holds exactly the rows staging holds. No more, no fewer.
--
-- fct_daily_prices is built from stg_prices by adding window functions and
-- nothing else -- no join, no filter, no aggregate. So its row count has to
-- equal staging's exactly, and today this test cannot fail. That is not an
-- argument against writing it.
--
-- It is a guard against the next edit rather than against the current code.
-- The obvious future change to this mart is joining something onto it: a
-- corporate-actions table, a second seed, an index to compare against. A join
-- whose right-hand side has two matching rows silently doubles the left, and
-- the symptom is a moving average computed over the same session twice. The
-- uniqueness test on price_key catches that particular case; a join that
-- DROPPED rows -- an inner join where a left join was meant -- produces no
-- duplicate keys at all and would pass every other test in the project.
--
-- Both counts are compared, not just one. Equal row counts with unequal
-- distinct-key counts would mean the mart traded a real row for a duplicate,
-- which is the one failure a single count would hide.
-- =============================================================================

{{ config(severity = 'error') }}

with

staging as (

    select
        count(*)                  as row_count,
        count(distinct price_key) as key_count

    from {{ ref('stg_prices') }}

),

fact as (

    select
        count(*)                  as row_count,
        count(distinct price_key) as key_count

    from {{ ref('fct_daily_prices') }}

)

select
    staging.row_count as staging_rows,
    fact.row_count    as fact_rows,
    staging.key_count as staging_keys,
    fact.key_count    as fact_keys

from staging
cross join fact

where staging.row_count <> fact.row_count
   or staging.key_count <> fact.key_count
