-- =============================================================================
-- The staging table is not empty.
--
-- Every other test in this project is written the same way: find the rows that
-- break a rule, and fail if there are any. That phrasing has one hole in it,
-- and it is a wide one. An empty table breaks no rules. Drop every row from
-- stg_prices and the uniqueness test passes, both not-null tests pass, the
-- relationships test passes, the price-sanity test passes, and dbt prints a
-- screen of green over a warehouse holding nothing at all.
--
-- This test asserts the one thing none of the others can. It is what makes the
-- rest of them mean something.
--
-- Deliberately not "fewer than N rows" or "fewer than 25 tickers". A
-- threshold tied to today's universe has to be edited every time a ticker is
-- added, and a test that people edit in order to make it pass has stopped
-- being a test. "A seeded ticker has no history at all" is a real question and
-- it has its own answer: the not-null test on dim_tickers.first_trade_date.
-- =============================================================================

{{ config(severity = 'error') }}

with counted as (

    select count(*) as row_count
    from {{ ref('stg_prices') }}

)

select row_count
from counted
where row_count = 0
