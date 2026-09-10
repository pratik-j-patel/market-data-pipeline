-- =============================================================================
-- A bar has to be internally consistent with itself.
--
-- The uniqueness and not-null tests check that a row EXISTS exactly once and
-- that its columns are populated. Neither of them looks at whether the numbers
-- in the row can all be true at the same time. A session's high is by
-- definition the highest price it traded at, so a row where high is below its
-- own close is not a surprising day in the market -- it is a broken row, and
-- there is no combination of trading that produces one.
--
-- These are the cheapest possible data-quality checks: they need no second
-- table, no history and no outside reference. Every fact is already in the row.
--
-- SEVERITY IS WARN, ON PURPOSE.
--   The rule agreed for this project is that a broken GRAIN turns the run red,
--   because it means this pipeline is wrong, and a strange NUMBER only warns,
--   because it means the provider sent something strange. A malformed bar is
--   the provider's. Failing the build on it would stop the marts from being
--   rebuilt -- discarding 12,724 good rows over one bad one -- and would put a
--   red DAG in front of Pratik at 7am for something he cannot fix.
--   It is still worth knowing about, which is what a warning is for.
--
-- NULLS ARE NOT THIS TEST'S JOB.
--   Every comparison below is false when either side is null, so a row with a
--   null close is invisible here. That is correct: "the number is missing" and
--   "the numbers contradict each other" are different faults, and the not-null
--   tests in models/staging/_models.yml own the first one. A test that tried to
--   answer both would report the same row twice under two names.
-- =============================================================================

{{ config(severity = 'warn') }}

select

    price_key,
    ticker,
    trade_date,
    open,
    high,
    low,
    close,
    volume,

    -- Which rules this row broke, as a list rather than one flag. A row that
    -- fails on several at once is a different kind of problem from a row that
    -- fails on one, and array_construct_compact drops the nulls so only the
    -- rules that actually fired appear.
    array_construct_compact(
        case when high < low                          then 'high is below low' end,
        case when high < open or high < close         then 'high is below open or close' end,
        case when low  > open or low  > close         then 'low is above open or close' end,
        case when least(open, high, low, close) <= 0  then 'a price is zero or negative' end,
        case when volume < 0                          then 'volume is negative' end
    ) as violations

from {{ ref('stg_prices') }}

where high < low
   or high < open or high < close
   or low  > open or low  > close
   or least(open, high, low, close) <= 0
   or volume < 0
