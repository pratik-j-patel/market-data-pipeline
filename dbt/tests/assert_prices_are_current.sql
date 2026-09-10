-- =============================================================================
-- The newest bar in the warehouse is as recent as the market allows.
--
-- THE OBVIOUS VERSION OF THIS TEST IS WRONG, AND IT WAS MEASURED WRONG RATHER
-- THAN ASSUMED WRONG.
--
--   "Fail if the data is more than one day old" is what a freshness check
--   normally looks like, and on this pipeline it would have failed on Tuesday
--   2026-09-08. Not because anything was broken -- because Friday closed at
--   2026-09-04, Saturday and Sunday have no bars, and Monday was Labor Day. The
--   newest bar was four calendar days old and the pipeline was working
--   perfectly. That gap was measured on 2026-09-07: three days is the ordinary
--   maximum, and it arrives several times a year.
--
--   A check that cries wolf every long weekend gets muted, and a muted check is
--   worse than no check, because the project can still claim to have one.
--
-- SO THE THRESHOLD IS COUNTED IN WEEKDAYS, NOT IN DAYS.
--
--   This query returns one row per weekday that has gone by since the newest
--   bar the warehouse holds. Saturdays and Sundays are not counted, so a normal
--   Monday morning returns one row whether or not a weekend intervened, and the
--   count no longer moves for reasons that have nothing to do with the
--   pipeline.
--
--   The count is the answer, so the thresholds live in config() below rather
--   than in the WHERE clause, and dbt does the comparing. That is what
--   warn_if and error_if are for.
--
-- WHY THE THRESHOLDS ARE 3 AND 5
--
--   1 weekday behind is the healthy state. A session's close is not published
--     until that evening at the earliest, so at 7am the newest bar available is
--     yesterday's.
--   2 weekdays behind is routine and expected. It is what a public holiday
--     produces, and it is also what a slow publisher produces: day D's bar can
--     appear as late as about 11am ET on D+1, after the 7am run has been and
--     gone. Both resolve themselves by the next morning.
--   3 or 4 weekdays behind WARNS. Legitimate, but only in combination -- a
--     holiday falling next to a weekend AND a late bar, or the rare two-day
--     market closure. Worth a look; not worth stopping the build.
--   5 or more weekdays behind is an ERROR. That is a full trading week with no
--     new data. There is no holiday schedule and no publishing delay that
--     produces it. Something is broken.
--
-- WHAT THIS TEST DOES NOT CLAIM
--
--   It does not notice a crash quickly, and it is not meant to. A pipeline that
--   dies on Monday reaches three weekdays behind on Wednesday -- Airflow going
--   red on the failing task is what catches that within minutes. This test
--   catches the other failure, the quiet one: every task green, every exit code
--   zero, and no new data arriving anyway. That is the failure mode this
--   project has actually produced before, and the one nothing else here would
--   notice.
--
-- WHY current_timestamp IS CONVERTED AND NOT USED AS IT COMES
--
--   Snowflake's current_timestamp follows the SESSION timezone, and this
--   account's default is not New York. "What trading day is it" is a New York
--   question -- the same distinction the ingestion code draws between trading
--   days in ET and instants in UTC -- so the conversion is named here rather
--   than inherited from whatever timezone the connection happened to open with.
--   Without it a late-evening run computes yesterday and reads one weekday
--   less behind than it really is.
--
-- An EMPTY stg_prices returns no rows here and therefore passes; that hole is
-- covered on purpose by tests/assert_warehouse_not_empty.sql, which exists so
-- that this test can be about lateness and only lateness.
-- =============================================================================

{{ config(
    severity = 'error',
    warn_if  = '>= 3',
    error_if = '>= 5'
) }}

with

bounds as (

    select
        max(trade_date) as newest_trade_date,

        -- Today, in the timezone the market keeps.
        convert_timezone('America/New_York', current_timestamp())::date as today_et

    from {{ ref('stg_prices') }}

),

-- Every calendar day after the newest bar the warehouse holds, out to a ceiling
-- of sixty. Sixty is not an expectation about how late the data can be; it is
-- just further than the count needs to go before the error threshold has been
-- passed many times over.
horizon as (

    select
        bounds.newest_trade_date,
        bounds.today_et,
        dateadd('day', generated.offset_days + 1, bounds.newest_trade_date) as calendar_day

    from bounds
    cross join (
        select seq4() as offset_days
        from table(generator(rowcount => 60))
    ) as generated

)

-- One row per weekday with no data behind it. The row count IS the measure.
select
    newest_trade_date,
    today_et,
    calendar_day as weekday_with_no_data

from horizon

where calendar_day <= today_et

  -- DAYOFWEEKISO, not DAYOFWEEK. Snowflake's plain DAYOFWEEK is numbered from
  -- whatever the WEEK_START session parameter says, which defaults to 0 =
  -- Sunday but is an account setting somebody can change. The ISO variant is
  -- fixed at 1 = Monday through 7 = Sunday and cannot be reconfigured out from
  -- under this test. Same reasoning as converting the timezone above: do not
  -- let a session setting decide what a weekday is.
  and dayofweekiso(calendar_day) between 1 and 5
