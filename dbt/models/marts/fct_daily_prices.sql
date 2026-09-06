-- =============================================================================
-- fct_daily_prices -- what happened, per ticker, per trading day.
--
-- stg_prices already holds every measure this table holds. The difference is
-- that stg_prices answers "what did the API send?" and this answers "what would
-- someone want to look at?" -- which turns out to require five things the API
-- never sends, because every one of them is a comparison between a row and its
-- neighbours rather than a property of the row itself.
--
-- Grain: one row per ticker per trading day. Unchanged from staging. A mart is
-- allowed to change the grain; this one does not, because the questions the
-- dashboard asks ("how did AAPL move on the 14th?") are asked at exactly this
-- grain. Widening or rolling up would be a different table, not this one.
--
-- Joins to dim_tickers on ticker for company name and sector. Deliberately does
-- NOT carry those columns itself: duplicating a sector across 12,529 rows to
-- save one join means a sector correction has to be made in two places, and the
-- second one gets forgotten.
-- =============================================================================

with

daily as (

    select
        price_key,
        ticker,
        trade_date,
        open,
        high,
        low,
        close,
        volume,
        vwap,
        transaction_count,
        loaded_at

    from {{ ref('stg_prices') }}

),

windowed as (

    -- Every window below reads `partition by ticker order by trade_date`, and
    -- both halves are load-bearing.
    --
    -- PARTITION BY TICKER is not decoration. Without it these functions walk one
    -- continuous stream of 12,529 rows sorted by date, and at every boundary
    -- between one ticker and the next they reach across it: AAPL's first close
    -- would be compared against AMZN's last. No error, no warning, no null --
    -- just a wrong number on 24 rows, which is exactly the kind of wrong that
    -- survives review because nothing about it looks broken.
    --
    -- ROWS, not RANGE. `rows between 19 preceding` counts twenty ROWS, which
    -- here means twenty trading sessions -- and twenty sessions is what "20-day
    -- moving average" means to anyone who works with prices. `range` would count
    -- by the value of trade_date instead, sweeping in weekends and holidays and
    -- averaging a different number of real observations on every row.
    select
        daily.*,

        -- Yesterday's close, on the same ticker. Null on each ticker's first
        -- row, which is correct: there is no prior session to compare against.
        lag(close) over (
            partition by ticker
            order by trade_date
        )                                                   as prior_close,

        -- Twenty-session average close -- null until twenty sessions exist.
        --
        -- The `count(...) = 20` guard is the whole point. Without it, row 5 of a
        -- ticker gets the average of five days and reports it in a column named
        -- moving_avg_20d. Nothing is null, nothing errors, and the column is
        -- quietly lying on the first nineteen rows of every ticker. A partial
        -- window is not a smaller answer to the same question; it is an answer
        -- to a different question, and the honest value here is nothing.
        case
            when count(close) over (
                     partition by ticker
                     order by trade_date
                     rows between 19 preceding and current row
                 ) = 20
            then avg(close) over (
                     partition by ticker
                     order by trade_date
                     rows between 19 preceding and current row
                 )
        end                                                 as moving_avg_20d,

        -- Rolling 52-week high and low, on the same rule. 252 sessions is the
        -- conventional trading year.
        --
        -- Consequence worth stating plainly: this dataset spans roughly two
        -- years, so about half of every ticker's rows have no full 252-session
        -- window behind them and these two columns are null there. That is not
        -- a gap in the data. It is the data declining to call 180 sessions a
        -- year.
        case
            when count(high) over (
                     partition by ticker
                     order by trade_date
                     rows between 251 preceding and current row
                 ) = 252
            then max(high) over (
                     partition by ticker
                     order by trade_date
                     rows between 251 preceding and current row
                 )
        end                                                 as high_52w,

        case
            when count(low) over (
                     partition by ticker
                     order by trade_date
                     rows between 251 preceding and current row
                 ) = 252
            then min(low) over (
                     partition by ticker
                     order by trade_date
                     rows between 251 preceding and current row
                 )
        end                                                 as low_52w

    from daily

),

measured as (

    select
        windowed.*,

        -- Day-over-day return, in percent. Null wherever prior_close is null,
        -- which is each ticker's first row.
        --
        -- Not coalesced to zero. A zero return is a claim that the price did not
        -- move, and "I have nothing to compare this to" is a different statement
        -- from "it did not move." The dashboard can render a blank.
        --
        -- Not rounded either. Rounding is a presentation decision and this is
        -- not the presentation layer.
        case
            when prior_close is not null and prior_close <> 0
            then (close - prior_close) / prior_close * 100
        end                                                 as daily_return_pct,

        -- Notional value traded: price times shares, in dollars. vwap is the
        -- volume-weighted average price, so vwap x volume is the closest thing
        -- to what actually changed hands. Falls back to close on the rare row
        -- where the API omits vwap.
        coalesce(vwap, close) * volume                      as dollar_volume

    from windowed

)

-- Columns listed rather than `select *`, so the order of a table people read is
-- a decision rather than an accident of which CTE added what.
--
-- NOT DEDUPLICATED, inheriting that from stg_prices on purpose. Worth being
-- clear-eyed about what that costs here: a duplicate (ticker, trade_date) would
-- not merely appear twice, it would shift every window that spans it -- the
-- moving average would be computed over nineteen real sessions and one repeat.
-- The answer is still not to hide it. It is that step 11's uniqueness test on
-- price_key is what catches it, and a mart that silently absorbed the duplicate
-- would leave that test green while carrying wrong averages.
select
    -- grain
    price_key,
    ticker,
    trade_date,

    -- as traded
    open,
    high,
    low,
    close,
    volume,
    vwap,
    transaction_count,

    -- derived
    prior_close,
    daily_return_pct,
    moving_avg_20d,
    high_52w,
    low_52w,
    dollar_volume,

    -- lineage: when COPY INTO wrote the underlying row. Carried so the dashboard
    -- can say "data as of", and so step 11 has a timestamp to test freshness on
    -- without reaching back through staging.
    loaded_at

from measured
order by ticker, trade_date
