-- =============================================================================
-- dim_tickers -- one row per ticker in the universe.
--
-- A dimension table answers "what is this thing?" A fact table answers "what
-- happened?" stg_prices is all fact: 12,529 rows of what happened, and not one
-- column saying what AAPL *is*. Everything a dashboard needs to group, filter
-- or label by -- company name, sector -- has to come from somewhere else,
-- because the price API does not send it. This is that somewhere else.
--
-- Grain: one row per ticker. Twenty-five rows.
--
-- KEY: the ticker symbol itself, not a generated integer.
--   Kimball's textbook answer is a surrogate key -- a meaningless integer ID
--   on every dimension. Surrogate keys mainly exist to serve SCD Type 2,
--   where one real-world ticker needs several rows (one per version of its
--   attributes) and the natural key therefore stops being unique. This
--   dimension is Type 1 -- an attribute change overwrites, no history kept --
--   so the natural key is still unique, and it is stable and readable besides.
--   A surrogate here would add a join hop and a package dependency and buy
--   nothing. Worth knowing this is a deliberate deviation, not an oversight.
-- =============================================================================

with

-- The hand-maintained side: symbol, company, sector. See seeds/_seeds.yml.
reference_data as (

    select
        ticker,
        company_name,
        sector

    from {{ ref('ticker_reference') }}

),

-- The observed side: what the warehouse has actually seen trade.
--
-- These three columns are derived from the fact table, which a purist would
-- object to on the grounds that a dimension describes things and a fact
-- measures events. The practical argument wins here: the step's definition of
-- done is "tables a dashboard can query with no further joins", and without
-- these the first question anyone asks of this table -- do I have full history
-- for every ticker? -- requires a join and a GROUP BY.
--
-- trading_days and price_rows are deliberately two columns, not one.
-- stg_prices is not deduplicated (step 7, on purpose), so counting rows and
-- counting distinct dates are different questions. Equal for all 25 tickers
-- means no duplicates. Unequal for one means a load fault on that ticker, and
-- it is visible here without anyone going looking for it.
traded as (

    select
        ticker,
        min(trade_date)             as first_trade_date,
        max(trade_date)             as last_trade_date,
        count(distinct trade_date)  as trading_days,
        count(*)                    as price_rows

    from {{ ref('stg_prices') }}

    group by ticker

),

combined as (

    -- FULL OUTER JOIN, not a LEFT JOIN from the reference side.
    --
    -- A left join would answer "for each ticker I have written down, what
    -- traded?" and would silently drop a ticker that trades but is missing
    -- from the seed. That is the same failure the staging model refuses to
    -- commit by not deduplicating: a query shaped so that a real problem
    -- upstream cannot produce a visible symptom downstream.
    --
    -- A full outer join makes both failures show up as NULLs in the output:
    --   trades but not in the seed  ->  NULL company_name, NULL sector
    --   in the seed but never traded ->  NULL first_trade_date, 0 history
    -- Step 11's not-null tests then have something real to fail on.
    select
        coalesce(reference_data.ticker, traded.ticker)  as ticker,

        reference_data.company_name,
        reference_data.sector,

        traded.first_trade_date,
        traded.last_trade_date,
        traded.trading_days,
        traded.price_rows

    from reference_data
    full outer join traded
        on reference_data.ticker = traded.ticker

)

select * from combined
order by ticker
