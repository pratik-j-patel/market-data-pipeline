-- =============================================================================
-- stg_prices -- the one clean table.
--
-- Everything upstream of here deliberately avoided making decisions. The file
-- writer wrote what the API returned. The uploader moved bytes. COPY INTO
-- landed each line in a single VARIANT column without casting anything. All of
-- that was so that the first place a type is decided would be a file I can
-- read, in a repository, under version control. This is that file.
--
-- Grain: one row per ticker per trading day.
-- =============================================================================

with

raw_prices as (

    select * from {{ source('raw', 'raw_prices') }}

),

typed as (

    select

        -- --------------------------------------------------------- identity
        payload:ticker::varchar                    as ticker,
        payload:trade_date::date                   as trade_date,

        -- ------------------------------------------------------------ prices
        -- Every one of these is cast by hand, and that is the entire reason
        -- the raw table is VARIANT.
        --
        -- JSON has one number type; a serializer writes 229 and 229.15
        -- differently only because one happens to be a whole number. Five of
        -- these columns -- open, high, low, close, volume -- therefore arrive
        -- as JSON integers on any day a value lands on a round number, and as
        -- floats otherwise. Anything that infers a schema from a sample will
        -- type some of them INTEGER and truncate the rest without complaining.
        --
        -- This is not hypothetical. AAPL closed at exactly 229 on 2024-08-30,
        -- and that is the first row of the raw table.
        payload:open::float                        as open,
        payload:high::float                        as high,
        payload:low::float                         as low,
        payload:close::float                       as close,

        -- Volume is a share count and looks like it should be an integer, but
        -- 26% of the values arrive fractional. The API is the authority on its
        -- own numbers; FLOAT keeps what it sent.
        payload:volume::float                      as volume,

        -- Volume-weighted average price: the day's average trade price with
        -- each trade weighted by its size. Not the same as (high+low)/2.
        payload:vwap::float                        as vwap,

        -- Number of individual trades. A genuine count, so a genuine integer.
        -- Renamed because "transactions" reads like the name of a table.
        payload:transactions::int                  as transaction_count,

        -- ----------------------------------------------------------- lineage
        -- When the API produced this row, versus when the warehouse received
        -- it. The gap between the two is the pipeline's own latency, and step
        -- 11's freshness checks are built on it.
        --
        -- ingested_at_utc arrives as an ISO string carrying a +00:00 offset,
        -- so it parses to TIMESTAMP_TZ. loaded_at is TIMESTAMP_NTZ holding
        -- UTC. Converting the first to UTC and dropping the zone puts both on
        -- one basis, so subtracting them is meaningful in November, when the
        -- clocks change and 1:30am happens twice.
        convert_timezone('UTC', payload:ingested_at_utc::timestamp_tz)
            ::timestamp_ntz                        as ingested_at_utc,

        source_file,
        file_row_number,
        loaded_at

    from raw_prices

),

keyed as (

    select
        -- The grain, written down as a column.
        --
        -- Step 11 has to prove that no (ticker, trade_date) appears twice --
        -- Snowflake's COPY load history expires after 64 days, and a file
        -- re-copied past that window duplicates its rows. dbt's built-in
        -- `unique` test works on one column, so materialising the compound key
        -- here means that test is four lines of YAML instead of an extra
        -- package dependency.
        ticker || '|' || trade_date::varchar       as price_key,
        typed.*

    from typed

)

-- Deliberately NOT deduplicated.
--
-- A `qualify row_number() over (partition by price_key ...) = 1` here would be
-- one line and would guarantee this table always looks correct. That is the
-- problem with it. The duplicate it silently absorbed would be a real load
-- fault upstream, and the step 11 uniqueness test -- the whole point of which
-- is to notice exactly that -- would be permanently, uselessly green.
--
-- The staging layer's job is to make the raw data typed and legible. It is not
-- to make it look clean.
select * from keyed
