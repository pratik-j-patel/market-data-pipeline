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

        -- A fingerprint of the BAR -- the seven numbers the provider sends --
        -- and deliberately not of the lineage columns, which differ between two
        -- loads of the same bar by construction. This is what makes the
        -- difference between "this file was loaded twice" and "the provider
        -- revised a number" a thing SQL can see, rather than something only a
        -- person comparing rows by hand can.
        hash(open, high, low, close, volume, vwap, transaction_count)
                                                   as bar_hash,
        typed.*

    from typed

)

-- DEDUPLICATED ONLY WHERE THE PROVIDER CHANGED SOMETHING -- ASKED PER FILE.
--
-- This model used to deduplicate nothing at all, and the argument for it was
-- good: a bare `qualify row_number() = 1` guarantees this table always looks
-- correct, and the duplicate it silently absorbs would be a real load fault --
-- leaving the uniqueness test on price_key permanently, uselessly green.
--
-- That argument assumed every duplicate means the same thing. On 2026-09-11 one
-- did not. The provider revised its volume-weighted average price for
-- 2026-09-08 on seven of twenty-five tickers, by between one and seven
-- ten-thousandths of a dollar. That changed the file's bytes, so S3 re-uploaded
-- it, so Snowflake re-copied it whole, so twenty-five rows arrived a second time
-- and the pipeline stopped -- over a correction of one hundredth of a cent that
-- no reader of this data would ever have seen.
--
-- So the two cases were separated. But they were separated per KEY, and that
-- was one layer off, which 2026-09-14 proved: a revision on 2026-09-09 and
-- 2026-09-10 moved 15 bars, and COPY brought the other 35 rows of those two day
-- files back UNCHANGED. Asked per key, those 35 look exactly like a file loaded
-- twice for no reason. They are not. They are passengers.
--
-- A COPY reloads a FILE, so the file is the unit that has to be judged:
--
--   nothing in the file changed  ->  keep every load of every key in it.
--       The file really was loaded twice and nothing upstream changed, so there
--       is nothing to absorb. The uniqueness test below still fails, which is
--       what it is for and what it caught on 2026-09-07.
--
--   anything in the file changed ->  keep the newest load of each key in it.
--       The provider revised at least one bar and the whole file came back with
--       it. The newest load is what S3 holds and what the file on disk says, so
--       it is what this table should agree with -- for the revised bars and for
--       the unchanged ones that travelled with them alike.
--
-- min(...) = max(...) rather than count(distinct ...): Snowflake does not accept
-- DISTINCT inside a window function, and when every value in a partition is
-- equal its minimum and maximum are the same value. A hash collision between two
-- genuinely different bars would keep both rows and fail the test -- a false
-- alarm rather than a silent pass, which is the correct direction to be wrong in.

, flagged as (

    select
        keyed.*,
        not equal_null(
            min(bar_hash) over (partition by price_key),
            max(bar_hash) over (partition by price_key)
        )                                          as key_was_revised

    from keyed

),

scoped as (

    -- Did ANY key in this source file carry a revision? Every row of the file
    -- inherits that answer, which is what makes the passengers visible as
    -- passengers.
    select
        flagged.*,
        max(iff(key_was_revised, 1, 0))
            over (partition by source_file) = 1    as file_was_revised

    from flagged

),

kept as (

    select * from scoped
    qualify
        not file_was_revised
        or row_number() over (partition by price_key order by loaded_at desc) = 1

)

-- The two flags are working columns, not part of this model's contract.
select * exclude (key_was_revised, file_was_revised) from kept
