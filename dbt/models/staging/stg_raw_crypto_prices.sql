with source as (
    select * from {{ source('raw', 'crypto_prices') }}
),

valid_prices as (
    select
        upper(trim(coin))   as coin,
        price_usd,
        fetched_at,
        source
    from source
    where price_usd is not null
      and price_usd > 0
      and coin      is not null
),

-- Keep only the latest price per coin per hour (dedup API retries)
deduped as (
    select distinct on (coin, date_trunc('hour', fetched_at))
        coin,
        price_usd,
        fetched_at,
        source
    from valid_prices
    order by coin, date_trunc('hour', fetched_at), fetched_at desc
)

select * from deduped
