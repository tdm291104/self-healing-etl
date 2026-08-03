with source as (
    select * from {{ source('raw', 'trades') }}
),

cleaned as (
    select
        id,
        trim(user_id)               as user_id,
        upper(trim(coin))           as coin,
        lower(trim(side))           as side,
        quantity::numeric(24, 8)    as quantity,
        trade_time,
        source_file,
        ingested_at
    from source
    where user_id   is not null
      and coin      is not null
      and trade_time is not null
      and quantity  is not null
      and quantity  > 0
)

select * from cleaned
