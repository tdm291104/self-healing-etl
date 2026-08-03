with trades as (
    select * from {{ ref('stg_raw_trades') }}
),

prices as (
    select * from {{ ref('stg_raw_crypto_prices') }}
),

-- LATERAL join: for each trade, find the closest price within ±15 minutes.
-- This is the core business join and is also the fallback the agent uses
-- when recent prices are missing (see Phase 3 self-healing logic).
trades_with_price as (
    select
        t.id                                             as trade_id,
        t.user_id,
        t.coin,
        t.side,
        t.quantity,
        t.trade_time,
        p.price_usd,
        case
            when p.price_usd is not null
                then t.quantity * p.price_usd
            else null
        end                                              as trade_value_usd,
        p.fetched_at                                     as price_fetched_at,
        abs(extract(epoch from (p.fetched_at - t.trade_time))) as price_lag_seconds
    from trades t
    left join lateral (
        select price_usd, fetched_at
        from prices p_inner
        where p_inner.coin = t.coin
          and p_inner.fetched_at between t.trade_time - interval '15 minutes'
                                     and t.trade_time + interval '15 minutes'
        order by abs(extract(epoch from (p_inner.fetched_at - t.trade_time)))
        limit 1
    ) p on true
)

select * from trades_with_price
