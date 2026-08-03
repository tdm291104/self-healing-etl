with trades_usd as (
    select * from {{ ref('fct_trades_usd_value') }}
),

-- Latest price per coin for mark-to-market valuation
latest_prices as (
    select distinct on (coin)
        coin,
        price_usd   as current_price_usd
    from {{ ref('stg_raw_crypto_prices') }}
    order by coin, fetched_at desc
),

-- Net coin holdings per user (buys add, sells subtract)
user_holdings as (
    select
        user_id,
        coin,
        sum(case when side = 'buy'  then  quantity
                 when side = 'sell' then -quantity
                 else 0
            end)             as net_quantity,
        sum(case when side = 'buy'  then  trade_value_usd
                 when side = 'sell' then -trade_value_usd
                 else 0
            end)             as net_cost_usd  -- positive = net spend, negative = net proceeds
    from trades_usd
    group by user_id, coin
),

portfolio_value as (
    select
        h.user_id,
        h.coin,
        h.net_quantity,
        h.net_cost_usd,
        lp.current_price_usd,
        h.net_quantity * lp.current_price_usd   as position_value_usd,
        now()                                    as calculated_at
    from user_holdings h
    left join latest_prices lp on h.coin = lp.coin
)

select * from portfolio_value
