"""
DAG: fetch_crypto_prices
Schedule: every hour
1. Fetch BTC/ETH/USDT/SOL prices from CoinGecko free API
2. Load into raw.crypto_prices
3. Record DQ check (expected 4 prices, flag if any are missing)
"""

import json
import time
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

COINGECKO_URL = 'https://api.coingecko.com/api/v3/simple/price'
COIN_IDS = {          # CoinGecko id → our internal symbol
    'bitcoin':   'BTC',
    'ethereum':  'ETH',
    'tether':    'USDT',
    'solana':    'SOL',
}
CONN_ID = 'crypto_dw'
MAX_RETRIES = 3
RETRY_BACKOFF = 30   # seconds between retries on 429


@dag(
    dag_id='fetch_crypto_prices',
    schedule='0 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ingestion', 'phase-1'],
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)},
)
def fetch_prices_dag():

    @task
    def fetch_from_coingecko(logical_date=None) -> dict:
        """Call CoinGecko API with retry logic on rate-limit errors (429)."""
        params = {
            'ids': ','.join(COIN_IDS.keys()),
            'vs_currencies': 'usd',
        }
        headers = {'Accept': 'application/json'}

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(COINGECKO_URL, params=params, headers=headers, timeout=15)
                if resp.status_code == 429:
                    print(f"Rate limited (attempt {attempt}/{MAX_RETRIES}). Sleeping {RETRY_BACKOFF}s…")
                    time.sleep(RETRY_BACKOFF)
                    continue
                resp.raise_for_status()
                data = resp.json()
                # Normalise to {symbol: price_usd}
                prices = {}
                for cg_id, symbol in COIN_IDS.items():
                    entry = data.get(cg_id, {})
                    price = entry.get('usd')
                    if price and price > 0:
                        prices[symbol] = float(price)
                    else:
                        print(f"WARNING: price missing or zero for {symbol} ({cg_id})")
                print(f"Fetched prices: {prices}")
                return prices
            except requests.RequestException as exc:
                print(f"Request failed (attempt {attempt}): {exc}")
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(10)

        return {}

    @task
    def load_prices(prices: dict, logical_date=None) -> int:
        """Insert fetched prices into raw.crypto_prices."""
        if not prices:
            print("No prices to load.")
            return 0

        fetch_ts = logical_date or datetime.utcnow()
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        rows_loaded = 0
        for symbol, price_usd in prices.items():
            cursor.execute(
                """
                INSERT INTO raw.crypto_prices (coin, price_usd, fetched_at, source)
                VALUES (%s, %s, %s, 'coingecko')
                """,
                (symbol, price_usd, fetch_ts),
            )
            rows_loaded += 1

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Loaded {rows_loaded} price records.")
        return rows_loaded

    @task
    def record_dq_check(rows_loaded: int, logical_date=None) -> None:
        """Flag if fewer than 4 coin prices were fetched."""
        expected = len(COIN_IDS)
        status = 'pass' if rows_loaded == expected else 'warning' if rows_loaded > 0 else 'fail'

        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO monitoring.dq_check_results
                (table_name, check_name, status, details)
            VALUES (%s, %s, %s, %s)
            """,
            (
                'raw.crypto_prices',
                'hourly_price_completeness',
                status,
                json.dumps({
                    'fetched_at': str(logical_date),
                    'rows_loaded': rows_loaded,
                    'expected': expected,
                }),
            ),
        )
        conn.commit()
        cursor.close()
        conn.close()
        print(f"DQ check recorded: {status}")

    prices = fetch_from_coingecko()
    rows = load_prices(prices)
    record_dq_check(rows)


fetch_prices_dag()
