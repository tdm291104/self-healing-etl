"""
DAG: ingest_trades
Schedule: daily at 06:00 UTC
1. Generate today's trades CSV (calls generate_trades_csv.py logic inline)
2. Load CSV rows into raw.trades
3. Record row-count data quality check
"""

import csv
import json
import os
import random
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

COINS = ['BTC', 'ETH', 'USDT', 'SOL']
NUM_USERS = 50
NUM_TRADES = 500
QUANTITY_RANGES = {
    'BTC':  (0.001, 2.0),
    'ETH':  (0.01, 20.0),
    'USDT': (10.0, 10_000.0),
    'SOL':  (0.1, 500.0),
}

DATA_DIR = '/opt/airflow/data'
CONN_ID = 'crypto_dw'


@dag(
    dag_id='ingest_trades',
    schedule='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ingestion', 'phase-1'],
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)},
)
def ingest_trades_dag():

    @task
    def generate_csv(ds: str) -> str:
        """Generate a clean trades CSV for the given execution date."""
        trade_date = datetime.strptime(ds, '%Y-%m-%d')
        os.makedirs(DATA_DIR, exist_ok=True)

        random.seed(ds)  # reproducible per date
        records = []
        for _ in range(NUM_TRADES):
            coin = random.choice(COINS)
            lo, hi = QUANTITY_RANGES[coin]
            trade_time = trade_date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )
            records.append({
                'user_id':    f'user_{random.randint(1, NUM_USERS):04d}',
                'coin':       coin,
                'side':       random.choice(['buy', 'sell']),
                'quantity':   round(random.uniform(lo, hi), 6),
                'trade_time': trade_time.strftime('%Y-%m-%d %H:%M:%S'),
            })

        filepath = os.path.join(DATA_DIR, f'trades_{ds}.csv')
        fieldnames = ['user_id', 'coin', 'side', 'quantity', 'trade_time']
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        print(f"Generated {len(records)} trades → {filepath}")
        return filepath

    @task
    def load_to_postgres(filepath: str, ds: str) -> int:
        """Insert CSV rows into raw.trades, skip duplicates by source_file."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        source_file = os.path.basename(filepath)

        # Idempotent: delete rows from this file before re-inserting
        cursor.execute("DELETE FROM raw.trades WHERE source_file = %s", (source_file,))

        rows_loaded = 0
        with open(filepath, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute(
                    """
                    INSERT INTO raw.trades
                        (user_id, coin, side, quantity, raw_quantity, trade_time, source_file)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        row['user_id'],
                        row['coin'],
                        row.get('side'),
                        row.get('quantity'),
                        row.get('quantity'),    # raw_quantity = original string
                        row['trade_time'],
                        source_file,
                    ),
                )
                rows_loaded += 1

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Loaded {rows_loaded} rows into raw.trades (source: {source_file})")
        return rows_loaded

    @task
    def record_dq_check(rows_loaded: int, ds: str) -> None:
        """Write row-count check result to monitoring.dq_check_results."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        status = 'pass' if rows_loaded > 0 else 'fail'
        cursor.execute(
            """
            INSERT INTO monitoring.dq_check_results
                (table_name, check_name, status, details)
            VALUES (%s, %s, %s, %s)
            """,
            (
                'raw.trades',
                'daily_row_count',
                status,
                json.dumps({'date': ds, 'rows_loaded': rows_loaded}),
            ),
        )
        conn.commit()
        cursor.close()
        conn.close()
        print(f"DQ check recorded: {status} ({rows_loaded} rows for {ds})")

    filepath = generate_csv()
    rows = load_to_postgres(filepath)
    record_dq_check(rows)


ingest_trades_dag()
