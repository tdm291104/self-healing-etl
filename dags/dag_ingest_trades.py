"""
DAG: ingest_trades
Schedule: daily at 06:00 UTC
1. Generate trades CSV with controlled error injection by simulation day
2. Load CSV rows into raw.trades (resilient: handles bad types, renamed/missing columns)
3. Run 5 DQ checks and write results to monitoring.dq_check_results
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
SIMULATION_START = datetime(2024, 1, 1)
EXPECTED_COLUMNS = {'user_id', 'coin', 'side', 'quantity', 'trade_time'}


def _sim_day(ds: str) -> int:
    return (datetime.strptime(ds, '%Y-%m-%d') - SIMULATION_START).days + 1


def _base_records(trade_date: datetime, seed: str) -> list[dict]:
    random.seed(seed)
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
    return records


def _inject_errors(records: list[dict], sim_day: int) -> tuple[list[dict], list[str]]:
    """Apply a controlled error based on simulation day. Returns (records, fieldnames)."""
    fieldnames = ['user_id', 'coin', 'side', 'quantity', 'trade_time']

    if sim_day == 3:
        # Schema drift: upstream renamed user_id → uid
        records = [
            {'uid': r['user_id'], 'coin': r['coin'], 'side': r['side'],
             'quantity': r['quantity'], 'trade_time': r['trade_time']}
            for r in records
        ]
        fieldnames = ['uid', 'coin', 'side', 'quantity', 'trade_time']

    elif sim_day == 5:
        # 15% duplicate rows (simulates source system double-send)
        n_dupes = int(len(records) * 0.15)
        records = records + random.sample(records, n_dupes)
        random.shuffle(records)

    elif sim_day == 7:
        # Quantity as string with unit: "0.123456 BTC" instead of 0.123456
        records = [{**r, 'quantity': f"{r['quantity']} {r['coin']}"} for r in records]

    elif sim_day == 10:
        # Missing 'side' column entirely (upstream schema change)
        records = [
            {'user_id': r['user_id'], 'coin': r['coin'],
             'quantity': r['quantity'], 'trade_time': r['trade_time']}
            for r in records
        ]
        fieldnames = ['user_id', 'coin', 'quantity', 'trade_time']

    elif sim_day == 12:
        # 90% volume drop (upstream outage)
        records = records[:max(1, int(len(records) * 0.10))]

    return records, fieldnames


@dag(
    dag_id='ingest_trades',
    schedule='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ingestion', 'phase-2'],
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)},
)
def ingest_trades_dag():

    @task
    def generate_csv(ds: str) -> str:
        trade_date = datetime.strptime(ds, '%Y-%m-%d')
        sim_day = _sim_day(ds)
        os.makedirs(DATA_DIR, exist_ok=True)

        records = _base_records(trade_date, seed=ds)
        records, fieldnames = _inject_errors(records, sim_day)

        filepath = os.path.join(DATA_DIR, f'trades_{ds}.csv')
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        print(f"[sim_day={sim_day}] Generated {len(records)} trades → {filepath}")
        print(f"  Columns: {fieldnames}")
        return filepath

    @task
    def load_to_postgres(filepath: str, ds: str) -> int:
        """
        Resilient load: handles renamed columns (uid/user_id), missing columns,
        and unparseable quantity strings. Bad values stored in raw_quantity for
        agent diagnosis.
        """
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        source_file = os.path.basename(filepath)
        cursor.execute("DELETE FROM raw.trades WHERE source_file = %s", (source_file,))

        rows_loaded = 0
        with open(filepath, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                user_id = row.get('user_id') or row.get('uid')
                raw_qty = row.get('quantity')
                try:
                    quantity = float(raw_qty) if raw_qty is not None else None
                except (ValueError, TypeError):
                    quantity = None  # type error — flagged by DQ check
                cursor.execute(
                    """
                    INSERT INTO raw.trades
                        (user_id, coin, side, quantity, raw_quantity, trade_time, source_file)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        user_id,
                        row.get('coin'),
                        row.get('side'),      # NULL when column is missing
                        quantity,
                        raw_qty,
                        row.get('trade_time'),
                        source_file,
                    ),
                )
                rows_loaded += 1

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Loaded {rows_loaded} rows → raw.trades (source: {source_file})")
        return rows_loaded

    @task
    def run_dq_checks(filepath: str, rows_loaded: int, ds: str) -> None:
        """Run 5 DQ checks and write results to monitoring.dq_check_results."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        source_file = os.path.basename(filepath)
        checks = []

        # 1. Row count: fail if nothing loaded
        checks.append((
            'raw.trades', 'daily_row_count',
            'pass' if rows_loaded > 0 else 'fail',
            {'date': ds, 'rows_loaded': rows_loaded},
        ))

        # 2. Schema drift: compare CSV header vs expected columns
        with open(filepath, newline='') as f:
            actual_cols = set(next(csv.reader(f)))
        missing_cols = sorted(EXPECTED_COLUMNS - actual_cols)
        extra_cols = sorted(actual_cols - EXPECTED_COLUMNS)
        checks.append((
            'raw.trades', 'schema_drift',
            'fail' if (missing_cols or extra_cols) else 'pass',
            {'missing_columns': missing_cols, 'unexpected_columns': extra_cols},
        ))

        conn = hook.get_conn()
        cursor = conn.cursor()

        # 3. Duplicate rows (same user_id, coin, side, quantity, trade_time)
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT user_id, coin, side, quantity, trade_time
                FROM raw.trades
                WHERE source_file = %s
                GROUP BY user_id, coin, side, quantity, trade_time
                HAVING COUNT(*) > 1
            ) dups
        """, (source_file,))
        dup_groups = cursor.fetchone()[0]
        dup_ratio = dup_groups / max(rows_loaded, 1)
        checks.append((
            'raw.trades', 'duplicate_rows',
            'fail' if dup_ratio > 0.05 else ('warning' if dup_groups > 0 else 'pass'),
            {'duplicate_groups': dup_groups, 'ratio': round(dup_ratio, 4)},
        ))

        # 4. Type errors: quantity is NULL but raw_quantity exists (bad string)
        cursor.execute("""
            SELECT COUNT(*) FROM raw.trades
            WHERE source_file = %s
              AND quantity IS NULL
              AND raw_quantity IS NOT NULL
        """, (source_file,))
        type_errors = cursor.fetchone()[0]
        checks.append((
            'raw.trades', 'quantity_type_error',
            'fail' if type_errors > 0 else 'pass',
            {'bad_rows': type_errors},
        ))

        # 5. Volume anomaly: today vs 7-day rolling average
        cursor.execute("""
            SELECT AVG(cnt) FROM (
                SELECT COUNT(*) AS cnt
                FROM raw.trades
                WHERE source_file != %s
                  AND ingested_at >= NOW() - INTERVAL '7 days'
                GROUP BY source_file
            ) daily
        """, (source_file,))
        avg_row = cursor.fetchone()[0]
        if avg_row and float(avg_row) > 0:
            vol_ratio = rows_loaded / float(avg_row)
            vol_status = 'fail' if vol_ratio < 0.2 else ('warning' if vol_ratio < 0.5 else 'pass')
            vol_detail = {
                'today': rows_loaded,
                '7day_avg': round(float(avg_row), 1),
                'ratio': round(vol_ratio, 4),
            }
        else:
            vol_status = 'pass'
            vol_detail = {'today': rows_loaded, '7day_avg': None, 'note': 'insufficient history'}
        checks.append(('raw.trades', 'volume_anomaly', vol_status, vol_detail))

        # Write all results
        cursor.executemany(
            """
            INSERT INTO monitoring.dq_check_results
                (table_name, check_name, status, details)
            VALUES (%s, %s, %s, %s)
            """,
            [(t, c, s, json.dumps(d)) for t, c, s, d in checks],
        )
        conn.commit()
        cursor.close()
        conn.close()

        failed = [c for _, c, s, _ in checks if s == 'fail']
        warnings = [c for _, c, s, _ in checks if s == 'warning']
        print(f"DQ checks: {len(checks)} total | {len(failed)} fail | {len(warnings)} warn")
        if failed:
            print(f"  FAILED:   {failed}")
        if warnings:
            print(f"  WARNINGS: {warnings}")

    filepath = generate_csv()
    rows = load_to_postgres(filepath)
    run_dq_checks(filepath, rows)


ingest_trades_dag()
