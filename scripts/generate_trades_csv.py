"""
Generate a synthetic daily trades CSV for the crypto ETL pipeline.

Phase 2: pass --sim-day to trigger a specific error scenario.

Usage:
    python generate_trades_csv.py --date 2024-01-03 --sim-day 3
    python generate_trades_csv.py --date 2024-01-05 --sim-day 5
    python generate_trades_csv.py --date 2024-01-01          # clean data

Error scenarios (by sim-day):
    3  — column rename: user_id → uid
    5  — 15% duplicate rows
    7  — quantity as string ("0.05 BTC" instead of 0.05)
    10 — missing 'side' column
    12 — 90% volume drop (~50 rows)
"""

import argparse
import csv
import os
import random
from datetime import datetime, timedelta

COINS = ['BTC', 'ETH', 'USDT', 'SOL']
NUM_USERS = 50
NUM_TRADES = 500
QUANTITY_RANGES = {
    'BTC':  (0.001, 2.0),
    'ETH':  (0.01, 20.0),
    'USDT': (10.0, 10_000.0),
    'SOL':  (0.1, 500.0),
}
SIMULATION_START = datetime(2024, 1, 1)


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
    fieldnames = ['user_id', 'coin', 'side', 'quantity', 'trade_time']

    if sim_day == 3:
        records = [
            {'uid': r['user_id'], 'coin': r['coin'], 'side': r['side'],
             'quantity': r['quantity'], 'trade_time': r['trade_time']}
            for r in records
        ]
        fieldnames = ['uid', 'coin', 'side', 'quantity', 'trade_time']

    elif sim_day == 5:
        n_dupes = int(len(records) * 0.15)
        records = records + random.sample(records, n_dupes)
        random.shuffle(records)

    elif sim_day == 7:
        records = [{**r, 'quantity': f"{r['quantity']} {r['coin']}"} for r in records]

    elif sim_day == 10:
        records = [
            {'user_id': r['user_id'], 'coin': r['coin'],
             'quantity': r['quantity'], 'trade_time': r['trade_time']}
            for r in records
        ]
        fieldnames = ['user_id', 'coin', 'quantity', 'trade_time']

    elif sim_day == 12:
        records = records[:max(1, int(len(records) * 0.10))]

    return records, fieldnames


def main():
    parser = argparse.ArgumentParser(description='Generate daily trades CSV')
    parser.add_argument('--date', default=datetime.today().strftime('%Y-%m-%d'),
                        help='Trade date YYYY-MM-DD (default: today)')
    parser.add_argument('--out', default=None,
                        help='Output path (default: data/trades_<date>.csv)')
    parser.add_argument('--sim-day', type=int, default=None,
                        help='Override simulation day for error injection (1–12+)')
    args = parser.parse_args()

    trade_date = datetime.strptime(args.date, '%Y-%m-%d')
    sim_day = args.sim_day if args.sim_day is not None else \
              (trade_date - SIMULATION_START).days + 1

    out_path = args.out or os.path.join(
        os.path.dirname(__file__), '..', 'data', f'trades_{args.date}.csv'
    )
    out_path = os.path.normpath(out_path)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    records = _base_records(trade_date, seed=args.date)
    records, fieldnames = _inject_errors(records, sim_day)

    with open(out_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"[sim_day={sim_day}] {len(records)} rows → {out_path}")
    print(f"  Columns: {fieldnames}")


if __name__ == '__main__':
    main()
