"""
Generate a synthetic daily trades CSV for the crypto ETL pipeline.

Phase 1: clean data only (no error injection).
Phase 2 will add a --day parameter to trigger controlled error scenarios.

Usage:
    python generate_trades_csv.py --date 2024-01-15 --out data/trades_2024-01-15.csv
    python generate_trades_csv.py  # defaults to today, auto output path
"""

import argparse
import csv
import os
import random
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()
random.seed(42)

COINS = ['BTC', 'ETH', 'USDT', 'SOL']
NUM_USERS = 50
NUM_TRADES_PER_DAY = 500

QUANTITY_RANGES = {
    'BTC':  (0.001, 2.0),
    'ETH':  (0.01, 20.0),
    'USDT': (10.0, 10_000.0),
    'SOL':  (0.1, 500.0),
}


def generate_trades(date: datetime) -> list[dict]:
    records = []
    for _ in range(NUM_TRADES_PER_DAY):
        coin = random.choice(COINS)
        lo, hi = QUANTITY_RANGES[coin]
        qty = round(random.uniform(lo, hi), 6)
        trade_time = date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        records.append({
            'user_id':    f'user_{random.randint(1, NUM_USERS):04d}',
            'coin':       coin,
            'side':       random.choice(['buy', 'sell']),
            'quantity':   qty,
            'trade_time': trade_time.strftime('%Y-%m-%d %H:%M:%S'),
        })
    return records


def save_csv(records: list[dict], filepath: str) -> None:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    fieldnames = ['user_id', 'coin', 'side', 'quantity', 'trade_time']
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print(f"Wrote {len(records)} records → {filepath}")


def main():
    parser = argparse.ArgumentParser(description='Generate daily trades CSV')
    parser.add_argument('--date', default=datetime.today().strftime('%Y-%m-%d'),
                        help='Trade date (YYYY-MM-DD), default: today')
    parser.add_argument('--out', default=None,
                        help='Output file path (default: data/trades_<date>.csv)')
    args = parser.parse_args()

    trade_date = datetime.strptime(args.date, '%Y-%m-%d')
    out_path = args.out or os.path.join(
        os.path.dirname(__file__), '..', 'data', f'trades_{args.date}.csv'
    )
    out_path = os.path.normpath(out_path)

    records = generate_trades(trade_date)
    save_csv(records, out_path)


if __name__ == '__main__':
    main()
