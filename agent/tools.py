import csv
import json
import os

from langchain_core.tools import tool

from agent.config import DATA_DIR
from agent.db import get_conn


@tool
def read_dq_results(date: str) -> str:
    """Read all DQ check results for a given date (YYYY-MM-DD) from monitoring.dq_check_results."""
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT check_name, status, details, timestamp
        FROM monitoring.dq_check_results
        WHERE details->>'date' = %s
        ORDER BY timestamp DESC
        """,
        (date,),
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    results = [
        {'check': r[0], 'status': r[1], 'details': r[2], 'at': str(r[3])}
        for r in rows
    ]
    return json.dumps(results, indent=2)


@tool
def get_schema_diff(table_name: str = 'raw.trades') -> str:
    """Compare actual columns in the DB table against the expected_schemas registry.
    Returns missing columns, unexpected columns, and type mismatches."""
    schema, tbl = table_name.split('.')
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, tbl),
    )
    actual = {r[0]: r[1] for r in cursor.fetchall()}

    cursor.execute(
        """
        SELECT column_name, data_type
        FROM monitoring.expected_schemas
        WHERE table_name = %s
        """,
        (table_name,),
    )
    expected = {r[0]: r[1] for r in cursor.fetchall()}
    cursor.close()
    conn.close()

    # Exclude internal columns not in expected_schemas
    internal = {'id', 'raw_quantity', 'source_file', 'ingested_at'}
    missing = {k: v for k, v in expected.items() if k not in actual}
    extra = {k: v for k, v in actual.items() if k not in expected and k not in internal}

    return json.dumps({
        'actual_columns': actual,
        'expected_columns': expected,
        'missing_from_actual': missing,
        'extra_in_actual': extra,
    }, indent=2)


@tool
def sample_bad_rows(source_file: str, issue_type: str) -> str:
    """Sample rows from raw.trades that illustrate a data quality issue.

    issue_type: 'null_user_id' | 'null_quantity' | 'null_side' | 'duplicate'
    """
    conn = get_conn()
    cursor = conn.cursor()

    if issue_type == 'null_user_id':
        cursor.execute(
            "SELECT id, user_id, coin, side, quantity, raw_quantity, trade_time "
            "FROM raw.trades WHERE source_file = %s AND user_id IS NULL LIMIT 5",
            (source_file,),
        )
    elif issue_type == 'null_quantity':
        cursor.execute(
            "SELECT id, user_id, coin, side, quantity, raw_quantity, trade_time "
            "FROM raw.trades WHERE source_file = %s "
            "AND quantity IS NULL AND raw_quantity IS NOT NULL LIMIT 5",
            (source_file,),
        )
    elif issue_type == 'null_side':
        cursor.execute(
            "SELECT id, user_id, coin, side, quantity, raw_quantity, trade_time "
            "FROM raw.trades WHERE source_file = %s AND side IS NULL LIMIT 5",
            (source_file,),
        )
    elif issue_type == 'duplicate':
        cursor.execute(
            "SELECT user_id, coin, side, quantity, trade_time, COUNT(*) AS cnt "
            "FROM raw.trades WHERE source_file = %s "
            "GROUP BY user_id, coin, side, quantity, trade_time HAVING COUNT(*) > 1 LIMIT 5",
            (source_file,),
        )
    else:
        cursor.execute(
            "SELECT id, user_id, coin, side, quantity, raw_quantity, trade_time "
            "FROM raw.trades WHERE source_file = %s LIMIT 5",
            (source_file,),
        )

    cols = [d[0] for d in cursor.description]
    rows = [dict(zip(cols, [str(v) for v in r])) for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    return json.dumps(rows, indent=2)


@tool
def check_source_file(source_file: str) -> str:
    """Check whether the source CSV file exists and report its column headers and row count."""
    filepath = os.path.join(DATA_DIR, source_file)
    if not os.path.exists(filepath):
        return json.dumps({'exists': False, 'path': filepath})

    stat = os.stat(filepath)
    with open(filepath, newline='') as f:
        reader = csv.reader(f)
        header = next(reader, [])
        row_count = sum(1 for _ in reader)

    return json.dumps({
        'exists': True,
        'path': filepath,
        'size_bytes': stat.st_size,
        'columns': header,
        'row_count': row_count,
    })
