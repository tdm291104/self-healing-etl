from agent.db import get_conn


def fix_duplicate_rows(source_file: str) -> str:
    """Delete duplicate rows for a source_file, keeping the row with the lowest id."""
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        DELETE FROM raw.trades
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM raw.trades
            WHERE source_file = %s
            GROUP BY user_id, coin, side, quantity, trade_time
        )
        AND source_file = %s
        """,
        (source_file, source_file),
    )
    deleted = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    return f"Removed {deleted} duplicate rows from {source_file}"


def fix_schema_drift_uid(source_file: str) -> str:
    """Verify uid→user_id mapping was applied correctly during load."""
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM raw.trades WHERE source_file = %s AND user_id IS NOT NULL",
        (source_file,),
    )
    ok = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(*) FROM raw.trades WHERE source_file = %s AND user_id IS NULL",
        (source_file,),
    )
    null = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return f"uid→user_id mapping: {ok} rows OK, {null} rows with NULL user_id in {source_file}"
