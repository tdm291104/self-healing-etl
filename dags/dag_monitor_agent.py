"""
DAG: monitor_agent
Schedule: daily at 07:00 UTC (1 hour after ingest_trades)
Reads today's DQ failures and triggers the LangGraph self-healing agent.
"""

import json
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = 'crypto_dw'


@dag(
    dag_id='monitor_agent',
    schedule='0 7 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['agent', 'monitoring', 'phase-3'],
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)},
)
def monitor_agent_dag():

    @task
    def fetch_failures(ds: str) -> list[dict]:
        """Read failed DQ checks for today from monitoring.dq_check_results."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT table_name, check_name, status, details
            FROM monitoring.dq_check_results
            WHERE timestamp::date = %s::date
              AND status IN ('fail', 'warning')
            ORDER BY
                CASE status WHEN 'fail' THEN 0 ELSE 1 END,
                timestamp DESC
            """,
            (ds,),
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        failures = [
            {'table_name': r[0], 'check_name': r[1], 'status': r[2], 'details': r[3]}
            for r in rows
        ]
        print(f"Found {len(failures)} DQ issues for {ds}")
        for f in failures:
            print(f"  [{f['status'].upper()}] {f['check_name']}: {json.dumps(f['details'])}")
        return failures

    @task
    def run_agent(failures: list[dict], ds: str) -> dict:
        """Invoke the LangGraph agent if there are any DQ failures."""
        if not failures:
            print(f"No DQ failures for {ds} — pipeline healthy, skipping agent.")
            return {'decision': 'no_action', 'failures': 0}

        # Import here so the module is loaded inside the worker process
        from agent.graph import run_agent as _run_agent

        source_file = f"trades_{ds}.csv"
        result = _run_agent(source_file, ds, failures)

        print(f"Agent decision : {result['decision']}")
        print(f"Confidence     : {result['confidence']:.0%}")
        print(f"Diagnosis      : {result['diagnosis']}")
        print(f"Action taken   : {result['action_taken']}")
        return result

    failures = fetch_failures()
    run_agent(failures)


monitor_agent_dag()
