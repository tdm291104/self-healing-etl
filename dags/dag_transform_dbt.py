"""
DAG: transform_dbt
Schedule: daily at 08:00 UTC (after ingest_trades at 06:00)
Runs dbt staging → mart models → dbt tests in sequence.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

DBT_DIR = '/opt/airflow/dbt'
DBT_CMD = f'cd {DBT_DIR} && dbt'

DBT_ENV = {
    'DBT_PROFILES_DIR': DBT_DIR,
    # DB credentials are already in the container environment via docker-compose;
    # dbt/profiles.yml reads them with env_var()
}


@dag(
    dag_id='transform_dbt',
    schedule='0 8 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['transform', 'dbt', 'phase-1'],
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)},
)
def transform_dbt_dag():

    dbt_staging = BashOperator(
        task_id='dbt_staging',
        bash_command=f'{DBT_CMD} run --select staging --profiles-dir {DBT_DIR}',
        env=DBT_ENV,
        append_env=True,
    )

    dbt_marts = BashOperator(
        task_id='dbt_marts',
        bash_command=f'{DBT_CMD} run --select marts --profiles-dir {DBT_DIR}',
        env=DBT_ENV,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'{DBT_CMD} test --profiles-dir {DBT_DIR}',
        env=DBT_ENV,
        append_env=True,
    )

    dbt_staging >> dbt_marts >> dbt_test


transform_dbt_dag()
