from __future__ import annotations

from datetime import datetime,timedelta

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

try:
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": 'taron',
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id='odds_api_live_sport_dag',
    description='DAG to fetch live sports data from Odds API and store in ClickHouse',
    default_args=default_args,
    schedule='*/15 * * * *',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=["odds-api", "clickhouse", "raw", "ingestion"], 
) as dag:

    ingest_live_sports = BashOperator(
        task_id='ingest_live_sports',
        bash_command="python /opt/airflow/dags/project/ingestion/ingest_live_sport_row_level.py"
    )

    trigger_incremental_backup = TriggerDagRunOperator(
        task_id='trigger_clickhouse_15min_incremental_backups',
        trigger_dag_id='clickhouse_15min_incremental_backups',
        trigger_run_id='live_incremental_backup__{{ dag_run.run_id }}',
        conf={
            "source_dag_id": "{{ dag.dag_id }}",
            "source_run_id": "{{ dag_run.run_id }}",
        },
        skip_when_already_exists=True,
    )

    ingest_live_sports >> trigger_incremental_backup
