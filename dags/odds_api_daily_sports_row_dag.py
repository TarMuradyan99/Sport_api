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
    dag_id='odds_api_daily_sports_dag',
    description='DAG to fetch daily sports data from Odds API and store in ClickHouse',
    default_args=default_args,
    schedule='0 0 * * *',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=["odds-api", "clickhouse", "raw", "ingestion"], 
) as dag:

    ingest_daily_sports = BashOperator(
        task_id='ingest_daily_sports',
        bash_command="python /opt/airflow/dags/project/ingestion/ingest_daily_sport_data.py"
    )

    trigger_daily_backup = TriggerDagRunOperator(
        task_id='trigger_clickhouse_daily_backups',
        trigger_dag_id='clickhouse_daily_backups',
        trigger_run_id='daily_backup__{{ dag_run.run_id }}',
        conf={
            "source_dag_id": "{{ dag.dag_id }}",
            "source_run_id": "{{ dag_run.run_id }}",
        },
        skip_when_already_exists=True,
    )

    ingest_daily_sports >> trigger_daily_backup
