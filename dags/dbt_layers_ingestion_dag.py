from __future__ import annotations

import os
import pendulum

try :
       from airflow.sdk import DAG
except ImportError:
    from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:
    from airflow.operators.bash import BashOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator



DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dags/project/dbt/sport_data_ingestion")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
DBT_TARGET = os.getenv("DBT_TARGET", "prod")
DBT_LOG_PATH = os.getenv("DBT_LOG_PATH", "/tmp/dbt/logs")
DBT_TARGET_PATH = os.getenv("DBT_TARGET_PATH", "/tmp/dbt/target")
DBT_PACKAGES_INSTALL_PATH = os.getenv("DBT_PACKAGES_INSTALL_PATH", "/tmp/dbt/dbt_packages")


LIVE_RAW_DAG_ID = "odds_api_live_sport_dag"
DAILY_RAW_DAG_ID = "odds_api_daily_sports_dag"


def bash_commands(command: str) -> str:
    return f"""
        set -e

        echo "DBT_PROJECT_DIR={DBT_PROJECT_DIR}"
        echo "DBT_PROFILES_DIR={DBT_PROFILES_DIR}"
        echo "DBT_TARGET={DBT_TARGET}"
        echo "DBT_LOG_PATH={DBT_LOG_PATH}"
        echo "DBT_TARGET_PATH={DBT_TARGET_PATH}"
        echo "DBT_PACKAGES_INSTALL_PATH={DBT_PACKAGES_INSTALL_PATH}"

        export DBT_LOG_PATH="{DBT_LOG_PATH}"
        export DBT_TARGET_PATH="{DBT_TARGET_PATH}"
        export DBT_PACKAGES_INSTALL_PATH="{DBT_PACKAGES_INSTALL_PATH}"
        mkdir -p "$DBT_LOG_PATH" "$DBT_TARGET_PATH" "$DBT_PACKAGES_INSTALL_PATH"

        cd "{DBT_PROJECT_DIR}"

        dbt {command} \
        --project-dir "{DBT_PROJECT_DIR}" \
        --profiles-dir "{DBT_PROFILES_DIR}" \
        --target "{DBT_TARGET}" \
        --log-path "{DBT_LOG_PATH}" \
        --no-use-colors
    """

with DAG(
    dag_id='dbt_layers_ingestion_dag',
    description='DAG to run dbt models for raw data layers after ingestion',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "clickhouse", "raw", "ingestion"], 
) as dag:

    wait_daily_raw_ingestion = TriggerDagRunOperator(
        task_id="run_daily_sports_raw_dag",
        trigger_dag_id=DAILY_RAW_DAG_ID,
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    wait_live_raw_ingestion = TriggerDagRunOperator(
        task_id="run_live_sports_raw_dag",
        trigger_dag_id=LIVE_RAW_DAG_ID,
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=bash_commands("debug"),
        retries=1
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=bash_commands("deps"),
        retries=1
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=bash_commands("seed"),
        retries=1
    )


    dbt_snapshot = BashOperator(
        task_id = "dbt_snapshot",
        bash_command = bash_commands("snapshot"),
        retries=1
    )


    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=bash_commands("test"),
        retries=1
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=bash_commands("run"),
        retries=1
    )


    [wait_daily_raw_ingestion, wait_live_raw_ingestion] >> dbt_debug >> dbt_deps >> dbt_seed >> dbt_run >> dbt_snapshot >> dbt_test
