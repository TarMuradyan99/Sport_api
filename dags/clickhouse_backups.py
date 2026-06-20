from __future__ import annotations

from datetime import timedelta
import pendulum
import requests

from airflow.decorators import dag, task

import os
from pathlib import Path
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")


CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "18123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")


DB_NAME = 'raw'

DAILY_TABLE = "daily_sports"
FIFTEEN_MIN_TABLE = "odds_api_sports"


def build_clickhouse_url(host: str, port: str) -> str:
    host = host.strip().rstrip("/")

    if host.startswith(("http://", "https://")):
        return host

    if ":" in host and not host.endswith("]"):
        return f"http://{host}"

    return f"http://{host}:{port}"


CLICKHOUSE_URL = build_clickhouse_url(CLICKHOUSE_HOST, CLICKHOUSE_PORT)
BACKUP_ALREADY_EXISTS = "BACKUP_ALREADY_EXISTS"


def run_clickhouse_query(query: str) -> str:
    response = requests.post(
        CLICKHOUSE_URL,
        params={
            "user": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
        },
        data=query.encode("utf-8"),
        timeout=300,
    )

    if response.status_code != 200:
        if BACKUP_ALREADY_EXISTS in response.text:
            return response.text

        raise Exception(
            f"ClickHouse query failed.\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text}\n"
            f"Query: {query}"
        )

    return response.text


@dag(
    dag_id="clickhouse_daily_backups",
    schedule=None,
    start_date=pendulum.datetime(2026, 6, 14, tz="Asia/Yerevan"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["clickhouse", "backup"],
)
def clickhouse_daily_backups():

    @task
    def backup_daily_sports():
        date_str = pendulum.now("Asia/Yerevan").format("YYYY-MM-DD")

        query = f"""
        BACKUP TABLE {DB_NAME}.{DAILY_TABLE}
        TO Disk('backups', 'daily_row/{DAILY_TABLE}_{date_str}.zip')
        """

        return run_clickhouse_query(query)

    @task
    def backup_odds_api_sports_base():
        date_str = pendulum.now("Asia/Yerevan").format("YYYY-MM-DD")

        query = f"""
        BACKUP TABLE {DB_NAME}.{FIFTEEN_MIN_TABLE}
        TO Disk('backups', 'for_15min_row/{FIFTEEN_MIN_TABLE}_{date_str}.zip')
        """

        return run_clickhouse_query(query)

    backup_daily_sports()
    backup_odds_api_sports_base()


clickhouse_daily_backups()


@dag(
    dag_id="clickhouse_15min_incremental_backups",
    schedule=None,
    start_date=pendulum.datetime(2026, 6, 14, tz="Asia/Yerevan"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["clickhouse", "backup"],
)
def clickhouse_15min_incremental_backups():

    @task
    def backup_odds_api_sports_incremental():
        now = pendulum.now("Asia/Yerevan")
        date_str = now.format("YYYY-MM-DD")
        time_str = now.format("HHmm")

        query = f"""
        BACKUP TABLE {DB_NAME}.{FIFTEEN_MIN_TABLE}
        TO Disk('backups', 'incremental/{FIFTEEN_MIN_TABLE}_{date_str}_{time_str}.zip')
        SETTINGS base_backup = Disk('backups', 'for_15min_row/{FIFTEEN_MIN_TABLE}_{date_str}.zip')
        """

        return run_clickhouse_query(query)

    backup_odds_api_sports_incremental()


clickhouse_15min_incremental_backups()
