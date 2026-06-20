import os
import logging
import uuid
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
import clickhouse_connect


PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


class OddsAPISport:

    def __init__(self) -> None:
        self.api_key = os.getenv("API_KEY") or os.getenv("ODDS_API_KEY")

        if not self.api_key:
            raise RuntimeError("API_KEY or ODDS_API_KEY is not set. Check your .env file.")


        self.url_sports = "https://api.the-odds-api.com/v4/sports"

        self.clickhouse_host = os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
        self.clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", "18123"))
        self.clickhouse_user = os.getenv("CLICKHOUSE_USER")
        self.clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD")
        self.clickhouse_database = os.getenv("CLICKHOUSE_DB", "casino_api")

        if not self.clickhouse_user:
            raise RuntimeError("CLICKHOUSE_USER is not set.")

        if not self.clickhouse_password:
            raise RuntimeError("CLICKHOUSE_PASSWORD is not set.")

        self.client = clickhouse_connect.get_client(
            host=self.clickhouse_host,
            port=self.clickhouse_port,
            username=self.clickhouse_user,
            password=self.clickhouse_password,
            database=self.clickhouse_database,
        )


    def create_database_and_table(self):
        self.client.command("""CREATE DATABASE IF NOT EXISTS raw""")

        self.client.command("""
            CREATE TABLE IF NOT EXISTS raw.odds_api_sports (
            key String,
            group_name String,
            title String,
            description String,
            active Boolean,
            has_outrights Boolean
            ) ENGINE = MergeTree()
            ORDER BY key
            """)
        logging.info("Database and table are ready: raw.odds_api_sports")

    def fetch_sports(self) -> list[dict[str, Any]]:
        response = requests.get(self.url_sports, params={"apiKey": self.api_key},timeout=30)
        
        logging.info("Status code: %s", response.status_code)
        logging.info("Requests remaining: %s", response.headers.get("x-requests-remaining"))
        logging.info("Requests used: %s", response.headers.get("x-requests-used"))

        if response.status_code != 200:
            logging.error("Failed to fetch sports: %s", response.text)
            response.raise_for_status()


        data = response.json()

        if not isinstance(data, list):
            raise RuntimeError(f"Expected list response, got {type(data).__name__}: {data}")
        

        if data:
            logging.info("Data keys: %s", list(data[0].keys()))

        logging.info("Number of records: %d", len(data))

        return data


    def build_rows(self, sports: list[dict[str, Any]]) -> list[tuple]:
        rows = []
        for item in sports:
            rows.append(
                [
                    item.get("key", ""),
                    item.get("group", ""),
                    item.get("title", ""),
                    item.get("description", ""),
                    bool(item.get("active", False)),
                    bool(item.get("has_outrights", False)),
                ]
            )
        return rows
    
    def insert_sports(self, rows: list[list[Any]]) -> None:
        if not rows:
            logging.warning("No rows to insert.")
            return

        deduped_rows = list({row[0]: row for row in rows if row[0]}.values())
        if len(deduped_rows) != len(rows):
            logging.info("Deduplicated sports catalog batch from %d to %d rows", len(rows), len(deduped_rows))

        if not deduped_rows:
            logging.warning("No rows with key to insert. Keeping existing raw.odds_api_sports data.")
            return

        target_table = "raw.odds_api_sports"
        load_table = f"raw.odds_api_sports__load_{uuid.uuid4().hex}"
        column_names = [
            "key",
            "group_name",
            "title",
            "description",
            "active",
            "has_outrights",
        ]

        self.client.command(f"CREATE TABLE {load_table} AS {target_table}")

        try:
            self.client.insert(load_table, deduped_rows, column_names=column_names)
            self.client.command(f"EXCHANGE TABLES {target_table} AND {load_table}")
        finally:
            self.client.command(f"DROP TABLE IF EXISTS {load_table}")

        logging.info("Replaced %s with %d latest rows", target_table, len(deduped_rows))

    def run(self):
        self.create_database_and_table()
        sports = self.fetch_sports()
        rows = self.build_rows(sports)
        self.insert_sports(rows)

if __name__ == "__main__":
    try:
        api_sport = OddsAPISport()
        api_sport.run()
    except Exception as e:
        logging.exception("An error occurred: %s", e)
        raise

        
