import os
import json
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
        self.api_key = os.getenv("ODDS_API_KEY") or os.getenv("API_KEY")

        if not self.api_key:
            raise RuntimeError("API_KEY or ODDS_API_KEY is not set. Check your .env file.")

        self.url_odds = "https://api.the-odds-api.com/v4/sports/upcoming/odds"

        self.regions = {
            1: "eu",
            2: "us",
            3: "au",
        }

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

    def create_database_and_table(self) -> None:
        self.client.command("CREATE DATABASE IF NOT EXISTS raw")

        self.client.command(
            """
            CREATE TABLE IF NOT EXISTS raw.daily_sports
            (
                region_id UInt8,
                region LowCardinality(String),
                event_id String,
                sport_key String,
                sport_title String,
                commence_time String,
                home_team String,
                away_team String,
                bookmaker_keys Array(String),
                payload String,
                ingested_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree
            ORDER BY (region_id, sport_key, commence_time, event_id)
            """
        )

        logging.info("Database and table are ready: raw.daily_sports")

    def fetch_sports(self) -> list[dict[str, Any]]:
        all_records = []

        for region_id, region_name in self.regions.items():
            response = requests.get(
                self.url_odds,
                params={
                    "apiKey": self.api_key,
                    "regions": region_name,
                    "markets": "h2h",
                    "oddsFormat": "decimal",
                },
                timeout=30,
            )

            logging.info("Region ID: %s", region_id)
            logging.info("Region: %s", region_name)
            logging.info("Status: %s", response.status_code)
            logging.info("Requests remaining: %s", response.headers.get("x-requests-remaining"))
            logging.info("Requests used: %s", response.headers.get("x-requests-used"))
            logging.info("Last request cost: %s", response.headers.get("x-requests-last"))

            if response.status_code != 200:
                logging.error("Error response: %s", response.text)
                response.raise_for_status()

            data = response.json()

            if not isinstance(data, list):
                raise RuntimeError(
                    f"Expected list response for region {region_name}, got {type(data).__name__}: {data}"
                )

            logging.info("Number of events for region %s: %d", region_name, len(data))

            if data:
                logging.info("First event keys: %s", list(data[0].keys()))

            all_records.append(
                {
                    "region_id": region_id,
                    "region": region_name,
                    "data": data,
                }
            )

        return all_records

    def build_rows(self, sports_by_region: list[dict[str, Any]]) -> list[list[Any]]:
        rows = []

        for region_block in sports_by_region:
            region_id = region_block["region_id"]
            region = region_block["region"]
            events = region_block["data"]

            for event in events:
                bookmakers = event.get("bookmakers", [])

                bookmaker_keys = [
                    bookmaker.get("key", "")
                    for bookmaker in bookmakers
                    if isinstance(bookmaker, dict)
                ]

                rows.append(
                    [
                        region_id,
                        region,
                        event.get("id", ""),
                        event.get("sport_key", ""),
                        event.get("sport_title", ""),
                        event.get("commence_time", ""),
                        event.get("home_team", ""),
                        event.get("away_team", ""),
                        bookmaker_keys,
                        json.dumps(event, ensure_ascii=False),
                    ]
                )

        return rows

    def insert_sports(self, rows: list[list[Any]]) -> None:
        if not rows:
            logging.warning("No rows to insert.")
            return

        deduped_rows = list({(row[0], row[2]): row for row in rows if row[2]}.values())
        if len(deduped_rows) != len(rows):
            logging.info("Deduplicated daily sports batch from %d to %d rows", len(rows), len(deduped_rows))

        if not deduped_rows:
            logging.warning("No rows with event_id to insert. Keeping existing raw.daily_sports data.")
            return

        target_table = "raw.daily_sports"
        load_table = f"raw.daily_sports__load_{uuid.uuid4().hex}"
        column_names = [
            "region_id",
            "region",
            "event_id",
            "sport_key",
            "sport_title",
            "commence_time",
            "home_team",
            "away_team",
            "bookmaker_keys",
            "payload",
        ]

        self.client.command(f"CREATE TABLE {load_table} AS {target_table}")

        try:
            self.client.insert(load_table, deduped_rows, column_names=column_names)
            self.client.command(f"EXCHANGE TABLES {target_table} AND {load_table}")
        finally:
            self.client.command(f"DROP TABLE IF EXISTS {load_table}")

        logging.info("Replaced %s with %d latest rows", target_table, len(deduped_rows))

    def run(self) -> None:
        self.create_database_and_table()
        sports = self.fetch_sports()
        rows = self.build_rows(sports)
        self.insert_sports(rows)


if __name__ == "__main__":
    try:
        OddsAPISport().run()
    except Exception as e:
        logging.exception("An error occurred: %s", e)
        raise
