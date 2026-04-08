"""Fetch country data from REST Countries API for countries in the merchants table."""

import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from pydantic import BaseModel, ValidationError, field_validator

from http_client import fetch_with_retries

log = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DB_PATH = DATA_DIR / "merchants.db"

API_BASE = os.getenv("REST_COUNTRIES_BASE_URL", "https://restcountries.com/v3.1")


class CountryData(BaseModel):
    common_name: str
    official_name: str
    cca2: str
    cca3: str
    region: str
    subregion: str | None = None
    population: int
    capital: str | None = None
    currencies: str | None = None
    languages: str | None = None

    @field_validator("common_name", "official_name", "cca2", "cca3", "region")
    @classmethod
    def not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be blank")
        return v.strip()


def init_countries_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS countries (
            country_name    TEXT PRIMARY KEY,
            common_name     TEXT NOT NULL,
            official_name   TEXT NOT NULL,
            cca2            TEXT NOT NULL,
            cca3            TEXT NOT NULL,
            region          TEXT NOT NULL,
            subregion       TEXT,
            population      INTEGER NOT NULL,
            capital         TEXT,
            currencies      TEXT,
            languages       TEXT,
            retrieved_at    TEXT NOT NULL
        )
    """)
    conn.commit()


def get_missing_countries(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("""
        SELECT DISTINCT m.country
        FROM merchants m
        LEFT JOIN countries c ON m.country = c.country_name
        WHERE c.country_name IS NULL
    """).fetchall()
    return [r[0] for r in rows]


def parse_country(data: dict) -> CountryData:
    currencies_raw = data.get("currencies", {})
    currencies_str = ", ".join(
        f"{code} ({info.get('name', '')})"
        for code, info in currencies_raw.items()
    ) if currencies_raw else None

    languages_raw = data.get("languages", {})
    languages_str = ", ".join(languages_raw.values()) if languages_raw else None

    capitals = data.get("capital", [])

    return CountryData(
        common_name=data.get("name", {}).get("common", ""),
        official_name=data.get("name", {}).get("official", ""),
        cca2=data.get("cca2", ""),
        cca3=data.get("cca3", ""),
        region=data.get("region", ""),
        subregion=data.get("subregion"),
        population=data.get("population", 0),
        capital=capitals[0] if capitals else None,
        currencies=currencies_str,
        languages=languages_str,
    )


def fetch_country(country_name: str) -> CountryData | None:
    try:
        resp = fetch_with_retries(f"{API_BASE}/name/{country_name}")
    except requests.RequestException as e:
        log.error("Failed to fetch '%s' after %d attempts: %s", country_name, MAX_RETRIES, e)
        return None

    results = resp.json()
    if not isinstance(results, list) or len(results) == 0:
        log.error("No results returned for '%s'", country_name)
        return None

    try:
        return parse_country(results[0])
    except ValidationError as e:
        log.warning("Validation failed for '%s': %s", country_name, e)
        return None


def main() -> None:
    if not DB_PATH.exists():
        log.error("Database not found: %s. Run ingest_merchants_csv.py first.", DB_PATH)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    init_countries_table(conn)

    missing = get_missing_countries(conn)
    if not missing:
        log.info("All countries already ingested.")
        conn.close()
        return

    log.info("Countries to fetch: %d", len(missing))
    retrieved_at = datetime.now(timezone.utc).isoformat()
    success_count = 0

    for country_name in missing:
        log.info("Fetching: %s", country_name)
        country = fetch_country(country_name)
        if country is None:
            continue

        conn.execute(
            """INSERT OR REPLACE INTO countries
               (country_name, common_name, official_name, cca2, cca3,
                region, subregion, population, capital, currencies, languages, retrieved_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                country_name,
                country.common_name,
                country.official_name,
                country.cca2,
                country.cca3,
                country.region,
                country.subregion,
                country.population,
                country.capital,
                country.currencies,
                country.languages,
                retrieved_at,
            ),
        )
        success_count += 1

    conn.commit()
    conn.close()

    log.info("Fetched: %d / %d countries", success_count, len(missing))


if __name__ == "__main__":
    main()
