"""Fetch company data from Companies House API for UK merchants with a registration number."""

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

API_BASE = os.getenv("COMPANIES_HOUSE_BASE_URL", "https://api.company-information.service.gov.uk")
API_KEY = os.getenv("COMPANIES_HOUSE_API_KEY", "")

RATE_LIMIT_WAIT = 300  # 5 minutes


class CompanyData(BaseModel):
    company_name: str
    company_number: str
    company_status: str
    company_type: str | None = None
    date_of_creation: str | None = None
    jurisdiction: str | None = None
    sic_codes: str | None = None
    address_line_1: str | None = None
    address_locality: str | None = None
    address_postal_code: str | None = None

    @field_validator("company_name", "company_number", "company_status")
    @classmethod
    def not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be blank")
        return v.strip()


def init_companies_house_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS companies_house (
            company_number      TEXT PRIMARY KEY,
            company_name        TEXT NOT NULL,
            company_status      TEXT NOT NULL,
            company_type        TEXT,
            date_of_creation    TEXT,
            jurisdiction        TEXT,
            sic_codes           TEXT,
            address_line_1      TEXT,
            address_locality    TEXT,
            address_postal_code TEXT,
            retrieved_at        TEXT NOT NULL
        )
    """)
    conn.commit()


def get_missing_companies_house(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Return (merchant_id, registration_number) pairs not yet in the companies_house table."""
    rows = conn.execute("""
        SELECT DISTINCT m.merchant_id, m.registration_number
        FROM merchants m
        LEFT JOIN companies_house c ON m.registration_number = c.company_number
        WHERE m.country = 'United Kingdom'
          AND m.registration_number IS NOT NULL
          AND m.registration_number != ''
          AND c.company_number IS NULL
    """).fetchall()
    return [(r[0], r[1]) for r in rows]


def parse_company(data: dict) -> CompanyData:
    address = data.get("registered_office_address", {})
    sic_codes = data.get("sic_codes", [])

    return CompanyData(
        company_name=data.get("company_name", ""),
        company_number=data.get("company_number", ""),
        company_status=data.get("company_status", ""),
        company_type=data.get("type"),
        date_of_creation=data.get("date_of_creation"),
        jurisdiction=data.get("jurisdiction"),
        sic_codes=", ".join(sic_codes) if sic_codes else None,
        address_line_1=address.get("address_line_1"),
        address_locality=address.get("locality"),
        address_postal_code=address.get("postal_code"),
    )


def fetch_company(registration_number: str) -> CompanyData | None:
    url = f"{API_BASE}/company/{registration_number}"
    auth = (API_KEY, "") if API_KEY else None

    try:
        resp = fetch_with_retries(url, auth=auth, rate_limit_wait=RATE_LIMIT_WAIT)
    except requests.RequestException as e:
        log.error("Failed to fetch company '%s' after %d attempts: %s", registration_number, MAX_RETRIES, e)
        return None

    try:
        return parse_company(resp.json())
    except ValidationError as e:
        log.warning("Validation failed for company '%s': %s", registration_number, e)
        return None


def main() -> None:
    if not DB_PATH.exists():
        log.error("Database not found: %s. Run ingest_merchants_csv.py first.", DB_PATH)
        sys.exit(1)

    if not API_KEY:
        log.error("COMPANIES_HOUSE_API_KEY not set.")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    init_companies_house_table(conn)

    missing = get_missing_companies_house(conn)
    if not missing:
        log.info("All companies already ingested.")
        conn.close()
        return

    log.info("Companies to fetch: %d", len(missing))
    retrieved_at = datetime.now(timezone.utc).isoformat()
    success_count = 0

    for merchant_id, reg_number in missing:
        log.info("Fetching company %s (merchant %s)", reg_number, merchant_id)
        company = fetch_company(reg_number)
        if company is None:
            continue

        conn.execute(
            """INSERT OR REPLACE INTO companies_house
               (company_number, company_name, company_status, company_type,
                date_of_creation, jurisdiction, sic_codes,
                address_line_1, address_locality, address_postal_code, retrieved_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                company.company_number,
                company.company_name,
                company.company_status,
                company.company_type,
                company.date_of_creation,
                company.jurisdiction,
                company.sic_codes,
                company.address_line_1,
                company.address_locality,
                company.address_postal_code,
                retrieved_at,
            ),
        )
        success_count += 1

    conn.commit()
    conn.close()

    log.info("Fetched: %d / %d companies", success_count, len(missing))


if __name__ == "__main__":
    main()
