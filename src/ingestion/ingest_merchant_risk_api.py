"""Fetch risk data from the internal Merchant Risk API for new merchants."""

import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from pydantic import BaseModel, Field, ValidationError, field_validator

from http_client import fetch_with_retries

log = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DB_PATH = DATA_DIR / "merchants.db"

API_BASE = os.getenv("MERCHANT_RISK_API_BASE_URL", "http://mock-api:8000")


class TransactionSummary(BaseModel):
    last_30d_volume: float = Field(ge=0)
    last_30d_txn_count: int = Field(ge=0)
    avg_ticket_size: float = Field(ge=0)


class MerchantRiskData(BaseModel):
    merchant_id: str
    internal_risk_flag: str
    transaction_summary: TransactionSummary
    last_review_date: str | None = None

    @field_validator("merchant_id")
    @classmethod
    def not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be blank")
        return v.strip()

    @field_validator("internal_risk_flag")
    @classmethod
    def valid_flag(cls, v: str) -> str:
        if v not in ("low", "medium", "high"):
            raise ValueError(f"must be low/medium/high, got '{v}'")
        return v


def init_merchant_risk_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS merchant_risk (
            merchant_id         TEXT PRIMARY KEY,
            internal_risk_flag  TEXT NOT NULL,
            last_30d_volume     REAL NOT NULL,
            last_30d_txn_count  INTEGER NOT NULL,
            avg_ticket_size     REAL NOT NULL,
            last_review_date    TEXT,
            retrieved_at        TEXT NOT NULL
        )
    """)
    conn.commit()


def get_missing_merchants(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("""
        SELECT DISTINCT m.merchant_id
        FROM merchants m
        LEFT JOIN merchant_risk r ON m.merchant_id = r.merchant_id
        WHERE r.merchant_id IS NULL
    """).fetchall()
    return [r[0] for r in rows]


def fetch_merchant_risk(merchant_id: str) -> MerchantRiskData | None:
    try:
        resp = fetch_with_retries(f"{API_BASE}/merchant-risk/{merchant_id}")
    except requests.RequestException as e:
        log.error("Failed to fetch risk for '%s' after %d attempts: %s", merchant_id, MAX_RETRIES, e)
        return None

    try:
        return MerchantRiskData(**resp.json())
    except ValidationError as e:
        log.warning("Validation failed for '%s': %s", merchant_id, e)
        return None


def main() -> None:
    if not DB_PATH.exists():
        log.error("Database not found: %s. Run ingest_merchants_csv.py first.", DB_PATH)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    init_merchant_risk_table(conn)

    missing = get_missing_merchants(conn)
    if not missing:
        log.info("All merchants already have risk data.")
        conn.close()
        return

    log.info("Merchants to fetch: %d", len(missing))
    retrieved_at = datetime.now(timezone.utc).isoformat()
    success_count = 0

    for merchant_id in missing:
        log.info("Fetching risk: %s", merchant_id)
        risk = fetch_merchant_risk(merchant_id)
        if risk is None:
            continue

        summary = risk.transaction_summary
        conn.execute(
            """INSERT OR REPLACE INTO merchant_risk
               (merchant_id, internal_risk_flag, last_30d_volume, last_30d_txn_count,
                avg_ticket_size, last_review_date, retrieved_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                risk.merchant_id,
                risk.internal_risk_flag,
                summary.last_30d_volume,
                summary.last_30d_txn_count,
                summary.avg_ticket_size,
                risk.last_review_date,
                retrieved_at,
            ),
        )
        success_count += 1

    conn.commit()
    conn.close()

    log.info("Fetched: %d / %d merchants", success_count, len(missing))


if __name__ == "__main__":
    main()
