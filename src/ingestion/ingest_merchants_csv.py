"""Load merchants.csv, validate each row with Pydantic, and store in SQLite."""

import csv
import logging
import os
import sqlite3
import sys
from datetime import date
from pathlib import Path

from pydantic import BaseModel, Field, field_validator

log = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
INPUT_PATH = DATA_DIR / "merchants.csv"
DB_PATH = DATA_DIR / "merchants.db"


class MerchantRow(BaseModel):
    merchant_id: str
    name: str
    country: str
    registration_number: str | None = None
    monthly_volume: int = Field(gt=0)
    dispute_count: int = Field(ge=0)
    transaction_count: int = Field(gt=0)

    @field_validator("merchant_id")
    @classmethod
    def merchant_id_format(cls, v: str) -> str:
        if not v or not v.startswith("M") or not v[1:].isdigit():
            raise ValueError(f"must match M<digits>, got '{v}'")
        return v

    @field_validator("name", "country")
    @classmethod
    def not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be blank")
        return v.strip()

    @field_validator("registration_number", mode="before")
    @classmethod
    def empty_to_none(cls, v: str | None) -> str | None:
        if v is None or v.strip() == "":
            return None
        return v.strip()


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS merchants (
            month           TEXT NOT NULL,
            merchant_id     TEXT NOT NULL,
            name            TEXT NOT NULL,
            country         TEXT NOT NULL,
            registration_number TEXT,
            monthly_volume  INTEGER NOT NULL,
            dispute_count   INTEGER NOT NULL,
            transaction_count INTEGER NOT NULL,
            PRIMARY KEY (month, merchant_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rejected_rows (
            line_number INTEGER,
            raw_data    TEXT NOT NULL,
            errors      TEXT NOT NULL
        )
    """)
    conn.commit()


def main() -> None:
    if not INPUT_PATH.exists():
        log.error("Input file not found: %s", INPUT_PATH)
        sys.exit(1)

    month = date.today().replace(day=1).isoformat()
    log.info("Ingesting for month: %s", month)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    valid_count = 0
    rejected_count = 0

    with open(INPUT_PATH, newline="") as f:
        reader = csv.DictReader(f)

        for line_num, row in enumerate(reader, start=2):
            try:
                merchant = MerchantRow(**row)
            except Exception as e:
                rejected_count += 1
                log.warning("Row %d rejected (merchant_id=%s): %s", line_num, row.get("merchant_id", "N/A"), e)
                conn.execute(
                    "INSERT INTO rejected_rows (line_number, raw_data, errors) VALUES (?, ?, ?)",
                    (line_num, str(row), str(e)),
                )
                continue

            conn.execute(
                """INSERT OR REPLACE INTO merchants
                   (month, merchant_id, name, country, registration_number,
                    monthly_volume, dispute_count, transaction_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    month,
                    merchant.merchant_id,
                    merchant.name,
                    merchant.country,
                    merchant.registration_number,
                    merchant.monthly_volume,
                    merchant.dispute_count,
                    merchant.transaction_count,
                ),
            )
            valid_count += 1

    conn.commit()
    conn.close()

    log.info("Valid rows: %d", valid_count)
    log.info("Rejected rows: %d", rejected_count)
    log.info("Database written to: %s", DB_PATH)


if __name__ == "__main__":
    main()
