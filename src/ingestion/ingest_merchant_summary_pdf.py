"""Extract structured data from merchant summary PDFs and load into SQLite."""

import asyncio
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pdfplumber
from pydantic import BaseModel, ValidationError, field_validator

log = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DB_PATH = DATA_DIR / "merchants.db"
PDF_PATH = DATA_DIR / "sample_merchant_summary.pdf"


class MerchantSummary(BaseModel):
    merchant_name: str
    country: str
    registration_number: str
    monthly_volume_band: str
    key_terms: str
    chargeback_liability: str
    settlement_terms: str
    internal_risk_flag: str
    last_review_date: str
    sanctions_hits: bool
    company_status: str

    @field_validator("merchant_name", "country", "registration_number", "monthly_volume_band")
    @classmethod
    def not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be blank")
        return v.strip()

    @field_validator("internal_risk_flag")
    @classmethod
    def valid_flag(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in ("low", "medium", "high"):
            raise ValueError(f"must be low/medium/high, got '{v}'")
        return v


def _extract(label: str, text: str) -> str:
    pattern = rf"{label}:\s*(.+)"
    match = re.search(pattern, text, re.IGNORECASE)
    return match.group(1).strip().rstrip(".") if match else ""


def parse_pdf_text(text: str) -> MerchantSummary:
    return MerchantSummary(
        merchant_name=_extract("Merchant", text),
        country=_extract("Country", text),
        registration_number=_extract("Registration", text),
        monthly_volume_band=_extract("Monthly volume band", text),
        key_terms=_extract("Standard BNPL terms apply", text) or "Standard BNPL terms apply",
        chargeback_liability=_extract("Chargeback liability", text),
        settlement_terms=_extract("Settlement", text),
        internal_risk_flag=_extract("Internal flag", text),
        last_review_date=_extract("Last review", text),
        sanctions_hits="no sanctions hits" in text.lower(),
        company_status="active" if "company active" in text.lower() else "unknown",
    )


def init_merchant_summaries_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS merchant_summaries (
            registration_number TEXT PRIMARY KEY,
            merchant_name       TEXT NOT NULL,
            country             TEXT NOT NULL,
            monthly_volume_band TEXT NOT NULL,
            key_terms           TEXT,
            chargeback_liability TEXT,
            settlement_terms    TEXT,
            internal_risk_flag  TEXT NOT NULL,
            last_review_date    TEXT,
            sanctions_hits      INTEGER NOT NULL,
            company_status      TEXT,
            retrieved_at        TEXT NOT NULL
        )
    """)
    conn.commit()


async def extract_text(pdf_path: Path) -> str:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _extract_text_sync, pdf_path)


def _extract_text_sync(pdf_path: Path) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)


async def main() -> None:
    if not PDF_PATH.exists():
        log.error("PDF not found: %s", PDF_PATH)
        return

    if not DB_PATH.exists():
        log.error("Database not found: %s. Run ingest_merchants_csv.py first.", DB_PATH)
        return

    log.info("Extracting text from: %s", PDF_PATH)
    text = await extract_text(PDF_PATH)

    if not text.strip():
        log.error("No text extracted from PDF.")
        return

    try:
        summary = parse_pdf_text(text)
    except ValidationError as e:
        log.error("Validation failed for PDF content: %s", e)
        return

    log.info("Parsed summary for: %s (%s)", summary.merchant_name, summary.registration_number)

    retrieved_at = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH)
    init_merchant_summaries_table(conn)

    conn.execute(
        """INSERT OR REPLACE INTO merchant_summaries
           (registration_number, merchant_name, country, monthly_volume_band,
            key_terms, chargeback_liability, settlement_terms,
            internal_risk_flag, last_review_date, sanctions_hits, company_status, retrieved_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            summary.registration_number,
            summary.merchant_name,
            summary.country,
            summary.monthly_volume_band,
            summary.key_terms,
            summary.chargeback_liability,
            summary.settlement_terms,
            summary.internal_risk_flag,
            summary.last_review_date,
            int(summary.sanctions_hits),
            summary.company_status,
            retrieved_at,
        ),
    )

    conn.commit()
    conn.close()

    log.info("Stored summary in merchant_summaries table.")


if __name__ == "__main__":
    asyncio.run(main())
