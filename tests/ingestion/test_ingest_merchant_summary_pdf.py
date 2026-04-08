"""Test that ingest_merchant_summary_pdf extracts and stores PDF data."""

import asyncio
import sqlite3
from pathlib import Path
from unittest.mock import patch

from src.ingestion.ingest_merchant_summary_pdf import main

SAMPLE_PDF_TEXT = """\
Merchant Underwriting Summary (Sample)
Section 1 - Merchant overview
Merchant: Sample Merchant Ltd.
Country: United Kingdom.
Registration: 09446239.
Monthly volume band: 50k-150k GBP.
Section 2 - Key terms (abbreviated)
Standard BNPL terms apply.
Chargeback liability: merchant responsible for fraud and service disputes.
Settlement: T+2.
Section 3 - Risk team notes
Internal flag: medium.
Last review 2025-01-10.
No sanctions hits.
Company active.
"""


def _seed_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE merchants (merchant_id TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()


def test_produces_data(tmp_path):
    db_path = tmp_path / "merchants.db"
    pdf_path = tmp_path / "sample_merchant_summary.pdf"
    _seed_db(db_path)

    # We don't need a real PDF — mock the text extraction
    async def fake_extract_text(path):
        return SAMPLE_PDF_TEXT

    with (
        patch("src.ingestion.ingest_merchant_summary_pdf.DB_PATH", db_path),
        patch("src.ingestion.ingest_merchant_summary_pdf.PDF_PATH", pdf_path),
        patch("src.ingestion.ingest_merchant_summary_pdf.extract_text", fake_extract_text),
    ):
        # Create a dummy file so the exists() check passes
        pdf_path.write_bytes(b"dummy")
        asyncio.run(main())

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT * FROM merchant_summaries").fetchall()
    conn.close()

    assert len(rows) == 1
    assert rows[0][0] == "09446239"  # registration_number
    assert rows[0][1] == "Sample Merchant Ltd"  # merchant_name
    assert rows[0][6] == "T+2"  # settlement_terms
    assert rows[0][7] == "medium"  # internal_risk_flag
