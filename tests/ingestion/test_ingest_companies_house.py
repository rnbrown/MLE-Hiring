"""Test that ingest_companies_house fetches and stores company data."""

import sqlite3
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.ingestion.ingest_companies_house import main


def _seed_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE merchants (
            month TEXT, merchant_id TEXT, name TEXT, country TEXT,
            registration_number TEXT, monthly_volume INTEGER,
            dispute_count INTEGER, transaction_count INTEGER,
            PRIMARY KEY (month, merchant_id)
        )
    """)
    conn.execute(
        "INSERT INTO merchants VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (date.today().replace(day=1).isoformat(), "M001", "Test Ltd", "United Kingdom", "09446239", 100000, 1, 3000),
    )
    conn.commit()
    conn.close()


FAKE_API_RESPONSE = {
    "company_name": "TEST LTD",
    "company_number": "09446239",
    "company_status": "active",
    "type": "ltd",
    "date_of_creation": "2015-01-01",
    "jurisdiction": "england-wales",
    "sic_codes": ["62012"],
    "registered_office_address": {
        "address_line_1": "1 Test Street",
        "locality": "London",
        "postal_code": "EC1A 1BB",
    },
}


def test_produces_data(tmp_path):
    db_path = tmp_path / "merchants.db"
    _seed_db(db_path)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = FAKE_API_RESPONSE

    with (
        patch("src.ingestion.ingest_companies_house.DB_PATH", db_path),
        patch("src.ingestion.ingest_companies_house.API_KEY", "test-key"),
        patch("src.ingestion.ingest_companies_house.fetch_with_retries", return_value=mock_resp),
    ):
        main()

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT * FROM companies_house").fetchall()
    conn.close()

    assert len(rows) == 1
    assert rows[0][0] == "09446239"  # company_number
    assert rows[0][1] == "TEST LTD"  # company_name
    assert rows[0][2] == "active"  # company_status
