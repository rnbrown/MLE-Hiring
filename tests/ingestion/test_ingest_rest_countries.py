"""Test that ingest_rest_countries fetches and stores country data."""

import json
import sqlite3
import textwrap
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.ingestion.ingest_rest_countries import main


def _seed_db(db_path: Path) -> None:
    """Create a merchants table with one row so there's a country to fetch."""
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
        (date.today().replace(day=1).isoformat(), "M001", "Test Ltd", "United Kingdom", "", 100000, 1, 3000),
    )
    conn.commit()
    conn.close()


FAKE_API_RESPONSE = [
    {
        "name": {"common": "United Kingdom", "official": "United Kingdom of Great Britain and Northern Ireland"},
        "cca2": "GB",
        "cca3": "GBR",
        "region": "Europe",
        "subregion": "Northern Europe",
        "population": 67886011,
        "capital": ["London"],
        "currencies": {"GBP": {"name": "British pound"}},
        "languages": {"eng": "English"},
    }
]


def test_produces_data(tmp_path):
    db_path = tmp_path / "merchants.db"
    _seed_db(db_path)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = FAKE_API_RESPONSE

    with (
        patch("src.ingestion.ingest_rest_countries.DB_PATH", db_path),
        patch("src.ingestion.ingest_rest_countries.fetch_with_retries", return_value=mock_resp),
    ):
        main()

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT * FROM countries").fetchall()
    conn.close()

    assert len(rows) == 1
    assert rows[0][1] == "United Kingdom"  # common_name
    assert rows[0][3] == "GB"  # cca2
