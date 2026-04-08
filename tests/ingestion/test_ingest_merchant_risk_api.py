"""Test that ingest_merchant_risk_api fetches and stores risk data."""

import sqlite3
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.ingestion.ingest_merchant_risk_api import main


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
        (date.today().replace(day=1).isoformat(), "M001", "Test Ltd", "United Kingdom", "09446239", 125000, 2, 3420),
    )
    conn.commit()
    conn.close()


FAKE_API_RESPONSE = {
    "merchant_id": "M001",
    "internal_risk_flag": "low",
    "transaction_summary": {
        "last_30d_volume": 125000.0,
        "last_30d_txn_count": 3420,
        "avg_ticket_size": 36.55,
    },
    "last_review_date": "2026-04-01",
}


def test_produces_data(tmp_path):
    db_path = tmp_path / "merchants.db"
    _seed_db(db_path)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = FAKE_API_RESPONSE

    with (
        patch("src.ingestion.ingest_merchant_risk_api.DB_PATH", db_path),
        patch("src.ingestion.ingest_merchant_risk_api.fetch_with_retries", return_value=mock_resp),
    ):
        main()

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT * FROM merchant_risk").fetchall()
    conn.close()

    assert len(rows) == 1
    assert rows[0][0] == "M001"  # merchant_id
    assert rows[0][1] == "low"  # internal_risk_flag
    assert rows[0][2] == 125000.0  # last_30d_volume
