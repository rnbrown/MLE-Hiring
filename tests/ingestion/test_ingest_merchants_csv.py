"""Test that ingest_merchants_csv.main() is idempotent."""

import sqlite3
import textwrap
from pathlib import Path
from unittest.mock import patch

from src.ingestion.ingest_merchants_csv import DB_PATH, main


def _write_csv(tmp_path: Path) -> Path:
    csv_file = tmp_path / "merchants.csv"
    csv_file.write_text(textwrap.dedent("""\
        merchant_id,name,country,registration_number,monthly_volume,dispute_count,transaction_count
        M001,GreenLeaf Retail Ltd,United Kingdom,09446239,125000,2,3420
        M002,MedSpa Wellness Co,United States,,89000,5,2100
    """))
    return csv_file


def test_idempotent(tmp_path):
    csv_file = _write_csv(tmp_path)
    db_path = tmp_path / "merchants.db"

    with (
        patch("src.ingestion.ingest_merchants_csv.INPUT_PATH", csv_file),
        patch("src.ingestion.ingest_merchants_csv.DB_PATH", db_path),
    ):
        main()
        main()

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT COUNT(*) FROM merchants").fetchone()[0]
    rejected = conn.execute("SELECT COUNT(*) FROM rejected_rows").fetchone()[0]
    conn.close()

    assert rows == 2
    assert rejected == 0
