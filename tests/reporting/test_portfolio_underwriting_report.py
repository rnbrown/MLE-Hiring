"""Test that portfolio_underwriting_report generates a report."""

import sqlite3
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.reporting.portfolio_underwriting_report import get_portfolio_stats, build_prompt, main


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
    month = date.today().replace(day=1).isoformat()
    conn.execute(
        "INSERT INTO merchants VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (month, "M001", "Test Ltd", "United Kingdom", "", 100000, 2, 3000),
    )
    conn.execute(
        "INSERT INTO merchants VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (month, "M002", "Other Co", "United States", "", 50000, 5, 1500),
    )
    conn.commit()
    conn.close()


def test_get_portfolio_stats(tmp_path):
    db_path = tmp_path / "merchants.db"
    _seed_db(db_path)

    with patch("src.reporting.portfolio_underwriting_report.DB_PATH", db_path):
        stats = get_portfolio_stats()

    assert stats["merchant_count"] == 2
    assert stats["total_volume"] == 150000
    assert stats["total_disputes"] == 7
    assert stats["total_transactions"] == 4500
    assert stats["overall_dispute_rate"] > 0
    assert len(stats["by_country"]) == 2
    assert len(stats["by_risk_level"]) > 0


def test_build_prompt_contains_stats(tmp_path):
    db_path = tmp_path / "merchants.db"
    _seed_db(db_path)

    with patch("src.reporting.portfolio_underwriting_report.DB_PATH", db_path):
        stats = get_portfolio_stats()

    prompt = build_prompt(stats)
    assert "Total merchants: 2" in prompt
    assert "$150,000" in prompt
    assert "United Kingdom" in prompt
    assert "United States" in prompt


def test_main_generates_report(tmp_path):
    db_path = tmp_path / "merchants.db"
    report_path = tmp_path / "underwriting_report.md"
    _seed_db(db_path)

    with (
        patch("src.reporting.portfolio_underwriting_report.DB_PATH", db_path),
        patch("src.reporting.portfolio_underwriting_report.REPORT_PATH", report_path),
        patch("src.reporting.portfolio_underwriting_report.GEMINI_API_KEY", "fake-key"),
        patch("src.reporting.portfolio_underwriting_report.call_gemini", return_value="# Test Report\n\nGenerated."),
    ):
        main()

    assert report_path.exists()
    content = report_path.read_text()
    assert "# Test Report" in content
