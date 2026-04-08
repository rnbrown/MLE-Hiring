"""Test that train_model trains a model and saves it."""

import sqlite3
from datetime import date
from pathlib import Path
from unittest.mock import patch

import joblib

from src.model.train_model import main


def _seed_db(db_path: Path) -> None:
    """Create merchants and countries tables with enough rows to train/test split."""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE merchants (
            month TEXT, merchant_id TEXT, name TEXT, country TEXT,
            registration_number TEXT, monthly_volume INTEGER,
            dispute_count INTEGER, transaction_count INTEGER,
            PRIMARY KEY (month, merchant_id)
        )
    """)
    conn.execute("""
        CREATE TABLE countries (
            country_name TEXT PRIMARY KEY, common_name TEXT, official_name TEXT,
            cca2 TEXT, cca3 TEXT, region TEXT, subregion TEXT,
            population INTEGER, capital TEXT, currencies TEXT, languages TEXT,
            retrieved_at TEXT
        )
    """)
    month = date.today().replace(day=1).isoformat()
    for i in range(1, 21):
        conn.execute(
            "INSERT INTO merchants VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (month, f"M{i:03d}", f"Merchant {i}", "United Kingdom", "",
             50000 + i * 1000, i % 6, 1000 + i * 100),
        )
    conn.execute(
        "INSERT INTO countries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("United Kingdom", "United Kingdom", "United Kingdom of Great Britain",
         "GB", "GBR", "Europe", "Northern Europe", 67000000, "London",
         "GBP (British pound)", "English", "2026-04-01"),
    )
    conn.commit()
    conn.close()


def test_trains_and_saves_model(tmp_path):
    db_path = tmp_path / "merchants.db"
    model_path = tmp_path / "model.joblib"
    _seed_db(db_path)

    with (
        patch("src.model.build_features.DB_PATH", db_path),
        patch("model.build_features.DB_PATH", db_path),
        patch("src.model.train_model.DB_PATH", db_path),
        patch("src.model.train_model.MODEL_PATH", model_path),
    ):
        main()

    assert model_path.exists()
    model = joblib.load(model_path)
    assert hasattr(model, "predict")
    assert model.n_features_in_ > 0
