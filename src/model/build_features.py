"""Join merchants and countries, compute features, and one-hot encode categoricals."""

import logging
import os
import re
import sqlite3
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DB_PATH = DATA_DIR / "merchants.db"

CATEGORICAL_COLS = [
    "country",
    "region",
    "subregion",
]

LIST_COLS = {
    "currencies": lambda v: [re.match(r"^(\w+)", s.strip()).group(1) for s in v.split(",") if re.match(r"^(\w+)", s.strip())],
    "languages": lambda v: [s.strip() for s in v.split(",")],
}

DROP_COLS = [
    "name",
    "month",
    "registration_number",
    "country_common_name",
    "country_official_name",
    "cca2",
    "cca3",
    "capital",
]


def load_joined_data() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql_query("""
        SELECT
            m.month,
            m.merchant_id,
            m.name,
            m.country,
            m.registration_number,
            m.monthly_volume,
            m.dispute_count,
            m.transaction_count,
            c.common_name       AS country_common_name,
            c.official_name     AS country_official_name,
            c.cca2,
            c.cca3,
            c.region,
            c.subregion,
            c.population,
            c.capital,
            c.currencies,
            c.languages
        FROM merchants m
        LEFT JOIN countries c ON m.country = c.country_name
    """, conn)

    conn.close()
    return df


def expand_list_column(df: pd.DataFrame, col: str, parser) -> pd.DataFrame:
    """Create a binary column for each unique value in a comma-separated list column."""
    all_values: set[str] = set()
    for val in df[col].dropna():
        all_values.update(parser(val))

    for v in sorted(all_values):
        col_name = f"{col}_{v.lower().replace(' ', '_')}"
        df[col_name] = df[col].apply(
            lambda x, v=v: int(v in parser(x)) if pd.notna(x) else 0
        )

    df = df.drop(columns=[col])
    return df


def encode_features(df: pd.DataFrame) -> pd.DataFrame:
    # Drop non-feature columns
    df = df.drop(columns=[c for c in DROP_COLS if c in df.columns])

    # Set merchant_id as index
    df = df.set_index("merchant_id")

    # Compute avg_ticket_size
    df["avg_ticket_size"] = (df["monthly_volume"] / df["transaction_count"]).round(2)

    # Expand list columns first (before get_dummies)
    for col, parser in LIST_COLS.items():
        if col in df.columns:
            df = expand_list_column(df, col, parser)

    # One-hot encode simple categoricals
    df = pd.get_dummies(df, columns=[c for c in CATEGORICAL_COLS if c in df.columns], dtype=int)

    return df


def main() -> None:
    if not DB_PATH.exists():
        log.error("Database not found: %s", DB_PATH)
        return

    df = load_joined_data()
    log.info("Joined table: %d rows, %d columns", len(df), len(df.columns))

    df = encode_features(df)
    log.info("Encoded table: %d rows, %d columns", len(df), len(df.columns))
    print(df.head(10).to_string())


if __name__ == "__main__":
    main()
