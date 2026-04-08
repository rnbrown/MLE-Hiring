import csv
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException

app = FastAPI(title="Internal Merchant Risk API")

DATA_DIR = os.getenv("DATA_DIR", "data")
DATA_PATH = Path(DATA_DIR) / "merchants.csv"

MERCHANTS: dict[str, dict] = {}


def _load_merchants() -> None:
    if MERCHANTS:
        return
    with open(DATA_PATH, newline="") as f:
        for row in csv.DictReader(f):
            MERCHANTS[row["merchant_id"]] = row


def _risk_flag(dispute_count: int, transaction_count: int) -> str:
    rate = dispute_count / transaction_count if transaction_count else 0.0
    if rate > 0.001:
        return "high"
    if rate > 0.0004:
        return "medium"
    return "low"


@app.on_event("startup")
def startup() -> None:
    _load_merchants()


@app.get("/merchant-risk/{merchant_id}")
def merchant_risk(merchant_id: str) -> dict:
    _load_merchants()
    merchant = MERCHANTS.get(merchant_id)
    if merchant is None:
        raise HTTPException(status_code=404, detail=f"Merchant {merchant_id} not found")

    volume = float(merchant["monthly_volume"])
    txn_count = int(merchant["transaction_count"])
    avg_ticket = round(volume / txn_count, 2) if txn_count else 0.0

    return {
        "merchant_id": merchant_id,
        "internal_risk_flag": _risk_flag(int(merchant["dispute_count"]), txn_count),
        "transaction_summary": {
            "last_30d_volume": volume,
            "last_30d_txn_count": txn_count,
            "avg_ticket_size": avg_ticket,
        },
        "last_review_date": "2026-04-01",
    }
