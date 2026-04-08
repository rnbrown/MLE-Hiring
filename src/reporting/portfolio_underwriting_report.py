"""Generate a portfolio underwriting report using Gemini."""

import logging
import os
import re
import sqlite3
from datetime import date
from pathlib import Path

import requests

log = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DB_PATH = DATA_DIR / "merchants.db"
REPORT_PATH = DATA_DIR / "underwriting_report.md"

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"


def get_portfolio_stats() -> dict:
    conn = sqlite3.connect(DB_PATH)

    stats = {}

    row = conn.execute("""
        SELECT
            COUNT(*) AS merchant_count,
            SUM(monthly_volume) AS total_volume,
            SUM(dispute_count) AS total_disputes,
            SUM(transaction_count) AS total_transactions,
            AVG(monthly_volume) AS avg_volume,
            MIN(monthly_volume) AS min_volume,
            MAX(monthly_volume) AS max_volume,
            SUM(
                CASE WHEN transaction_count > 0
                     THEN CAST(dispute_count AS REAL) / transaction_count * monthly_volume
                     ELSE 0
                END
            ) AS expected_loss
        FROM merchants
    """).fetchone()

    stats["merchant_count"] = row[0]
    stats["total_volume"] = row[1]
    stats["total_disputes"] = row[2]
    stats["total_transactions"] = row[3]
    stats["avg_volume"] = round(row[4], 2)
    stats["min_volume"] = row[5]
    stats["max_volume"] = row[6]
    stats["overall_dispute_rate"] = round(row[2] / row[3] * 100, 4) if row[3] else 0
    stats["expected_loss"] = round(row[7], 2) if row[7] else 0

    # By country
    country_rows = conn.execute("""
        SELECT country, COUNT(*) AS n, SUM(monthly_volume) AS vol,
               SUM(dispute_count) AS disputes, SUM(transaction_count) AS txns,
               SUM(
                   CASE WHEN transaction_count > 0
                        THEN CAST(dispute_count AS REAL) / transaction_count * monthly_volume
                        ELSE 0
                   END
               ) AS expected_loss
        FROM merchants
        GROUP BY country
        ORDER BY vol DESC
    """).fetchall()
    stats["by_country"] = [
        {"country": r[0], "merchants": r[1], "volume": r[2],
         "disputes": r[3], "transactions": r[4],
         "dispute_rate": round(r[3] / r[4] * 100, 4) if r[4] else 0,
         "expected_loss": round(r[5], 2) if r[5] else 0}
        for r in country_rows
    ]

    # By risk flag (from dispute rate thresholds)
    risk_rows = conn.execute("""
        SELECT
            CASE
                WHEN CAST(dispute_count AS REAL) / transaction_count > 0.001 THEN 'high'
                WHEN CAST(dispute_count AS REAL) / transaction_count > 0.0004 THEN 'medium'
                ELSE 'low'
            END AS risk_level,
            COUNT(*) AS n,
            SUM(monthly_volume) AS vol,
            SUM(dispute_count) AS disputes,
            SUM(CAST(dispute_count AS REAL) / transaction_count * monthly_volume) AS expected_loss
        FROM merchants
        WHERE transaction_count > 0
        GROUP BY risk_level
        ORDER BY risk_level
    """).fetchall()
    stats["by_risk_level"] = [
        {"risk_level": r[0], "merchants": r[1], "volume": r[2], "disputes": r[3],
         "expected_loss": round(r[4], 2) if r[4] else 0}
        for r in risk_rows
    ]

    conn.close()
    return stats


def build_prompt(stats: dict) -> str:
    country_lines = "\n".join(
        f"  - {c['country']}: {c['merchants']} merchants, volume ${c['volume']:,}, "
        f"disputes {c['disputes']}, dispute rate {c['dispute_rate']}%, "
        f"expected loss ${c['expected_loss']:,.2f}"
        for c in stats["by_country"]
    )
    risk_lines = "\n".join(
        f"  - {r['risk_level']}: {r['merchants']} merchants, volume ${r['volume']:,}, "
        f"disputes {r['disputes']}, expected loss ${r['expected_loss']:,.2f}"
        for r in stats["by_risk_level"]
    )

    today = date.today().isoformat()

    return f"""You are a senior underwriting analyst. Write a professional portfolio underwriting
report in markdown format based on the following portfolio statistics.

MANDATORY formatting rules — you must follow these exactly:
- The report header must start with: **Date:** {today}
- Do NOT include any "Prepared For" or "Prepared By" lines.
- ALL currency values MUST use US dollar signs ($). NEVER use pound signs (£).
- Expected loss is computed per merchant as dispute_rate * monthly_volume, then summed.

The report should include:
1. An executive summary (include total expected loss)
2. Portfolio overview (size, volume, dispute rates, expected loss)
3. Geographic breakdown (with expected loss per country)
4. Risk distribution analysis (with expected loss per risk tier)
5. Key observations and recommendations

Portfolio Statistics:
- Total merchants: {stats['merchant_count']}
- Total monthly volume: ${stats['total_volume']:,}
- Total transactions: {stats['total_transactions']:,}
- Total disputes: {stats['total_disputes']}
- Overall dispute rate: {stats['overall_dispute_rate']}%
- Portfolio expected loss: ${stats['expected_loss']:,.2f}
- Average merchant volume: ${stats['avg_volume']:,}
- Volume range: ${stats['min_volume']:,} - ${stats['max_volume']:,}

By Country:
{country_lines}

By Risk Level:
{risk_lines}

Write the report now. Use markdown formatting with headers, tables where appropriate, and clear professional language.
Remember: $ for all currencies, date is {today}, no "Prepared For" / "Prepared By"."""


SYSTEM_INSTRUCTION = (
    "You are a senior underwriting analyst writing reports for a US-based BNPL company. "
    "STRICT RULES: 1) All currency values use US dollars ($), NEVER pounds (£). "
    "2) The report header is ONLY '**Date:** <date>' — no Prepared For, Prepared By, To, From, or Subject lines. "
    "3) Use the exact date provided in the prompt."
)


def call_gemini(prompt: str) -> str:
    resp = requests.post(
        GEMINI_URL,
        params={"key": GEMINI_API_KEY},
        json={
            "system_instruction": {"parts": [{"text": SYSTEM_INSTRUCTION}]},
            "contents": [{"parts": [{"text": prompt}]}],
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["candidates"][0]["content"]["parts"][0]["text"]


def post_process(report: str, report_date: str) -> str:
    """Fix formatting the LLM may get wrong: currency symbols, date, attribution lines."""
    # Replace pound signs with dollar signs
    report = report.replace("£", "$")

    # Remove Prepared For / Prepared By / To / From / Subject lines
    lines = report.splitlines()
    filtered: list[str] = []
    for line in lines:
        stripped = line.strip().lower()
        if any(stripped.startswith(prefix) for prefix in (
            "**prepared for", "**prepared by", "**to:", "**from:", "**subject:",
            "prepared for", "prepared by", "to:", "from:", "subject:",
        )):
            continue
        filtered.append(line)
    report = "\n".join(filtered)

    # Fix the date to match the actual report date
    report = re.sub(
        r"(\*\*Date:\*\*)\s*\S.*",
        rf"\1 {report_date}",
        report,
    )

    return report


def main() -> None:
    if not DB_PATH.exists():
        log.error("Database not found: %s", DB_PATH)
        return

    if not GEMINI_API_KEY:
        log.error("GEMINI_API_KEY not set.")
        return

    log.info("Gathering portfolio statistics...")
    stats = get_portfolio_stats()
    log.info(
        "Portfolio: %d merchants, $%s total volume, %s%% dispute rate",
        stats["merchant_count"],
        f"{stats['total_volume']:,}",
        stats["overall_dispute_rate"],
    )

    log.info("Generating report with Gemini...")
    prompt = build_prompt(stats)
    report = call_gemini(prompt)
    report = post_process(report, date.today().isoformat())

    REPORT_PATH.write_text(report)
    log.info("Report written to: %s", REPORT_PATH)


if __name__ == "__main__":
    main()
