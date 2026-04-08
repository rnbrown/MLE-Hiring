"""Run the full pipeline: ingestion, model training, and reporting."""

import asyncio
import logging
import subprocess
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

SYNC_STEPS = [
    ("Ingest merchants CSV", "ingestion.ingest_merchants_csv"),
    ("Ingest REST Countries", "ingestion.ingest_rest_countries"),
    ("Ingest Companies House", "ingestion.ingest_companies_house"),
    ("Ingest Merchant Risk API", "ingestion.ingest_merchant_risk_api"),
]

ASYNC_STEPS = [
    ("Ingest Merchant Summary PDF", "ingestion.ingest_merchant_summary_pdf"),
    ("Ingest ClarityPay", "ingestion.ingest_clarity_pay"),
]

POST_STEPS = [
    ("Train model", "model.train_model"),
    ("Generate underwriting report", "reporting.portfolio_underwriting_report"),
]


def run_module(name: str, module: str) -> bool:
    log.info("=== %s ===", name)
    result = subprocess.run(
        [sys.executable, "-m", module],
        capture_output=False,
    )
    if result.returncode != 0:
        log.error("%s failed (exit code %d)", name, result.returncode)
        return False
    return True


async def run_async_module(name: str, module: str) -> bool:
    log.info("=== %s ===", name)
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", module,
    )
    returncode = await proc.wait()
    if returncode != 0:
        log.error("%s failed (exit code %d)", name, returncode)
        return False
    return True


async def run_async_steps() -> bool:
    results = await asyncio.gather(
        *[run_async_module(name, module) for name, module in ASYNC_STEPS]
    )
    return all(results)


def main() -> None:
    log.info("Starting pipeline")

    # Run sync ingestion steps sequentially
    for name, module in SYNC_STEPS:
        if not run_module(name, module):
            # Companies House may fail without API key — continue anyway
            if module == "ingestion.ingest_companies_house":
                log.warning("Skipping Companies House (no API key?)")
                continue
            sys.exit(1)

    # Run async ingestion steps concurrently
    if not asyncio.run(run_async_steps()):
        log.warning("Some async ingestion steps failed, continuing...")

    # Post-ingestion steps
    for name, module in POST_STEPS:
        if not run_module(name, module):
            log.error("Pipeline failed at: %s", name)
            sys.exit(1)

    log.info("Pipeline complete")


if __name__ == "__main__":
    main()
