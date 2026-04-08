"""Scrape the ClarityPay homepage marquee and expandable cards."""

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from urllib.parse import unquote

from bs4 import BeautifulSoup

from http_client import async_fetch_with_retries

log = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
CLARITY_PAY_URL = "https://www.claritypay.com/"

HAS_DIGIT = re.compile(r"\d")


def _partner_name_from_img(img) -> str:
    """Extract partner name from img alt text, falling back to the src filename."""
    alt = (img.get("alt") or "").strip()
    if alt:
        return re.sub(r"\s*(logo|Logo)\s*$", "", alt).strip()

    src = img.get("src", "")
    # Get the last path segment and strip extension
    filename = unquote(src.rsplit("/", 1)[-1]).split(".")[0]
    # Take the meaningful part before size/color suffixes
    # e.g. "SageDental_SCREEN_Stacked_Wordmark_White" -> "SageDental"
    parts = filename.split("_")
    name = parts[0]
    # If the first part looks like a CDN hash, try subsequent parts
    if re.fullmatch(r"[0-9a-f]+", name):
        for p in parts[1:]:
            if not re.fullmatch(r"[0-9a-f]+", p) and len(p) > 2:
                name = p
                break
    # Insert spaces before capitals: SageDental -> Sage Dental
    return re.sub(r"([a-z])([A-Z])", r"\1 \2", name).strip()


def extract_marquee_data(html: str) -> dict:
    """Extract all marquee items and classify into stats and proud partners."""
    soup = BeautifulSoup(html, "html.parser")

    marquee = soup.find("section", class_=lambda c: c and "marquee" in c)
    if not marquee:
        marquee = soup.find("div", class_=lambda c: c and "marquee" in c)
    if not marquee:
        log.warning("No marquee section found.")
        return {"stats": [], "proud_partners": []}

    stats: list[str] = []
    proud_partners: list[str] = []
    seen_stats: set[str] = set()
    seen_partners: set[str] = set()

    for box in marquee.find_all(True, recursive=True):
        classes = box.get("class", [])
        if not any("marquee" in c for c in classes):
            continue

        texts = [t for t in box.stripped_strings]
        if not texts:
            continue

        combined = " ".join(texts)

        # Check if this box is a child of another marquee box with the same text
        parent_marquee = None
        for p in box.parents:
            if p == marquee:
                break
            p_classes = p.get("class") or []
            if any("marquee" in c for c in p_classes):
                parent_marquee = p
                break
        if parent_marquee:
            parent_text = " ".join(parent_marquee.stripped_strings)
            if combined in parent_text:
                continue

        # Classify
        if "proud partner" in combined.lower():
            img = box.find("img")
            if img:
                name = _partner_name_from_img(img)
                if name and name not in seen_partners:
                    proud_partners.append(name)
                    seen_partners.add(name)
        elif HAS_DIGIT.search(combined):
            if combined not in seen_stats:
                stats.append(combined)
                seen_stats.add(combined)

    return {
        "stats": stats,
        "proud_partners": proud_partners,
    }


def extract_value_propositions(html: str) -> list[str]:
    """Extract headers from the expandable accordion cards."""
    soup = BeautifulSoup(html, "html.parser")
    headers: list[str] = []

    for title in soup.find_all(True, class_=lambda c: c and "tabs1_title-wrapper" in c):
        text = title.get_text(strip=True)
        if text:
            headers.append(text)

    return headers


async def main() -> dict:
    log.info("Fetching: %s", CLARITY_PAY_URL)
    html = await async_fetch_with_retries(CLARITY_PAY_URL)

    result = extract_marquee_data(html)
    result["value_propositions"] = extract_value_propositions(html)

    log.info(
        "Stats: %d, Proud Partners: %d, Value Propositions: %d",
        len(result["stats"]),
        len(result["proud_partners"]),
        len(result["value_propositions"]),
    )
    output_path = DATA_DIR / "scrape_clarity_pay.json"
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    log.info("Written to %s", output_path)

    return result


if __name__ == "__main__":
    asyncio.run(main())
