"""Test that ingest_clarity_pay extracts stats, partners, and value propositions."""

import asyncio
import json
from pathlib import Path
from unittest.mock import patch

from src.ingestion.ingest_clarity_pay import main

SAMPLE_HTML = """\
<html><body>
<section class="section_marquee">
  <div class="marquee_item">
    <span class="marquee_text">5 stars on Google</span>
    <span class="marquee_text">Reviews</span>
    <img alt="5 blue stars" src="stars.svg"/>
  </div>
  <div class="marquee_item">
    <span class="marquee_text">+91</span>
    <span class="marquee_text">Net Promoter Score</span>
    <img alt="NPS icon" src="nps.svg"/>
  </div>
  <div class="marquee_item">
    <span class="marquee_label">Proud Partner</span>
    <img alt="Acme Corp logo" src="acme.svg"/>
  </div>
  <div class="marquee_item">
    <span class="marquee_label">Proud Partner</span>
    <img src="https://cdn.example.com/abc123/SageDental_Wordmark_White.avif"/>
  </div>
  <div class="marquee_item">
    <span class="marquee_label">Proud Member</span>
    <img alt="Some Council" src="council.svg"/>
  </div>
</section>
<div class="tabs1_title-wrapper">Instant Pre-Approval,No Credit Impact</div>
<div class="tabs1_title-wrapper">Clear &amp; Transparent Offers</div>
</body></html>
"""


def test_produces_data(tmp_path):
    async def fake_fetch(url, **kwargs):
        return SAMPLE_HTML

    with (
        patch("src.ingestion.ingest_clarity_pay.async_fetch_with_retries", fake_fetch),
        patch("src.ingestion.ingest_clarity_pay.DATA_DIR", tmp_path),
    ):
        result = asyncio.run(main())

    assert len(result["stats"]) == 2
    assert "5 stars on Google Reviews" in result["stats"]
    assert "+91 Net Promoter Score" in result["stats"]

    assert len(result["proud_partners"]) == 2
    assert "Acme Corp" in result["proud_partners"]
    assert "Sage Dental" in result["proud_partners"]

    assert len(result["value_propositions"]) == 2
    assert "Instant Pre-Approval,No Credit Impact" in result["value_propositions"]

    output_file = tmp_path / "scrape_clarity_pay.json"
    assert output_file.exists()
    saved = json.loads(output_file.read_text())
    assert saved == result
