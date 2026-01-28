"""
eBay main ingestion 

Purpose:
- Fetch active eBay listings for top Pokemon cards from eBay's Browse API
"""


from __future__ import annotations
import json
import os
import time
import random
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime, timezone
import logging

import pandas as pd

from src.ingestion.ebay.ebay_auth import EbayAuthClient
from src.ingestion.ebay.ebay_client import EbayClient
from src.utils.latest_top_tcg_week_date import get_latest_price_date

# ======================
# Config
# ======================

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
WEEKLY_TOP_TCG_PATH = PROJECT_ROOT / "data" / "processed" / "analytics" / "weekly_top_tcg_cards"
CARD_MASTER_PATH = PROJECT_ROOT / "data" / "processed" / "card_master"
RAW_OUTPUT_BASE = (PROJECT_ROOT/ "data" / "raw" / "ebay" / "listings")

@dataclass(frozen=True)
class IngestConfig:
    top_n_cards: int = 10
    page_size: int = 50
    max_results_per_card: int = 200
    max_pages: int = 10
    retry_attempts: int = 5
    retry_base_seconds: float = 0.75


# ======================
# Helpers
# ======================

def utc_today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def load_latest_top_card(project_root: Path, price_date: str, top_n: int) -> pd.DataFrame:
    path = (project_root / "data" / "processed" / "analytics" / "weekly_top_tcg_cards" / f"price_date={price_date}")

    if not path.exists():
        raise FileNotFoundError(f"weekly_top_tcg_cards not found for price_date={price_date}")
    
    df = pd.read_parquet(path)
    return df.head(top_n)


def build_search_query(row: pd.Series) -> str:
    """
    Will be replaced by ebay_search.py post-exploratory phase.
    """
    number = row["number"]
    printed_total = row.get("set_printedTotal")

    if pd.notna(printed_total):
        card_number = f"{number}/{int(printed_total)}"
    else:
        card_number = number

    parts = [
        row["card_name"],
        card_number,
        row["set_name"]
    ]

    return " ".join(str(p) for p in parts if pd.notna(p))


def write_raw_json(
    *,
    price_date: str,
    ingestion_date: str,
    card_id: str,
    payload: Dict[str, Any]
) -> Path:
    output_dir = RAW_OUTPUT_BASE / f"price_date={price_date}" / f"ingestion_date={ingestion_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{card_id}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    return output_path


def summarize_listings(items: List[Dict[str, Any]]) -> str:
    prices: List[float] = []

    for item in items:
        try:
            prices.append(float(item["price"]["value"]))
        except Exception:
            continue

    if not prices:
        return "Listings: 0 | no valid prices"
    
    return f"Listings: {len(prices)} | min: ${min(prices):.2f} | max: ${max(prices):.2f}"

def fetch_all_items_for_query(
        *,
        client: EbayClient,
        query: str,
        page_size: int,
        max_results: int,
        max_pages: int
) -> Dict[str, Any]:
    """
    Fetches multiple pages from eBay Browse API and returns the acummulated itemSummaries.
    """

    all_items: List[Dict[str,Any]] = []
    merged_payload: Dict[str, Any] = {}

    offset = 0
    pages = 0

    while len(all_items) < max_results and pages < max_pages:
        pages += 1
        limit = min(page_size, max_results - len(all_items))

        payload = client.search_items(
            query=query,
            limit=limit,
            offset=offset
        )

        if not isinstance(payload, dict):
            break

        items = payload.get("itemSummaries", []) or []

        if not items:
            break

        all_items.extend(items)
        merged_payload = payload

        if len(items) < limit:
            break

        offset += len(items)

    merged_payload["itemSummaries"] = all_items
    return merged_payload


# ======================
# Main
# ======================


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )

    cfg = IngestConfig()

    logger.info("Starting eBay ingestion")
    ingestion_date = utc_today_iso()


    latest_price_date = get_latest_price_date()
    logger.info("Latest price_date detected: %s", latest_price_date)

    top_cards_df = load_latest_top_card(PROJECT_ROOT, latest_price_date, cfg.top_n_cards)

    card_master_df = pd.read_parquet(CARD_MASTER_PATH)

    enriched_df = top_cards_df.merge(
        card_master_df,
        on = "card_id",
        how = "left",
        validate = "many_to_one"
    )

    auth = EbayAuthClient()
    client = EbayClient(auth)

    passed = 0
    failed = 0

    for _, row in enriched_df.iterrows():
        card_id = row["card_id"]
        query = build_search_query(row)

        logger.info("Searching eBay | card_id=%s | query=%s", card_id, query)

        try:
            response = fetch_all_items_for_query(
                client=client,
                query=query,
                page_size=cfg.page_size,
                max_results=cfg.max_results_per_card,
                max_pages=cfg.max_pages
            )

            output_path = write_raw_json(
                price_date=latest_price_date,
                ingestion_date=ingestion_date,
                card_id=card_id,
                payload=response
            )

            items = response.get("itemSummaries", []) or []
            logger.info("card_id: %s results | %s", card_id, summarize_listings(items))
            passed += 1

        except Exception as e:
            failed += 1
            logger.exception("Failed | card_id=%s | query=%s | err=%s", card_id, query, e)

    logger.info(
        "eBay ingestion complete | passed=%d | failed=%d | ingestion_date=%s | price_date=%s",
        passed,
        failed,
        ingestion_date,
        latest_price_date
    )


if __name__ == "__main__":
    main()