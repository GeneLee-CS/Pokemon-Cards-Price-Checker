"""
eBay main ingestion 

Purpose:
- Fetch active eBay listings for top Pokemon cards from eBay's Browse API
"""


from __future__ import annotations
import json
import os
from pathlib import Path
from typing import Dict, Any, List
from datetime import date

import pandas as pd

from src.ingestion.ebay.ebay_auth import EbayAuthClient
from src.ingestion.ebay.ebay_client import EbayClient
from src.utils.latest_top_tcg_week_date import get_latest_price_date

# ======================
# Config
# ======================

PROJECT_ROOT = Path(__file__).resolve().parents[3]
WEEKLY_TOP_TCG_PATH = PROJECT_ROOT / "data" / "processed" / "analytics" / "weekly_top_tcg_cards"
CARD_MASTER_PATH = PROJECT_ROOT / "data" / "processed" / "card_master"
card_master_df = pd.read_parquet(CARD_MASTER_PATH)

TOP_N_CARDS = 10
RESULTS_PER_CARD = 50

RAW_OUTPUT_BASE = (
    PROJECT_ROOT/ "data" / "raw" / "ebay" / "listings"
)

INGESTION_DATE = date.today().isoformat()


# ======================
# Helpers
# ======================

def load_latest_top_card(price_date: str) -> pd.DataFrame:
    path = (
        PROJECT_ROOT / "data" / "processed" / "analytics" / "weekly_top_tcg_cards" / f"price_date={price_date}"
    )

    if not path.exists():
        raise FileNotFoundError(f"weekly_top_tcg_cards not found for {price_date}")
    
    df = pd.read_parquet(path)
    return df.head(TOP_N_CARDS)


def build_search_query(row: pd.Series) -> str:
    """
    Naive query builder for temporary use.
    This will change later.
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
    price_date: str,
    card_id: str,
    payload: Dict[str, Any]
) -> None:
    output_dir = RAW_OUTPUT_BASE / f"price_date={price_date}" / f"ingestion_date={INGESTION_DATE}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{card_id}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Saved raw JSON -> {output_path}")


def summarize_listings(items: List[Dict[str, Any]]) -> None:
    prices = []

    for item in items:
        try:
            price = float(item["price"]["value"])
            prices.append(price)
        except Exception:
            continue

    if not prices:
        print("No valid prices found")
        return
    
    print(
        f"Listings: {len(prices)} | "
        f"min: ${min(prices):.2f} | "
        f"max: ${max(prices):.2f}"
    )


# ======================
# Main
# ======================


def main() -> None:
    print("Starting exploratory eBay ingestion")

    latest_price_date = get_latest_price_date()

    print(f"Latest price_date detected as: {latest_price_date}")

    top_cards_df = load_latest_top_card(latest_price_date)

    enriched_df = top_cards_df.merge(
        card_master_df,
        on = "card_id",
        how = "left",
        validate = "many_to_one"
    )

    auth = EbayAuthClient()
    client = EbayClient(auth)

    for _, row in enriched_df.iterrows():
        card_id = row["card_id"]
        query = build_search_query(row)

        print(f"Searching eBay for : {query}")

        response = client.search_items(
            query = query,
            limit = RESULTS_PER_CARD
        )

        write_raw_json(
            price_date = latest_price_date,
            card_id = card_id,
            payload = response
        )

        items = response.get("itemSummaries", [])
        summarize_listings(items)

    print("eBay ingestion complete")


if __name__ == "__main__":
    main()