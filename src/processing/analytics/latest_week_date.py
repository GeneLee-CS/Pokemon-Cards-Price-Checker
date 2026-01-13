"""
latest_week_date.py

Purpose:
- Determines the most recent weekly snapshot of weekly_top_tcg_cards for downstream eBay ingestion
- Reads only the partition of the latest price_date
- Produces the list of Top N card_ids for the latest week
"""


from pathlib import Path
import argparse
import pandas as pd
import re



# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]
WEEKLY_TOP_PATH = PROJECT_ROOT / "data" / "processed" / "analytics" / "weekly_top_tcg_cards"

PRICE_DATE_PATTERN = re.compile(r"price_date=(\d{4}-\d{2}-\d{2})")


# -------------------------------------------------------------------
# Latest Price Date
# -------------------------------------------------------------------

def get_latest_price_date() -> str:
    price_dates = []

    for path in WEEKLY_TOP_PATH.iterdir():
        if not path.is_dir():
            continue

        match = PRICE_DATE_PATTERN.fullmatch(path.name)
        if match:
            price_dates.append(match.group(1))

    if not price_dates:
        raise RuntimeError("No price_date partitions found")
    
    return max(price_dates)

# -------------------------------------------------------------------
# Load Latest Top Cards
# -------------------------------------------------------------------

def load_latest_top_cards(top_n: int = 200) -> pd.DataFrame:
    latest_price_date = get_latest_price_date()
    partition_path = WEEKLY_TOP_PATH / f"price_date={latest_price_date}"

    df = pd.read_parquet(partition_path)

    df = (
        df.sort_values("rank").head(top_n).reset_index(drop=True)
    )

    return df, latest_price_date


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main(top_n: int):
    df, price_date = load_latest_top_cards(top_n)

    print(f"Latest price_date = {price_date}")
    print(f"Top {len(df)} loaded")
    print(df[["rank", "card_id", "max_market_price"]].head(10))

# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--top-n",
        type=int,
        help="Number of top cards to load"
    )
    args = parser.parse_args()

    main(args.top_n)
