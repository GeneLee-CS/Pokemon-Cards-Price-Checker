"""
eBay card market snapshot transform

Purpose:
- Transforms validated eBay listings into a stable analytics table
- Produces a snapshot of active eBay listings mapped to internal Pokemon card_ids
- Serves as the primary data source for website and downstream analytics

Notes:
- Partitioned by ingestion_date
- Append-only table

"""

from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# -------------------------------------------------
# Paths 
# -------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

STAGING_PATH = (PROJECT_ROOT / "data" / "staging" / "ebay" / "listings")
ANALYTICS_PATH = (PROJECT_ROOT / "data" / "analytics" / "ebay_market_snapshot")


# -------------------------------------------------
# Transformation
# -------------------------------------------------

def transform_ebay_card_market_snapshot(df: pd.DataFrame) -> pd.DataFrame:

    analytics_df = df[
        [
            "listing_id",
            "card_id",
            "price_value",
            "currency",
            "condition",
            "is_graded",
            "title_match_confidence",
            "title",
            "ingestion_date"
        ]
    ].copy()

    # Keep only 'high' and 'medium' matches
    analytics_df = analytics_df[
        analytics_df["title_match_confidence"].isin(["high", "medium"])
    ]

    analytics_df = analytics_df.dropna(subset=["listing_id", "card_id"])

    return analytics_df


# -------------------------------------------------
# Main
# -------------------------------------------------

def main() -> None:

    if not STAGING_PATH.exists():
        raise FileNotFoundError(f"Staging path does not exist: {STAGING_PATH}")
    
    # ---------------------------------------------------------------
    # Select latest price_date (TCG week)
    # ---------------------------------------------------------------
    price_date_dirs = sorted(
        STAGING_PATH.glob("price_date=*"),
        key=lambda p: p.name.split("=")[1]
    )

    if not price_date_dirs:
        raise RuntimeError("No price_date partitions found in eBay staging data.")
    
    latest_price_date_dir = price_date_dirs[-1]
    latest_price_date = latest_price_date_dir.name.split("=")[1]

    # ---------------------------------------------------------------
    # Select latest ingestion_date within the price_date
    # ---------------------------------------------------------------
    ingestion_date_dirs = sorted(
        latest_price_date_dir.glob("ingestion_date=*"),
        key=lambda p: p.name.split("=")[1]
    )

    if not ingestion_date_dirs:
        raise RuntimeError(f"No ingestion_date partitions found under {latest_price_date_dir}")
    
    latest_ingestion_dir = ingestion_date_dirs[-1]
    ingestion_date = latest_ingestion_dir.name.split("=")[1]

    print(
        f"Processing eBay snapshot for "
        f"price_date={latest_price_date}, "
        f"ingestion_date={ingestion_date}"
    )

    # ---------------------------------------------------------------
    # Read parquet files and transform
    # ---------------------------------------------------------------

    dfs: list[pd.DataFrame] = []

    for parquet_file in latest_ingestion_dir.glob("*.parquet"):
        df = pd.read_parquet(parquet_file)
        df["ingestion_date"] = ingestion_date
        dfs.append(df)

    if not dfs:
        raise RuntimeError("No parquet files found for latest eBay snapshot.")
    
    staging_df = pd.concat(dfs, ignore_index=True)

    analytics_df = transform_ebay_card_market_snapshot(staging_df)

    if analytics_df.empty:
        print("No valid eBay listings after filtering")
        return
    
    # ---------------------------------------------------------------
    # Output
    # ---------------------------------------------------------------
    
    output_dir = ANALYTICS_PATH / f"ingestion_date={ingestion_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(analytics_df, preserve_index=False)
    pq.write_table(table, output_dir / "part-000.parquet")

    print(
        f"Succesfully wrote eBay card market snapshot: "
        f"{len(analytics_df)} rows"
    )


if __name__ == "__main__":
    main()