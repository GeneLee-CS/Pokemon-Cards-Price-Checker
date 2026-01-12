"""
weekly_top_tcg_cards_transform.py

Purpose:
- Builds an append-only weekly leaderboard of the Top 200 Pokémon cards based on max TCG market price across all price types.
- Partitioned by price_date
- Write outputs to S3 / local staging layer in Parquet format
"""


from pathlib import Path
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml


# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]

PROCESSED_PATH = PROJECT_ROOT / "data" / "processed" 
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "processed" / "weekly_top_tcg_cards.yaml"

TCG_PRICE_HISTORY_PATH = PROCESSED_PATH / "tcg_price_history"
CARD_PRICE_VARIANT_MASTER_PATH = PROCESSED_PATH / "card_price_variant_master"
CARD_MASTER_PATH = PROCESSED_PATH / "card_master"

OUTPUT_PATH = PROCESSED_PATH / "analytics" / "weekly_top_tcg_cards"


# -------------------------------------------------------------------
# Utilities
# -------------------------------------------------------------------

def load_schema(schema_path: Path) -> dict:
    with open(schema_path, "r") as f:
        return yaml.safe_load(f)
    
def validate_schema(df: pd.DataFrame, schema: dict) -> None:
    expected_columns = schema["columns"].keys()

    missing_columns = set(expected_columns) - set(df.columns)
    extra_columns = set(df.columns) - set(expected_columns)

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    if extra_columns:
        raise ValueError(f"Unexpected columns present: {extra_columns}")
    

# -------------------------------------------------------------------
# Transformation
# -------------------------------------------------------------------

def build_weekly_top_tcg_cards(price_date:str) -> None:
    print(f"Building weekly_top_tcg_cards for price_date = {price_date}")

    # -------------------------------------------------------------------
    # Load Data
    # -------------------------------------------------------------------

    
    price_df = pd.read_parquet(TCG_PRICE_HISTORY_PATH / f"price_date={price_date}")
    print(f"Loaded price history data from {TCG_PRICE_HISTORY_PATH} with {len(price_df)} rows")

    variant_df = pd.read_parquet(CARD_PRICE_VARIANT_MASTER_PATH)
    print(f"Loaded card price variant master data from {CARD_PRICE_VARIANT_MASTER_PATH} with {len(variant_df)} rows")

    card_df = pd.read_parquet(CARD_MASTER_PATH)
    print(f"Loaded card master data from {CARD_MASTER_PATH} with {len(card_df)} rows")

    # -------------------------------------------------------------------
    # Join: price, variant, card
    # -------------------------------------------------------------------

    df = (
        price_df.merge(
            variant_df[["card_price_variant_id"]],
            on="card_price_variant_id",
            how="inner",
            validate="many_to_one"
        ).merge(
            card_df,
            on="card_id",
            how="inner"
        )
    )
    print("Joined tables (tcg_price_history, card_price_variant_master, card_master)")

    # -------------------------------------------------------------------
    # Aggregate to card-level (max market price)
    # -------------------------------------------------------------------

    agg_df = (
        df.groupby("card_id", as_index=False).agg(max_market_price=("market_price", "max"))
    )
    print("Grouped card_id along with their max price variants")

    # -------------------------------------------------------------------
    # Ranking top 200 cards
    # -------------------------------------------------------------------

    agg_df = (
        agg_df
        .sort_values("max_market_price", ascending=False)
        .head(200)
        .reset_index(drop=True)
    )

    agg_df["rank"] = agg_df.index + 1
    agg_df["price_date"] = price_date

    print("Ranked card_id based on top 200 market price descending")

    # ---------------------------------------------------------------
    # Attach card metadata
    # ---------------------------------------------------------------

    final_df = agg_df.merge(
        card_df,
        on="card_id",
        how="left"
    )
    print("Merged dataframe with card metadata")

    # ---------------------------------------------------------------
    # Schema alignment
    # ---------------------------------------------------------------

    schema = load_schema(SCHEMA_PATH)
    final_df["ingestion_date"] = price_df["ingestion_date"].iloc[0]
    final_df = final_df[list(schema["columns"].keys())]
    

    # ---------------------------------------------------------------
    # Validate
    # ---------------------------------------------------------------

    validate_schema(final_df, schema)
    print("Schema validation passed")

    # ---------------------------------------------------------------
    # Write append-only Parquet
    # ---------------------------------------------------------------
    
    output_dir = OUTPUT_PATH /f"price_date={price_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(final_df)

    pq.write_table(table, output_dir / "weekly_top_tcg_cards.parquet")

    print(f"Wrote {len(final_df)} rows to {output_dir}")

# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--price-date",
        required=True,
        help="Weekly price_date partition (YYYY-MM-DD)"
    )

    args = parser.parse_args()
    build_weekly_top_tcg_cards(args.price_date)