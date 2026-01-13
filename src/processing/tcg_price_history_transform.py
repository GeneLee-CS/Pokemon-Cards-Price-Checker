"""
TCG Price History Transform Script

Purpose:
- Transforms staging TCG card price data into an append-only fact table
- Fact table can be used to track historical TCG market prices for each card_price_variant_id.
- *NO OVERWRITES*
- Write outputs to S3 / local processed layer in Parquet format
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml



PROJECT_ROOT = Path(__file__).resolve().parents[2]

STAGING_PRICES_PATH = PROJECT_ROOT / "data" / "staging" / "pokemon_tcg" / "card_prices"
CARD_VARIANT_MASTER_PATH = PROJECT_ROOT / "data" / "processed" / "card_price_variant_master"
PROCESSED_PATH = PROJECT_ROOT / "data" / "processed" / "tcg_price_history"
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "processed" / "tcg_price_history.yaml"

SNAPSHOT_PRICE_DATE = datetime.now(timezone.utc).date().isoformat()

# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------

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
    
def read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {path}")
    return pd.read_parquet(path)

# ---------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------

def transform_price_history(df_prices: pd.DataFrame, df_variant_master: pd.DataFrame) -> pd.DataFrame:
    """
    Transform staged price data into append-only price history fact table.
    """

    df = df_prices[
        [
            "card_id",
            "price_type",
            "tcg_update_date",
            "market",
            "ingestion_date"
        ]
    ].copy()

    df["price_date"] = SNAPSHOT_PRICE_DATE
    df.rename(columns={"market": "market_price"}, inplace=True)

    df["price_date"] = pd.to_datetime(df["price_date"]).dt.date
    df["ingestion_date"] = pd.to_datetime(df["ingestion_date"]).dt.date

    df = df.merge(df_variant_master, on=["card_id", "price_type"], how="inner")

    # Final column selection
    df = df[
        [
            "card_id",
            "card_price_variant_id",
            "price_date",
            "tcg_update_date",
            "market_price",
            "ingestion_date"
        ]
    ]

    df = df.drop_duplicates(
        subset=["card_id", "card_price_variant_id", "price_date"]
    )

    return df

# ---------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------

def write_parquet(df: pd.DataFrame, output_path: Path) -> None:
    output_path.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(df, preserve_index=False)

    pq.write_to_dataset(
        table, 
        root_path=output_path, 
        partition_cols=["price_date"]
        )
    
# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:
    print("Starting tcg_price_history_transform...")

    schema = load_schema(SCHEMA_PATH)

    df_prices = read_parquet(STAGING_PRICES_PATH)
    print(f"Loaded {len(df_prices)} staged price records")

    df_variant_master = read_parquet(CARD_VARIANT_MASTER_PATH)
    print(f"Loaded {len(df_variant_master)} card price variants")

    df_price_history =transform_price_history(df_prices, df_variant_master)

    print(f"Produced {len(df_price_history)} rows of price history records")

    validate_schema(df_price_history, schema)
    print("schema validaiton passed")

    write_parquet(df_price_history, PROCESSED_PATH)
    print(f"tcg_price_history written to {PROCESSED_PATH}")

    print("tcg_price_history transformation complete")


if __name__ == "__main__":
    main()