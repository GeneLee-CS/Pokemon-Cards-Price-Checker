"""
Card Price Variant Master Transform Script

Purpose:
- Builds the processed card_price_variant_master dimension table from the tcg_card_prices parquet.
- Each row represent a unique (card_id, price_variant_type) combination.
- Deterministic BIGINT hash for price_variant_id
- Write outputs to S3 / local processed layer in Parquet format
"""

from pathlib import Path
from datetime import datetime
import hashlib
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml



PROJECT_ROOT = Path(__file__).resolve().parents[2]

STAGING_CARDS_PATH = PROJECT_ROOT / "data" / "staging" / "pokemon_tcg" / "card_prices"
PROCESSED_OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "card_price_variant_master"
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "processed" / "card_price_variant_master.yaml"

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
    
def read_staging_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Staging path does not exist: {path}")
    
    return pd.read_parquet(path)

def deterministic_bigint_hash(card_id: str, variant_type: str) -> int:
    """
    Generate a deteministic BIGINT hash for (card_id, price_variant_type)
    """
    raw = f"{card_id}|{variant_type}"
    return int(hashlib.sha1(raw.encode("utf-8")).hexdigest(), 16) % (10**18)

# ---------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------

def transform_card_price_variant_master(df: pd.DataFrame) -> pd.DataFrame:

    variant_df = df[
        [
            "card_id",
            "price_type"
        ]
    ].copy()

    variant_df = variant_df.drop_duplicates()

    # hash ID generation
    variant_df['card_price_variant_id'] = variant_df.apply(lambda row:deterministic_bigint_hash(row["card_id"], row["price_type"]), axis=1)

    variant_df = variant_df[
        [
            "card_price_variant_id",
            "card_id",
            "price_type"
        ]
    ]

    return variant_df


# ---------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------


def write_parquet(df: pd.DataFrame, output_path: Path) -> None:
    output_path.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(df, preserve_index = False)

    pq.write_to_dataset(table, root_path = output_path)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:
    print("Starting card_price_variant_master transform...")

    schema = load_schema(SCHEMA_PATH)

    df_prices = read_staging_data(STAGING_CARDS_PATH)
    print(f"Loaded {len(df_prices)} staged records")

    df_variant_master = transform_card_price_variant_master(df_prices)
    print(f"Produced {len(df_variant_master)} unique price variants and generated price variant IDs")

    validate_schema(df_variant_master, schema)
    print("Schema validaiton passed")

    write_parquet(df_variant_master, PROCESSED_OUTPUT_PATH)
    print(f"card_price_variant_master written to {PROCESSED_OUTPUT_PATH}")

    print("card_price_variant_master transformation complete")


if __name__ == "__main__":
    main()