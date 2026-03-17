"""
Card Price Variant Master Transform Script

Purpose:
- Builds the processed card_price_variant_master dimension table from the tcg_card_prices parquet.
- Each row represent a unique (card_id, price_variant_type) combination.
- Deterministic BIGINT hash for price_variant_id
- Write outputs to S3 processed layer in Parquet format.
- Overwrites the dimension table each run.
"""

from pathlib import Path
import hashlib
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import yaml

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SCHEMA_PATH = PROJECT_ROOT / "schemas" / "processed" / "card_price_variant_master.yaml"

STAGING_CARD_PRICES_PATH = "s3://pokemon-tcg-data-lake/staging/pokemon_tcg/tcg_card_prices/"
PROCESSED_OUTPUT_PATH = "s3://pokemon-tcg-data-lake/processed/card_price_variant_master/"

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

def align_columns_to_schema(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    expected_columns = list(schema["columns"].keys())
    return df[expected_columns].copy()


def get_filesystem_and_path(uri: str):
    fs, path = pafs.FileSystem.from_uri(uri)
    return fs, path


def path_exists(fs: pafs.FileSystem, path: str) -> bool:
    info = fs.get_file_info(path)
    return info.type != pafs.FileType.NotFound
  
def read_staging_data(uri: str) -> pd.DataFrame:
    fs, path = get_filesystem_and_path(uri)

    if not path_exists(fs, path):
        raise FileNotFoundError(f"Staging path does not exist: {uri}")

    logger.info("Reading staging parquet from %s", uri)
    dataset = ds.dataset(path, filesystem=fs, format="parquet")
    table = dataset.to_table()
    df = table.to_pandas()

    if df.empty:
        raise ValueError(f"No records found in staging path: {uri}")

    return df

def clear_output_path(uri: str) -> None:
    fs, path = get_filesystem_and_path(uri)

    info = fs.get_file_info(path)
    if info.type == pafs.FileType.NotFound:
        logger.info("Output path does not yet exist, nothing to clear: %s", uri)
        return

    logger.info("Clearing existing output path for idempotent overwrite: %s", uri)
    fs.delete_dir(path)

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
    logger.info("Transforming staged tcg_card_prices into card_price_variant_master")

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
    ].drop_duplicates(subset=["card_id", "price_type"])

    return variant_df


# ---------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------


def write_parquet(df: pd.DataFrame, output_uri: str) -> None:
    fs, path = get_filesystem_and_path(output_uri)

    table = pa.Table.from_pandas(df, preserve_index=False)

    logger.info("Writing card_price_variant_master parquet to %s", output_uri)
    pq.write_to_dataset(
        table,
        root_path=path,
        filesystem=fs,
    )

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:
    logger.info("Starting card_price_variant_master transform")

    schema = load_schema(SCHEMA_PATH)

    df_prices = read_staging_data(STAGING_CARD_PRICES_PATH)
    logger.info("Loaded %s staged card price records", len(df_prices))

    df_variant_master = transform_card_price_variant_master(df_prices)
    logger.info(
        "Produced %s unique card price variants",
        len(df_variant_master),
    )

    validate_schema(df_variant_master, schema)
    df_variant_master = align_columns_to_schema(df_variant_master, schema)
    logger.info("Schema validation passed")

    clear_output_path(PROCESSED_OUTPUT_PATH)
    write_parquet(df_variant_master, PROCESSED_OUTPUT_PATH)

    logger.info("card_price_variant_master written to %s", PROCESSED_OUTPUT_PATH)
    logger.info("card_price_variant_master transformation complete")


if __name__ == "__main__":
    main()