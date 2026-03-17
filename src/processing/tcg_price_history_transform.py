"""
TCG Price History Transform Script

Purpose:
- Transforms staging TCG card price data into an append-only fact table
- Fact table can be used to track historical TCG market prices for each card_price_variant_id.
- *NO OVERWRITES*
- Write outputs to S3 processed layer in Parquet format, partitioned by `price_date`.
"""

from pathlib import Path
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

SCHEMA_PATH = PROJECT_ROOT / "schemas" / "processed" / "tcg_price_history.yaml"

STAGING_PRICES_PATH = "s3://pokemon-tcg-data-lake/staging/pokemon_tcg/tcg_card_prices/"
CARD_VARIANT_MASTER_PATH = "s3://pokemon-tcg-data-lake/processed/card_price_variant_master/"
PROCESSED_PATH = "s3://pokemon-tcg-data-lake/processed/tcg_price_history/"

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
  
def read_parquet_dataset(uri: str) -> pd.DataFrame:
    fs, path = get_filesystem_and_path(uri)

    if not path_exists(fs, path):
        raise FileNotFoundError(f"Path does not exist: {uri}")

    logger.info("Reading parquet dataset from %s", uri)
    dataset = ds.dataset(path, filesystem=fs, format="parquet")
    table = dataset.to_table()
    df = table.to_pandas()

    if df.empty:
        raise ValueError(f"No records found at path: {uri}")

    return df

def get_latest_ingestion_date(df: pd.DataFrame) -> pd.Timestamp.date:
    if "ingestion_date" not in df.columns:
        raise ValueError("Expected column 'ingestion_date' not found in staged price data")

    ingestion_dates = pd.to_datetime(df["ingestion_date"], errors="coerce").dt.date

    if ingestion_dates.isna().any():
        raise ValueError("Found null or invalid ingestion_date values in staged price data")

    latest_ingestion_date = ingestion_dates.max()
    logger.info("Latest staged ingestion_date detected: %s", latest_ingestion_date)
    return latest_ingestion_date

def check_partition_does_not_exist(output_uri: str, price_date: str) -> None:
    fs, base_path = get_filesystem_and_path(output_uri)
    partition_path = f"{base_path.rstrip('/')}/price_date={price_date}"

    info = fs.get_file_info(partition_path)
    if info.type != pafs.FileType.NotFound:
        raise FileExistsError(
            f"Append-only protection triggered: partition already exists for price_date={price_date} "
            f"at s3://{partition_path}"
        )
    
# ---------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------

def transform_price_history(
    df_prices: pd.DataFrame,
    df_variant_master: pd.DataFrame,
) -> tuple[pd.DataFrame, str]:
    """
    Transform staged price data into append-only price history fact table.

    Returns:
        (df_price_history, snapshot_price_date)
    """

    latest_ingestion_date = get_latest_ingestion_date(df_prices)

    df = df_prices[
        [
            "card_id",
            "price_type",
            "tcg_update_date",
            "market",
            "ingestion_date",
        ]
    ].copy()

    df["ingestion_date"] = pd.to_datetime(df["ingestion_date"]).dt.date
    df = df[df["ingestion_date"] == latest_ingestion_date].copy()

    if df.empty:
        raise ValueError("No staged price records found for the selected latest ingestion_date")

    snapshot_price_date = latest_ingestion_date.isoformat()

    df["price_date"] = pd.to_datetime(snapshot_price_date).date()
    df.rename(columns={"market": "market_price"}, inplace=True)

    df["tcg_update_date"] = pd.to_datetime(df["tcg_update_date"], errors="coerce")
    df["price_date"] = pd.to_datetime(df["price_date"]).dt.date
    df["ingestion_date"] = pd.to_datetime(df["ingestion_date"]).dt.date

    df = df.merge(
        df_variant_master,
        on=["card_id", "price_type"],
        how="inner",
    )

    df = df[
        [
            "card_id",
            "card_price_variant_id",
            "price_date",
            "tcg_update_date",
            "market_price",
            "ingestion_date",
        ]
    ]

    df = df.drop_duplicates(
        subset=["card_id", "card_price_variant_id", "price_date"]
    )

    return df, snapshot_price_date

# ---------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------

def write_parquet(df: pd.DataFrame, output_uri: str) -> None:
    fs, path = get_filesystem_and_path(output_uri)

    table = pa.Table.from_pandas(df, preserve_index=False)

    logger.info("Writing tcg_price_history parquet to %s", output_uri)
    pq.write_to_dataset(
        table,
        root_path=path,
        filesystem=fs,
        partition_cols=["price_date"],
    )
    
# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:
    logger.info("Starting tcg_price_history transform")

    schema = load_schema(SCHEMA_PATH)

    df_prices = read_parquet_dataset(STAGING_PRICES_PATH)
    logger.info("Loaded %s staged price records", len(df_prices))

    df_variant_master = read_parquet_dataset(CARD_VARIANT_MASTER_PATH)
    logger.info("Loaded %s card price variants", len(df_variant_master))

    df_price_history, snapshot_price_date = transform_price_history(
        df_prices,
        df_variant_master,
    )
    logger.info(
        "Produced %s rows of price history for price_date=%s",
        len(df_price_history),
        snapshot_price_date,
    )

    validate_schema(df_price_history, schema)
    df_price_history = align_columns_to_schema(df_price_history, schema)
    logger.info("Schema validation passed")

    check_partition_does_not_exist(PROCESSED_PATH, snapshot_price_date)
    write_parquet(df_price_history, PROCESSED_PATH)

    logger.info("tcg_price_history written to %s", PROCESSED_PATH)
    logger.info("tcg_price_history transformation complete")


if __name__ == "__main__":
    main()