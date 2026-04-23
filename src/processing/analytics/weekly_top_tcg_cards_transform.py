"""
weekly_top_tcg_cards_transform.py

Purpose:
- Builds an append-only weekly leaderboard of the Top 200 Pokémon cards based on max TCG market price across all price types.
- Partitioned by price_date
- Write outputs to S3 staging layer in Parquet format
"""


from pathlib import Path
import argparse
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import yaml

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]

SCHEMA_PATH = PROJECT_ROOT / "schemas" / "processed" / "weekly_top_tcg_cards.yaml"

TCG_PRICE_HISTORY_PATH = "s3://pokemon-tcg-data-lake/processed/tcg_price_history/"
CARD_MASTER_PATH = "s3://pokemon-tcg-data-lake/processed/card_master/"
OUTPUT_PATH = "s3://pokemon-tcg-data-lake/processed/analytics/weekly_top_tcg_cards/"


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


def read_price_date_partition(base_uri: str, price_date: str) -> pd.DataFrame:
    partition_uri = f"{base_uri.rstrip('/')}/price_date={price_date}"
    return read_parquet_dataset(partition_uri)


def check_partition_does_not_exist(output_uri: str, price_date: str) -> None:
    fs, base_path = get_filesystem_and_path(output_uri)
    partition_path = f"{base_path.rstrip('/')}/price_date={price_date}"

    info = fs.get_file_info(partition_path)
    if info.type != pafs.FileType.NotFound:
        raise FileExistsError(
            f"Append-only protection triggered: partition already exists for "
            f"price_date={price_date} at s3://{partition_path}"
        )
    

# -------------------------------------------------------------------
# Transformation
# -------------------------------------------------------------------

def build_weekly_top_tcg_cards(price_date: str) -> pd.DataFrame:
    logger.info("Building weekly_top_tcg_cards for price_date=%s", price_date)

    # -------------------------------------------------------------------
    # Load Data
    # -------------------------------------------------------------------

    price_df = read_price_date_partition(TCG_PRICE_HISTORY_PATH, price_date)
    logger.info(
        "Loaded tcg_price_history partition for %s with %s rows",
        price_date,
        len(price_df)
    )

    card_df = read_parquet_dataset(CARD_MASTER_PATH)
    logger.info(
        "Loaded card_master with %s rows",
        len(card_df)
    )

    # -------------------------------------------------------------------
    # Aggregate to card-level (max market price)
    # -------------------------------------------------------------------

    agg_df = (
        price_df.groupby("card_id", as_index=False)
        .agg(max_market_price=("market_price", "max"))
        .sort_values(["max_market_price", "card_id"], ascending=[False, True])
        .head(1000)
        .reset_index(drop=True)
    )

    agg_df["rank"] = agg_df.index + 1
    agg_df["price_date"] = pd.to_datetime(price_date).date()

    logger.info("Computed Top 200 ranked cards for price_date=%s", price_date)

    # -------------------------------------------------------------------
    # Attach card metadata
    # -------------------------------------------------------------------

    card_df = card_df.drop_duplicates(subset=["card_id"]).copy()

    final_df = agg_df.merge(
        card_df,
        on="card_id",
        how="left",
        validate="one_to_one"
    )

    logger.info("Merged leaderboard with card metadata")

    # -------------------------------------------------------------------
    # Add ingestion lineage
    # -------------------------------------------------------------------

    if "ingestion_date" not in price_df.columns:
        raise ValueError("Expected ingestion_date in tcg_price_history partition")

    unique_ingestion_dates = pd.to_datetime(
        price_df["ingestion_date"], errors="coerce"
    ).dt.date.dropna().unique()

    if len(unique_ingestion_dates) != 1:
        raise ValueError(
            f"Expected exactly 1 ingestion_date in price_date partition {price_date}, "
            f"found {len(unique_ingestion_dates)}"
        )

    final_df["ingestion_date"] = unique_ingestion_dates[0]

    return final_df


# -------------------------------------------------------------------
# Output
# -------------------------------------------------------------------

def write_parquet(df: pd.DataFrame, output_uri: str) -> None:
    fs, path = get_filesystem_and_path(output_uri)

    table = pa.Table.from_pandas(df, preserve_index=False)

    logger.info("Writing weekly_top_tcg_cards parquet to %s", output_uri)
    pq.write_to_dataset(
        table,
        root_path=path,
        filesystem=fs,
        partition_cols=["price_date"],
    )
# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main(price_date: str) -> None:
    logger.info("Starting weekly_top_tcg_cards transform")

    schema = load_schema(SCHEMA_PATH)

    df_top = build_weekly_top_tcg_cards(price_date)
    logger.info("Produced %s leaderboard rows", len(df_top))

    expected_columns = list(schema["columns"].keys())
    missing_columns = set(expected_columns) - set(df_top.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

    df_top = align_columns_to_schema(df_top, schema)
    validate_schema(df_top, schema)
    logger.info("Schema validation passed")

    check_partition_does_not_exist(OUTPUT_PATH, price_date)
    write_parquet(df_top, OUTPUT_PATH)

    logger.info("weekly_top_tcg_cards written to %s", OUTPUT_PATH)
    logger.info("weekly_top_tcg_cards transformation complete")

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--price-date",
        required=True,
        help="Weekly price_date partition (YYYY-MM-DD)"
    )
    args = parser.parse_args()

    main(args.price_date)