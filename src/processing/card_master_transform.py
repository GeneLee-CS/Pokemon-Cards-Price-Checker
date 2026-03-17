"""
Card Master Transform Script

Purpose:
- Builds the processed card_master dimension table from the full staging TCG card data parquet.
- Write outputs to S3 processed layer in Parquet format.
- Overwrites the full card_master dataset each run.

"""

from pathlib import Path
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.fs as pafs
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

STAGING_CARDS_PATH = "s3://pokemon-tcg-data-lake/staging/pokemon_tcg/tcg_cards/"
PROCESSED_OUTPUT_PATH = "s3://pokemon-tcg-data-lake/processed/card_master/"
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "processed" / "card_master.yaml"

# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------


def load_schema(schema_path: Path) -> dict:
    with open(schema_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_schema(df: pd.DataFrame, schema: dict) -> None:
    expected_columns = list(schema["columns"].keys())

    missing_columns = set(expected_columns) - set(df.columns)
    extra_columns = set(df.columns) - set(expected_columns)

    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

    if extra_columns:
        raise ValueError(f"Unexpected columns present: {sorted(extra_columns)}")
    
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

# ---------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------

def transform_card_master(df: pd.DataFrame) -> pd.DataFrame:
    # Transform staging card data (tcg_cards) into card_master dimensions.

    card_master = df[
        [
            "card_id",
            "name",
            "supertype",
            "rarity",
            "set_id",
            "set_name",
            "number",
            "set_printedTotal",
            "image_small_url",
            "image_large_url",
            "set_releaseDate"
        ]
    ].copy()

    card_master.rename(
        columns = {
            "name": "card_name",
            "set_releaseDate": "release_date"
        },
        inplace = True
    )

    card_master["card_number"] = card_master.apply(
        lambda r: (
            f"{r['number']}/{int(r['set_printedTotal'])}"
            if pd.notna(r["set_printedTotal"])
            else r["number"]
        ),
        axis=1
    )

    card_master = card_master[
        [
            "card_id",
            "card_name",
            "supertype",
            "rarity",
            "set_id",
            "set_name",
            "number",
            "card_number",
            "set_printedTotal",
            "image_small_url",
            "image_large_url",
            "release_date",
        ]
    ].drop_duplicates(subset=["card_id"])

    return card_master

# ---------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------

def write_parquet(df: pd.DataFrame, output_uri: str) -> None:
    fs, path = get_filesystem_and_path(output_uri)

    table = pa.Table.from_pandas(df, preserve_index=False)

    logger.info("Writing card_master parquet to %s", output_uri)
    pq.write_to_dataset(
        table,
        root_path=path,
        filesystem=fs,
    )


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:
    logger.info("Starting card_master transform")

    schema = load_schema(SCHEMA_PATH)

    df_staging = read_staging_data(STAGING_CARDS_PATH)
    logger.info("Loaded %s staged card records", len(df_staging))

    df_card_master = transform_card_master(df_staging)   
    logger.info("Produced %s unique card_master records", len(df_card_master))

    validate_schema(df_card_master, schema)
    df_card_master = align_columns_to_schema(df_card_master, schema)
    logger.info("Schema validation passed")

    clear_output_path(PROCESSED_OUTPUT_PATH)
    write_parquet(df_card_master, PROCESSED_OUTPUT_PATH)

    logger.info("card_master written to %s", PROCESSED_OUTPUT_PATH)
    logger.info("card_master transformation complete")


if __name__ == "__main__":
    main()