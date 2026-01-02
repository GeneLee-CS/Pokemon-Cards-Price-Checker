"""
Card Master Transform Script

Purpose:
- Builds the processed card_master dimension table from the full staging TCG card data parquet.
- Write outputs to S3 / local processed layer in Parquet format
"""

from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]

STAGING_CARDS_PATH = PROJECT_ROOT / "data" / "staging" / "pokemon_tcg" / "cards"
PROCESSED_OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "card_master"
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "processed" / "card_master.yaml"

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

    card_master = card_master.drop_duplicates(subset=["card_id"])

    return card_master

# ---------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------

def write_parquet(df: pd.DataFrame, output_path: Path) -> None:
    output_path.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(df, preserve_index = False)

    pq.write_to_dataset(table, root_path = output_path)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:
    print("Starting card_master transform...")

    schema = load_schema(SCHEMA_PATH)

    df_staging = read_staging_data(STAGING_CARDS_PATH)
    print(f"Loaded {len(df_staging)} staged card records")

    df_card_master = transform_card_master(df_staging)
    print(f"Produced {len(df_card_master)} unique cards")

    validate_schema(df_card_master, schema)
    print("Schema validation passed")

    write_parquet(df_card_master, PROCESSED_OUTPUT_PATH)
    print(f"card_master written to {PROCESSED_OUTPUT_PATH}")

    print("card_master transformationn complete")

if __name__ == "__main__":
    main()