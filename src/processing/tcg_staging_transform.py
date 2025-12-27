"""
Staging Transform Script

Purpose:
- Read raw TCG card JSON data from S3
- Validate structure against schema contracts (YAML)
- Normalize data into staging datasets:
    1. cards
    2. card_prices
- Write outputs to S3 / local staging layer in Parquet format
"""


from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import date
import re

import pandas as pd
import yaml

# Optional (will be used later)
# import boto3
# import pyarrow as pa
# import pyarrow.parquet as pq


# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "local" / "pokemon_tcg" / "cards" / "2025-12-10"
STAGING_DATA_PATH = PROJECT_ROOT / "data" / "staging" / "pokemon_tcg"
SCHEMA_PATH = PROJECT_ROOT / "schemas"

CARDS_SCHEMA_FILE = SCHEMA_PATH / "tcg_cards.yaml"
CARD_PRICES_SCHEMA_FILE = SCHEMA_PATH / "tcg_card_prices.yaml"


# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Schema Loading
# -------------------------------------------------------------------

def load_schema(schema_path: Path) -> Dict:
    """
    Load a YAML schema contract.

    Returns:
        Dictionary representation of the schema.
    """

    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    
    logger.info(f"Loading schema: {schema_path.name}")

    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            schema = yaml.safe_load(f)
    except yaml.YAMLError as e:
        logger.error(f"invalid YAML in schema file {schema_path}: {e}")
        raise

    if not isinstance(schema, dict):
        raise ValueError(f"Schema file {schema_path} must define a YAML mapping")
    
    if "columns" not in schema:
        raise ValueError(f"Schema file {schema_path} missing required 'columns' key")
    
    logger.info(
        f"Loaded schema '{schema.get('table', 'unknown')}' "
        f"with {len(schema['columns'])} columns"
    )

    return schema

# -------------------------------------------------------------------
# Raw Data Loading
# -------------------------------------------------------------------

def load_raw_cards(raw_path: Path) -> List[Dict]:
    """
    Load raw Pokémon card JSON records.

    Expected:
    - One or more JSON files
    - Each file contains a list of card objects
    """

    if not raw_path.exists():
        raise FileNotFoundError(f"Raw data path does not exist: {raw_path}")
    
    json_files = list(raw_path.rglob("*.json"))

    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {raw_path}")
    
    logger.info(f"Found {len(json_files)} raw JSON files")

    all_cards: List[Dict] = []

    for file_path in json_files:
        logger.info(f"Loading file: {file_path.name}")

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                payload = json.load(f)

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in file {file_path}: {e}")
            raise

        if isinstance(payload, list):
            cards = payload
        else:
            raise ValueError(f"Unexpected JSON structure in {file_path}. Expected list.")

        all_cards.extend(cards)

        logger.info(f"Loaded {len(cards)} cards from {file_path.name}")


    logger.info(f"Total cards loaded: {len(all_cards)}")

    return all_cards   


# -------------------------------------------------------------------
# Extract Ingestion Date
# -------------------------------------------------------------------

def extract_ingestion_date(file_path: Path) -> date:
    """
    Extract intestion date from file path.

    Supported formats:
    - date/raw/local/pokemon_tcg/cards/2025-12-10/file.json
    """

    for part in file_path.parts:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", part):
            return date.fromisoformat(part)
        
    raise ValueError(f"Could not extract valid ingestion date from path: {file_path}")

# -------------------------------------------------------------------
# Transformation
# -------------------------------------------------------------------

def transform_cards(raw_cards: List[Dict], raw_path: Path) -> pd.DataFrame:
    """
    Transform raw card objects into the cards staging table.
    """

    logger.info("Transforming cards dataset")

    ingestion_date = extract_ingestion_date(raw_path)
    logger.info(f"Using ingestion_date={ingestion_date}")

    records = []

    for card in raw_cards:
        record = {
            "card_id": card.get("id"),
            "name": card.get("name"),
            "supertype": card.get("supertype"),
            "set_printedTotal": card.get("set", {}).get("printedTotal"),
            "number": card.get("number"),
            "rarity": card.get("rarity"),
            "set_id": card.get("set", {}).get("id"),
            "set_name": card.get("set", {}).get("name"),
            "set_releaseDate": card.get("set", {}).get("releaseDate"),
            "ingestion_date": ingestion_date
        }
        records.append(record)

    df = pd.DataFrame(records)

    logger.info(f"Cards DataFrame created with {len(df)} rows and {len(df.columns)} columns")

    return df


def transform_card_prices(raw_cards: List[Dict], raw_path: Path) -> pd.DataFrame:
    """
    Extract and normalize pricing data into the card_prices staging table.

    Expected output:
    - One row per card_id + price_type + metric
    """
    logger.info("Transforming card prices dataset")

    ingestion_date = extract_ingestion_date(raw_path)
    logger.info(f"Using ingestion_date={ingestion_date}")

    records = []

    for card in raw_cards:

        card_id = card.get("id")

        tcgplayer = card.get("tcgplayer")
        if not tcgplayer:
            continue

        tcg_update_date = tcgplayer.get("updatedAt")

        prices = tcgplayer.get("prices")
        if not prices:
            continue

        for price_type, metrics in prices.items():
            if not isinstance(metrics, dict):
                continue

            market = metrics.get("market")
            if market is None:
                continue

            records.append({
                "card_id": card_id,
                "price_type": price_type,
                "market": market,
                "tcg_update_date": tcg_update_date,
                "ingestion_date": ingestion_date
            })



    df = pd.DataFrame(records)
    unique_ids = df["card_id"].nunique()

    logger.info(f"Card prices DataFrame created with {len(df)} rows and {len(df.columns)} columns.")
    logger.info(f"Total unique card IDs with prices :{unique_ids}")

    return df


# -------------------------------------------------------------------
# Validation
# -------------------------------------------------------------------

def validate_dataframe(df: pd.DataFrame, schema: Dict, table_name: str) -> None:
    """
    Validate a DataFrame against its schema contract.

    Raises:
        ValueError if validation fails.
    """

    logger.info(f"Validating Dataframe for table {table_name}")

    schema_columns = schema.get("columns", {})
    expected_columns = set(schema_columns.keys())
    actual_columns = set(df.columns)


    # 1. Missing columns

    missing_columns = expected_columns - actual_columns
    if missing_columns:
        raise ValueError(f"Table {table_name} is missing required columns: {missing_columns}")

    # 2. Unexpected columns

    unexpected_columns = actual_columns - expected_columns
    if unexpected_columns:
        raise ValueError(f"Table {table_name} has unexpected columns: {unexpected_columns}")
    
    # 3. Nullability enforcement

    for column_name, column_meta in schema_columns.items():
        nullable = column_meta.get("nullable", True)

        if not nullable:
            null_count = df[column_name].isna().sum()
            if null_count > 0:
                raise ValueError(
                    f"Column '{column_name}' in table '{table_name}' "
                    f"contains {null_count} null values but is marked nullable: false"
                )
            
    logger.info(f"Validation passed for table '{table_name}'")

    

# -------------------------------------------------------------------
# Write Outputs
# -------------------------------------------------------------------

def write_parquet(df: pd.DataFrame, output_path: Path) -> None:
    """
    Write DataFrame to Parquet.
    """
    if "ingestion_date" not in df.columns:
        raise ValueError("DataFrame must contain 'ingestion_date' column")
    
    logger.info(f"Writing Parquet to {output_path}")

    output_path.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        output_path,
        engine='pyarrow',
        partition_cols=["ingestion_date"],
        index=False
    )

    logger.info(f"Parquet write complete: {len(df)} rows")

# -------------------------------------------------------------------
# Orchestration
# -------------------------------------------------------------------

def run() -> None:
    """
    Main execution entrypoint.
    """
    logger.info("Starting staging transform")

    # Load schemas
    cards_schema = load_schema(CARDS_SCHEMA_FILE)
    prices_schema = load_schema(CARD_PRICES_SCHEMA_FILE)

    # Load raw data
    raw_cards = load_raw_cards(RAW_DATA_PATH)

    # Transform
    cards_df = transform_cards(raw_cards, RAW_DATA_PATH)
    prices_df = transform_card_prices(raw_cards, RAW_DATA_PATH)

    # Validate
    validate_dataframe(cards_df, cards_schema, "cards")
    validate_dataframe(prices_df, prices_schema, "card_prices")

    # Write outputs
    write_parquet(cards_df, STAGING_DATA_PATH / "cards")
    write_parquet(prices_df, STAGING_DATA_PATH / "card_prices")

    logger.info("Staging transform complete")


# -------------------------------------------------------------------
# CLI Entry
# -------------------------------------------------------------------

if __name__ == "__main__":
    run()
