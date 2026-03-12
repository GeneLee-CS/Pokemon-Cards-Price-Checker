"""
Staging Transform Script

Purpose:
- Read raw TCG card JSON data from S3
- Validate structure against schema contracts (YAML)
- Normalize data into staging datasets:
    1. tcg_cards
    2. tcg_card_prices
- Write outputs to S3 staging layer in Parquet format

Raw input expectation:
- One or more JSON files under:
    data/raw/pokemon_tcg/cards/<ingestion_date>/

Output:
- s3://<bucket>/staging/pokemon_tcg/tcg_cards/ingestion_date=YYYY-MM-DD/part-00000.parquet
- s3://<bucket>/staging/pokemon_tcg/tcg_card_prices/ingestion_date=YYYY-MM-DD/part-00000.parquet
"""


from __future__ import annotations

import json
import logging
import io
import os
from pathlib import Path
from typing import Dict, List
import argparse

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = PROJECT_ROOT / "schemas"

CARDS_SCHEMA_FILE = SCHEMA_PATH / "staging" / "tcg_cards.yaml"
CARD_PRICES_SCHEMA_FILE = SCHEMA_PATH / "staging" / "tcg_card_prices.yaml"

DEFAULT_RAW_PREFIX = "raw/pokemon_tcg/cards"
DEFAULT_STAGING_PREFIX = "staging/pokemon_tcg"


# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="TCG raw -> staging transform (S3-native)")
    parser.add_argument(
        "--ingestion-date",
        required=True,
        help="Ingestion date partition to process (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("S3_BUCKET") or os.getenv("AWS_S3_BUCKET"),
        help="S3 bucket name. Defaults to S3_BUCKET or AWS_S3_BUCKET env var.",
    )
    parser.add_argument(
        "--raw-prefix",
        default=DEFAULT_RAW_PREFIX,
        help=f"Root raw prefix. Default: {DEFAULT_RAW_PREFIX}",
    )
    parser.add_argument(
        "--staging-prefix",
        default=DEFAULT_STAGING_PREFIX,
        help=f"Root staging prefix. Default: {DEFAULT_STAGING_PREFIX}",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete existing staging partition before writing.",
    )
    return parser.parse_args()

# -------------------------------------------------------------------
# Schema Loading
# -------------------------------------------------------------------

def load_schema(schema_path: Path) -> Dict:

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
# S3 Helpers
# -------------------------------------------------------------------

def get_s3_client():
    return boto3.client("s3")


def build_raw_partition_prefix(raw_prefix: str, ingestion_date: str) -> str:
    return f"{raw_prefix}/ingestion_date={ingestion_date}/"


def build_staging_dataset_prefix(staging_prefix: str, dataset_name: str, ingestion_date: str) -> str:
    return f"{staging_prefix}/{dataset_name}/ingestion_date={ingestion_date}/"


def list_s3_keys(bucket: str, prefix: str, suffix: str = "") -> List[str]:
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    keys: List[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if suffix and not key.endswith(suffix):
                continue
            keys.append(key)

    return sorted(keys)


def read_json_from_s3(bucket: str, key: str) -> Dict:
    s3 = get_s3_client()
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)


def delete_s3_prefix(bucket: str, prefix: str) -> None:
    s3 = get_s3_client()
    keys = list_s3_keys(bucket, prefix)

    if not keys:
        logger.info(f"No existing objects found under s3://{bucket}/{prefix}")
        return

    logger.info(f"Deleting {len(keys)} existing object(s) under s3://{bucket}/{prefix}")

    for i in range(0, len(keys), 1000):
        batch = keys[i:i + 1000]
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in batch]},
        )


def upload_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    logger.info(f"Uploading Parquet to s3://{bucket}/{key}")

    table = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    s3 = get_s3_client()
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

    logger.info(f"Parquet upload complete: {len(df)} rows")


# -------------------------------------------------------------------
# Raw Data Loading
# -------------------------------------------------------------------

def load_raw_cards_from_s3(bucket: str, raw_prefix: str, ingestion_date: str) -> List[Dict]:
    """
    Load raw Pokémon card JSON records from S3.

    Expected:
    - Canonical raw file:
      raw/pokemon_tcg/cards/ingestion_date=YYYY-MM-DD/cards.json
    - Ignore temp / backup artifacts such as:
      *.backfill_tmp.json
      *.pre_backfill_backup.json
    """
    partition_prefix = build_raw_partition_prefix(raw_prefix, ingestion_date)

    candidate_keys = list_s3_keys(bucket, partition_prefix, suffix=".json")
    json_keys = [key for key in candidate_keys if key.endswith("/cards.json")]

    if not json_keys:
        raise FileNotFoundError(
            f"No canonical cards.json file found under s3://{bucket}/{partition_prefix}"
        )

    logger.info(
        f"Found {len(json_keys)} canonical raw JSON file(s) under s3://{bucket}/{partition_prefix}"
    )

    all_cards: List[Dict] = []

    for key in json_keys:
        logger.info(f"Loading file: s3://{bucket}/{key}")
        payload = read_json_from_s3(bucket, key)

        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected JSON structure in {key}. Expected dict payload.")

        if "records" not in payload:
            raise ValueError(f"Missing required 'records' key in {key}")

        records = payload["records"]

        if not isinstance(records, list):
            raise ValueError(f"Invalid 'records' value in {key}. Expected list.")

        payload_ingestion_date = payload.get("ingestion_date")
        if payload_ingestion_date and payload_ingestion_date != ingestion_date:
            raise ValueError(
                f"Ingestion date mismatch in {key}: "
                f"payload={payload_ingestion_date}, expected={ingestion_date}"
            )

        logger.info(
            "Payload metadata | source=%s run_id=%s ingestion_date=%s records=%s",
            payload.get("source"),
            payload.get("run_id"),
            payload_ingestion_date,
            len(records),
        )

        all_cards.extend(records)

    logger.info(f"Total cards loaded: {len(all_cards)}")

    return all_cards


# -------------------------------------------------------------------
# Transformation
# -------------------------------------------------------------------

def transform_cards(raw_cards: List[Dict], ingestion_date: str) -> pd.DataFrame:
    logger.info("Transforming tcg_cards dataset")

    records = []

    for card in raw_cards:
        records.append(
            {
                "card_id": card.get("id"),
                "name": card.get("name"),
                "supertype": card.get("supertype"),
                "set_printedTotal": card.get("set", {}).get("printedTotal"),
                "number": card.get("number"),
                "rarity": card.get("rarity"),
                "set_id": card.get("set", {}).get("id"),
                "set_name": card.get("set", {}).get("name"),
                "set_releaseDate": card.get("set", {}).get("releaseDate"),
                "image_small_url": card.get("images", {}).get("small"),
                "image_large_url": card.get("images", {}).get("large"),
                "ingestion_date": ingestion_date,
            }
        )

    df = pd.DataFrame(records)

    logger.info(f"tcg_cards DataFrame created with {len(df)} rows and {len(df.columns)} columns")

    return df


def transform_card_prices(raw_cards: List[Dict], ingestion_date: str) -> pd.DataFrame:
    logger.info("Transforming tcg_card_prices dataset")

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

            records.append(
                {
                    "card_id": card_id,
                    "price_type": price_type,
                    "market": market,
                    "tcg_update_date": tcg_update_date,
                    "ingestion_date": ingestion_date,
                }
            )

    expected_columns = [
        "card_id",
        "price_type",
        "market",
        "tcg_update_date",
        "ingestion_date",
    ]
    df = pd.DataFrame(records, columns=expected_columns)

    unique_ids = df["card_id"].nunique() if not df.empty else 0

    logger.info(
        f"tcg_card_prices DataFrame created with {len(df)} rows and {len(df.columns)} columns"
    )
    logger.info(f"Total unique card IDs with prices: {unique_ids}")

    return df


# -------------------------------------------------------------------
# Validation
# -------------------------------------------------------------------

def validate_dataframe(df: pd.DataFrame, schema: Dict, table_name: str) -> None:
    logger.info(f"Validating DataFrame for table {table_name}")

    schema_columns = schema.get("columns", {})
    expected_columns = set(schema_columns.keys())
    actual_columns = set(df.columns)

    missing_columns = expected_columns - actual_columns
    if missing_columns:
        raise ValueError(f"Table {table_name} is missing required columns: {missing_columns}")

    unexpected_columns = actual_columns - expected_columns
    if unexpected_columns:
        raise ValueError(f"Table {table_name} has unexpected columns: {unexpected_columns}")

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

def write_dataset_partition(
    df: pd.DataFrame,
    bucket: str,
    staging_prefix: str,
    dataset_name: str,
    ingestion_date: str,
    overwrite: bool,
) -> None:
    if "ingestion_date" not in df.columns:
        raise ValueError("DataFrame must contain 'ingestion_date' column")

    dataset_partition_prefix = build_staging_dataset_prefix(
        staging_prefix=staging_prefix,
        dataset_name=dataset_name,
        ingestion_date=ingestion_date,
    )

    if overwrite:
        delete_s3_prefix(bucket, dataset_partition_prefix)

    output_key = f"{dataset_partition_prefix}part-00000.parquet"
    upload_parquet_to_s3(df, bucket, output_key)


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main() -> None:
    logger.info("Starting TCG staging transform")

    args = parse_args()

    if not args.bucket:
        raise ValueError(
            "S3 bucket is required. Pass --bucket or set S3_BUCKET / AWS_S3_BUCKET."
        )

    ingestion_date = args.ingestion_date
    bucket = args.bucket

    logger.info(
        "Run context | bucket=%s ingestion_date=%s raw_prefix=%s staging_prefix=%s overwrite=%s",
        bucket,
        ingestion_date,
        args.raw_prefix,
        args.staging_prefix,
        args.overwrite,
    )

    cards_schema = load_schema(CARDS_SCHEMA_FILE)
    prices_schema = load_schema(CARD_PRICES_SCHEMA_FILE)

    raw_cards = load_raw_cards_from_s3(
        bucket=bucket,
        raw_prefix=args.raw_prefix,
        ingestion_date=ingestion_date,
    )

    cards_df = transform_cards(raw_cards, ingestion_date)
    prices_df = transform_card_prices(raw_cards, ingestion_date)

    validate_dataframe(cards_df, cards_schema, "tcg_cards")
    validate_dataframe(prices_df, prices_schema, "tcg_card_prices")

    write_dataset_partition(
        df=cards_df,
        bucket=bucket,
        staging_prefix=args.staging_prefix,
        dataset_name="tcg_cards",
        ingestion_date=ingestion_date,
        overwrite=args.overwrite,
    )

    write_dataset_partition(
        df=prices_df,
        bucket=bucket,
        staging_prefix=args.staging_prefix,
        dataset_name="tcg_card_prices",
        ingestion_date=ingestion_date,
        overwrite=args.overwrite,
    )

    logger.info("TCG staging transform complete")


if __name__ == "__main__":
    main()