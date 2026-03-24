"""
eBay card market snapshot transform

Purpose:
- Transforms validated eBay listings into a stable analytics table
- Produces a snapshot of active eBay listings mapped to internal Pokemon card_ids
- Serves as the primary data source for website and downstream analytics

Notes:
- Partitioned by ingestion_date
- Append-only table unless --force is supplied
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Any, List

import boto3
import fsspec
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from src.utils.run_context import RunContext, build_parser
from src.utils.s3_paths import S3Paths
from src.utils.s3_partitions import partition_exists, split_s3_uri

logger = logging.getLogger(__name__)

# -------------------------------------------------
# Paths 
# -------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "analytics" / "ebay_card_market_snapshot.yaml"

# -------------------------------------------------
# S3 helpers
# -------------------------------------------------

def list_s3_parquet_files(partition_s3_uri: str) -> List[str]:
    bucket, prefix = split_s3_uri(partition_s3_uri)
    client = boto3.client("s3")

    paginator = client.get_paginator("list_objects_v2")
    out: List[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                out.append(f"s3://{bucket}/{key}")

    return sorted(out)


def delete_s3_prefix(prefix_s3_uri: str) -> int:
    bucket, prefix = split_s3_uri(prefix_s3_uri)
    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")

    deleted = 0
    batch: List[Dict[str, str]] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            batch.append({"Key": obj["Key"]})

            if len(batch) == 1000:
                client.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                deleted += len(batch)
                batch = []

    if batch:
        client.delete_objects(Bucket=bucket, Delete={"Objects": batch})
        deleted += len(batch)

    return deleted


# -------------------------------------------------
# Schema Validation
# -------------------------------------------------
def load_schema(schema_path: Path) -> list[str]:
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


# -------------------------------------------------
# Transformation
# -------------------------------------------------

def transform_ebay_card_market_snapshot(df: pd.DataFrame) -> pd.DataFrame:

    analytics_df = df[
        [
            "listing_id",
            "card_id",
            "price_value",
            "currency",
            "condition",
            "is_graded",
            "title_match_confidence",
            "title",
            "listing_url",
            "ingestion_date"
        ]
    ].copy()

    # Keep only 'high' and 'medium' matches
    analytics_df = analytics_df[
        analytics_df["title_match_confidence"].isin(["high", "medium"])
    ]

    analytics_df = analytics_df.dropna(subset=["listing_id", "card_id"])

    return analytics_df

def write_snapshot_parquet(df: pd.DataFrame, output_s3_uri: str) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    output_file = f"{output_s3_uri.rstrip('/')}/part-000.parquet"

    with fsspec.open(output_file, "wb") as f:
        pq.write_table(table, f)

# -------------------------------------------------
# Main
# -------------------------------------------------

def parse_args():
    parser = build_parser("Build eBay card market snapshot from staging eBay listings.")
    return parser.parse_args()

def main(run_ctx: RunContext) -> None:
    paths = S3Paths(bucket=run_ctx.bucket)

    staging_partition = paths.staging_ebay_listings_partition(
        price_date=run_ctx.price_date,
        ingestion_date=run_ctx.ingestion_date,
    )
    output_partition = paths.analytics_ebay_market_snapshot_partition(
        price_date=run_ctx.price_date,
        ingestion_date=run_ctx.ingestion_date,
    )

    logger.info(
        "Starting eBay market snapshot transform | bucket=%s | price_date=%s | ingestion_date=%s | run_id=%s",
        run_ctx.bucket,
        run_ctx.price_date_str,
        run_ctx.ingestion_date_str,
        run_ctx.run_id,
    )

    if not partition_exists(staging_partition):
        raise FileNotFoundError(f"Staging partition not found: {staging_partition}")

    if partition_exists(output_partition):
        if run_ctx.force:
            logger.warning(
                "Existing analytics partition found and --force supplied. Deleting: %s",
                output_partition,
            )
            deleted_count = delete_s3_prefix(output_partition)
            logger.info("Deleted %d existing objects from %s", deleted_count, output_partition)
        else:
            raise FileExistsError(
                f"Analytics snapshot partition already exists: {output_partition}. "
                f"Re-run with --force to replace it."
            )

    parquet_files = list_s3_parquet_files(staging_partition)
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found under {staging_partition}")

    logger.info("Found %d staging parquet files under %s", len(parquet_files), staging_partition)

    dfs: List[pd.DataFrame] = []
    for parquet_file in parquet_files:
        with fsspec.open(parquet_file, "rb") as f:
            df = pd.read_parquet(f)
        dfs.append(df)

    staging_df = pd.concat(dfs, ignore_index=True)

    analytics_df = transform_ebay_card_market_snapshot(staging_df)

    schema = load_schema(SCHEMA_PATH)
    validate_schema(analytics_df, schema)

    if analytics_df.empty:
        logger.warning("No valid eBay listings after filtering")
        return

    if run_ctx.dry_run:
        logger.info("DRY RUN | would write %d rows to %s", len(analytics_df), output_partition)
        return

    write_snapshot_parquet(analytics_df, output_partition)

    logger.info(
        "Successfully wrote eBay card market snapshot | rows=%d | path=%s",
        len(analytics_df),
        output_partition,
    )


if __name__ == "__main__":
    args = parse_args()
    run_ctx = RunContext.from_args(args)
    main(run_ctx)