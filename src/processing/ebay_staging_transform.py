"""
eBay staging transform

Purpose:
- Transform raw eBay Browse API JSON results into a structured staging table (ebay_listings)
- Normalize titles, extract PSA grading and compute title match confidence.
- Reject listings that fall below match confidence. (Classified as 'reject', not dropped from table)
- Outputs append-only, partitioned Parquet validated against it's schema (ebay_listing.yaml)

Notes:
- Current staging layer only classifies data without removing any rows. Downstream layers decide what to filter.

Matching confidence:
(card number referenced here is the combination of card_number/set_printedTotal [e.g. 104/98])
- HIGH:
    - Card Name match + Card Number match + Set Name match
    - Card Name match + Card Number match
- MEDIUM:
    - Card Name match + Set Name match + Card Number not present
- LOW:
    - Card Name match + Card Number not present + Set Name not matched
- REJECT:
    - Card Name not matched
    - Card Name match + Card Number not matched
"""


from __future__ import annotations

import io
import json
import logging
import re
from typing import Dict, Any, Optional, List

import boto3
import fsspec
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.processing.ebay_filters import is_non_card_listing
from src.utils.run_context import RunContext, build_parser
from src.utils.s3_paths import S3Paths
from src.utils.s3_partitions import partition_exists, split_s3_uri

logger = logging.getLogger(__name__)

# -------------------------------------------------
# S3 helpers
# -------------------------------------------------

def list_s3_json_files(partition_s3_uri: str) -> List[str]:
    """
    Return full s3:// URIs for all .json objects directly under a partition prefix.
    """
    bucket, prefix = split_s3_uri(partition_s3_uri)
    client = boto3.client("s3")

    paginator = client.get_paginator("list_objects_v2")
    out: List[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                out.append(f"s3://{bucket}/{key}")

    return sorted(out)


def delete_s3_prefix(prefix_s3_uri: str) -> int:
    """
    Delete all objects under an S3 prefix. Used only when --force is supplied.
    Returns number of deleted objects.
    """
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


def load_card_master(paths: S3Paths) -> pd.DataFrame:
    path = paths.processed_card_master_root()
    logger.info("Loading card_master from %s", path)
    return pd.read_parquet(path)

# -------------------------------------------------
# Normalization helpers
# -------------------------------------------------
"""
Functions used to normalize card_name in card_master and eBay listing titles before matching
"""

def normalize_title(text: str) -> str:
    """
    Normalize listing title:
    - lowercase
    - remove punctuation
    - collapse extra whitespace
    
    Note:
    Descriptors like EX, VMAX, GX are intentionally preserved here.
    """
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def normalize_card_name(card_name: str) -> str:
    """
    Normalize internal card name to base Pokemons. Removes descriptors and symbols (-EX, -GX, δ).
    """
    card_name = card_name.lower()
    card_name = re.sub(r"[^\w\s]", " ", card_name)
    card_name = re.sub(
        r"\b(ex|gx|v|vmax|vstar|promo|alt|art|lvx|gold foil)\b", " ",
        card_name
    )
    card_name = card_name.replace("δ", "")
    card_name = re.sub(r"\s+", " ", card_name)
    return card_name.strip()

def normalize_set_name(set_name: str) -> str:
    """
    Normalize internal set name
    """
    return normalize_title(set_name)


# -------------------------------------------------
# Matching helpers
# -------------------------------------------------

def card_name_match(title: str, card_name: str) -> bool:
    base_name = normalize_card_name(card_name)
    title_name = normalize_title(title)  
    return base_name in title_name

def extract_card_number(title: str) -> Optional[str]:
    match = re.search(r"\b(\d{1,3}/\d{1,3})\b", title)
    return match.group(1) if match else None

def card_number_match(title: str, card_number: str) -> Optional[bool]:
    title_number = extract_card_number(title)
    if not title_number:
        return None
    return title_number == card_number


# -------------------------------------------------
# PSA extraction
# -------------------------------------------------

def extract_psa_grade(title_normalized: str) -> Optional[int]:
    match = re.search(r"\bpsa\s*(\d{1,2})\b", title_normalized)
    if match:
        return int(match.group(1))
    return None

# -------------------------------------------------
# Confidence scoring
# -------------------------------------------------

def compute_title_match_confidence(
        *,
        name_match: bool,
        number_match: Optional[bool],
        set_match: bool
) -> str:
    
    if not name_match:
        return "reject"
    
    if number_match is False:
        return "reject"
    
    if number_match is True:
        return "high"
    
    if number_match is None and set_match:
        return "medium"
    
    return "low"

# -------------------------------------------------
# Transformation
# -------------------------------------------------

def transform_listing(
    item_summary: Dict[str, Any],
    card_row: Dict[str, Any],
    price_date: str,
    ingestion_date: str
) -> Dict[str, Any]:
    """
    Transforms a listing (single API itemSummary) into a staging row
    """
    
    raw_title = item_summary.get("title", "")
    title_normalized = normalize_title(raw_title)

    # Ignoring listings that are not Pokemon cards
    if is_non_card_listing(title_normalized):
        logger.debug("Listing rejected by non-card filter: %s", raw_title)
        return None
    
    canonical_name = card_row["card_name"]
    canonical_card_number = card_row["card_number"]
    canonical_set = card_row["set_name"]

    name_match = card_name_match(title_normalized, canonical_name)
    number_match = card_number_match(raw_title, canonical_card_number)

    normalized_set = normalize_set_name(canonical_set)
    set_match = bool(normalized_set) and normalized_set in title_normalized

    title_match_confidence = compute_title_match_confidence(
        name_match=name_match,
        number_match=number_match,
        set_match=set_match
    )

    psa_grade = extract_psa_grade(title_normalized)
    is_graded = psa_grade is not None

    price_info = item_summary.get("price", {})
    price_value = None
    if isinstance(price_info, dict) and price_info.get("value") is not None:
        try:
            price_value = float(price_info["value"])
        except (TypeError, ValueError):
            price_value = None

    return {
        "listing_id": item_summary.get("itemId"),
        "card_id": card_row["card_id"],
        "price_date": price_date,
        "ingestion_date": ingestion_date,

        "title": raw_title,
        "title_normalized": title_normalized,

        "image_url": item_summary.get("image", {}).get("imageUrl"),
        "thumbnail_url": (
            item_summary.get("thumbnailImages", [{}])[0].get("imageUrl")
            if item_summary.get("thumbnailImages") else None
        ),

        "price_value": price_value,
        "currency": price_info.get("currency"),

        "condition": item_summary.get("condition"),
        "condition_id": item_summary.get("conditionId"),
        
        "is_graded": is_graded,
        "grade_value": psa_grade,
        "parsed_grade": f"PSA {psa_grade}" if psa_grade is not None else None,

        "listing_url": item_summary.get("itemWebUrl"),

        "card_number_match": number_match is True,
        "set_match": set_match,
        "title_match_confidence": title_match_confidence
    }

def write_staging_parquet_to_s3(df: pd.DataFrame, output_s3_uri: str) -> None:
    """
    Write a single parquet file to S3.
    """
    table = pa.Table.from_pandas(df, preserve_index=False)
    output_file = f"{output_s3_uri.rstrip('/')}/part-000.parquet"

    with fsspec.open(output_file, "wb") as f:
        pq.write_table(table, f)


# -------------------------------------------------
# Main
# -------------------------------------------------

def parse_args():
    parser = build_parser("Transform raw eBay Browse API JSON into staging ebay_listings.")
    return parser.parse_args()

def main(run_ctx: RunContext) -> None:
    paths = S3Paths(bucket=run_ctx.bucket)

    raw_partition = paths.raw_ebay_listings_partition(
        price_date=run_ctx.price_date,
        ingestion_date=run_ctx.ingestion_date,
    )
    output_partition = paths.staging_ebay_listings_partition(
        price_date=run_ctx.price_date,
        ingestion_date=run_ctx.ingestion_date,
    )

    logger.info(
        "Starting eBay staging transform | bucket=%s | price_date=%s | ingestion_date=%s | run_id=%s",
        run_ctx.bucket,
        run_ctx.price_date_str,
        run_ctx.ingestion_date_str,
        run_ctx.run_id,
    )

    if not partition_exists(raw_partition):
        raise FileNotFoundError(f"Raw eBay partition not found: {raw_partition}")

    if partition_exists(output_partition):
        if run_ctx.force:
            logger.warning("Existing staging partition found and --force supplied. Deleting: %s", output_partition)
            deleted_count = delete_s3_prefix(output_partition)
            logger.info("Deleted %d existing objects from %s", deleted_count, output_partition)
        else:
            raise FileExistsError(
                f"Staging eBay partition already exists: {output_partition}. "
                f"Re-run with --force to replace it."
            )

    card_master_df = load_card_master(paths)

    required_cols = {"card_id", "card_name", "card_number", "set_name"}
    missing_cols = required_cols - set(card_master_df.columns)
    if missing_cols:
        raise ValueError(f"card_master missing required columns: {missing_cols}")

    card_master_lookup = (
        card_master_df[list(required_cols)]
        .drop_duplicates(subset=["card_id"])
        .set_index("card_id")
        .to_dict("index")
    )

    json_files = list_s3_json_files(raw_partition)
    if not json_files:
        raise FileNotFoundError(f"No raw JSON files found under {raw_partition}")

    logger.info("Found %d raw JSON files under %s", len(json_files), raw_partition)

    rows: List[Dict[str, Any]] = []
    rejected_count = 0
    skipped_missing_card_master = 0
    empty_payload_count = 0

    for json_file in json_files:
        card_id = json_file.rsplit("/", 1)[-1].replace(".json", "")

        card_row = card_master_lookup.get(card_id)
        if not card_row:
            skipped_missing_card_master += 1
            logger.warning("Skipping raw file with missing card_master row | card_id=%s | file=%s", card_id, json_file)
            continue

        card_row = dict(card_row)
        card_row["card_id"] = card_id

        with fsspec.open(json_file, "r") as f:
            raw_json = json.load(f)

        if not isinstance(raw_json, dict):
            logger.warning("Skipping malformed raw JSON (expected dict) | file=%s", json_file)
            continue

        items = raw_json.get("itemSummaries", [])
        if not items:
            empty_payload_count += 1
            continue

        for item_summary in items:
            row = transform_listing(
                item_summary=item_summary,
                card_row=card_row,
                price_date=run_ctx.price_date_str,
                ingestion_date=run_ctx.ingestion_date_str,
            )
            if row is None:
                rejected_count += 1
                continue

            rows.append(row)

    if not rows:
        logger.warning(
            "No listings processed | skipped_missing_card_master=%d | rejected_non_card=%d | empty_payload_count=%d",
            skipped_missing_card_master,
            rejected_count,
            empty_payload_count,
        )
        return

    df = pd.DataFrame(rows)

    pre_dedupe_count = len(df)

    # Deduplicate overlapping listings caused by pagination drift
    df = df.drop_duplicates(subset=["listing_id"], keep="first").reset_index(drop=True)

    deduped_count = pre_dedupe_count - len(df)

    logger.info("Accepted rows before dedupe: %d", pre_dedupe_count)
    logger.info("Duplicate listing_ids removed: %d", deduped_count)
    logger.info("Final accepted rows after dedupe: %d", len(df))
    logger.info("Rejected non-card rows: %d", rejected_count)
    logger.info("Skipped missing card_master rows: %d", skipped_missing_card_master)
    logger.info("Empty payload files: %d", empty_payload_count)

    if run_ctx.dry_run:
        logger.info("DRY RUN | would write %d rows to %s", len(df), output_partition)
        return

    write_staging_parquet_to_s3(df, output_partition)
    logger.info("Wrote %d rows to %s", len(df), output_partition)


if __name__ == "__main__":
    args = parse_args()
    run_ctx = RunContext.from_args(args)
    main(run_ctx)