"""
eBay main ingestion 

Purpose:
- Fetch active eBay listings for top Pokemon cards from eBay's Browse API
- Store raw JSON to s3 (partitioned by ingestion_date + card_id)
- Raw JSON preserves original eBay structure

Example:
poetry run python -m src.ingestion.ebay.ebay_ingest \
  --bucket pokemon-tcg-data-lake \
  --top-n-cards 200

"""


from __future__ import annotations

import json
import logging
import random
import time
from typing import Any, Dict, List, Optional

import boto3
import fsspec
import pandas as pd

from src.ingestion.ebay.ebay_auth import EbayAuthClient
from src.ingestion.ebay.ebay_client import EbayClient
from src.ingestion.ebay.ebay_search import EbaySearchConfig, generate_search_requests

from src.utils.run_context import RunContext, build_parser, parse_iso_date
from src.utils.s3_paths import S3Paths
from src.utils.s3_partitions import (
    get_latest_partition_value,
    partition_exists,
    split_s3_uri,
)

# ======================
# Config
# ======================

logger = logging.getLogger(__name__)

class IngestConfig:
    def __init__(
        self,
        *,
        top_n_cards: int = 300,
        page_size: int = 200,
        max_results_per_card: int = 200,
        max_pages: int = 10,
        retry_attempts: int = 5,
        retry_base_seconds: float = 0.75,
    ) -> None:
        self.top_n_cards = top_n_cards
        self.page_size = page_size
        self.max_results_per_card = max_results_per_card
        self.max_pages = max_pages
        self.retry_attempts = retry_attempts
        self.retry_base_seconds = retry_base_seconds

    @property
    def search_config(self) -> EbaySearchConfig:
        return EbaySearchConfig(
            limit=self.page_size,
            max_pages=self.max_pages,
        )


# ======================
# Helpers
# ======================

def resolve_price_date(args, paths: S3Paths) -> None:
    """
    Mutates args.price_date if the user did not provide one.

    Default behavior for eBay ingestion:
    - align to the latest available weekly_top_tcg_cards price_date in S3
    """
    if args.price_date:
        return

    latest_price_date = get_latest_partition_value(
        root_s3_uri=paths.processed_weekly_top_tcg_cards_root(),
        partition_key="price_date",
    )

    if latest_price_date is None:
        raise FileNotFoundError(
            "No weekly_top_tcg_cards price_date partitions found in S3."
        )

    args.price_date = latest_price_date

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

def load_top_cards(paths: S3Paths, *, price_date, top_n: int) -> pd.DataFrame:
    path = paths.processed_weekly_top_tcg_cards_partition(price_date=price_date)
    logger.info("Loading weekly_top_tcg_cards from %s", path)
    df = pd.read_parquet(path)
    return df.head(top_n).copy()


def load_card_master(paths: S3Paths) -> pd.DataFrame:
    path = paths.processed_card_master_root()
    logger.info("Loading card_master from %s", path)
    return pd.read_parquet(path)

def ensure_search_metadata(top_cards_df: pd.DataFrame, card_master_df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the dataframe has the columns needed to build eBay search queries.

    Required columns:
    - card_id
    - card_name
    - set_name
    - number
    - set_printedTotal

    Strategy:
    - If some are already present on top_cards_df, keep them
    - Merge only the missing columns from card_master
    """
    required_cols = ["card_id", "card_name", "set_name", "number", "set_printedTotal"]

    missing_cols = [c for c in required_cols if c not in top_cards_df.columns]
    if not missing_cols:
        return top_cards_df.copy()

    card_master_subset = card_master_df[required_cols].drop_duplicates(subset=["card_id"])

    merged = top_cards_df.merge(
        card_master_subset,
        on="card_id",
        how="left",
        validate="many_to_one",
        suffixes=("", "_card_master"),
    )

    # In case overlapping columns somehow exist, prefer original non-null values
    for col in required_cols:
        alt_col = f"{col}_card_master"
        if alt_col in merged.columns:
            if col in merged.columns:
                merged[col] = merged[col].combine_first(merged[alt_col])
            else:
                merged[col] = merged[alt_col]
            merged = merged.drop(columns=[alt_col])

    return merged

def build_card_number(row: pd.Series) -> Optional[str]:
    number = row.get("number")
    printed_total = row.get("set_printedTotal")

    if pd.isna(number):
        return None

    if pd.notna(printed_total):
        try:
            return f"{number}/{int(printed_total)}"
        except Exception:
            return str(number)

    return str(number)

def search_items_with_retry(
    *,
    client: EbayClient,
    query: str,
    limit: int,
    offset: int,
    category_ids: str,
    retry_attempts: int,
    retry_base_seconds: float,
) -> Dict[str, Any]:
    """
    Execute a single Browse API search with retry/backoff.
    """
    last_err: Optional[Exception] = None

    for attempt in range(1, retry_attempts + 1):
        try:
            return client.search_items(
                query=query,
                limit=limit,
                offset=offset,
                category_ids=category_ids,
            )
        except Exception as err:
            last_err = err
            if attempt == retry_attempts:
                break

            sleep_seconds = retry_base_seconds * (2 ** (attempt - 1)) + random.uniform(0, 0.35)
            logger.warning(
                "eBay request failed (attempt %d/%d) | query=%s | offset=%s | err=%s | retrying in %.2fs",
                attempt,
                retry_attempts,
                query,
                offset,
                err,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(
        f"eBay request failed after {retry_attempts} attempts | query={query} | offset={offset} | err={last_err}"
    )

def fetch_all_items_for_card(
    *,
    client: EbayClient,
    card_name: str,
    set_name: Optional[str],
    card_number: Optional[str],
    config: IngestConfig,
) -> Dict[str, Any]:
    """
    Fetch and merge multiple Browse API pages for a single card.

    Returns:
    - The last payload received, with itemSummaries replaced by the accumulated items.
    """
    search_requests = generate_search_requests(
        card_name=card_name,
        set_name=set_name,
        card_number=card_number,
        config=config.search_config,
    )

    all_items: List[Dict[str, Any]] = []
    merged_payload: Dict[str, Any] = {}

    for params in search_requests:
        if len(all_items) >= config.max_results_per_card:
            break


        if config.page_size >= config.max_results_per_card and int(params["offset"]) > 0:
            break

        limit = min(int(params["limit"]), config.max_results_per_card - len(all_items))

        payload = search_items_with_retry(
            client=client,
            query=params["q"],
            limit=limit,
            offset=int(params["offset"]),
            category_ids=str(params["category_ids"]),
            retry_attempts=config.retry_attempts,
            retry_base_seconds=config.retry_base_seconds,
        )

        if not isinstance(payload, dict):
            break

        items = payload.get("itemSummaries", []) or []
        if not items:
            break

        all_items.extend(items)
        merged_payload = payload

        if len(items) < limit:
            break

    merged_payload["itemSummaries"] = all_items[: config.max_results_per_card]
    return merged_payload

def write_raw_json(
    *,
    paths: S3Paths,
    price_date,
    ingestion_date,
    card_id: str,
    payload: Dict[str, Any],
) -> str:
    partition_path = paths.raw_ebay_listings_partition(
        price_date=price_date,
        ingestion_date=ingestion_date,
    )
    output_path = f"{partition_path}/{card_id}.json"

    with fsspec.open(output_path, "w") as f:
        json.dump(payload, f, indent=2)

    return output_path

def summarize_listings(items: List[Dict[str, Any]]) -> str:
    prices: List[float] = []

    for item in items:
        try:
            prices.append(float(item["price"]["value"]))
        except Exception:
            continue

    if not prices:
        return "Listings: 0 | no valid prices"

    return f"Listings: {len(prices)} | min: ${min(prices):.2f} | max: ${max(prices):.2f}"

def parse_args():
    parser = build_parser("Fetch active eBay listings for top Pokémon cards.")
    parser.add_argument(
        "--top-n-cards",
        type=int,
        default=400,
        help="Number of top cards from weekly_top_tcg_cards to query on eBay. Default: 300.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=200,
        help="Browse API page size per request. Default: 50.",
    )
    parser.add_argument(
        "--max-results-per-card",
        type=int,
        default=200,
        help="Maximum number of eBay listings to collect per card. Default: 200.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Maximum number of paginated eBay requests per card. Default: 10.",
    )
    parser.add_argument(
        "--retry-attempts",
        type=int,
        default=5,
        help="Number of retry attempts per eBay request. Default: 5.",
    )
    parser.add_argument(
        "--retry-base-seconds",
        type=float,
        default=0.75,
        help="Base seconds for exponential backoff. Default: 0.75.",
    )
    return parser.parse_args()

# ======================
# Main
# ======================


def main() -> None:
    args = parse_args()

    # Need S3Paths first so we can resolve latest weekly_top_tcg_cards price_date
    pre_paths = S3Paths(bucket=args.bucket)
    resolve_price_date(args, pre_paths)

    run_ctx = RunContext.from_args(args)
    paths = S3Paths(bucket=run_ctx.bucket)

    cfg = IngestConfig(
        top_n_cards=args.top_n_cards,
        page_size=args.page_size,
        max_results_per_card=args.max_results_per_card,
        max_pages=args.max_pages,
        retry_attempts=args.retry_attempts,
        retry_base_seconds=args.retry_base_seconds,
    )

    logger.info(
        "Starting eBay ingestion | bucket=%s | price_date=%s | ingestion_date=%s | run_id=%s",
        run_ctx.bucket,
        run_ctx.price_date_str,
        run_ctx.ingestion_date_str,
        run_ctx.run_id,
    )

    raw_partition_path = paths.raw_ebay_listings_partition(
        price_date=run_ctx.price_date,
        ingestion_date=run_ctx.ingestion_date,
    )

    if partition_exists(raw_partition_path):
        if run_ctx.force:
            logger.warning("Existing raw partition found and --force supplied. Deleting: %s", raw_partition_path)
            deleted_count = delete_s3_prefix(raw_partition_path)
            logger.info("Deleted %d existing objects from %s", deleted_count, raw_partition_path)
        else:
            raise FileExistsError(
                f"Raw eBay partition already exists: {raw_partition_path}. "
                f"Re-run with --force to replace it."
            )

    top_cards_df = load_top_cards(
        paths,
        price_date=run_ctx.price_date,
        top_n=cfg.top_n_cards,
    )

    card_master_df = load_card_master(paths)
    enriched_df = ensure_search_metadata(top_cards_df, card_master_df)

    missing_required = enriched_df[
        enriched_df[["card_name", "set_name", "number"]].isna().any(axis=1)
    ]
    if not missing_required.empty:
        logger.warning(
            "Some rows are missing search metadata; they may fail or produce weak queries. Missing rows=%d",
            len(missing_required),
        )

    auth = EbayAuthClient()
    client = EbayClient(auth)

    passed = 0
    failed = 0

    for _, row in enriched_df.iterrows():
        card_id = row["card_id"]
        card_name = row.get("card_name")
        set_name = row.get("set_name")
        card_number = build_card_number(row)

        logger.info(
            "Searching eBay | card_id=%s | card_name=%s | set_name=%s | card_number=%s",
            card_id,
            card_name,
            set_name,
            card_number,
        )

        try:
            payload = fetch_all_items_for_card(
                client=client,
                card_name=str(card_name),
                set_name=None if pd.isna(set_name) else str(set_name),
                card_number=card_number,
                config=cfg,
            )

            items = payload.get("itemSummaries", []) or []

            if run_ctx.dry_run:
                logger.info(
                    "DRY RUN | would write card_id=%s to %s | %s",
                    card_id,
                    raw_partition_path,
                    summarize_listings(items),
                )
            else:
                output_path = write_raw_json(
                    paths=paths,
                    price_date=run_ctx.price_date,
                    ingestion_date=run_ctx.ingestion_date,
                    card_id=card_id,
                    payload=payload,
                )
                logger.info(
                    "Wrote raw JSON | card_id=%s | path=%s | %s",
                    card_id,
                    output_path,
                    summarize_listings(items),
                )

            passed += 1

        except Exception as e:
            failed += 1
            logger.exception("Failed | card_id=%s | err=%s", card_id, e)

    logger.info(
        "eBay ingestion complete | passed=%d | failed=%d | bucket=%s | price_date=%s | ingestion_date=%s",
        passed,
        failed,
        run_ctx.bucket,
        run_ctx.price_date_str,
        run_ctx.ingestion_date_str,
    )


if __name__ == "__main__":
    main()