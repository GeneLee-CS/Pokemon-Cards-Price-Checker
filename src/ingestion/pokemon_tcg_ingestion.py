"""
TCG API ingestion Script

Purpose:
- Fetches the full Pokémon TCG card catalog from the Pokémon TCG API
- Write outputs to S3 in JSON format
- Writes a failed-pages manifest to S3 meta zone (partitioned by ingestion_date) if any pages fail

Design:
- Full ingestion weekly for correctness/simplicity
"""


from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import logging
import sys

import boto3
import requests

from src.utils.run_context import RunContext

# -----------------------------
# Config
# -----------------------------

TCG_BASE_URL = "https://api.pokemontcg.io/v2/cards"

DEFAULT_PAGE_SIZE = 200  # API supports up to 250
DEFAULT_MAX_RETRIES = 5

RETRYABLE_STATUS_CODES = {429, 404, 500, 502, 503, 504}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
    force=True,
)

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class IngestionConfig:
    bucket: str
    raw_prefix: str 
    meta_prefix: str 
    page_size: int = DEFAULT_PAGE_SIZE
    max_retries: int = DEFAULT_MAX_RETRIES
    request_timeout: Tuple[int, int] = (30, 180) 
    polite_sleep_seconds: float = 3


def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise ValueError(f"Missing required environment variable: {name}")
    return val

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _s3_client():
    return boto3.client("s3")

def _s3_head_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

def _s3_put_json(s3, bucket: str, key: str, payload: Any) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8"
    )

def _fetch_page(
    session: requests.Session,
    api_key: str,
    page: int,
    page_size: int,
    timeout: Tuple[int, int],
    max_retries: int,
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[int], Optional[int]]:
    
    headers = {"X-Api-Key": api_key}

    for attempt in range(max_retries):
        try:
            resp = session.get(
                TCG_BASE_URL,
                params={"page": page, "pageSize": page_size},
                headers=headers,
                timeout=timeout,
            )
            status = resp.status_code

            if status in RETRYABLE_STATUS_CODES:
                wait = 2 * (attempt + 1)
                logger.warning(f"{status} on page {page}. Retrying in {wait}s (attempt {attempt+1}/{max_retries})...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            body = resp.json()

            cards = body.get("data", [])
            total = body.get("totalCount")
            return cards, total, status

        except requests.exceptions.RequestException as e:
            wait = 2 * (attempt + 1)
            logger.warning(f"Network/HTTP error on page {page}: {e}. Retrying in {wait}s (attempt {attempt+1}/{max_retries})...")
            time.sleep(wait)

    # Failed after retries
    return None, None, None


def run_full_ingestion(cfg: IngestionConfig) -> None:
    run_date = datetime.now(timezone.utc).date().isoformat()

    ctx = RunContext(
        bucket=cfg.bucket,
        price_date=run_date,
        ingestion_date=run_date,
        run_id=f"pokemon_tcg_ingestion_{run_date}",
    )

    ingestion_date = ctx.ingestion_date  # frozen per run (string YYYY-MM-DD)

    api_key = _require_env("POKEMON_TCG_API_KEY")


    raw_key = f"{cfg.raw_prefix}/ingestion_date={ingestion_date}/cards.json"
    failed_key = f"{cfg.meta_prefix}/ingestion_date={ingestion_date}/failed_pages.json"

    s3 = _s3_client()

    if _s3_head_exists(s3, cfg.bucket, raw_key):
        logger.info(f"Raw output already exists. Skipping: s3://{cfg.bucket}/{raw_key}")
        return

    failed_pages: List[int] = []
    all_cards: List[Dict[str, Any]] = []

    page = 1
    total_count: Optional[int] = None

    with requests.Session() as session:
        while True:

            cards, total, status = _fetch_page(
                session=session,
                api_key=api_key,
                page=page,
                page_size=cfg.page_size,
                timeout=cfg.request_timeout,
                max_retries=cfg.max_retries,
            )

            if cards is None:
                logger.error(f"Failed to fetch page {page} after {cfg.max_retries} retries. Recording failed page and continuing.")
                failed_pages.append(page)
                page += 1
                time.sleep(1.0)  # small cooldown
                continue

            # termination condition
            if not cards:
                logger.info("No more cards. Ingestion complete.")
                break

            all_cards.extend(cards)
            total_count = total_count or total

            if total_count:
                logger.info(f"Page {page} fetched. Progress ({len(all_cards)}/{total_count})")
            else:
                logger.info(f"Page {page} fetched. Progress ({len(all_cards)}/unknown)")

            page += 1
            time.sleep(cfg.polite_sleep_seconds)

    # Write raw JSON to S3
    payload = {
        "source": "pokemon_tcg_api",
        "run_id": f"pokemon_tcg_ingestion_{ingestion_date}",
        "ingestion_date": ingestion_date,
        "extracted_at_utc": _utc_now_iso(),
        "page_size": cfg.page_size,
        "total_count": total_count,
        "records": all_cards,
    }

    logger.info(f"Writing raw payload to s3://{cfg.bucket}/{raw_key} (cards={len(all_cards)})")
    _s3_put_json(s3, cfg.bucket, raw_key, payload)

    # Write failed pages manifest if needed
    if failed_pages:
        failed_payload = {
            "source": "pokemon_tcg_api",
            "run_id": f"pokemon_tcg_ingestion_{ingestion_date}",
            "ingestion_date": ingestion_date,
            "failed_pages": sorted(set(failed_pages)),
            "extracted_at_utc": _utc_now_iso(),
            "note": "Pages listed here failed after retries; raw output may be incomplete.",
        }
        logger.warning(f"Writing failed pages manifest to s3://{cfg.bucket}/{failed_key}")
        _s3_put_json(s3, cfg.bucket, failed_key, failed_payload)

    logger.info("Done.")

def main() -> None:
    bucket = _require_env("S3_BUCKET")

    cfg = IngestionConfig(
        bucket=bucket,
        raw_prefix="raw/pokemon_tcg/cards",
        meta_prefix="meta/pokemon_tcg/failed",
        page_size=int(os.getenv("POKEMON_TCG_PAGE_SIZE", str(DEFAULT_PAGE_SIZE))),
        max_retries=int(os.getenv("POKEMON_TCG_MAX_RETRIES", str(DEFAULT_MAX_RETRIES))),
        polite_sleep_seconds=float(os.getenv("POKEMON_TCG_POLITE_SLEEP_SECONDS", "0.2")),
    )

    run_full_ingestion(cfg)


if __name__ == "__main__":
    main()