"""
Pokémon TCG API backfill ingestion

Purpose:
- Reads failed_pages manifest from S3
- Re-fetches only failed Pokémon TCG API catalog pages
- Merges recovered cards into the canonical raw cards.json payload
- Safely promotes repaired payload back to the canonical S3 key
- Updates or removes failed_pages manifest depending on outcome

Design:
- Safe overwrite pattern: temp write -> validate -> backup -> promote
- Dedupe by card id
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
import requests

from src.utils.run_context import RunContext

TCG_BASE_URL = "https://api.pokemontcg.io/v2/cards"
DEFAULT_PAGE_SIZE = 200
DEFAULT_MAX_RETRIES = 5
RETRYABLE_STATUS_CODES = {429, 404, 500, 502, 503, 504}


@dataclass(frozen=True)
class BackfillConfig:
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
    
def _s3_get_json(s3, bucket: str, key: str) -> Any:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))

def _s3_put_json(s3, bucket: str, key: str, payload: Any) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )

def _s3_copy_object(s3, bucket: str, source_key: str, dest_key: str) -> None:
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=dest_key,
    )


def _s3_delete_object(s3, bucket: str, key: str) -> None:
    s3.delete_object(Bucket=bucket, Key=key)

def _fetch_page(
    session: requests.Session,
    api_key: str,
    page: int,
    page_size: int,
    timeout: Tuple[int, int],
    max_retries: int,
) -> Optional[List[Dict[str, Any]]]:
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
                print(f"[WARN] {status} on page {page}. Retrying in {wait}s...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            body = resp.json()
            return body.get("data", [])

        except requests.exceptions.RequestException as e:
            wait = 2 * (attempt + 1)
            print(f"[WARN] Network/HTTP error on page {page}: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    return None

def _dedupe_cards_by_id(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[str, Dict[str, Any]] = {}

    for card in cards:
        card_id = card.get("id")
        if not card_id:
            continue
        deduped[card_id] = card

    return list(deduped.values())

def _validate_payload(payload: Dict[str, Any]) -> None:
    required_keys = {"source", "run_id", "ingestion_date", "records"}
    missing = required_keys - set(payload.keys())
    if missing:
        raise ValueError(f"Payload missing required keys: {sorted(missing)}")

    if not isinstance(payload["records"], list):
        raise ValueError("Payload 'records' must be a list")

    if len(payload["records"]) == 0:
        raise ValueError("Payload 'records' is empty after backfill")
    
def run_backfill(cfg: BackfillConfig) -> None:
    api_key = _require_env("POKEMON_TCG_API_KEY")
    run_date = datetime.now(timezone.utc).date().isoformat()

    ctx = RunContext(
        bucket=cfg.bucket,
        price_date=run_date,
        ingestion_date=run_date,
        run_id=f"pokemon_tcg_backfill_{run_date}",
    )

    ingestion_date = ctx.ingestion_date

    raw_key = f"{cfg.raw_prefix}/ingestion_date={ingestion_date}/cards.json"
    failed_key = f"{cfg.meta_prefix}/ingestion_date={ingestion_date}/failed_pages.json"
    temp_key = f"{cfg.raw_prefix}/ingestion_date={ingestion_date}/cards.backfill_tmp.json"
    backup_key = f"{cfg.raw_prefix}/ingestion_date={ingestion_date}/cards.pre_backfill_backup.json"

    s3 = _s3_client()

    if not _s3_head_exists(s3, cfg.bucket, raw_key):
        raise FileNotFoundError(f"Canonical raw file not found: s3://{cfg.bucket}/{raw_key}")

    if not _s3_head_exists(s3, cfg.bucket, failed_key):
        print(f"[INFO] No failed pages manifest found. Nothing to backfill: s3://{cfg.bucket}/{failed_key}")
        return

    raw_payload = _s3_get_json(s3, cfg.bucket, raw_key)
    failed_payload = _s3_get_json(s3, cfg.bucket, failed_key)

    original_records = raw_payload.get("records", [])
    failed_pages = failed_payload.get("failed_pages", [])

    if not failed_pages:
        print("[INFO] Failed pages manifest exists but contains no failed pages. Nothing to do.")
        return

    print(f"[INFO] Found {len(failed_pages)} failed pages to backfill.")

    recovered_cards: List[Dict[str, Any]] = []
    remaining_failed_pages: List[int] = []

    with requests.Session() as session:
        for idx, page in enumerate(failed_pages, start=1):
            cards = _fetch_page(
                session=session,
                api_key=api_key,
                page=page,
                page_size=cfg.page_size,
                timeout=cfg.request_timeout,
                max_retries=cfg.max_retries,
            )

            if cards is None:
                print(f"[ERROR] Failed to recover page {page}. Keeping it in failed_pages manifest.")
                remaining_failed_pages.append(page)
            else:
                recovered_cards.extend(cards)
                print(f"[INFO] Recovered page {page} ({idx}/{len(failed_pages)})")

            time.sleep(cfg.polite_sleep_seconds)

    merged_records = _dedupe_cards_by_id(original_records + recovered_cards)

    if len(merged_records) < len(original_records):
        raise ValueError(
            f"Merged record count shrank unexpectedly: "
            f"original={len(original_records)}, merged={len(merged_records)}"
        )

    repaired_payload = dict(raw_payload)
    repaired_payload["records"] = merged_records
    repaired_payload["backfill_applied_at_utc"] = _utc_now_iso()
    repaired_payload["backfill_recovered_pages"] = sorted(set(failed_pages) - set(remaining_failed_pages))
    repaired_payload["backfill_remaining_failed_pages"] = sorted(set(remaining_failed_pages))

    _validate_payload(repaired_payload)

    print(f"[INFO] Writing repaired payload to temp key: s3://{cfg.bucket}/{temp_key}")
    _s3_put_json(s3, cfg.bucket, temp_key, repaired_payload)

    if not _s3_head_exists(s3, cfg.bucket, temp_key):
        raise RuntimeError("Temp repaired payload was not written successfully.")

    print(f"[INFO] Backing up original canonical raw file to: s3://{cfg.bucket}/{backup_key}")
    _s3_copy_object(s3, cfg.bucket, raw_key, backup_key)

    print(f"[INFO] Promoting repaired payload to canonical key: s3://{cfg.bucket}/{raw_key}")
    _s3_copy_object(s3, cfg.bucket, temp_key, raw_key)

    if remaining_failed_pages:
        updated_failed_payload = {
            "source": "pokemon_tcg_api_backfill",
            "run_id": raw_payload.get("run_id"),
            "ingestion_date": ingestion_date,
            "failed_pages": sorted(set(remaining_failed_pages)),
            "extracted_at_utc": _utc_now_iso(),
            "note": "Pages listed here still failed after backfill retries.",
        }
        print(f"[WARN] Updating failed pages manifest with remaining failures: {remaining_failed_pages}")
        _s3_put_json(s3, cfg.bucket, failed_key, updated_failed_payload)
    else:
        try:
            print("[INFO] All failed pages recovered successfully. Removing failed_pages manifest.")
            _s3_delete_object(s3, cfg.bucket, failed_key)
        except Exception as e:
            print(f"[WARN] Backfill succeeded, but could not delete failed_pages manifest: {e}")

    print(f"[INFO] Cleaning up temp key: s3://{cfg.bucket}/{temp_key}")

    try:
        _s3_delete_object(s3, cfg.bucket, temp_key)
        print(f"[INFO] Cleaned up temp key: s3://{cfg.bucket}/{temp_key}")
    except Exception as e:
        print(f"[WARN] Could not delete temp key: {e}")

    print(
        f"[INFO] Backfill complete. "
        f"original_records={len(original_records)}, "
        f"recovered_cards={len(recovered_cards)}, "
        f"final_records={len(merged_records)}"
    )

def main() -> None:
    cfg = BackfillConfig(
        bucket=_require_env("S3_BUCKET"),
        raw_prefix="raw/pokemon_tcg/cards",
        meta_prefix="meta/pokemon_tcg/failed",
        page_size=int(os.getenv("POKEMON_TCG_PAGE_SIZE", str(DEFAULT_PAGE_SIZE))),
        max_retries=int(os.getenv("POKEMON_TCG_MAX_RETRIES", str(DEFAULT_MAX_RETRIES))),
        polite_sleep_seconds=float(os.getenv("POKEMON_TCG_POLITE_SLEEP_SECONDS", "0.2")),
    )

    run_backfill(cfg)


if __name__ == "__main__":
    main()
