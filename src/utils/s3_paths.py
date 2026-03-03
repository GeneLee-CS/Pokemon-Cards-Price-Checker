"""
S3 path conventions for the Pokémon TCG data lake.

Purpose:
- Centralize S3 dataset root paths and partition path builders so that all scripts write/read consistently.
- Avoid hardcoding string prefixes in multiple scripts.

Assumptions:
- Data lake zones are organized as:
  - raw/
  - staging/
  - processed/
  - analytics/

Partition conventions (Hive style):
- ingestion_date=YYYY-MM-DD
- price_date=YYYY-MM-DD
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional


def _s3_uri(bucket: str, key_prefix: str) -> str:
    bucket = bucket.replace("s3://", "").strip("/")
    key_prefix = key_prefix.strip("/")
    return f"s3://{bucket}/{key_prefix}" if key_prefix else f"s3://{bucket}"

def hive_partition(key: str, value: str) -> str:
    return f"{key}={value}"

@dataclass(frozen=True)
class S3Paths:
    """
    Canonical S3 dataset paths.

    This class only knows naming/layout rules.
    """

    bucket: str

    # ---- Base zones ----
    @property
    def raw_root(self) -> str:
        return _s3_uri(self.bucket, "raw")

    @property
    def staging_root(self) -> str:
        return _s3_uri(self.bucket, "staging")

    @property
    def processed_root(self) -> str:
        return _s3_uri(self.bucket, "processed")

    @property
    def analytics_root(self) -> str:
        return _s3_uri(self.bucket, "analytics")

    # ---- RAW ----
    # Pokémon TCG raw cards JSON (ingestion_date partition)
    def raw_pokemon_tcg_cards_root(self) -> str:
        return _s3_uri(self.bucket, "raw/pokemon_tcg/cards")

    def raw_pokemon_tcg_cards_partition(self, *, ingestion_date: date) -> str:
        return _s3_uri(
            self.bucket,
            f"raw/pokemon_tcg/cards/{hive_partition('ingestion_date', ingestion_date.isoformat())}",
        )

    # eBay raw listings JSON (price_date + ingestion_date)
    def raw_ebay_listings_root(self) -> str:
        return _s3_uri(self.bucket, "raw/ebay/listings")

    def raw_ebay_listings_partition(self, *, price_date: date, ingestion_date: date) -> str:
        return _s3_uri(
            self.bucket,
            "raw/ebay/listings/"
            f"{hive_partition('price_date', price_date.isoformat())}/"
            f"{hive_partition('ingestion_date', ingestion_date.isoformat())}",
        )
    
    # ---- STAGING ----
    def staging_tcg_cards_root(self) -> str:
        return _s3_uri(self.bucket, "staging/pokemon_tcg/cards")

    def staging_tcg_cards_partition(self, *, ingestion_date: date) -> str:
        return _s3_uri(
            self.bucket,
            f"staging/pokemon_tcg/cards/{hive_partition('ingestion_date', ingestion_date.isoformat())}",
        )

    def staging_tcg_card_prices_root(self) -> str:
        return _s3_uri(self.bucket, "staging/pokemon_tcg/card_prices")

    def staging_tcg_card_prices_partition(self, *, ingestion_date: date) -> str:
        return _s3_uri(
            self.bucket,
            f"staging/pokemon_tcg/card_prices/{hive_partition('ingestion_date', ingestion_date.isoformat())}",
        )

    def staging_ebay_listings_root(self) -> str:
        return _s3_uri(self.bucket, "staging/ebay/listings")

    def staging_ebay_listings_partition(self, *, price_date: date, ingestion_date: date) -> str:
        return _s3_uri(
            self.bucket,
            "staging/ebay/listings/"
            f"{hive_partition('price_date', price_date.isoformat())}/"
            f"{hive_partition('ingestion_date', ingestion_date.isoformat())}",
        )
    
    # ---- PROCESSED ----
    def processed_card_master_root(self) -> str:
        return _s3_uri(self.bucket, "processed/card_master")

    def processed_card_price_variant_master_root(self) -> str:
        return _s3_uri(self.bucket, "processed/card_price_variant_master")

    def processed_tcg_price_history_root(self) -> str:
        return _s3_uri(self.bucket, "processed/tcg_price_history")

    def processed_tcg_price_history_partition(self, *, price_date: date) -> str:
        return _s3_uri(
            self.bucket,
            f"processed/tcg_price_history/{hive_partition('price_date', price_date.isoformat())}",
        )

    def processed_weekly_top_tcg_cards_root(self) -> str:
        return _s3_uri(self.bucket, "processed/analytics/weekly_top_tcg_cards")

    def processed_weekly_top_tcg_cards_partition(self, *, price_date: date) -> str:
        return _s3_uri(
            self.bucket,
            f"processed/analytics/weekly_top_tcg_cards/{hive_partition('price_date', price_date.isoformat())}",
        )
    
    # ---- ANALYTICS ----
    def analytics_ebay_market_snapshot_root(self) -> str:
        return _s3_uri(self.bucket, "analytics/ebay_market_snapshot")

    def analytics_ebay_market_snapshot_partition(self, *, price_date: date, ingestion_date: date) -> str:
        return _s3_uri(
            self.bucket,
            "analytics/ebay_market_snapshot/"
            f"{hive_partition('ingestion_date', ingestion_date.isoformat())}",
        )

