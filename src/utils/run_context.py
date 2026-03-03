"""
Run context utilities 

Purpose:
- Standardize per-run metadata across ingestion/transforms:
  - price_date (UTC date, frozen once per run)
  - ingestion_date (UTC date, frozen once per run)
  - run_id (UTC timestamp string)
  - log_level
- Provide a single place to parse common CLI args.
- Avoid dotenv usage here (runtime should supply env vars).

Notes:
- Use this in all scripts so CI + local + cloud runs behave consistently.
- "price_date" and "ingestion_date" default to UTC "today" unless provided.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, timezone
from typing import Optional
import argparse
import logging


def utc_today() -> date:
    return datetime.now(timezone.utc).date()

def parse_iso_date(value: str) -> date:
    """
    Parse YYYY-MM-DD into date. Raises ValueError if invalid.
    """
    return date.fromisoformat(value)

def default_run_id() -> str:
    """
    Stable-ish run identifier for logging and output traceability.
    """
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def configure_logging(log_level: str) -> None:
    """
    Configure root logging once, with consistent formatting.
    """
    level = getattr(logging, log_level.upper(), None)
    if not isinstance(level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

@dataclass(frozen=True)
class RunContext:
    """
    A single object passed through ingestion/transform entrypoints.

    Required:
    - bucket: S3 bucket name

    Dates:
    - price_date: primary partition date for weekly TCG snapshots + weekly analytics
    - ingestion_date: lineage date for raw/staging writes

    Behavior:
    - force: overwrite existing partitions (when applicable)
    - dry_run: log actions without writing
    """

    bucket: str
    price_date: date
    ingestion_date: date
    run_id: str
    log_level: str = "INFO"
    force: bool = False
    dry_run: bool = False

    @property
    def price_date_str(self) -> str:
        return self.price_date.isoformat()

    @property
    def ingestion_date_str(self) -> str:
        return self.ingestion_date.isoformat()

    @staticmethod
    def add_common_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        """
        Add shared CLI arguments to a parser. Scripts can add their own args on top.
        """
        parser.add_argument(
            "--bucket",
            required=True,
            help="S3 bucket name (no s3:// prefix)",
        )
        parser.add_argument(
            "--price-date",
            default=None,
            help="Price date partition in YYYY-MM-DD. Default: today (UTC).",
        )
        parser.add_argument(
            "--ingestion-date",
            default=None,
            help="Ingestion date partition in YYYY-MM-DD. Default: today (UTC).",
        )
        parser.add_argument(
            "--log-level",
            default="INFO",
            help="Logging level: DEBUG, INFO, WARNING, ERROR. Default: INFO.",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Overwrite/replace existing partition outputs where applicable.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Log intended actions without writing outputs.",
        )
        return parser

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "RunContext":
        """
        Build a RunContext from parsed args. Also configures logging.
        """
        price_date = parse_iso_date(args.price_date) if args.price_date else utc_today()
        ingestion_date = (
            parse_iso_date(args.ingestion_date) if args.ingestion_date else utc_today()
        )

        # Configure logging once at startup.
        configure_logging(args.log_level)

        return cls(
            bucket=args.bucket,
            price_date=price_date,
            ingestion_date=ingestion_date,
            run_id=default_run_id(),
            log_level=args.log_level,
            force=bool(getattr(args, "force", False)),
            dry_run=bool(getattr(args, "dry_run", False)),
        )
    

def build_parser(
    description: str,
    *,
    with_common_args: bool = True,
) -> argparse.ArgumentParser:
    """
    Convenience helper to build a script parser with common args included.
    """
    parser = argparse.ArgumentParser(description=description)
    if with_common_args:
        RunContext.add_common_args(parser)
    return parser