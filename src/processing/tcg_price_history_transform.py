"""
TCG Price History Transform Script

Purpose:
- Transforms staging TCG card price data into an append-only fact table
- Fact table can be used to track historical TCG market prices for each card_price_variant_id.
- *NO OVERWRITES*
- Write outputs to S3 / local processed layer in Parquet format
"""

from pathlib import Path
from datetime import datetime
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]

STAGING_PRICES_PATH = PROJECT_ROOT / "data" / "staging" / "pokemon_tcg" / "card_prices"
CARD_VARIANT