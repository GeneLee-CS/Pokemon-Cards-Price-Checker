from typing import Any
import duckdb

from src.analytics.duckdb.card_detail import get_card_detail

def fetch_card_detail(con: duckdb.DuckDBPyConnection, card_id: str) -> dict | None:
    return get_card_detail(con, card_id)