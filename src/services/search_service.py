from typing import Any
import duckdb

from src.analytics.duckdb.search import search_cards_by_name

def search_cards(con: duckdb.DuckDBPyConnection, query: str, limit: int) -> list[dict]:
    return search_cards_by_name(con, query, limit)