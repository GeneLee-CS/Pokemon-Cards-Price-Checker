from src.analytics.duckdb.duckdb_client import get_connection
from src.analytics.duckdb.search import search_cards_by_name

def search_cards(query: str, limit: int) -> list[dict]:
    con = get_connection()
    df = search_cards_by_name(con, query, limit)
    return df.to_dict(orient="records")