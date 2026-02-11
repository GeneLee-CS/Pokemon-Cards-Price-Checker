from src.analytics.duckdb.card_detail import get_card_detail

def fetch_card_detail(card_id: str) -> dict | None:
    return get_card_detail(card_id)