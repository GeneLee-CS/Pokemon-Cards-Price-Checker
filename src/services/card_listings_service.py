from typing import Literal

from src.analytics.duckdb.duckdb_client import get_connection
from src.analytics.duckdb.card_listings import get_card_listings

def fetch_card_listings(
        card_id: str,
        sort: Literal["price_asc", "price_desc"],
        limit: int
) -> dict:
    
    con = get_connection()

    listings = get_card_listings(
        card_id=card_id,
        sort=sort,
        limit=limit
    )

    if not listings:
        return {
            "card_id": card_id,
            "ingestion_date": None,
            "listings": []
        }
    
    ingestion_date = listings[0].get("ingestion_date")

    return {
        "card_id": card_id,
        "ingestion_date": ingestion_date,
        "listings": listings
    }