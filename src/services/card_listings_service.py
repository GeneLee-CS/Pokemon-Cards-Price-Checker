from typing import Literal

import duckdb
from src.analytics.duckdb.card_listings import get_card_listings

def fetch_card_listings(
        con: duckdb.DuckDBPyConnection,
        card_id: str,
        sort: Literal["price_asc", "price_desc"],
        limit: int
) -> dict:
    
 
    listings = get_card_listings(
        con=con,
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