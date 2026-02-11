from fastapi import APIRouter, Query
from typing import Literal

from src.analytics.duckdb.duckdb_client import get_connection

router = APIRouter()

@router.get("/cards/{card_id}/listings")
def get_card_listings(
    card_id: str,
    sort: Literal["price_asc", "price_desc"] = "price_asc",
    limit: int = Query(20, ge=1, le=50)
):
    con = get_connection()

    order_clause = "ASC" if sort == "price_asc" else "DESC"

    rows = con.execute(
        f"""
        WITH latest_snapshot AS (
            SELECT MAX(ingestion_date) AS ingestion_date
            FROM ebay_market_snapshot
        )
            SELECT
                listing_id,
                title,
                price_value AS price,
                currency,
                listing_url,
                ingestion_date
            FROM ebay_market_snapshot
            WHERE card_id = ?
            AND ingestion_date = (SELECT ingestion_date FROM latest_snapshot)
            ORDER BY price {order_clause}
            LIMIT ?;
        """,
        [card_id, limit]
    ).fetchall()

    columns = [c[0] for c in con.description]

    if not rows:
        return {
            "card_id": card_id,
            "ingestion_date": None,
            "listings": []
        }
    
    listings = [dict(zip(columns, row)) for row in rows]

    return listings