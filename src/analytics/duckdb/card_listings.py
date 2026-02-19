
from typing import Literal
import duckdb


def get_card_listings(
    con: duckdb.DuckDBPyConnection,
    card_id: str,
    sort: Literal["price_asc", "price_desc"] = "price_asc",
    limit: int = 20
):

    order_clause = "ASC" if sort == "price_asc" else "DESC"

    query = f"""
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
        """

    rows = con.execute(query, [card_id, limit]).fetchall()
    columns = [c[0] for c in con.description]

    listings = [dict(zip(columns, row)) for row in rows]

    return listings