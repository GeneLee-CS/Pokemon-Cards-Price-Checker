from fastapi import APIRouter, HTTPException
from src.analytics.duckdb.duckdb_client import get_connection

router = APIRouter()


@router.get("/cards/{card_id}")
def get_card_detail(card_id: str):
    con = get_connection()

    # Card metadata
    card = con.execute(
        """
        SELECT
            card_id,
            card_name,
            set_name,
            card_number,
            rarity,
            release_date,
            image_small_url,
            image_large_url
        FROM card_master
        WHERE card_id = ?;
        """,
        [card_id]
    ).fetchone()

    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    
    card_dict = {
        "card_id": card[0],
        "card_name": card[1],
        "set_name": card[2],
        "card_number": card[3],
        "rarity": card[4],
        "release_date": card[5],
        "image_small_url": card[6],
        "image_large_url": card[7]
    }


    # Latest TCG price
    latest_price = con.execute(
        """
        SELECT
            price_date,
            market_price
        FROM tcg_price_history
        WHERE card_id = ?
        ORDER BY price_date DESC
        LIMIT 1
        """,
        [card_id]
    ).fetchone()

    latest_price_dict = None
    if latest_price:
        latest_price_dict = {
            "price_date": latest_price[0],
            "market_price": float(latest_price[1])
        }

    # Price history
    price_history_rows = con.execute(
        """
        SELECT
            price_date,
            market_price
        FROM tcg_price_history
        WHERE card_id = ?
        ORDER BY price_date ASC
        """,
        [card_id],
    ).fetchall()

    price_history = [
        {
            "price_date": row[0],
            "market_price": float(row[1])
        }
        for row in price_history_rows
    ]

    # ebay market summary
    ebay_market = con.execute(
        """
        SELECT
            price_date,
            listing_count,
            min_price,
            median_price,
            max_price,
            graded_listing_count,
            ungraded_listing_count
        FROM ebay_card_market_summary
        WHERE card_id = ?
        ORDER BY price_date DESC
        LIMIT 1
        """,
        [card_id]
    ).fetchone()

    ebay_market_dict = None
    if ebay_market:
        ebay_market_dict = {
            "price_date": ebay_market[0],
            "listing_count": ebay_market[1],
            "min_price": float(ebay_market[2]),
            "median_price": float(ebay_market[3]),
            "max_price": float(ebay_market[4]),
            "graded_listing_count": ebay_market[5],
            "ungraded_listing_count": ebay_market[6]
        }
    
    return {
        "card": card_dict,
        "latest_price": latest_price_dict,
        "price_history": price_history,
        "ebay_market": ebay_market_dict
    }

