from dotenv import load_dotenv
load_dotenv()

from typing import Literal
from fastapi import FastAPI, Query, HTTPException

from src.services.search_service import search_cards
from src.services.card_service import fetch_card_detail
from src.services.card_listings_service import fetch_card_listings
from src.api.schemas.card_detail import CardDetailResponse


app = FastAPI(title="Pokemon TCG API")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/search")
def search_cards_endpoint(
    query: str = Query(..., min_length=2),
    limit: int = Query(20, le=50)
):
    return search_cards(query, limit)


@app.get("/cards/{card_id}", response_model=CardDetailResponse)
def card_detail(card_id: str):
    result = fetch_card_detail(card_id)

    if result is None:
        raise HTTPException(status_code=404, detail = "Card not found")
    
    return result

@app.get("/cards/{card_id}/listings")
def card_listings(
    card_id: str,
    sort: Literal["price_asc", "price_desc"] = "price_asc",
    limit: int = Query(20, ge=1, le=50),
):
    return fetch_card_listings(card_id, sort, limit)