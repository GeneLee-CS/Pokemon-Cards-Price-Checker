from contextlib import asynccontextmanager

from typing import Literal
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

from src.services.search_service import search_cards
from src.services.card_service import fetch_card_detail
from src.services.card_listings_service import fetch_card_listings
from src.api.schemas.card_detail import CardDetailResponse
from src.analytics.duckdb.duckdb_client import get_connection


@asynccontextmanager
async def lifespan(app: FastAPI):
    con = get_connection()
    app.state.duckdb_con = con
    try:
        yield
    finally:
        try:
            con.close()
        except Exception:
            pass

app = FastAPI(title="Pokemon TCG API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _get_con(request: Request):
    return request.app.state.duckdb_con

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/search")
def search_cards_endpoint(
    request: Request,
    query: str = Query(..., min_length=2),
    limit: int = Query(20, ge=1, le=50)
):
    return search_cards(_get_con(request), query, limit)


@app.get("/cards/{card_id}", response_model=CardDetailResponse)
def card_detail(request: Request, card_id: str):
    result = fetch_card_detail(_get_con(request), card_id)

    if result is None:
        raise HTTPException(status_code=404, detail = "Card not found")
    
    return result

@app.get("/cards/{card_id}/listings")
def card_listings(
    request: Request,
    card_id: str,
    sort: Literal["price_asc", "price_desc"] = "price_asc",
    limit: int = Query(20, ge=1, le=200),
):
    result = fetch_card_listings(_get_con(request), card_id, sort, limit)

    if result is None:
        raise HTTPException(status_code=404, detail="Card not found")
    
    return result