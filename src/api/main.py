from fastapi import FastAPI, Query, HTTPException

from src.analytics.duckdb.duckdb_client import get_connection
from src.analytics.duckdb.search import search_cards_by_name
from src.analytics.duckdb.card_detail import get_card_detail

app = FastAPI(title="Pokemon TCG API")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/search")
def search_cards(
    query: str = Query(..., min_length=2),
    limit: int = Query(20, le=50)
):
    con = get_connection()

    df = search_cards_by_name(con, query, limit)

    return df.to_dict(orient='records')


@app.get("/cards/{card_id}")
def card_detail(card_id: str):
    con = get_connection()

    result = get_card_detail(card_id)

    if result is None:
        raise HTTPException(status_code=404, detail = "Card not found")
    
    return result