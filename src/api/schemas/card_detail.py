from datetime import date
from typing import List, Optional
from pydantic import BaseModel, Field


class CardMetadata(BaseModel):
    card_id: str
    card_name: str
    set_name: str
    card_number: str
    rarity: Optional[str]
    release_date: Optional[date]
    image_small_url: Optional[str]
    image_large_url: Optional[str]


class TCGPricePoint(BaseModel):
    price_date: date
    market_price: float


class EbayMarketSummary(BaseModel):
    price_date: date
    listing_count: int
    min_price: Optional[float]
    median_price: Optional[float]
    max_price: Optional[float]
    graded_listing_count: int
    ungraded_listing_count: int


class CardDetailResponse(BaseModel):
    card: CardMetadata

    latest_tcg_price: Optional[TCGPricePoint]
    tcg_price_history: List[TCGPricePoint]

    ebay_market: Optional[EbayMarketSummary]