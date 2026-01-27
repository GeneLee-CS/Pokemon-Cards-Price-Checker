"""
eBay listing data models

Purpose:
- Defines structured representations for raw and normalized eBay listings
- Provides clear data contracts between ingestion, normalization, and analytics layers

Notes:
- Models are not responsible for API calls or persistence
- Raw models closely reflect eBay Browse API responses
"""


from dataclasses import dataclass
from typing import Optional


# -------------------------
# Raw Models
# -------------------------

@dataclass
class RawEbayListing:
    """
    Represents a minimally extracted eBay listing from the Browse API response.
    """

    item_id: str
    title: str
    price_value: float
    price_currency: str
    condition: Optional[str]
    listing_url: Optional[str]


# -------------------------
# Normalized Models
# -------------------------

@dataclass
class NormalizedEbayListing:
    """
    Represents a cleaned and standardized eBay listing suitable for downstream analytics and aggregation.
    """


    card_id: str
    title: str

    price_value: float
    price_currency: str

    grading_bucket: str         #e.g. PSA_10, PSA_9, UNGRADED
    condition: Optional[str]

    listing_url: Optional[str]