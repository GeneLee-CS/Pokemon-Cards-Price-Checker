"""
eBya Browse API client

Purpose:
- Provides a thin, authenticated HTTP client for interacting with the eBay Browse API.
- Executes search queries for active eBay listings.
- Attaches OAuth tokens to the API requests via ebay_auth

Notes:
- Returns raw JSON responses for downstream ingestion
"""


import requests
from typing import Dict, Any

from src.ingestion.ebay.ebay_auth import EbayAuthClient



class EbayClientError(Exception):
    pass


class EbayClient:
    """
    The HTTP client for eBay Browse API.
    Responsible only for making authenticated requests.
    """

    BROWSE_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

    def __init__(self, auth:EbayAuthClient):
        self.auth = auth

    # -------------------------
    # Headers
    # -------------------------
    def _get_headers(self) -> Dict[str, str]:
        """
        Builds requests headers using an OAuth access token.
        """
        access_token = self.auth.get_access_token()
        return{
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }


    # -------------------------
    # Browse API
    # -------------------------
    def search_items(
            self,
            query: str,
            limit: int=50,
            offset: int=0,
            category_ids: str = "183454" #Pokemon Trading Card Game Category ID
    ) -> Dict[str, Any]:
        """
        Searches active eBay listings using the Browse API.

        query: The text search query (e.g. card name + set name)
        limit: Number of resutls to return (max 200 per eBay spec)
        offset: Pagination offset
        category_ids: eBay category filter (default set to Pokemon TCG)

        Output:
        -Raw JSON response from the eBay Browse API
        """

        params = {
            "q": query,
            "limit": limit,
            "offset": offset,
            "category_ids": category_ids
        }

        response = requests.get(
            self.BROWSE_SEARCH_URL,
            headers=self._get_headers(),
            params=params,
            timeout=30
        )

        if response.status_code != 200:
            raise EbayClientError(
                f"Browse API search failed "
                f"(status={response.status_code}, body={response.text})"
            )
        
        return response.json()