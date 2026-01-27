"""
eBay API search + query builder

Purpose:
- Constructs search queries for eBay Browse API requests
- Includes search-related logic (keywords, limites, pagination)

Notes:
- Designed to be used by ebay_ingest.py
- Currency handling is deferred to downstream analytics
"""


from typing import Dict, Any, List, Optional

# eBay category ID for Pokemon TCG cards
POKEMON_TCG_CATEGORY_ID = "183454"


class EbaySearchConfig:
    """
    Configuration object for eBay searches.
    """

    def __init__(
            self,
            limit: int = 50,
            max_pages: int = 1,
            category_id: str = POKEMON_TCG_CATEGORY_ID
    ):
        self.limit = limit
        self.max_pages = max_pages
        self.category_id = category_id



def build_search_query(
        card_name: str,
        set_name: Optional[str] = None,
        card_number: Optional[str] = None
) -> str:
    """
    Builds an eBay search query from card metadata.

    card_name: Name of the Pokemon card (e.g. "Charizard")
    set_name: Name of the set the card is from (e.g. "Base Set")
    card_number: Combination of card number in the set + total in set, often used to distinguish between Pokemon cards (e.g. "4/102")

    Returns: Search query string to use on eBay's Browse API
    """

    parts: List[str] = [card_name]
    
    if set_name:
        parts.append(set_name)

    if card_number:
        parts.append(card_number)

    return " ".join(parts)


def build_search_params(
        query: str,
        limit: int,
        offset: int,
        category_id: str,
) -> Dict[str, Any]:
    """
    Build request parameters for a single Browse API call.
    
    query: The text search query
    limit: Number of listings per page
    offset: Pagination offset
    category_id: eBay category filter

    Returns: Dictionary of query parameters
    """

    return {
        "q": query,
        "limit": limit,
        "offset": offset,
        "category_ids": category_id
    }


def generate_search_requests(
        card_name: str,
        set_name: Optional[str],
        card_number: Optional[str],
        config: EbaySearchConfig
) -> List[Dict[str, Any]]:
    """
    Generates a list of search parameter dictionaries for a card.

    This function controls pagination and caps total requests

    card_name: Pokemon card name
    set_name: Name of the set the Pokemon card belongs to
    card_number: Combination of number of card in set + total numbers in a set
    config: EbaySearchConfig object

    Returns: List of parameter dictionaries to be passed to ebay_client.search_items
    """

    query = build_search_query(
        card_name = card_name,
        set_name = set_name,
        card_number = card_number
    )

    requests: List[Dict[str, Any]] = []

    for page in range(config.max_pages):
        offset = page * config.limit

        params = build_search_params(
            query = query,
            limit = config.limit,
            offset = offset,
            category_id = config.category_id
        )

        requests.append(params)

    return requests