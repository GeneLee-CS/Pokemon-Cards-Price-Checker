from typing import Any
import duckdb

def search_cards_by_name(con: duckdb.DuckDBPyConnection, search_term: str, limit: int = 20) -> list[dict[str, Any]]:
    query = """
    SELECT
        card_id,
        card_name,
        set_name,
        card_number,
        image_small_url
    FROM card_master
    WHERE LOWER(card_name) LIKE '%' || LOWER(?) || '%'
    ORDER BY card_name
    LIMIT ?
    """

    rows = con.execute(query, [search_term, limit]).fetchall()
    columns = [c[0] for c in con.description]
    return [dict(zip(columns, row)) for row in rows]