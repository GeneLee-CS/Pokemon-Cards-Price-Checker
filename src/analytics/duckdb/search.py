from typing import Any
import duckdb

def search_cards_by_name(
    con: duckdb.DuckDBPyConnection,
    search_term: str,
    limit: int = 20
) -> list[dict[str, Any]]:
    tokens = [t.strip().lower() for t in search_term.split() if t.strip()]

    if not tokens:
        return []

    combined_expr = """
        LOWER(
            COALESCE(card_name, '') || ' ' ||
            COALESCE(set_name, '') || ' ' ||
            COALESCE(card_number, '')
        )
    """

    where_clauses = [f"{combined_expr} LIKE ?" for _ in tokens]
    where_sql = " AND ".join(where_clauses)

    query = f"""
    SELECT
        card_id,
        card_name,
        set_name,
        card_number,
        image_small_url
    FROM card_master
    WHERE {where_sql}
    ORDER BY card_name
    LIMIT ?;
    """

    params = [f"%{token}%" for token in tokens] + [limit]

    rows = con.execute(query, params).fetchall()
    columns = [c[0] for c in con.description]
    return [dict(zip(columns, row)) for row in rows]