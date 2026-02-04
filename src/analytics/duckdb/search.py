import duckdb
con = duckdb.connect()

def search_cards_by_name(con, search_term: str, limit: int = 20):
    query = """
    SELECT
        card_id,
        card_name,
        set_name,
        card_number,
        image_small_url
    FROM read_parquet('s3://pokemon-tcg-data-lake/processed/card_master/*.parquet')
    WHERE LOWER(card_name) LIKE '%' || LOWER(?) || '%'
    ORDER BY card_name
    LIMIT ?
    """

    return con.execute(query, [search_term, limit]).fetchdf()