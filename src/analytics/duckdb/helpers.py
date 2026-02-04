def get_latest_price_date(con):
    """
    Returns the latest TCG price_date.
    """

    result = con.execute(
        """
        SELECT MAX(price_date)
        FROM weekly_top_tcg_cards
        """
    ).fetchone()

    return result[0] if result else None