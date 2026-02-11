from dotenv import load_dotenv
load_dotenv()

def build_ebay_card_market_summary(con):
    """
    Builds card-level eBay market summary aligned to the latest TCG price_date
    """

    con.execute(
        """
        CREATE OR REPLACE TABLE ebay_card_market_summary AS
        WITH latest_week AS (
            SELECT MAX(price_date) AS price_date
            FROM weekly_top_tcg_cards
            ),
            aggregated AS (
                SELECT
                    card_id,
                    count(*) AS listing_count,
                    MIN(price_value) AS min_price,
                    quantile_cont(price_value, 0.5) AS median_price,
                    MAX(price_value) AS max_price,
                    SUM(CASE WHEN is_graded THEN 1 ELSE 0 END) AS graded_listing_count,
                    SUM(CASE WHEN NOT is_graded THEN 1 ELSE 0 END) AS ungraded_listing_count
                FROM ebay_market_snapshot
                WHERE currency = 'USD'
                GROUP BY card_id
                )
            SELECT
                a.card_id,
                l.price_date,
                a.listing_count,
                a.min_price,
                a.median_price,
                a.max_price,
                a.graded_listing_count,
                a.ungraded_listing_count
            FROM aggregated a
            CROSS JOIN latest_week l;
        """
    )

if __name__ == "__main__":
    import duckdb

    con = duckdb.connect("data/duckdb/pokemon.duckdb")
    build_ebay_card_market_summary(con)
    con.close()
    