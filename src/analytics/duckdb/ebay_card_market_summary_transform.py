from src.analytics.duckdb.duckdb_client import get_connection

def build_ebay_card_market_summary(con):
    """
    Builds card-level eBay market summary aligned to the latest TCG price_date
    """

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ebay_card_market_summary (
            card_id VARCHAR,
            price_date DATE,
            listing_count BIGINT,
            min_price DOUBLE,
            median_price DOUBLE,
            max_price DOUBLE,
            graded_listing_count BIGINT,
            ungraded_listing_count BIGINT
            );
        """
    )

    con.execute(
        """
        WITH latest_week AS (
            SELECT MAX(price_date) AS price_date
            FROM weekly_top_tcg_cards
            ),
            latest_snapshot AS(
                SELEXT MAX(ingestion_date) AS ingestion_date
                FROM ebay_market_snapshot
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
                    AND ingestion_date = (SELECT ingestion_date FROM latest_snapshot)
                GROUP BY card_id
                )
        INSERT INTO ebay_card_market_summary
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
            CROSS JOIN latest_week l
            WHERE NOT EXISTS(
                SELECT 1
                FROM ebay_card_market_summary e
                where r.price_date = l.price_date
                    AND e.card_id = a.card_id
                );
        """
    )

if __name__ == "__main__":
    from src.analytics.duckdb.duckdb_client import get_connection

    con = get_connection()
    build_ebay_card_market_summary(con)
    con.close()
    