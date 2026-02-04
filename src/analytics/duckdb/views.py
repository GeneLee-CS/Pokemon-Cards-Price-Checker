def create_views(con):
    """
    Registers DuckDB views over analytics Parquet files.
    """
    #Weekly top TCH cards
    con.execute(
        """
        CREATE OR REPLACE VIEW weekly_top_tcg_cards AS
        SELECT *
        FROM read_parquet(
            's3://pokemon-tcg-data-lake/processed/analytics/weekly_top_tcg_cards/**/*.parquet',
            hive_partitioning = true
            );
        """
    )

    # eBay listing-level snapshot
    con.execute(
        """
        CREATE OR REPLACE VIEW ebay_market_snapshot AS
        SELECT *
        FROM read_parquet(
            's3://pokemon-tcg-data-lake/analytics/ebay_market_snapshot/**/*.parquet',
            hive_partitioning = true
        );
        """
    )

    # card master
    con.execute(
        """
        CREATE OR REPLACE VIEW card_master AS
        SELECT *
        FROM read_parquet(
            's3://pokemon-tcg-data-lake/processed/card_master/**/*.parquet'
        );
        """
    )

    # tcg price history
    con.execute(
        """
        CREATE OR REPLACE VIEW tcg_price_history AS
        SELECT *
        from read_parquet(
            's3://pokemon-tcg-data-lake/processed/tcg_price_history/**/*.parquet',
            hive_partitioning = true
        );
        """
    )