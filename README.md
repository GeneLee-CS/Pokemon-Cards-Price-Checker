# Pokémon Card Price Tracker
An end-to-end data engineering project that tracks Pokémon card market value by combining two data sources, Pokémon TCG API for authoritative weekly market price, and eBay Browse API for current active listings and market conditions. The project builds a complete data pipeline from ingestion to analytics, and exposes the data through a FastAPI backend and a React frontend.

## Website: https://pokepricechecker.com/
---


## Architecture

### High-Level Flow

```text
Pokémon TCG API (Weekly) + Backfill
        ↓
Raw Layer (S3 JSON)
        ↓        
Staging Layer (Parquet)
        ↓
Processed Layer (card_master, card_price_variant_master, tcg_price_history)
        ↓
TCG Analytics Layer (weekly_top_tcg_cards for top N cards)
        ↓
eBay Browse API Ingestion (S3 JSON)
        ↓
eBay Staging Transform (cleaning)
        ↓
ebay_market_snapshot (Parquet)
        ↓
ebay_card_market_summary (DuckDB table)
        ↓
DuckDB Analytics Layer
        ↓
FastAPI Backend (EC2)
        ↓
React Frontend (User UI)
```

### Data Architecture (S3 Data Lake)

#### Raw Layer (TCG)
Source: Pokemon TCG API
Format: JSON
Partitioned by: ingestion_date=YYYY-MM-DD
- Stores full API payload + ingestion metadata in S3

#### Staging Layer (TCG)
Format: Parquet
Tables: `tcg_cards`, `tcg_card_prices`
- Flattens JSON, normalize schema and validates YAML contracts