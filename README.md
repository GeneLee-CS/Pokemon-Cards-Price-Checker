# Pokémon Card Price Tracker
An end-to-end data engineering project that tracks Pokémon card market value by combining two data sources, Pokémon TCG API for authoritative weekly market price, and eBay Browse API for current active listings and market conditions. The project builds a complete data pipeline from ingestion to analytics, and exposes the data through a FastAPI backend and a React frontend.

## Website: https://pokepricechecker.com/
---

## Tech Stack
- Python (Pandas, PyArrow, FastAPI)
- SQL/DuckDB
- AWS S3 → Data lake
- EC2 → API hosting
- React + Vite → Frontend
- Airflow → Orchestration

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

## Data Architecture (S3 Data Lake)


### TCG Pipeline

#### Raw Layer (TCG)
Source: Pokemon TCG API  
Format: JSON  
Partitioned by: ingestion_date=YYYY-MM-DD  
- Stores full API payload + ingestion metadata in S3

#### Staging Layer (TCG)
Format: Parquet  
Tables: `tcg_cards`, `tcg_card_prices`  
- Flattens JSON, normalize schema and validates YAML contracts

#### Processed Layer (TCG)
Dimension Tables:  
`card_master`: Card metadata  
`card_price_variant_master`: Unique (card_id, price_type) combinations  
  
Fact Tables:
`tcg_price_history`: Append only, tracks weekly TCG market price
- Grain: (card_id, price_variant_id, price_date)

#### Analytics Layer (TCG)
Tables: `weekly_top_tcg_cards`
- Top N cards for latest TCG ingestion based on MAX(market_price) across variants


### eBay Pipeline

#### Raw Layer (eBay)
Source: eBay Browse API  
Format: JSON  
Partitioned by: price_date=YYYY-MM-DD, ingestion_date=YYYY-MM-DD
- Queries the API based on top N cards from `weekly_top_tcg_cards`, full API payload stored in S3


#### Staging Layer (eBay, Data quality & matching)
Format: Parquet
Partitioned by: price_date=YYYY-MM-DD, ingestion_date=YYYY-MM-DD
- Extracts title, price, condition, URL, currency, images
- Normalizes title, confidence scoring, keyword filtering and applies card matching logic

#### Analytics Layer (eBay)
`ebay_market_snapshot`: Cleaned listing dataset, partitioned by price_date, ingestion_date  
`ebay_card_market_summary`: Aggregated metrics (listing count, min/max/median price, graded/ungraded counts)

#### Analytics Layer (DuckDB)
- Reads parquets directly from S3
- Registers datasets as views
- Builds derived tables (`ebay_card_market_summary`)

## API Layer (FastAPI)

### Endpoints

`/search`
- Search cards by name
- Returns metadata + image URL

`/cards/{card_id}`
- Returns card metadata, latest TCG price, price history, eBay market summary

`/cards/{card_id}/listings`
- Returns listing-level data
- Supports sorting and limits

## Frontend (React + Vite)

#### Features
- Autocomplete search
- Card detail page
- Price trend visualization
- eBay listings with sorting, pagination, direct links

## Orchestration (Airflow)
Docker-based local setup, with the following 2 DAGs:

`tcg_refresh_pipeline`:
- TCG API ingestion
- Transformations (staging → processed → analytics)
- Writes to S3

`ebay_refresh_pipeline`:
- eBay Browse API ingestion
- Transformation (staging → processed → analytics)
- Summary table refresh (DuckDB)
- EC2 FastAPI refresh