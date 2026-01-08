# Pokémon Card Price Tracker
An end-to-end **data engineering** project that collects, processes, stores, analyzes and visualizes current Pokémon card sale listings from eBay.

---

## Tech Stack
- Python 3.11
- Poetry (dependency management)
- Pandas / PyArrow
- GitHub CI/CD 
- AWS S3 
- dbt & Airflow (later)

## Data Sources
- Pokemon TCG API
    - Full card catalog
    - Card metadata and image URLs
    - Weekly market price snapshots

- eBay (later)
    - Current listings for top performing cards


## Architecture

**Data Lake Zones**:
    - **Raw**: Immutable JSON data ingested directly from source API.
    - **Staging**: Cleaned, normalized, typed datasets (Parquet). Schema definitions are maintained in top-level 'schemas/' directory.
    - **Processed**: Analytics-ready tables for downstream consumption.

1. **Ingestion Layer (base dataset)**
    - Description: Pulls full pokemon card dataset from TCG Player's API

    - **Design Note**:
        - The TCG Player API is relatively slow and can be unreliable for full-dataset pulls (timeouts, rate limits)
        - To avoid unnecessary re-fetching and to improve reliability, the API ingestion step is decoupled from the S3 upload step.
        - API pulls are persisted locally for now before uploading to S3.
        - Intend to avoid full dataset pulls unless neccessary, only delta updates moving forward. 

2. **Raw Data Layer (S3)**
    - Description: Stores unmodified TCG Player API responses in Amazon S3.
    - Data stored in JSON format and partitioned by process and ingestion date.

3. **Staging / Processing Layer**
    - Description: Transformation processes responsible forming schema-validated, flattened Parquet datasets


## Repo Structure
```text
.
├── src/
│   ├── ingestion/
│   ├── processing/
│   └── analytics/
├── schemas/
│   ├── staging/
│   ├── processed/
│   └── analytics/
├── pyproject.toml
├── poetry.lock
├── README.md
└── .gitignore
```

## Data Models

**Staging**
- 'tcg_cards' - Flatted card metadata with schema validation
- 'tcg_card_prices' - Flatted card price type(variants) and subsequent market prices

**Processed**
- 'card_master' - Static card reference dimension
- 'card_price_variant_master' - Deterministic price variant dimension
- 'tcg_price_history' - Append-only weekly price fact table

**Analytics
- 'weekly_top_tcg_cards' - Weekly card-level price rankings based on TCG

## Current Status
- Scaffholding complete
- Pokemon TCG data pipeline fully implemented
- Full TCG card catalog ingested and backfilled
- Raw JSON stored in S3
- Staging and processed layers implemented with schema validation
- Card and price variant master tables populated
- Price history fact table complete
- Working on analytics layer for TCG data prior to eBay ingestion
