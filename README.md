# Pokémon Card Price Tracker
An end-to-end **data engineering** project that collects, processes, stores, and visualizes historical Pokémon card sale prices from eBay.  

---

## Tech Stack
- Python
- Poetry
- GitHub CI/CD (later)
- AWS S3 (later)
- dbt & Airflow (later)

## Architecture

### Data Lake Zones:
    - **Raw**: Immutable JSON data ingested directly from source API.
    - **Staging**: Cleaned, normalized, typed datasets (Parquet). Schema definitions are maintained in top-level 'schema/' directory.
    - **Processed**: Analytics-ready tables for downstream consumption.

1. **Ingestion Layer (base dataset)**
    - Description: Pulls full pokemon card dataset from TCG Player's API

    - **Design Note**:
        - The TCG Player API is relatively slow and can be unreliable for full-dataset pulls (timeouts, rate limits)
        - To avoid unnecessary re-fetching and to improve reliability, the API ingestion step is decoupled from the S3 upload step.
        - API pulls are persisted locally before uploading to S3.
        - Intend to avoid full dataset pulls unless neccessary, only delta updates moving forward. 

2. **Raw Data Layer (S3)**
    - Description: Stores unmodified TCG Player API responses in Amazon S3.
    - Data stored in JSON format and partitioned by process and ingestion date.

3. **Staging / Processing Layer**
    - Description: Transforms raw JSON data into structured Parquet datasets.
    - Handles schema normalization, type casting and basic data quality checks.

## Current Status
- Project scaffolding complete
- TCGPlayer API ingestion complete
- TCG data backfill complete (full dateset acquired)
- S3 set up complete

