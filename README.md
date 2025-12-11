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

1. **Ingestion Layer (base dataset)**
    - Description: Pulls full pokemon card dataset from TCG Player's API
    - Tools: Python, Requests
    - Output: Raw JSON saved to '/data/raw'


## Current Status
- Project scaffolding complete
- TCGPlayer API ingestion complete
- Working on TCG data backfill

