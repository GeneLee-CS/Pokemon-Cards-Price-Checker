# 🃏 Pokémon Card Price Tracker
An end-to-end **data engineering** project that collects, processes, stores, and visualizes historical Pokémon card sale prices from eBay.  
Project aims to demonstrate real-world skills in **data ingestion, pipelines, cloud storage, data warehousing, analytics engineering, and API + dashboard development**.

---

## 📌 Project Overview
The goal of this project is to build a fully automated system that:

1. **Fetches recent sold prices** of selected Pokémon cards from eBay.
2. **Cleans and normalizes** the raw data.
3. **Stores it in a cloud data lake** (Google Cloud Storage / AWS S3).
4. **Loads structured tables into a cloud warehouse** (BigQuery / Snowflake / Redshift).
5. **Builds daily/weekly aggregated analytics** (dbt or SQL).
6. **Exposes the data via an API** (FastAPI).
7. **Allows for search and display of interactive charts on a web dashboard** (Streamlit or Next.js).


---

## 🏗️ Architecture

External API → Ingestion Script → Data Lake → Data Warehouse → Transformations → API → Dashboard


### **Data Flow**
1. **eBay API / Scraper**  
   - Pulls last 1–3 months of sold listings for selected cards
2. **Raw data stored in Cloud Storage**  
   - JSON or Parquet files partitioned by date
3. **Warehouse tables**  
   - Cleaned fact table: `fact_sales`
   - Reference table: `dim_cards`
4. **dbt / SQL transformations**  
   - Daily price summary
   - Weekly moving averages
   - Price volatility & trends
5. **REST API**  
   - `/cards/{card_id}/history`
   - `/cards/top`
   - `/search?q=charizard`
6. **Web dashboard**  
   - Search cards  
   - View historical charts  
   - Compare multiple cards  

---

## 🧰 Tech Stack

### **Data & Backend**
- Python 3
- eBay API (or scraping fallback)
- Requests / HTTPX
- Pandas / PyArrow
- Google Cloud Storage (OR AWS S3)
- BigQuery (OR Snowflake / Redshift)
- dbt (optional but strongly recommended)
- FastAPI REST API

### **Frontend (Optional)**
- Streamlit (easiest)
- or Next.js + Chart.js (more professional)

### **Orchestration & Scheduling**
- Airflow (optional)
- Google Cloud Scheduler (simple)
- Cron (local testing)

---

## 📂 Project Structure

pokemon-price-tracker/
│
├── data_ingestion/
│ ├── fetch_sales.py
│ ├── ebay_client.py
│ └── card_list.json
│
├── warehouse/
│ ├── schema.sql
│ ├── load_to_bigquery.py
│ └── transformations/
│ └── dbt models (optional)
│
├── api/
│ ├── main.py # FastAPI routes
│ └── queries.py
│
├── dashboard/
│ └── app.py # Streamlit or Next.js app
│
├── docs/
│ ├── architecture.png
│ ├── datasets.md
│ └── api_endpoints.md
│
└── README.md # This file

