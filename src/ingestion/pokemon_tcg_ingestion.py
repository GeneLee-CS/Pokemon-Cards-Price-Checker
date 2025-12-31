"""
TCG API ingestion Script

Purpose:
- Queries TCG's API for full catalog data
- Outputs raw JSON file and file for missing pages due to API errors
- Write outputs to S3 / local raw layer in JSON format
"""


import os
import requests
from dotenv import load_dotenv
import json
from pathlib import Path
from datetime import date, datetime
from math import ceil
import time

load_dotenv()

# change parents[index] if file depth changes, parents[2] points to root.
BASE_DIR = Path(__file__).resolve().parents[2] 

DATA_DIR = BASE_DIR / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)
FAILED_DATA_DIR =  BASE_DIR / "data" / "meta"
FAILED_DATA_DIR.mkdir(parents=True, exist_ok=True)

current_date = date.today()
run_id = f"pokemon_tcg_ingestion_{current_date}"
output_path = DATA_DIR / f"{current_date}_full.json"
failed_output_path = FAILED_DATA_DIR / f"{current_date}_failed.json"

API_KEY = os.getenv("POKEMON_TCG_API_KEY")

base_url = "https://api.pokemontcg.io/v2/cards"

headers = {
    "X-Api-Key": API_KEY
}

# for pages that failed after 5 attempts.
failed_pages = []
all_cards = []
page = 1

done = False

while not done:

    success = False

    # attempts to retry if server side timeouts 
    for attempts in range(5):
        try:
            resp = requests.get(base_url, params={"page":page, "pageSize":50}, headers=headers, timeout=(30,180))
            resp.raise_for_status()
            body = resp.json()
            cards = body.get("data", [])
            total = body.get("totalCount")

            # termination condition
            if not cards:
                print("No more cards. Ingestion complete.")
                done = True
                success = True
                break

            all_cards.extend(cards)
            total = body.get("totalCount")
            print(f"Fetching page {page}, progress ({len(all_cards)}/{total})")

            time.sleep(5)
            page += 1
            success = True
            break
        
        except requests.exceptions.HTTPError as e:

            if resp.status_code in (504, 429, 404):
                wait = 5 * (attempts + 1)
                print(f"{resp.status_code} on page {page}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"Error code {resp.status_code} on page {page}.")
                failed_pages.append(page)
                success = True
                page += 1
                break
        
        except requests.exceptions.RequestException as e:
            wait = 5 * (attempts + 1)
            print(f"Network error on page {page}: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    if not success:
        print(f"Failed to fetch page {page} after 5 retries. Skipping page")
        failed_pages.append(page)
        page += 1
        time.sleep(60)
        continue



with open(output_path, "w", encoding="utf-8") as f:
    json.dump(all_cards, f, indent=4, ensure_ascii=False)

print(f"JSON written to: {output_path}")

if failed_pages:
    print("Failed pages: ", failed_pages)

    failed_payload = {
    "source": "pokemon_tcg_api",
    "run_id": run_id,
    "failed_pages": sorted(set(failed_pages)),
    "last_updated_est": datetime.now().isoformat()
}

    with open(failed_output_path, "w", encoding="utf-8") as f:
        json.dump(failed_payload, f, indent=4, ensure_ascii=False)

