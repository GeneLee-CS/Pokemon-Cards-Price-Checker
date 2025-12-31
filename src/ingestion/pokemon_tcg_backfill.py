"""
TCG API backfill ingestion Script

Purpose:
- Queries TCG's API for missing catalog data due to errors by using the failed pages JSON
- Write outputs to S3 / local raw layer in JSON format
"""


import os
import requests
from dotenv import load_dotenv
import json
from pathlib import Path
from datetime import date, datetime
import time

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2] 
DATA_DIR = BASE_DIR / "data" / "raw"
FAILED_DATA_DIR =  BASE_DIR / "data" / "meta"

current_date = date.today()
failed_pages_path = FAILED_DATA_DIR / f"{current_date}_failed.json"
output_path = DATA_DIR / f"{current_date}_backfill.json"
fail_persisted_output_path = FAILED_DATA_DIR / f"{current_date}_fail_persisted.json"

API_KEY = os.getenv("POKEMON_TCG_API_KEY")

base_url = "https://api.pokemontcg.io/v2/cards"

headers = {
    "X-Api-Key": API_KEY
}

all_cards = []

with open(failed_pages_path, 'r') as file:
    data = json.load(file)

failed_pages = data['failed_pages']
run_id = data["run_id"]
updated_failed_pages = []
page_count = 1

for page in failed_pages:

    success = False

    # attempts to retry if server side timeouts 
    for attempts in range(5):
        try:
            resp = requests.get(base_url, params={"page":page, "pageSize":50}, headers=headers, timeout=(30,180))
            resp.raise_for_status()
            body = resp.json()
            cards = body.get("data", [])               
            all_cards.extend(cards)
            print(f"Fetching failed page {page}, progress ({page_count}/{len(failed_pages)})")
            success = True
            page_count += 1
            time.sleep(5)
            break
        
        except requests.exceptions.HTTPError as e:

            if resp.status_code in (504, 429, 404):
                wait = 5 * (attempts + 1)
                print(f"{resp.status_code} on page {page}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"Error code {resp.status_code} on page {page}.")
                updated_failed_pages.append(page)
                page_count += 1
                success = True
                break
        
        except requests.exceptions.RequestException as e:
            wait = 5 * (attempts + 1)
            print(f"Network error on page {page}: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    if not success:
        print(f"Failed to fetch page {page} after 5 retries. Skipping page")
        updated_failed_pages.append(page)
        page_count += 1
        time.sleep(60)
        continue

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(all_cards, f, indent=4, ensure_ascii=False)

print(f"JSON written to: {output_path}")

if updated_failed_pages:
    print("Failed pages: ", updated_failed_pages)

    failed_payload = {
    "source": "tcg_api_backfill_fails",
    "run_id": run_id,
    "failed_pages": sorted(set(updated_failed_pages)),
    "last_updated_est": datetime.now().isoformat()
}

    with open(fail_persisted_output_path, "w", encoding="utf-8") as f:
        json.dump(failed_payload, f, indent=4, ensure_ascii=False)

    print("Remaining failed pages saved:", updated_failed_pages)

else:
    failed_pages_path.unlink()
    print("All failed pages recovered successfully")