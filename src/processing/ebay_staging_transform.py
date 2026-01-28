"""
eBay staging transform

Purpose:
- Transform raw eBay Browse API JSON results into a structured staging table (ebay_listings)
- Normalize titles, extract PSA grading and compute title match confidence.
- Reject listings that fall below match confidence. (Classified as 'reject', not dropped from table)
- Outputs append-only, partitioned Parquet validated against it's schema (ebay_listing.yaml)

Notes:
- Current staging layer only classifies data without removing any rows. Downstream layers decide what to filter.

Matching confidence:
(card number referenced here is the combination of card_number/set_printedTotal [e.g. 104/98])
- HIGH:
    - Card Name match + Card Number match + Set Name match
    - Card Name match + Card Number match
- MEDIUM:
    - Card Name match + Set Name match + Card Number not present
- LOW:
    - Card Name match + Card Number not present + Set Name not matched
- REJECT:
    - Card Name not matched
    - Card Name match + Card Number not matched
"""


from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, Any, Optional, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.latest_top_tcg_week_date import get_latest_price_date


# -------------------------------------------------
# Paths 
# -------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CARD_MASTER_PATH = PROJECT_ROOT / "data" / "processed" / "card_master"
RAW_EBAY_PATH = PROJECT_ROOT / "data" / "raw" / "ebay" / "listings"
STAGING_OUTPUT_PATH = PROJECT_ROOT / "data" / "staging" / "ebay" / "listings"

# -------------------------------------------------
# Normalization helpers
# -------------------------------------------------
"""
Functions used to normalize card_name in card_master and eBay listing titles before matching
"""

def normalize_title(text: str) -> str:
    """
    Normalize listing title:
    - lowercase
    - remove punctuation
    - collapse extra whitespace
    
    Note:
    Descriptors like EX, VMAX, GX are intentionally preserved here.
    """
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def normalize_card_name(card_name: str) -> str:
    """
    Normalize internal card name to base Pokemons. Removes descriptors and symbols (-EX, -GX, δ).
    """
    card_name = card_name.lower()
    card_name = re.sub(r"[^\w\s]", " ", card_name)
    card_name = re.sub(
        r"\b(ex|gx|v|vmax|vstar|promo|alt|art|lvx)\b",
        " ",
        card_name,
    )
    card_name = card_name.replace("δ", "")
    card_name = re.sub(r"\s+", " ", card_name)
    return card_name.strip()

def normalize_set_name(set_name: str) -> str:
    """
    Normalize internal set name
    """
    return normalize_title(set_name)


# -------------------------------------------------
# Matching helpers
# -------------------------------------------------

def card_name_match(title: str, card_name: str) -> bool:
    base_name = normalize_card_name(card_name)
    title_name = normalize_title(title)  
    return base_name in title_name

def extract_card_number(title: str) -> Optional[str]:
    match = re.search(r"\b(\d{1,3}/\d{1,3})\b", title)
    return match.group(1) if match else None

def card_number_match(title: str, card_number: str) -> Optional[bool]:
    title_number = extract_card_number(title)
    if not title_number:
        return None
    return title_number == card_number


# -------------------------------------------------
# PSA extraction
# -------------------------------------------------

def extract_psa_grade(title_normalized: str) -> Optional[int]:
    match = re.search(r"\bpsa\s*(\d{1,2})\b", title_normalized)
    if match:
        return int(match.group(1))
    return None

# -------------------------------------------------
# Confidence scoring
# -------------------------------------------------

def compute_title_match_confidence(
        *,
        name_match: bool,
        number_match: Optional[bool],
        set_match: bool
) -> str:
    
    if not name_match:
        return "reject"
    
    if number_match is False:
        return "reject"
    
    if number_match is True:
        return "high"
    
    if number_match is None and set_match:
        return "medium"
    
    return "low"

# -------------------------------------------------
# Transformation
# -------------------------------------------------

def transform_listing(
    item_summary: Dict[str, Any],
    card_row: Dict[str, Any],
    price_date: str,
    ingestion_date: str
) -> Dict[str, Any]:
    """
    Transforms a listing (single API itemSummary) into a staging row
    """
    
    raw_title = item_summary.get("title", "")
    title_normalized = normalize_title(raw_title)

    # Ignoring listings that are not Pokemon cards
    NON_CARD_KEYWORDS = {"proxy","fan art","fanart","custom","sticker","display","print","poster","reprint","fake","unofficial", "replica", "art case", "artwork case"}

    for kw in NON_CARD_KEYWORDS:
        if kw in title_normalized:
            print(f"Listing rejected: {raw_title}")
            return None
    
    canonical_name = card_row["card_name"]
    canonical_card_number = card_row["card_number"]
    canonical_set = card_row["set_name"]

    name_match = card_name_match(title_normalized, canonical_name)
    number_match = card_number_match(raw_title, canonical_card_number)

    normalized_set = normalize_set_name(canonical_set)
    set_match = bool(normalized_set) and normalized_set in title_normalized

    title_match_confidence = compute_title_match_confidence(
        name_match=name_match,
        number_match=number_match,
        set_match=set_match
    )

    psa_grade = extract_psa_grade(title_normalized)
    is_graded = psa_grade is not None

    price_info = item_summary.get("price", {})
    price_value = None
    if isinstance(price_info, dict) and price_info.get("value") is not None:
        try:
            price_value = float(price_info["value"])
        except (TypeError, ValueError):
            price_value = None

    return {
        "listing_id": item_summary.get("itemId"),
        "card_id": card_row["card_id"],
        "price_date": price_date,
        "ingestion_date": ingestion_date,

        "title": raw_title,
        "title_normalized": title_normalized,

        "image_url": item_summary.get("image", {}).get("imageUrl"),
        "thumbnail_url": (
            item_summary.get("thumbnailImages", [{}])[0].get("imageUrl")
            if item_summary.get("thumbnailImages") else None
        ),

        "price_value": price_value,
        "currency": price_info.get("currency"),

        "condition": item_summary.get("condition"),
        "condition_id": item_summary.get("conditionId"),
        
        "is_graded": is_graded,
        "grade_value": psa_grade,
        "parsed_grade": f"PSA {psa_grade}" if psa_grade is not None else None,

        "listing_url": item_summary.get("itemWebUrl"),

        "card_number_match": number_match is True,
        "set_match": set_match,
        "title_match_confidence": title_match_confidence
    }


# -------------------------------------------------
# Main
# -------------------------------------------------

def main(price_date: str, ingestion_date: str) -> None:
    # Load card_master
    card_master_files = sorted(CARD_MASTER_PATH.glob("*.parquet"))
    if not card_master_files:
        raise FileNotFoundError(f"No parquet files under {CARD_MASTER_PATH}")
    
    card_master_df = pd.read_parquet(card_master_files[0])

    required_cols = {"card_id", "card_name", "card_number", "set_name"}
    
    card_master_lookup = card_master_df.set_index("card_id").to_dict("index")

    base_path = (RAW_EBAY_PATH / f"price_date={price_date}" / f"ingestion_date={ingestion_date}")
    if not base_path.exists():
        raise FileNotFoundError(f"raw path not found :{base_path}")
    
    rows: List[Dict[str, Any]] = []
    rejected_count = 0

    for json_file in base_path.glob("*.json"):
        card_id = json_file.stem 

        card_row = card_master_lookup.get(card_id)
        if not card_row:
            continue

        card_row = dict(card_row)
        card_row["card_id"] = card_id

        with open(json_file, "r", encoding="utf-8") as f:
            raw_json = json.load(f)

        items = raw_json.get("itemSummaries", [])
        if not items:
            continue

        for item_summary in items:
            row = transform_listing(
                    item_summary=item_summary,
                    card_row=card_row,
                    price_date=price_date,
                    ingestion_date=ingestion_date
                )
            if row is None:
                rejected_count += 1
                continue

            rows.append(row)

    if not rows:
        print("No listings processed")
        return
    
    df = pd.DataFrame(rows)
    
    print(f"Accepted rows: {len(df)}")
    print(f"Rejected rows: {rejected_count}")

    table = pa.Table.from_pandas(df, preserve_index = False)
    output_path = STAGING_OUTPUT_PATH / f"price_date={price_date}" / f"ingestion_date={ingestion_date}"
    output_path.mkdir(parents=True, exist_ok=True)

    pq.write_table(table, output_path / "part-000.parquet")
    print(f"Wrote {len(df)} rows to {output_path}")

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        raise ValueError(
            "Usage: python ebay_staging_transform.py <ingestion_date>"
        )
    
    ingestion_date = sys.argv[1]

    price_date = get_latest_price_date()

    print(f"Using price_date={price_date} for ingestion_date={ingestion_date}")
    
    main(price_date=price_date, ingestion_date=ingestion_date)