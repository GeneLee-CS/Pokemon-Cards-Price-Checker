"""
Raw TCG JSON data upload to S3 Script

Purpose:
- Uploads raw TCG JSON data to S3
"""


import boto3
from pathlib import Path
from datetime import date, datetime, timezone

s3 = boto3.client("s3")

BUCKET = "pokemon-tcg-data-lake"
RUN_DATE = datetime.now(timezone.utc).date().isoformat()
current_date = date.today()

LOCAL_RAW_PATH = Path(f"data/raw/pokemon_tcg/cards/{current_date}")

for file_path in LOCAL_RAW_PATH.glob("*.json"):
    s3_key = (
        f"raw/pokemon_tcg/cards/"
        f"ingestion_date={RUN_DATE}/"
        f"{file_path.name}"
    )

    s3.upload_file(str(file_path), BUCKET, s3_key)
    print(f"Uploaded {file_path.name}")