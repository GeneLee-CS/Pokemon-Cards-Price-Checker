import duckdb
import os
from dotenv import load_dotenv
from src.analytics.duckdb.views import create_views

load_dotenv()
DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    "data/duckdb/pokemon.duckdb"
)

def get_connection():
    con = duckdb.connect(DUCKDB_PATH)

    # Enable HTTPFS extension
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    con.execute("INSTALL aws;")
    con.execute("LOAD aws;")

    # Configure AWS credentials and region
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "us-east-2")


    con.execute(f"SET s3_region='{aws_region}';")
    con.execute(f"SET s3_access_key_id='{aws_access_key}';")
    con.execute(f"SET s3_secret_access_key='{aws_secret_key}';")

    create_views(con)

    return con