import os
import duckdb


def configure_duckdb_s3(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    con.execute("SET s3_region='us-east-2';")

    con.execute(
        f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';"
    )
    con.execute(
        f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';"
    )

    if "AWS_SESSION_TOKEN" in os.environ:
        con.execute(
            f"SET s3_session_token='{os.environ['AWS_SESSION_TOKEN']}';"
        )