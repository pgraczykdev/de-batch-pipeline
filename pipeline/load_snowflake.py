import os
import snowflake.connector
import logging
import polars as pl
from pathlib import Path

from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

PARQUET_DIR = Path("data/raw")
TABLES = ["products", "users", "dates", "orders"]

def connect() -> snowflake.connector.connect:
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"]
    )

def load_table(conn, table_name: str):
    files = list(PARQUET_DIR.glob(f"{table_name}_*.parquet")) 
    df = pl.concat([pl.read_parquet(f) for f in files])
    logging.info(f"{table_name}: {len(df)} rows read from {len(files)} files")
    pandas_df = df.to_pandas()
    write_pandas(conn, pandas_df, table_name.upper(), schema="STAGING", auto_create_table=True, overwrite=True, quote_identifiers=False)

def main():
    conn = None
    try:
        conn = connect()
        conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS STAGING")
    except Exception as e:
        logging.error(f"Connection or schema error: {e}")
        raise

    for table in TABLES:
        try:
            load_table(conn, table)
        except Exception as e:
            logging.error(f"Error loading {table}: {e}")

    conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
 