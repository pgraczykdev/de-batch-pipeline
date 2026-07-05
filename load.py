import duckdb, logging
from pathlib import Path


DB_PATH = Path("data/warehouse.duckdb")
PARQUET_DIR = Path("data/raw")
DIMENSION_TABLES = ["channels", "products", "customers", "times"]
FACT_TABLES = ["sales"]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def find_latest_parquet_file(table_name: str) -> Path:
    parquet_files = list(PARQUET_DIR.glob(f"{table_name}_*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found for table {table_name}")
    latest_file = max(parquet_files, key=lambda file: file.stat().st_mtime)
    return latest_file

def load_dimension(conn, table_name: str):
    sql = f"CREATE OR REPLACE TABLE staging.{table_name} AS SELECT * FROM read_parquet('{PARQUET_DIR}/{table_name}_*.parquet')"
    logging.info(f"Loading {table_name} table into staging table...")
    conn.execute(sql)
    sql = f"SELECT COUNT(*) FROM staging.{table_name}"
    result = conn.execute(sql).fetchone()
    logging.info(f"Loaded {result[0]} rows into staging table.")


def load_fact(conn, table_name: str):
    parquet_file = find_latest_parquet_file(table_name)
    sql = f"CREATE OR REPLACE TABLE staging.{table_name} AS SELECT * FROM read_parquet('{parquet_file}')"
    logging.info(f"Loading {table_name} table into staging table...")
    conn.execute(sql)
    sql = f"SELECT COUNT(*) FROM staging.{table_name}"
    result = conn.execute(sql).fetchone()
    logging.info(f"Loaded {result[0]} rows into staging table.")


def main(conn):
    sql = "CREATE SCHEMA IF NOT EXISTS staging"
    conn.execute(sql)
    for table in DIMENSION_TABLES:
        try:
           load_dimension(conn, table)
        except Exception as e:
           logging.error(f"Error occurred while loading {table}: {e}")

    # Load fact tables
    for table in FACT_TABLES:
        try:
            load_fact(conn, table)
        except Exception as e:
            logging.error(f"Error occurred while loading {table}: {e}")


if __name__ == "__main__":
    with duckdb.connect(DB_PATH) as conn:
        main(conn)
