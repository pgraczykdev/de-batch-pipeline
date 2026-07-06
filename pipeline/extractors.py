
from datetime import datetime
from pathlib import Path
from typing import Protocol
import logging

import polars as pl

from .io import write_parquet, save_watermark

logger = logging.getLogger(__name__)

SALES_BATCH_SIZE = 50000


class SourceExtractor(Protocol):
    def extract_dimensions(self, tables: list[str]) -> list[Path]: ...
    def extract_facts(self, watermark: datetime | None) -> Path | None: ...


class OracleExtractor:
    def __init__(self, conn):
        self.conn = conn


    def extract_dimensions(self, tables: list[str]) -> list[Path]:
        paths = []
        for table in tables:
            df = pl.read_database(f"SELECT * FROM sh.{table}", connection=self.conn)
            path = write_parquet(df, table)
            logger.info(f"{table}: {df.height} rows -> {path}")
            paths.append(path)
        return paths


    def extract_facts(self, watermark: datetime | None) -> Path | None:
        if watermark is None:
            logger.info("No watermark found, extracting full sales table.")
            return self._extract_full()
        return self._extract_incremental(watermark)

    def _extract_full(self) -> Path:
        df_chunks = []
        for df_chunk in pl.read_database("SELECT * FROM sh.sales", connection=self.conn, iter_batches=True, batch_size=SALES_BATCH_SIZE):
            df_chunks.append(df_chunk)
            logger.info(f"Sales chunk {len(df_chunks)}: {df_chunk.height} rows")

        df = pl.concat(df_chunks)
        save_watermark(df["TIME_ID"].max())
        path = write_parquet(df, "sales")
        logger.info(f"sales: {df.height} rows -> {path}")
        return path

    def _extract_incremental(self, watermark: datetime) -> Path | None:
        query = f"""
            SELECT *
            FROM sh.sales
            WHERE time_id > TO_DATE('{watermark.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
        """
        df_chunks = []
        for df_chunk in pl.read_database(query, connection=self.conn, iter_batches=True, batch_size=SALES_BATCH_SIZE):
            df_chunks.append(df_chunk)
            logger.info(f"Sales incremental chunk {len(df_chunks)}: {df_chunk.height} rows")

        if not df_chunks:
            logger.info("No new rows since last watermark.")
            return None

        df = pl.concat(df_chunks)
        save_watermark(df["TIME_ID"].max())
        path = write_parquet(df, "sales_incremental")
        logger.info(f"sales_incremental: {df.height} rows -> {path}")
        return path
