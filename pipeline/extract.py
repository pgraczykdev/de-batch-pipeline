import os
import logging

import oracledb

from dotenv import load_dotenv

from .extractors import OracleExtractor
from .io import read_watermark


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger(__name__)

load_dotenv()

oracledb.defaults.fetch_decimals = True

DIMENSION_TABLES = ["channels", "products", "customers", "times"]


def get_connection():
    return oracledb.connect(
        user=os.environ["ORACLE_USER"],
        password=os.environ["ORACLE_PASSWORD"],
        dsn=os.environ["ORACLE_DSN"],
        config_dir=os.environ["WALLET_DIR"],
        wallet_location=os.environ["WALLET_DIR"],
        wallet_password=os.environ["WALLET_PASSWORD"],
    )


if __name__ == "__main__":
    with get_connection() as conn:
        extractor = OracleExtractor(conn)
        try:
            extractor.extract_dimensions(DIMENSION_TABLES)
        except Exception as e:
            logger.error(f"Dimensions failed: {e}")

        watermark = read_watermark()
        logger.info(f"Using watermark: {watermark}")
        try:
            extractor.extract_facts(watermark)
        except Exception as e:
            logger.error(f"Failed to extract sales: {e}")
