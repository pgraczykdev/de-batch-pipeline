import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl


OUTPUT_DIR = Path("data/raw")
WATERMARK_FILE = Path("data/watermark.json")


def write_parquet(df: pl.DataFrame, name: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = OUTPUT_DIR / f"{name}_{stamp}.parquet"
    df.write_parquet(path)
    return path


def read_watermark() -> datetime | None:
    if not WATERMARK_FILE.exists():
        return None
    with open(WATERMARK_FILE, "r") as f:
        data = json.load(f)
        return datetime.fromisoformat(data["last_extracted"])


def save_watermark(timestamp: datetime) -> None:
    with open(WATERMARK_FILE, "w") as f:
        json.dump({"last_extracted": timestamp.isoformat()}, f)
