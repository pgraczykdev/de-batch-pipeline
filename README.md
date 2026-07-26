# de-batch-pipeline

Batch pipeline in Python that extracts data from a REST API (DummyJSON) or Oracle ADB, stores it as Parquet files (Data Lake), loads it into DuckDB, transforms it with dbt into a star schema, and orchestrates the full flow with Apache Airflow running in Docker.

---

## Architecture

```
Airflow DAG (de_pipeline) — scheduled daily at 06:00
          │
          ▼  extract_task
API (DummyJSON) / Oracle ADB
          │
          ▼  pipeline/extract.py
  data/raw/*.parquet   (Data Lake — immutable, timestamped)
          │
          ▼  load_task
          │  pipeline/load.py
  DuckDB  staging.*    (raw 1:1 copy from Parquet)
          │
          ▼  dbt_task
          │  dbt run
  DuckDB  dbt_dev.*
     ├── staging layer   dj_stg_* / o_stg_*   (rename, normalize)
     ├── core layer      dim_products, dim_users, dim_dates, fct_orders
     └── mart layer      mart_revenue_by_category
```

---

## Tech Stack

| Tool | Role |
|---|---|
| Python 3.12 | Pipeline orchestration |
| Polars | DataFrame processing, Parquet I/O |
| DuckDB | Local analytical warehouse |
| dbt-duckdb | SQL transformations, star schema, data tests |
| Apache Airflow | DAG orchestration — scheduling, retries, monitoring |
| Docker | Airflow runs in containers (LocalExecutor) |
| python-oracledb (thin) | Oracle ADB connector — wallet/mTLS, no Oracle Client needed |
| requests | REST API client (DummyJSON) |
| python-dotenv | Secrets management via `.env` |

---

## Data Sources

The active source is selected via the `EXTRACTOR` environment variable:

| `EXTRACTOR` | Source | Dimensions | Facts |
|---|---|---|---|
| `DUMMY_JSON` | [DummyJSON API](https://dummyjson.com) | products, users, dates | orders (carts) |
| `oracle` (default) | Oracle ADB — schema `SH` | channels, products, customers, times | sales |

---

## How to Run

**1. Clone and install**

```bash
git clone https://github.com/pgraczykdev/de-batch-pipeline
cd de-batch-pipeline
python -m venv .venv
.venv\Scripts\activate      # Windows
pip install -r requirements.txt
```

**2. Configure environment**

Create `.env` in the project root (never commit this):

```env
EXTRACTOR=DUMMY_JSON

# Required only when EXTRACTOR=oracle
ORACLE_USER=...
ORACLE_PASSWORD=...
ORACLE_DSN=...
WALLET_DIR=...
WALLET_PASSWORD=...
```

**3. Configure dbt**

Create `~/.dbt/profiles.yml`:

```yaml
oracle_pipeline:
  outputs:
    dev:
      type: duckdb
      path: /absolute/path/to/de-batch-pipeline/data/warehouse.duckdb
      threads: 1
      schema: dbt_dev
  target: dev
```

**4. Run via Airflow (recommended)**

```bash
cd airflow
docker compose up -d
```

Open `http://localhost:8080` (login: `airflow` / `airflow`), trigger the `de_pipeline` DAG manually or wait for the daily schedule.

**4b. Run manually (without Airflow)**

```bash
# Extract — fetch data, save as Parquet
python -m pipeline.extract

# Load — Parquet → DuckDB staging + core schemas
python -m pipeline.load

# Transform — run all dbt layers
cd dbt
dbt run

# Test — validate data quality
dbt test
```

---

## Key DE Concepts Demonstrated

**ELT pattern** — data lands raw in the warehouse (Load), transformations happen inside the warehouse (dbt). Parquet = Data Lake; DuckDB = warehouse.

**Star schema** — `fct_orders` as the central fact table surrounded by `dim_products`, `dim_users`, `dim_dates`. Every analytical query follows a predictable JOIN shape.

**Watermark incremental load** — after a full extract, the max date is saved to `data/watermark.json`. Subsequent runs fetch only rows newer than the watermark.

**OOP extractor abstraction** — `OracleExtractor` and `DummyJsonExtractor` both implement the `SourceExtractor` Protocol (`extract_dimensions`, `extract_facts`). Switching sources requires only setting `EXTRACTOR` in `.env`.

**dbt layers** — staging renames and normalizes; core builds the star schema as TABLE; marts provide ready-to-use business aggregations.

**Data quality tests** — dbt `schema.yml` enforces `not_null`, `unique`, and `relationships` constraints on core models. Tests run as SQL queries against live warehouse data.

**Type precision** — `oracledb.defaults.fetch_decimals = True` forces money columns to `Decimal`, not `float`, through the full round-trip: Oracle → Polars → Parquet → DuckDB.

**Airflow orchestration** — a single DAG (`de_pipeline`) chains extract → load → dbt run with `BashOperator`. `default_args` applies `retries=2` and `retry_delay=1min` to all tasks. Scheduled via cron (`0 6 * * *`). Airflow runs in Docker with `LocalExecutor` — no Celery/Redis overhead needed for a single-machine setup.

**Secrets outside code** — credentials live in `.env`, excluded from version control via `.gitignore`.

---

## Project Structure

```
de-batch-pipeline/
├── pipeline/
│   ├── extract.py       entry point — selects source, runs extraction
│   ├── extractors.py    OracleExtractor + DummyJsonExtractor (Protocol)
│   ├── load.py          Parquet → DuckDB (staging + core schemas)
│   └── io.py            write_parquet, watermark read/save
├── dbt/
│   └── models/
│       ├── staging/
│       │   ├── oracle/      o_stg_channels, o_stg_products, o_stg_customers, o_stg_times, o_stg_sales
│       │   └── dummyjson/   dj_stg_products, dj_stg_users, dj_stg_dates, dj_stg_orders
│       ├── core/            dim_products, dim_users, dim_dates, fct_orders + schema tests
│       └── marts/           mart_revenue_by_category
├── airflow/
│   ├── dags/
│   │   └── pipeline_dag.py  Airflow DAG — extract → load → dbt run
│   ├── Dockerfile           custom image: Airflow + project dependencies
│   └── docker-compose.yaml  LocalExecutor setup (webserver, scheduler, postgres)
├── data/
│   ├── raw/             Parquet files (gitignored)
│   ├── warehouse.duckdb DuckDB warehouse (gitignored)
│   └── watermark.json   incremental load bookmark (gitignored)
├── requirements.txt
└── .env                 secrets — never committed

```

---

## Roadmap

| Phase | Goal | Key tools | Status |
|---|---|---|---|
| 1 | Extract Oracle → Parquet | python-oracledb, polars | done |
| 2 | Load into a local warehouse | DuckDB, star schema | done |
| 3 | Transform with dbt | dbt-core, dbt-duckdb | done |
| 4 | Orchestrate the pipeline | Apache Airflow, Docker | done |
| 5 | Streaming | Kafka / Oracle AQ | planned |
