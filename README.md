# Retail Sales & Inventory Analytics Pipeline (PySpark)

A PySpark ETL pipeline that joins a large transactional sales dataset (1.5M rows) with small lookup tables (stores, products), analyzes top-selling products per city using Window Functions, and writes the result as partitioned Parquet — with a measured performance comparison between a standard join and a broadcast join.

## Why this project

Most tutorials skip the part that actually matters in production: *how much does an optimization technique really save?* This project includes a real, measured benchmark rather than just claiming "broadcast joins are faster."

## Architecture

```
data/raw/
  ├── transactions.csv   (1,500,000 rows — fact table)
  ├── products.csv       (200 rows — lookup table)
  └── stores.csv         (50 rows — lookup table)
        │
        ▼
  ┌─────────────────┐
  │  Broadcast Join  │   transactions ⋈ products ⋈ stores
  └─────────────────┘
        │
        ▼
  ┌─────────────────────────┐
  │  GroupBy + Window Func   │   total quantity sold per (city, product)
  │  (dense_rank per city)   │   → top-selling product per city
  └─────────────────────────┘
        │
        ▼
data/output/top_products/
  ├── city=Cairo/
  ├── city=Giza/
  ├── city=Alexandria/
  ├── city=Aswan/
  └── city=Mansoura/
```

## Performance: Standard Join vs. Broadcast Join

`products` (200 rows) and `stores` (50 rows) are small enough to be broadcast to every executor, avoiding a full shuffle of the 1.5M-row transactions table.

| Join Strategy | Time (local, 1.5M rows) |
|---|---|
| Standard join | 1.61s |
| Broadcast join | 1.13s |
| **Improvement** | **~30% faster** |

> Note: broadcast joins provide a much larger benefit on a multi-node cluster, where they eliminate network shuffle entirely. On a single local machine there's no network hop between executors, so the gain here comes mainly from avoiding shuffle/sort overhead rather than network I/O — the improvement would be substantially larger in a distributed cluster environment.

## Tech Stack

- **PySpark** — DataFrame API, Window Functions, Broadcast Joins
- **Python** (pandas, numpy, faker) — synthetic data generation
- **Parquet** — partitioned columnar output

## Project Structure

```
retail-sales-pyspark/
├── data/
│   ├── raw/              # generated input CSVs
│   └── output/            # partitioned Parquet output
├── scripts/
│   ├── generate_data.py   # synthetic data generator
│   └── etl_pipeline.py    # main PySpark pipeline
├── requirements.txt
└── README.md
```

## How to Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate the synthetic dataset (~1.5M transaction rows)
python scripts/generate_data.py

# 3. Run the pipeline
python scripts/etl_pipeline.py
```

Output: the top-selling product per city, printed to console and written as partitioned Parquet to `data/output/top_products/`.

## Key PySpark Concepts Demonstrated

- **Lazy evaluation** — transformations vs. actions
- **Broadcast joins** — avoiding shuffle for small lookup tables
- **Window functions** — `partitionBy` + `orderBy` + `dense_rank()` for per-group ranking
- **Partitioned Parquet writes** — enabling partition pruning for downstream readers
- **Pipeline structure** — modular, testable functions instead of a single script

## Possible Extensions

- Run on a real multi-node cluster (e.g. Databricks) to measure the broadcast join benefit at scale
- Add `pytest` unit tests for each pipeline function
- Orchestrate with Apache Airflow for scheduled runs