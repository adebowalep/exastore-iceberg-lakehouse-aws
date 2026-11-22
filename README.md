# Apache Iceberg Lakehouse on AWS

Build a medallion-architecture data lakehouse using Apache Iceberg, AWS Glue, S3 Tables, and Athena. Ingest CDC and clickstream data through Bronze → Silver → Gold layers, then explore Iceberg features like snapshots and time-travel.

```
CDC JSON (S3)    ──→  Glue: CDC Bronze    ──→  S3 Iceberg (orders, users, products)
Events JSON (S3) ──→  Glue: Events Bronze ──→  S3 Iceberg (events)
                      Glue: Silver Layer   ──→  S3 Tables  (order_details, enriched_events, product_performance)
                      Glue: Gold Layer     ──→  S3 Tables  (customer_analytics, realtime_metrics)
```

## Project Structure

```text
.
├── datasets/            # sample CDC (Debezium JSON), supplemental CDC and clickstream events
├── glue-jobs/
│   ├── cdc-batch/       # Bronze: CDC MERGE for orders, users, products
│   ├── events/          # Bronze: append-only events
│   ├── silver/          # Silver: enriched tables on S3 Tables
│   └── gold/            # Gold: analytics tables on S3 Tables
├── project.ipynb        # runs the jobs, validates each layer, snapshots and time travel
└── rubric-verification.md

## What You'll Build

| Layer | Storage | Tables | Technique |
|-------|---------|--------|-----------|
| Bronze | S3 + Glue Catalog Iceberg | orders, users, products, events | CDC MERGE INTO, append-only INSERT |
| Silver | S3 Tables (managed Iceberg) | order_details, enriched_events, product_performance | JOINs, window functions, aggregations |
| Gold | S3 Tables (managed Iceberg) | customer_analytics, realtime_metrics | Monthly/hourly business aggregations |

## Implementation Notes

**Bronze - CDC (`glue-jobs/cdc-batch`)**
- Debezium JSON -> `cdc_<t>` view (primary key via `COALESCE(after.pk, before.pk)`; deletes read business columns from `before`) -> `staged_<t>` view (`ROW_NUMBER() OVER (PARTITION BY pk ORDER BY source_lsn DESC, ts_ms DESC, delete-first)`, `_rn = 1`) -> `MERGE INTO` (DELETE / UPDATE / INSERT).
- `MERGE` clauses also compare `source_lsn`, so an older event can never overwrite a newer one (safe re-runs).
- Tables: `orders` (`days(created_at)`), `users` and `products` (`months(created_at)`); Iceberg v2, parquet + snappy.
- The job raises (Glue run = FAILED) if any table fails.
- Optional argument `--supplemental_path` replays `datasets/cdc_data/supplemental/` through the same MERGE job (used for snapshots and time travel).

**Bronze - Events (`glue-jobs/events`)**: parse -> single-pass data-quality counts (logged, not fatal) -> filter -> append-only `INSERT INTO`, partitioned by `(year, month)`. Re-running the job appends the same events again by design.

**Silver / Gold**: `SparkSession.builder` registers `s3tablescatalog` (GlueCatalog, `glue.id = <account>:s3tablescatalog/<table bucket>`); bronze is read through `glue_catalog`. Namespaces are created with boto3 `create_database`. `create_*_table()` materialises the DataFrame, then `DROP TABLE ... PURGE` (only inside the project's namespace) and `writeTo().using("iceberg").tableProperty("format-version","2").create()`. Recreating a table resets its snapshot history.
- `product_performance` rates are percentages: `purchases / page_views * 100` and `purchases / add_to_carts * 100`, 0 when the denominator is 0.
- `enriched_events.revenue` is the product price on purchase events (as specified).

## Running the Project

1. Connect AWS credentials (`aws configure set ...`, then `aws sts get-caller-identity`).
2. Open `project.ipynb` and run top to bottom. Step 1 derives the account ID and checks that every resource exists - confirm the bucket / workgroup / table-bucket names match your console.
3. Order matters: Bronze jobs -> Silver -> Gold -> Step 11 (supplemental CDC) -> Step 12 (time travel) -> Step 13 (summary).
4. Re-upload scripts (Step 3) after any script change.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `ExpiredToken` / `InvalidClientTokenId` | Refresh credentials from the Cloud Resources tab |
| Glue job fails immediately | Re-run Step 3 (scripts must be in `s3://<bucket>/glue-scripts/`) |
| "table not found" in Silver/Gold | Run layers in order |
| Athena fails on S3 Tables | Run Step 2; pass `catalog=S3T_CATALOG` and the database |
| "Cannot drop table without PURGE" | S3 Tables needs `DROP TABLE ... PURGE` |

## Status

All four Glue jobs and the supplemental CDC run SUCCEEDED from the Udacity workspace, and all 9 tables (4 Bronze, 3 Silver, 2 Gold) are populated. The notebook demonstrates Iceberg snapshots (`orders$snapshots`, `orders$history`) and time travel (`FOR TIMESTAMP AS OF`: 2133 orders before the supplemental CDC vs 2131 after). `rubric-verification.md` lists the job run IDs and evidence per rubric category.

Known limitations:
- The events job is append-only and was run twice, so the events table holds each event twice (996 = 2 x 498). Absolute event counts and revenue in Silver/Gold are doubled; rates are unaffected. Run the events job only once per fresh table.
- Silver and Gold were built before the supplemental CDC, so they reflect the pre-supplemental Bronze state.
- 5 orders have an invalid date (2025-02-29) and are excluded from the monthly Gold aggregation.
