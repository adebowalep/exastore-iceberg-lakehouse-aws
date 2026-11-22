# Rubric Verification

Executed from the Udacity workspace against AWS account `124435207106`, region `us-east-1`, using the
pre-provisioned resources (bucket `ecommerce-analytics-datalake-dev-124435207106`, table bucket
`ecommerce-analytics-gold-dev-124435207106`, workgroup `ecommerce-analytics-workgroup-dev`).
All evidence below is taken from the saved outputs in `project.ipynb` (one top-to-bottom run).

## Glue job runs (this notebook run)

| Job | JobRunId | Status | Duration |
|---|---|---|---|
| CDC Bronze (initial CDC) | jr_764188bd21bc9c6f0c71ccb4f8cde48064d086b3fd845731c3569621c1d35791 | SUCCEEDED | 175s |
| Events Bronze | jr_dd5b37ae54998ad98bcc09806e22fc31f592964e8eba6e0bb73fa8dc14276d44 | SUCCEEDED | 84s |
| Silver | jr_b369d14c8b0b4f9f0df3b34d38c9873601054ceb15f9c07603571e15d6b6b2c1 | SUCCEEDED | 95s |
| Gold | jr_5d7f592bf8870128ca9a54a956b767715487e49472e358640383c1264c16b907 | SUCCEEDED | 80s |
| CDC Bronze (supplemental) | jr_4ea642fd48dc0a7a7d02a388ef675d5ec56a776d30b8222037275b6708096776 | SUCCEEDED | 108s |

## Rubric

| Rubric Category | Status | Evidence | Issues |
|---|---|---|---|
| Environment setup | PASS | Step 1 verifies bucket, workgroup, table bucket and the 4 Glue jobs exist; Step 2 registers and re-reads `s3tablescatalog`; scripts and datasets uploaded | Account ID is read from the session; no credentials in the notebook |
| CDC Bronze | PASS | 8 TODOs implemented; job SUCCEEDED; orders 2133 / users 912 / products 393 after the initial CDC | - |
| Events Bronze | PASS | Job SUCCEEDED; events table has 996 rows | Table holds each event twice (2 x 498): the append-only job was run twice. See caveats |
| Bronze validation | PASS | Counts above; duplicate-order query returned 0; event types page_view 416, add_to_cart 240, purchase 186, click 154 | Event counts are doubled (see above) |
| Silver implementation | PASS | Job SUCCEEDED; order_details 2024, enriched_events 996, product_performance 393 | order_details is an inner JOIN, so orders without a matching user are excluded (per spec) |
| Silver validation | PASS | Segments: Loyal 1076, Repeat 516, VIP 217, New 215 orders; funnel by category shown for all 5 categories | Only Sports, Electronics, Home & Garden and Books have events (events reference product ids 1-10 only); Clothing shows 0. Funnel counts are doubled, rates are unaffected |
| Gold implementation | PASS | Job SUCCEEDED; customer_analytics 14 rows, realtime_metrics 491 rows | 5 orders have the invalid source date 2025-02-29 (casts to NULL) and are excluded from the monthly aggregation |
| Gold validation | PASS | Monthly rows 2024-12 .. 2025-12 plus 2026-12 (14); 58 hourly rows with revenue > 0, top-revenue hours displayed; Athena queries use `catalog=S3T_CATALOG` | The 2026-12 row (2 orders, 784.28) comes from supplemental updates that changed two orders' `created_at`. Latest hours show 0 revenue because purchase revenue is the product price and some purchased products have no product match |
| Snapshot history | PASS | `orders$snapshots` returns 4 snapshots (append 1402999689570794797 then overwrites 8158321595349035171, 4812097465724720033, 720563117546524448); `orders$history` shows the parent lineage | The first two snapshots come from an earlier run of the same job on the same table |
| Time travel | PASS | `FOR TIMESTAMP AS OF 2026-11-12 16:26:34.033 UTC`: historical 2133 orders / 556462.16 revenue vs current 2131 / 555744.83 (deletes and updates from the supplemental CDC); the notebook asserts the two differ | - |
| Final summary | PASS | Step 13 shows all 9 tables non-empty (counts from live Athena queries) and the latest Glue runs from the Glue API | - |
| Submission readiness | PASS (pending your review) | Notebook saved with outputs, no error outputs, no credentials in the file | AWS Console screenshots must be captured manually |

## Known caveats
- **Duplicated events:** the events job is append-only and ran twice, so the events table, Silver `enriched_events` and Gold `realtime_metrics` count each event twice. Conversion rates are unaffected; absolute counts and revenue are doubled.
- Silver and Gold were built before the supplemental CDC, so they reflect the pre-supplemental Bronze state.
- The supplemental CDC changes some `created_at` values, which puts two orders into 2026-12 in `customer_analytics`.
- Replaying the original CDC restores orders deleted by the supplemental CDC; the supplemental step then removes them again.
