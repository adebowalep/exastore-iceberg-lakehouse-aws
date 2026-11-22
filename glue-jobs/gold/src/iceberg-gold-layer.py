"""
AWS Glue Job: Gold Layer
Reads from Silver Iceberg tables (S3 Tables), writes aggregated business
metrics to Gold Iceberg tables, also on S3 Tables (managed Iceberg).

Your task: Complete the TODOs to build gold-layer aggregations.
  1. customer_analytics  — monthly customer/revenue/segment aggregates
  2. realtime_metrics    — hourly event/traffic/revenue aggregates

Key concepts:
  - Single s3tablescatalog handles both silver (read) and gold (write) namespaces
  - Same SparkSession.builder + writeTo().create() pattern as silver
  - Gold namespace created via Glue API (boto3), not Spark SQL
"""

import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'database_name', 'data_lake_bucket',
    'gold_table_bucket_arn', 'aws_region'
])

BUCKET = args['data_lake_bucket']
GOLD_TABLE_BUCKET_ARN = args['gold_table_bucket_arn']
AWS_REGION = args.get('aws_region', 'us-east-1')

# Parse the S3 Tables bucket ARN
arn_parts = GOLD_TABLE_BUCKET_ARN.split(":")
ACCOUNT_ID = arn_parts[4]
TABLE_BUCKET_NAME = arn_parts[5].split("/")[1]

DB_SILVER = "ecommerce_silver"   # Read source: silver namespace in S3 Tables
DB_GOLD = "ecommerce_gold"      # Write target: gold namespace in S3 Tables
S3T_CAT = "s3tablescatalog"     # Single catalog handles both silver and gold

sc = SparkContext()
glueContext = GlueContext(sc)

# One catalog serves both silver (read) and gold (write). getOrCreate() returns the
# Glue-created session and applies the catalog settings to it.
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{S3T_CAT}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{S3T_CAT}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{S3T_CAT}.glue.id", f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}") \
    .config(f"spark.sql.catalog.{S3T_CAT}.warehouse", f"s3://{BUCKET}/gold-warehouse/") \
    .getOrCreate()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info(f"S3 Tables catalog: {S3T_CAT} → {TABLE_BUCKET_NAME}")
logger.info(f"Reading silver from: {S3T_CAT}.{DB_SILVER}")
logger.info(f"Writing gold to: {S3T_CAT}.{DB_GOLD}")

import boto3
glue_client = boto3.client('glue', region_name=AWS_REGION)
try:
    glue_client.create_database(
        CatalogId=f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}",
        DatabaseInput={'Name': DB_GOLD}
    )
    logger.info(f"Created namespace {DB_GOLD}")
except glue_client.exceptions.AlreadyExistsException:
    logger.info(f"Namespace {DB_GOLD} already exists")
# any other error (e.g. AccessDenied) propagates and fails the job

# Verify the silver source tables are visible before doing any work.
spark.sql(f"SHOW TABLES IN {S3T_CAT}.{DB_SILVER}").show(truncate=False)


def create_gold_table(name, query):
    """Query silver, write results to S3 Tables gold namespace."""
    fqn = f"{S3T_CAT}.{DB_GOLD}.{name}"

    # Safety: only ever drop tables inside this project's gold namespace.
    assert fqn.startswith(f"{S3T_CAT}.{DB_GOLD}."), f"Refusing to drop outside gold: {fqn}"

    df = spark.sql(query).cache()
    row_count = df.count()          # materialise first: a failing query must not destroy the old table
    logger.info(f"{name}: {row_count} rows to write")
    if row_count == 0:
        df.unpersist()
        raise RuntimeError(f"{name}: query returned 0 rows; keeping existing table")

    # S3 Tables requires PURGE. Silver source tables are never touched.
    spark.sql(f"DROP TABLE IF EXISTS {fqn} PURGE")
    df.writeTo(fqn).using("iceberg").tableProperty("format-version", "2").create()
    df.unpersist()
    logger.info(f"{name}: written to {fqn}")


SILVER = f"{S3T_CAT}.{DB_SILVER}"

# ── 1. customer_analytics (monthly) ──────────────────────────────────────────
# Distinct counts use COUNT(DISTINCT user_id) so a customer with several orders in
# a month is counted once. A "new" customer's order has customer_order_sequence = 1.
CUSTOMER_ANALYTICS_SQL = f"""
    SELECT
        date_trunc('month', order_date) AS analysis_month,
        COUNT(DISTINCT user_id) AS total_customers,
        COUNT(DISTINCT CASE WHEN customer_order_sequence = 1 THEN user_id END) AS new_customers,
        COUNT(DISTINCT CASE WHEN customer_order_sequence > 1 THEN user_id END) AS returning_customers,
        COUNT(*) AS total_orders,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_order_value,
        SUM(CASE WHEN customer_segment = 'New' THEN 1 ELSE 0 END) AS new_customer_orders,
        SUM(CASE WHEN customer_segment = 'Repeat' THEN 1 ELSE 0 END) AS repeat_customer_orders,
        SUM(CASE WHEN customer_segment = 'Loyal' THEN 1 ELSE 0 END) AS loyal_customer_orders,
        SUM(CASE WHEN customer_segment = 'VIP' THEN 1 ELSE 0 END) AS vip_customer_orders,
        COUNT(DISTINCT city) AS cities_served,
        COUNT(DISTINCT state) AS states_served,
        SUM(CASE WHEN customer_segment = 'New' THEN total_amount ELSE 0 END) AS new_customer_revenue,
        SUM(CASE WHEN customer_segment = 'Repeat' THEN total_amount ELSE 0 END) AS repeat_customer_revenue,
        SUM(CASE WHEN customer_segment = 'Loyal' THEN total_amount ELSE 0 END) AS loyal_customer_revenue,
        SUM(CASE WHEN customer_segment = 'VIP' THEN total_amount ELSE 0 END) AS vip_customer_revenue,
        AVG(customer_lifetime_value) AS avg_customer_lifetime_value,
        current_timestamp() AS gold_processed_at
    FROM {SILVER}.order_details
    WHERE order_date IS NOT NULL   -- source has invalid dates (e.g. 2025-02-29) that cast to NULL
    GROUP BY date_trunc('month', order_date)
"""
create_gold_table("customer_analytics", CUSTOMER_ANALYTICS_SQL)

# ── 2. realtime_metrics (hourly) ─────────────────────────────────────────────
# revenue in silver.enriched_events is the product price on purchase events.
REALTIME_METRICS_SQL = f"""
    SELECT
        date_trunc('hour', event_timestamp) AS metric_hour,
        COUNT(*) AS total_events,
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(DISTINCT session_id) AS unique_sessions,
        SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_views,
        SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_carts,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
        SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS clicks,
        COALESCE(SUM(CASE WHEN event_type = 'purchase' THEN revenue END), 0.0) AS hourly_revenue,
        COALESCE(AVG(CASE WHEN event_type = 'purchase' THEN revenue END), 0.0) AS avg_purchase_value,
        SUM(CASE WHEN source = 'google' THEN 1 ELSE 0 END) AS google_traffic,
        SUM(CASE WHEN source = 'facebook' THEN 1 ELSE 0 END) AS facebook_traffic,
        SUM(CASE WHEN source = 'direct' THEN 1 ELSE 0 END) AS direct_traffic,
        SUM(CASE WHEN source = 'instagram' THEN 1 ELSE 0 END) AS instagram_traffic,
        SUM(CASE WHEN source = 'youtube' THEN 1 ELSE 0 END) AS youtube_traffic,
        SUM(CASE WHEN source = 'email' THEN 1 ELSE 0 END) AS email_traffic,
        SUM(CASE WHEN source = 'referral' THEN 1 ELSE 0 END) AS referral_traffic,
        COUNT(DISTINCT product_category) AS categories_viewed,
        current_timestamp() AS gold_processed_at
    FROM {SILVER}.enriched_events
    WHERE event_date >= DATE '2024-01-01'
    GROUP BY date_trunc('hour', event_timestamp)
"""
create_gold_table("realtime_metrics", REALTIME_METRICS_SQL)

logger.info("Gold layer complete — tables written to S3 Tables")
job.commit()
