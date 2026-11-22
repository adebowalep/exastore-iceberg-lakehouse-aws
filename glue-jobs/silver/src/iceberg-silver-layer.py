"""
AWS Glue Job: Silver Layer
Reads from Bronze Iceberg tables (Glue Catalog on S3), writes enriched/joined
Silver tables to S3 Tables (managed Iceberg).

Your task: Complete the TODOs below to build 3 silver tables.
  1. order_details       — orders enriched with customer context + window functions
  2. enriched_events     — events enriched with user + product context
  3. product_performance — product-level aggregates from events data

Key concepts:
  - Dual catalogs: glue_catalog (read bronze from S3) + s3tablescatalog (write silver to S3 Tables)
  - SparkSession.builder configures the S3 Tables catalog at startup
  - writeTo().create() writes DataFrames to S3 Tables (not CTAS SQL)
  - DROP TABLE ... PURGE is required for S3 Tables (managed Iceberg)
  - Namespaces are created via the Glue API (boto3), not Spark SQL
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

DB_BRONZE = args['database_name']
BUCKET = args['data_lake_bucket']
GOLD_TABLE_BUCKET_ARN = args['gold_table_bucket_arn']
AWS_REGION = args.get('aws_region', 'us-east-1')

# Parse the S3 Tables bucket ARN to extract account ID and bucket name
# ARN format: arn:aws:s3tables:region:ACCOUNT_ID:bucket/BUCKET_NAME
arn_parts = GOLD_TABLE_BUCKET_ARN.split(":")
ACCOUNT_ID = arn_parts[4]
TABLE_BUCKET_NAME = arn_parts[5].split("/")[1]

DB_SILVER = "ecommerce_silver"   # Namespace in S3 Tables bucket
CATALOG = "glue_catalog"         # Reads bronze from standard Glue Catalog
S3T_CAT = "s3tablescatalog"      # Writes silver to S3 Tables via federated catalog

sc = SparkContext()
glueContext = GlueContext(sc)

# Register the S3 Tables catalog next to the job's default glue_catalog (bronze).
# getOrCreate() returns the Glue-created session and applies these settings to it;
# the catalog is only instantiated on first use, so we verify it below.
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{S3T_CAT}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{S3T_CAT}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{S3T_CAT}.glue.id", f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}") \
    .config(f"spark.sql.catalog.{S3T_CAT}.warehouse", f"s3://{BUCKET}/silver-warehouse/") \
    .getOrCreate()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info(f"S3 Tables catalog: {S3T_CAT} → {TABLE_BUCKET_NAME}")
logger.info(f"Reading bronze from: {CATALOG}.{DB_BRONZE}")

import boto3
glue_client = boto3.client('glue', region_name=AWS_REGION)
try:
    glue_client.create_database(
        CatalogId=f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET_NAME}",
        DatabaseInput={'Name': DB_SILVER}
    )
    logger.info(f"Created namespace {DB_SILVER}")
except glue_client.exceptions.AlreadyExistsException:
    logger.info(f"Namespace {DB_SILVER} already exists")
# any other error (e.g. AccessDenied) propagates and fails the job

# Verify both catalogs are usable in this session before doing any work.
spark.sql(f"SHOW TABLES IN {CATALOG}.{DB_BRONZE}").show(truncate=False)
spark.sql(f"SHOW TABLES IN {S3T_CAT}.{DB_SILVER}").show(truncate=False)


def create_silver_table(name, query):
    """Query bronze, write results to S3 Tables silver namespace."""
    fqn = f"{S3T_CAT}.{DB_SILVER}.{name}"

    # Safety: only ever drop tables inside this project's silver namespace.
    assert fqn.startswith(f"{S3T_CAT}.{DB_SILVER}."), f"Refusing to drop outside silver: {fqn}"

    df = spark.sql(query).cache()
    row_count = df.count()          # materialise first: a failing query must not destroy the old table
    logger.info(f"{name}: {row_count} rows to write")
    if row_count == 0:
        df.unpersist()
        raise RuntimeError(f"{name}: query returned 0 rows; keeping existing table")

    # S3 Tables requires PURGE. Recreating the table resets its snapshot history.
    spark.sql(f"DROP TABLE IF EXISTS {fqn} PURGE")
    df.writeTo(fqn).using("iceberg").tableProperty("format-version", "2").create()
    df.unpersist()
    logger.info(f"{name}: written to {fqn}")


BRONZE = f"{CATALOG}.{DB_BRONZE}"

# ── 1. order_details ─────────────────────────────────────────────────────────
# orders JOIN users, with per-customer window functions. Segment thresholds use the
# customer's total order count: 1=New, 2-3=Repeat, 4-10=Loyal, >10=VIP.
ORDER_DETAILS_SQL = f"""
    SELECT
        o.order_id,
        o.user_id,
        o.total_amount,
        CAST(o.created_at AS DATE) AS order_date,
        date_format(o.created_at, 'yyyy-MM') AS order_month,
        concat(u.first_name, ' ', u.last_name) AS customer_name,
        u.email,
        u.city,
        u.state,
        CAST(u.created_at AS DATE) AS registration_date,
        COUNT(*) OVER (PARTITION BY o.user_id ORDER BY o.created_at, o.order_id
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS customer_order_sequence,
        SUM(o.total_amount) OVER (PARTITION BY o.user_id ORDER BY o.created_at, o.order_id
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS customer_lifetime_value,
        CASE WHEN o.total_amount < 50 THEN 'Low'
             WHEN o.total_amount < 200 THEN 'Medium'
             WHEN o.total_amount < 500 THEN 'High'
             ELSE 'Premium' END AS order_value_segment,
        CASE WHEN COUNT(*) OVER (PARTITION BY o.user_id) = 1 THEN 'New'
             WHEN COUNT(*) OVER (PARTITION BY o.user_id) <= 3 THEN 'Repeat'
             WHEN COUNT(*) OVER (PARTITION BY o.user_id) <= 10 THEN 'Loyal'
             ELSE 'VIP' END AS customer_segment,
        date_format(o.created_at, 'EEEE') AS order_day_of_week,
        hour(o.created_at) AS order_hour,
        current_timestamp() AS silver_processed_at
    FROM {BRONZE}.orders o
    JOIN {BRONZE}.users u ON o.user_id = u.id
"""
create_silver_table("order_details", ORDER_DETAILS_SQL)

# ── 2. enriched_events ───────────────────────────────────────────────────────
# LEFT JOINs keep every event even if the user/product is unknown. users.id and
# products.id are unique after the CDC MERGE, so no row multiplication.
ENRICHED_EVENTS_SQL = f"""
    SELECT
        e.event_id,
        e.session_id,
        e.user_id,
        e.product_id,
        e.page_url,
        e.source,
        e.event_type,
        e.event_timestamp,
        concat(u.first_name, ' ', u.last_name) AS user_name,
        u.email AS user_email,
        datediff(CAST(e.event_timestamp AS DATE), CAST(u.created_at AS DATE)) AS days_since_registration,
        u.city AS user_city,
        u.state AS user_state,
        p.name AS product_name,
        p.category AS product_category,
        p.price AS product_price,
        CASE WHEN e.event_type = 'purchase' THEN p.price END AS revenue,
        COUNT(*) OVER (PARTITION BY e.session_id ORDER BY e.event_timestamp, e.event_id
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS event_sequence_in_session,
        hour(e.event_timestamp) AS event_hour,
        date_format(e.event_timestamp, 'EEEE') AS event_day_of_week,
        CAST(e.event_timestamp AS DATE) AS event_date,
        current_timestamp() AS silver_processed_at
    FROM {BRONZE}.events e
    LEFT JOIN {BRONZE}.users u ON e.user_id = u.id
    LEFT JOIN {BRONZE}.products p ON e.product_id = p.id
    WHERE CAST(e.event_timestamp AS DATE) >= DATE '2024-01-01'
"""
create_silver_table("enriched_events", ENRICHED_EVENTS_SQL)

# ── 3. product_performance ───────────────────────────────────────────────────
# One row per product. Rates are percentages (0-100) and are 0 when the
# denominator is 0:
#   view_to_purchase_rate = purchases / page_views * 100
#   cart_to_purchase_rate = purchases / add_to_carts * 100
PRODUCT_PERFORMANCE_SQL = f"""
    SELECT
        p.id AS product_id,
        p.name AS product_name,
        p.category,
        p.price,
        p.stock_quantity,
        p.is_active,
        SUM(CASE WHEN e.event_type = 'page_view' THEN 1 ELSE 0 END) AS page_views,
        SUM(CASE WHEN e.event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_carts,
        SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
        SUM(CASE WHEN e.event_type = 'click' THEN 1 ELSE 0 END) AS clicks,
        COUNT(e.event_id) AS total_events,
        COUNT(DISTINCT e.user_id) AS unique_visitors,
        COUNT(DISTINCT e.session_id) AS unique_sessions,
        COALESCE(SUM(CASE WHEN e.event_type = 'purchase' THEN e.order_value END), 0.0) AS total_revenue,
        COALESCE(AVG(CASE WHEN e.event_type = 'purchase' THEN e.order_value END), 0.0) AS avg_purchase_value,
        COALESCE(ROUND(100.0 * SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END)
                 / NULLIF(SUM(CASE WHEN e.event_type = 'page_view' THEN 1 ELSE 0 END), 0), 2), 0.0) AS view_to_purchase_rate,
        COALESCE(ROUND(100.0 * SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END)
                 / NULLIF(SUM(CASE WHEN e.event_type = 'add_to_cart' THEN 1 ELSE 0 END), 0), 2), 0.0) AS cart_to_purchase_rate,
        current_timestamp() AS silver_processed_at
    FROM {BRONZE}.products p
    LEFT JOIN {BRONZE}.events e ON p.id = e.product_id
    GROUP BY p.id, p.name, p.category, p.price, p.stock_quantity, p.is_active
"""
create_silver_table("product_performance", PRODUCT_PERFORMANCE_SQL)

logger.info("Silver layer complete")
job.commit()
