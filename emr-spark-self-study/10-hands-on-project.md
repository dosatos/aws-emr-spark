# Module 10: Hands-On Project
## Petabyte-Scale Periodic S3 Pipeline

> **Difficulty:** 🔴 Advanced
> **Prerequisite:** Modules 01–09
> **Est. time:** 8–12 hours (build) + ongoing experimentation

---

## Project Overview

**Name:** E-Commerce Event Processing Pipeline

**Business description:**
An e-commerce platform generates clickstream events (views, searches, add-to-cart, purchases, returns) from web and mobile users. These events land in S3 as JSON every 5 minutes via a Kafka consumer. Your job is to build the processing pipeline.

**What you're building:**
A fully operational, production-grade batch pipeline that:
1. Reads raw JSON events from an S3 Bronze layer
2. Cleans and standardizes them into a Parquet Silver layer
3. Computes daily user behavior metrics into a Gold layer
4. Writes high-priority user metrics to DynamoDB for API lookups
5. Is idempotent, observable, and cost-optimized
6. Runs nightly on EMR

---

## Architecture Overview

```
                     RAW DATA SOURCES
                           │
                    (Kafka Consumer)
                    writes every 5 min
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER                              │
│   s3://ecom-data/bronze/events/ingestion_date=YYYY-MM-DD/        │
│   Format: JSONL (gzipped)                                        │
│   Schema: raw, minimal validation                                │
│   Retention: 7 years (regulatory)                                │
└──────────────────────────────────────────────────────────────────┘
                           │
                  (Daily Spark Job A: Bronze→Silver)
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER                              │
│   s3://ecom-data/silver/events/event_date=YYYY-MM-DD/            │
│   Format: Parquet (Snappy compressed)                            │
│   Schema: explicit, validated, deduplicated, enriched            │
│   Retention: 3 years                                             │
└──────────────────────────────────────────────────────────────────┘
                           │
                  (Daily Spark Job B: Silver→Gold)
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER                                │
│   s3://ecom-data/gold/user_daily_metrics/date=YYYY-MM-DD/        │
│   Format: Parquet (Snappy compressed)                            │
│   Schema: aggregated user metrics                                │
│   Retention: 5 years                                             │
└──────────────────────────────────────────────────────────────────┘
                           │
                  (DynamoDB Write: Gold→DynamoDB)
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                    DYNAMODB (Serving Layer)                        │
│   Table: UserDailyMetrics                                         │
│   PK: USER#<user_id>, SK: METRICS#<date>                        │
│   Used by: User Profile API (real-time lookups)                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Part 1: Project Setup

### 1.1 Create S3 buckets

```bash
# Create bucket structure
aws s3 mb s3://ecom-data-{your-account-id} --region us-east-1

# Create prefixes (folders)
aws s3api put-object --bucket ecom-data-{your-account-id} --key bronze/
aws s3api put-object --bucket ecom-data-{your-account-id} --key silver/
aws s3api put-object --bucket ecom-data-{your-account-id} --key gold/
aws s3api put-object --bucket ecom-data-{your-account-id} --key jobs/
aws s3api put-object --bucket ecom-data-{your-account-id} --key checkpoints/
aws s3api put-object --bucket ecom-data-{your-account-id} --key emr-logs/
```

### 1.2 Create DynamoDB table

```bash
aws dynamodb create-table \
  --table-name UserDailyMetrics \
  --attribute-definitions \
    AttributeName=pk,AttributeType=S \
    AttributeName=sk,AttributeType=S \
  --key-schema \
    AttributeName=pk,KeyType=HASH \
    AttributeName=sk,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1
```

### 1.3 Create IAM roles

```bash
# EMR service role (use default or customize)
aws iam create-role --role-name EMR_DefaultRole \
  --assume-role-policy-document file://emr-trust-policy.json

# EC2 instance profile for cluster nodes
aws iam create-role --role-name EMR_EC2_DefaultRole \
  --assume-role-policy-document file://ec2-trust-policy.json

# Attach S3 and DynamoDB access
aws iam put-role-policy \
  --role-name EMR_EC2_DefaultRole \
  --policy-name DataAccess \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": ["s3:*"],
        "Resource": ["arn:aws:s3:::ecom-data-*", "arn:aws:s3:::ecom-data-*/*"]
      },
      {
        "Effect": "Allow",
        "Action": ["dynamodb:PutItem", "dynamodb:BatchWriteItem", "dynamodb:GetItem"],
        "Resource": "arn:aws:dynamodb:us-east-1:*:table/UserDailyMetrics"
      },
      {
        "Effect": "Allow",
        "Action": ["cloudwatch:PutMetricData"],
        "Resource": "*"
      }
    ]
  }'
```

---

## Part 2: Generate Test Data

```python
# generate_test_data.py
# Run locally to create synthetic test data in S3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random

def generate_events(spark: SparkSession, date: str, num_events: int = 5_000_000):
    """Generate synthetic e-commerce events."""

    # Create a DataFrame with random data
    df = spark.range(num_events).select(
        F.expr("uuid()").alias("event_id"),
        F.expr("concat('user_', cast(abs(rand() * 1000000) as int))").alias("user_id"),
        F.when(F.rand() < 0.5, "view")
         .when(F.rand() < 0.75, "search")
         .when(F.rand() < 0.9, "add_to_cart")
         .when(F.rand() < 0.97, "purchase")
         .otherwise("return").alias("event_type"),
        (F.rand() * 500).alias("amount"),
        F.when(F.rand() < 0.6, "US")
         .when(F.rand() < 0.8, "EU")
         .when(F.rand() < 0.9, "APAC")
         .otherwise("LATAM").alias("region"),
        F.when(F.rand() < 0.6, "web")
         .when(F.rand() < 0.85, "mobile_ios")
         .otherwise("mobile_android").alias("platform"),
        # Simulate events throughout the day
        (F.to_timestamp(F.lit(date)) +
         F.expr(f"INTERVAL {int(random.random() * 86400)} SECONDS")).alias("event_time"),
        # Simulate ~2% late arrivals (events will appear in Bronze the next day)
        F.lit(date).alias("ingestion_date"),
        F.expr("concat('prod_', cast(abs(rand() * 10000) as int))").alias("product_id"),
    )

    return df


if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else "2024-01-15"
    bucket = sys.argv[2] if len(sys.argv) > 2 else "ecom-data-123456789"

    spark = SparkSession.builder \
        .appName("generate-test-data") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    df = generate_events(spark, date)

    # Convert to JSON (simulating Kafka consumer output)
    output_path = f"s3://{bucket}/bronze/events/ingestion_date={date}/"
    df.coalesce(50) \
      .write.mode("overwrite") \
      .json(output_path)

    print(f"Generated {df.count():,} events → {output_path}")
    spark.stop()
```

---

## Part 3: Bronze to Silver Pipeline

```python
# bronze_to_silver.py
"""
Bronze → Silver transformation.
Reads raw JSON events, cleans, validates, deduplicates, and writes Parquet.
"""

import argparse
import logging
import json
from datetime import datetime
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)


# Explicit schema for raw JSON (be permissive — this is Bronze)
RAW_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=True),
    StructField("user_id", StringType(), nullable=True),
    StructField("event_type", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("region", StringType(), nullable=True),
    StructField("platform", StringType(), nullable=True),
    StructField("event_time", TimestampType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("ingestion_date", StringType(), nullable=True),
])

VALID_EVENT_TYPES = {"view", "search", "add_to_cart", "purchase", "return"}
VALID_REGIONS = {"US", "EU", "APAC", "LATAM", "ME"}
VALID_PLATFORMS = {"web", "mobile_ios", "mobile_android", "api"}


def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("bronze-to-silver") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(128 * 1024 * 1024)) \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.speculation", "true") \
        .getOrCreate()


def read_bronze(spark: SparkSession, bucket: str, ingestion_date: str):
    """Read raw JSON events from Bronze layer."""
    path = f"s3://{bucket}/bronze/events/ingestion_date={ingestion_date}/"
    logger.info(f"Reading Bronze: {path}")

    df = spark.read \
        .schema(RAW_SCHEMA) \
        .json(path)

    count = df.count()
    logger.info(f"Loaded {count:,} raw events")
    return df


def validate_and_clean(df):
    """Apply business rules: clean, validate, standardize."""
    logger.info("Applying validation and cleaning")

    # Cast event_time to UTC (assume source is UTC)
    # Mark records with validation failures rather than dropping them
    df_validated = df \
        .withColumn("event_id",
            F.when(F.col("event_id").isNull(), F.expr("uuid()"))
             .otherwise(F.col("event_id"))
        ) \
        .withColumn("event_type_clean",
            F.when(F.lower(F.col("event_type")).isin(list(VALID_EVENT_TYPES)),
                   F.lower(F.col("event_type")))
             .otherwise(F.lit("unknown"))
        ) \
        .withColumn("region_clean",
            F.when(F.upper(F.col("region")).isin(list(VALID_REGIONS)),
                   F.upper(F.col("region")))
             .otherwise(F.lit("OTHER"))
        ) \
        .withColumn("amount_clean",
            F.when(F.col("amount").isNull() | (F.col("amount") < 0), F.lit(0.0))
             .otherwise(F.col("amount"))
        ) \
        .withColumn("is_valid",
            F.col("user_id").isNotNull() &
            F.col("event_time").isNotNull() &
            (F.col("amount_clean") >= 0)
        ) \
        .withColumn("event_date",
            F.date_format(F.col("event_time"), "yyyy-MM-dd")
        ) \
        .select(
            "event_id",
            "user_id",
            F.col("event_type_clean").alias("event_type"),
            F.col("amount_clean").alias("amount"),
            F.col("region_clean").alias("region"),
            "platform",
            "event_time",
            "event_date",
            "product_id",
            "is_valid",
        )

    valid_count = df_validated.filter(F.col("is_valid")).count()
    invalid_count = df_validated.filter(~F.col("is_valid")).count()
    logger.info(f"Validation: {valid_count:,} valid, {invalid_count:,} invalid")

    # Only pass valid events downstream (store invalid separately if needed)
    return df_validated.filter(F.col("is_valid")).drop("is_valid")


def deduplicate(df):
    """Remove duplicate events, keeping the latest version."""
    logger.info("Deduplicating events")

    from pyspark.sql.window import Window

    window = Window.partitionBy("event_id").orderBy(F.desc("event_time"))

    df_deduped = df \
        .withColumn("rn", F.row_number().over(window)) \
        .filter(F.col("rn") == 1) \
        .drop("rn")

    before = df.count()
    after = df_deduped.count()
    logger.info(f"Deduplication: {before:,} → {after:,} events ({before-after:,} duplicates removed)")

    return df_deduped


def write_silver(df, bucket: str, ingestion_date: str):
    """Write cleaned events to Silver layer, partitioned by event_date."""
    # Note: we write by event_date (when the event happened),
    # not ingestion_date (when it was processed)
    # This means events can write to multiple event_date partitions

    output_path = f"s3://{bucket}/silver/events/"
    logger.info(f"Writing Silver: {output_path}")

    # Repartition to control file size
    # Target: 128 MB per file, estimate ~1M events = ~500 MB uncompressed
    # Silver is Parquet (compressed): expect ~5x compression → ~100 MB per 1M events
    # If 5M events → ~500 MB compressed → 4 files per event_date
    df.repartition("event_date") \
      .coalesce(5) \
      .write \
      .mode("overwrite") \
      .partitionBy("event_date") \
      .parquet(output_path)

    logger.info("Silver write complete")


def publish_metrics(pipeline: str, date: str, metrics: dict):
    """Publish metrics to CloudWatch."""
    cloudwatch = boto3.client('cloudwatch', region_name='us-east-1')
    metric_data = [
        {
            'MetricName': name,
            'Dimensions': [
                {'Name': 'Pipeline', 'Value': pipeline},
                {'Name': 'Date', 'Value': date},
            ],
            'Value': float(value),
            'Unit': 'Count' if isinstance(value, int) else 'None',
        }
        for name, value in metrics.items()
    ]
    cloudwatch.put_metric_data(Namespace='DataPlatform', MetricData=metric_data)


def main():
    parser = argparse.ArgumentParser(description="Bronze to Silver pipeline")
    parser.add_argument("--ingestion-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--bucket", default="ecom-data-123456789")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    start_time = datetime.now()
    logger.info(f"Starting bronze-to-silver for ingestion_date={args.ingestion_date}")

    spark = create_spark_session()

    try:
        # ETL
        raw = read_bronze(spark, args.bucket, args.ingestion_date)
        cleaned = validate_and_clean(raw)
        deduped = deduplicate(cleaned)

        if args.dry_run:
            logger.info("[DRY RUN] Showing sample output:")
            deduped.show(20, truncate=False)
            return

        write_silver(deduped, args.bucket, args.ingestion_date)

        # Publish metrics
        duration = (datetime.now() - start_time).total_seconds()
        publish_metrics("bronze-to-silver", args.ingestion_date, {
            "InputRowCount": raw.count(),
            "OutputRowCount": deduped.count(),
            "DurationSeconds": int(duration),
        })

        logger.info(f"Pipeline complete in {duration:.0f}s")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

---

## Part 4: Silver to Gold Pipeline

```python
# silver_to_gold.py
"""
Silver → Gold transformation.
Computes daily user behavior metrics from clean event data.
"""

import argparse
import logging
from datetime import datetime
import boto3
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


# Silver layer schema (explicit — never infer in production)
SILVER_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("region", StringType(), nullable=True),
    StructField("platform", StringType(), nullable=True),
    StructField("event_time", TimestampType(), nullable=False),
    StructField("event_date", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=True),
])


def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("silver-to-gold") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.shuffle.partitions", "400") \
        .getOrCreate()


def read_silver(spark: SparkSession, bucket: str, event_date: str):
    """Read cleaned events for a specific event date."""
    path = f"s3://{bucket}/silver/events/event_date={event_date}/"
    logger.info(f"Reading Silver: {path}")

    df = spark.read.schema(SILVER_SCHEMA).parquet(path)
    count = df.count()
    logger.info(f"Loaded {count:,} events for date={event_date}")

    assert count > 0, f"No events found for date={event_date}"
    return df


def compute_user_daily_metrics(df, event_date: str):
    """Compute per-user per-day metrics."""
    logger.info("Computing user daily metrics")

    # Compute comprehensive user metrics
    metrics = df.groupBy("user_id", "region", "platform") \
        .agg(
            # Activity metrics
            F.count("*").alias("total_events"),
            F.countDistinct("event_id").alias("unique_events"),
            F.countDistinct("product_id").alias("unique_products_viewed"),

            # Purchase metrics
            F.sum(F.when(F.col("event_type") == "purchase", F.col("amount")).otherwise(0))
             .alias("purchase_amount"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0))
             .alias("purchase_count"),

            # Funnel metrics
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0))
             .alias("view_count"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0))
             .alias("add_to_cart_count"),
            F.sum(F.when(F.col("event_type") == "return", 1).otherwise(0))
             .alias("return_count"),

            # Session metrics
            F.min("event_time").alias("first_event_time"),
            F.max("event_time").alias("last_event_time"),
        ) \
        .withColumn("date", F.lit(event_date)) \
        .withColumn("session_duration_minutes",
            (F.unix_timestamp("last_event_time") -
             F.unix_timestamp("first_event_time")) / 60
        ) \
        .withColumn("conversion_rate",
            F.when(F.col("view_count") > 0,
                   F.col("purchase_count") / F.col("view_count"))
             .otherwise(0.0)
        )

    logger.info(f"Computed metrics for {metrics.count():,} users")
    return metrics


def write_gold(df, bucket: str):
    """Write metrics to Gold layer."""
    output_path = f"s3://{bucket}/gold/user_daily_metrics/"
    logger.info(f"Writing Gold: {output_path}")

    # Gold layer has fewer rows per date partition (aggregated from events)
    # Target: ~1 file per 128 MB; if 1M users × ~1 KB = ~1 GB → 8 files
    df.coalesce(8) \
      .write \
      .mode("overwrite") \
      .partitionBy("date") \
      .parquet(output_path)

    logger.info("Gold write complete")


def write_dynamo(df, dynamo_table: str, date: str):
    """Write high-value user metrics to DynamoDB for API serving."""
    logger.info(f"Writing to DynamoDB: {dynamo_table}")

    # Only write users with meaningful activity (e.g., made a purchase)
    high_value_users = df.filter(F.col("purchase_count") > 0)
    count = high_value_users.count()
    logger.info(f"Writing {count:,} high-value users to DynamoDB")

    def write_partition_to_dynamo(rows, table_name=dynamo_table):
        """Write a partition of rows to DynamoDB using batch writes."""
        import boto3
        from decimal import Decimal

        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table(table_name)

        BATCH_SIZE = 25
        batch = []

        for row in rows:
            item = {
                'pk': f"USER#{row.user_id}",
                'sk': f"METRICS#{row.date}",
                # Remove None values (DynamoDB doesn't support null)
                **{k: (Decimal(str(v)) if isinstance(v, float) else v)
                   for k, v in row.asDict().items()
                   if v is not None and k not in ('pk', 'sk')
                }
            }
            batch.append({'PutRequest': {'Item': item}})

            if len(batch) == BATCH_SIZE:
                _flush_batch(dynamodb, table_name, batch)
                batch = []

        if batch:
            _flush_batch(dynamodb, table_name, batch)

    def _flush_batch(dynamodb, table_name: str, batch: list, retries: int = 3):
        for attempt in range(retries):
            response = dynamodb.batch_write_item(
                RequestItems={table_name: batch}
            )
            unprocessed = response.get('UnprocessedItems', {}).get(table_name, [])
            if not unprocessed:
                return
            batch = unprocessed
            import time
            time.sleep(2 ** attempt)  # exponential backoff
        raise RuntimeError(f"Failed to write {len(batch)} items after {retries} retries")

    high_value_users.foreachPartition(write_partition_to_dynamo)
    logger.info("DynamoDB write complete")


def main():
    parser = argparse.ArgumentParser(description="Silver to Gold pipeline")
    parser.add_argument("--event-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--bucket", default="ecom-data-123456789")
    parser.add_argument("--dynamo-table", default="UserDailyMetrics")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    start_time = datetime.now()
    logger.info(f"Starting silver-to-gold for event_date={args.event_date}")

    spark = create_spark_session()

    try:
        events = read_silver(spark, args.bucket, args.event_date)
        metrics = compute_user_daily_metrics(events, args.event_date)

        if args.dry_run:
            logger.info("[DRY RUN] Sample output:")
            metrics.show(10, truncate=False)
            return

        write_gold(metrics, args.bucket)
        write_dynamo(metrics, args.dynamo_table, args.event_date)

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Pipeline complete in {duration:.0f}s")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

---

## Part 5: EMR Cluster Configuration

```python
# create_cluster.py
"""Create a transient EMR cluster for a pipeline run."""

import boto3
import time
import argparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_emr_cluster(
    date: str,
    bucket: str,
    region: str = "us-east-1",
    core_instance_count: int = 10,
    task_instance_count: int = 20,
) -> str:
    """Create a transient EMR cluster and return cluster ID."""

    emr = boto3.client('emr', region_name=region)

    response = emr.run_job_flow(
        Name=f"ecom-pipeline-{date}",
        ReleaseLabel="emr-7.2.0",
        Applications=[{"Name": "Spark"}, {"Name": "Ganglia"}],

        Instances={
            "InstanceFleets": [
                {
                    "InstanceFleetType": "MASTER",
                    "TargetOnDemandCapacity": 1,
                    "InstanceTypeConfigs": [{"InstanceType": "m5.xlarge"}],
                },
                {
                    "InstanceFleetType": "CORE",
                    "TargetOnDemandCapacity": core_instance_count,
                    "InstanceTypeConfigs": [
                        {"InstanceType": "r5.4xlarge"},
                        {"InstanceType": "r5a.4xlarge"},
                        {"InstanceType": "r6g.4xlarge"},
                    ],
                },
                {
                    "InstanceFleetType": "TASK",
                    "TargetOnDemandCapacity": 0,
                    "TargetSpotCapacity": task_instance_count,
                    "InstanceTypeConfigs": [
                        {"InstanceType": "r5.4xlarge", "BidPrice": "OnDemandPrice"},
                        {"InstanceType": "r5a.4xlarge", "BidPrice": "OnDemandPrice"},
                        {"InstanceType": "r5d.4xlarge", "BidPrice": "OnDemandPrice"},
                    ],
                    "LaunchSpecifications": {
                        "SpotSpecification": {
                            "TimeoutDurationMinutes": 10,
                            "TimeoutAction": "SWITCH_TO_ON_DEMAND",
                        }
                    },
                },
            ],
            "Ec2SubnetId": "subnet-xxxxxxxxxx",  # Replace with your subnet
            "KeepJobFlowAliveWhenNoSteps": False,  # Auto-terminate when done
        },

        Configurations=[
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.sql.adaptive.skewJoin.enabled": "true",
                    "spark.sql.sources.partitionOverwriteMode": "dynamic",
                    "spark.speculation": "true",
                    "spark.speculation.multiplier": "2.0",
                    "spark.executor.memoryOverhead": "8g",
                }
            },
            {
                "Classification": "spark",
                "Properties": {
                    "maximizeResourceAllocation": "false",  # We control executor config
                }
            },
        ],

        Steps=[
            # Step 1: Bronze → Silver
            {
                "Name": "Bronze-to-Silver",
                "ActionOnFailure": "TERMINATE_CLUSTER",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode", "cluster",
                        "--executor-memory", "40g",
                        "--executor-cores", "5",
                        "--driver-memory", "8g",
                        "--conf", "spark.executor.memoryOverhead=8g",
                        "--conf", "spark.sql.shuffle.partitions=400",
                        f"s3://{bucket}/jobs/bronze_to_silver.py",
                        "--ingestion-date", date,
                        "--bucket", bucket,
                    ]
                }
            },
            # Step 2: Silver → Gold + DynamoDB
            {
                "Name": "Silver-to-Gold",
                "ActionOnFailure": "TERMINATE_CLUSTER",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode", "cluster",
                        "--executor-memory", "40g",
                        "--executor-cores", "5",
                        "--driver-memory", "8g",
                        "--conf", "spark.executor.memoryOverhead=8g",
                        "--conf", "spark.sql.shuffle.partitions=200",
                        f"s3://{bucket}/jobs/silver_to_gold.py",
                        "--event-date", date,
                        "--bucket", bucket,
                        "--dynamo-table", "UserDailyMetrics",
                    ]
                }
            }
        ],

        LogUri=f"s3://{bucket}/emr-logs/",
        ServiceRole="EMR_DefaultRole",
        JobFlowRole="EMR_EC2_DefaultRole",
        VisibleToAllUsers=True,
        AutoTerminate=True,  # Terminate cluster when all steps complete

        Tags=[
            {"Key": "Project", "Value": "ecom-pipeline"},
            {"Key": "Date", "Value": date},
            {"Key": "Environment", "Value": "production"},
        ],
    )

    cluster_id = response["JobFlowId"]
    logger.info(f"Created cluster: {cluster_id}")
    return cluster_id


def wait_for_cluster(cluster_id: str, region: str = "us-east-1") -> bool:
    """Wait for cluster to complete. Returns True on success, False on failure."""
    emr = boto3.client('emr', region_name=region)

    TERMINAL_STATES = {"TERMINATED", "TERMINATED_WITH_ERRORS", "FAILED"}
    SUCCESS_STATES = {"TERMINATED"}

    while True:
        response = emr.describe_cluster(ClusterId=cluster_id)
        state = response["Cluster"]["Status"]["State"]
        state_reason = response["Cluster"]["Status"]["StateChangeReason"].get("Message", "")

        logger.info(f"Cluster {cluster_id} state: {state}")

        if state in TERMINAL_STATES:
            if state == "TERMINATED":
                # Check if all steps succeeded
                steps = emr.list_steps(ClusterId=cluster_id)["Steps"]
                failed_steps = [s for s in steps if s["Status"]["State"] != "COMPLETED"]
                if failed_steps:
                    logger.error(f"Steps failed: {[s['Name'] for s in failed_steps]}")
                    return False
                return True
            else:
                logger.error(f"Cluster failed: {state_reason}")
                return False

        time.sleep(60)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--bucket", default="ecom-data-123456789")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    cluster_id = create_emr_cluster(args.date, args.bucket, args.region)
    success = wait_for_cluster(cluster_id, args.region)

    if success:
        logger.info(f"Pipeline for {args.date} completed successfully")
    else:
        logger.error(f"Pipeline for {args.date} FAILED — cluster: {cluster_id}")
        exit(1)


if __name__ == "__main__":
    main()
```

---

## Part 6: Testing Your Pipeline Locally

Before running on EMR, test locally with PySpark:

```bash
# Install PySpark locally
pip install pyspark==3.5.0 boto3 pytest

# Generate small local test data
python generate_test_data.py 2024-01-15 my-test-bucket

# Test Bronze → Silver locally (small dataset)
python bronze_to_silver.py \
  --ingestion-date 2024-01-15 \
  --bucket my-test-bucket \
  --dry-run

# Test Silver → Gold locally
python silver_to_gold.py \
  --event-date 2024-01-15 \
  --bucket my-test-bucket \
  --dry-run
```

---

## Part 7: Deploy and Run on EMR

```bash
# 1. Upload job scripts to S3
aws s3 cp bronze_to_silver.py s3://ecom-data-123456789/jobs/
aws s3 cp silver_to_gold.py s3://ecom-data-123456789/jobs/

# 2. Generate test data in S3
aws emr create-cluster \
  --release-label emr-7.2.0 \
  --applications Name=Spark \
  --instance-groups \
    InstanceGroupType=MASTER,InstanceType=m5.xlarge,InstanceCount=1 \
  --steps \
    Name=GenerateTestData,ActionOnFailure=TERMINATE_CLUSTER,\
    HadoopJarStep={Jar=command-runner.jar,Args=[\
      spark-submit,--deploy-mode,cluster,\
      s3://ecom-data-123456789/jobs/generate_test_data.py,\
      2024-01-15,ecom-data-123456789]} \
  --auto-terminate \
  --log-uri s3://ecom-data-123456789/emr-logs/

# 3. Run the full pipeline
python create_cluster.py --date 2024-01-15 --bucket ecom-data-123456789

# 4. Verify output
aws s3 ls s3://ecom-data-123456789/silver/events/event_date=2024-01-15/
aws s3 ls s3://ecom-data-123456789/gold/user_daily_metrics/date=2024-01-15/
```

---

## Part 8: Validate the Pipeline

```python
# validate_pipeline.py — run after pipeline to verify correctness
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def validate_pipeline_output(spark: SparkSession, bucket: str, date: str):
    """Comprehensive validation of pipeline output."""
    print(f"\n{'='*60}")
    print(f"Pipeline Validation for date={date}")
    print('='*60)

    # --- Silver Layer Checks ---
    silver_path = f"s3://{bucket}/silver/events/event_date={date}/"
    silver = spark.read.parquet(silver_path)

    silver_count = silver.count()
    print(f"\n[Silver Layer]")
    print(f"  Row count: {silver_count:,}")

    # Check for nulls in critical columns
    for col in ["event_id", "user_id", "event_time"]:
        null_count = silver.filter(F.col(col).isNull()).count()
        status = "✅" if null_count == 0 else "❌"
        print(f"  {status} Null {col}: {null_count}")

    # Check event type distribution
    print("\n  Event type distribution:")
    silver.groupBy("event_type").count().orderBy(F.desc("count")).show()

    # Check for negative amounts
    neg_amounts = silver.filter(F.col("amount") < 0).count()
    print(f"  {'✅' if neg_amounts == 0 else '❌'} Negative amounts: {neg_amounts}")

    # --- Gold Layer Checks ---
    gold_path = f"s3://{bucket}/gold/user_daily_metrics/date={date}/"
    gold = spark.read.parquet(gold_path)

    gold_count = gold.count()
    print(f"\n[Gold Layer]")
    print(f"  User count: {gold_count:,}")
    print(f"  Users with purchases: {gold.filter(F.col('purchase_count') > 0).count():,}")
    print(f"  Total purchase amount: ${gold.agg(F.sum('purchase_amount')).collect()[0][0]:,.2f}")

    # Check gold-silver relationship
    gold_total_events = gold.agg(F.sum("total_events")).collect()[0][0]
    ratio = gold_total_events / silver_count
    status = "✅" if 0.95 <= ratio <= 1.05 else "⚠️"
    print(f"  {status} Events ratio (gold/silver): {ratio:.3f} (should be ~1.0)")

    # --- Sample Output ---
    print("\n[Sample Gold Output]")
    gold.filter(F.col("purchase_count") > 0) \
        .orderBy(F.desc("purchase_amount")) \
        .select("user_id", "region", "purchase_count", "purchase_amount", "view_count") \
        .show(10)

    print("\n✅ Validation complete")


if __name__ == "__main__":
    import sys
    date = sys.argv[1]
    bucket = sys.argv[2] if len(sys.argv) > 2 else "ecom-data-123456789"

    spark = SparkSession.builder.appName("validate").getOrCreate()
    validate_pipeline_output(spark, bucket, date)
    spark.stop()
```

---

## Part 9: Cost Analysis

For this pipeline running daily on the described cluster:

```
Cluster: 1 master (m5.xlarge) + 10 core (r5.4xlarge On-Demand) + 20 task (r5.4xlarge Spot)

Instance costs (us-east-1, On-Demand):
  Master:   1 × $0.192/hr
  Core:    10 × $1.008/hr = $10.08/hr
  Task:    20 × $1.008/hr × 0.3 (Spot ~70% discount) = $6.05/hr

EMR markup (~25% of EC2):
  Core EMR: $10.08 × 0.25 = $2.52/hr
  Task EMR: $6.05 × 0.25 = $1.51/hr

Total per hour: $0.192 + $10.08 + $6.05 + $2.52 + $1.51 = ~$20.35/hr

Pipeline runtime estimate:
  Cluster startup: ~8 min
  Step 1 (Bronze→Silver, 5M events): ~20 min
  Step 2 (Silver→Gold + DynamoDB): ~15 min
  Cluster shutdown: ~3 min
  Total: ~46 min ≈ 0.77 hours

Daily cost: 0.77 × $20.35 ≈ $15.67/day
Monthly cost: $15.67 × 30 ≈ $470/month
Annual cost: ~$5,650/year

Data storage (S3):
  Assuming 500 GB new data/day:
  Bronze (JSON, gzip): 500 GB × $0.023/GB-month = $11.50/month
  Silver (Parquet): 500 GB × 0.2 (compression) × $0.023 = $2.30/month
  Gold (Parquet): 50 GB × $0.023 = $1.15/month
  Storage total: ~$15/month (first year), growing with data volume

Total estimated monthly: ~$485/month for 150M events/month
```

---

## Part 10: Common Failure Scenarios and Remediation

### Scenario A: Pipeline fails with "No data in Bronze"

```bash
# Diagnose:
aws s3 ls s3://ecom-data-123456789/bronze/events/ingestion_date=2024-01-15/

# If empty: upstream Kafka consumer is delayed
# Check Kafka consumer health, then wait and retry

# Retry the pipeline:
python create_cluster.py --date 2024-01-15 --bucket ecom-data-123456789
# (pipeline is idempotent — safe to retry)
```

### Scenario B: Spot instance termination during shuffle

```
EMR Cluster logs show: ExecutorLostFailure / ShuffleMapTask failed
Spark retries the shuffle stage automatically
If cluster still has enough nodes: job completes (maybe slower)
If too many nodes lost: job fails
```

```bash
# Check if cluster failed:
aws emr describe-cluster --cluster-id j-XXXXX | grep State

# If failed, re-run (idempotent):
python create_cluster.py --date 2024-01-15

# Prevention: reduce spot ratio or use larger core fleet
```

### Scenario C: DynamoDB throttling

```
Error: ProvisionedThroughputExceededException
```

```python
# Fix: increase DynamoDB write capacity before pipeline
dynamodb = boto3.client('dynamodb')
dynamodb.update_table(
    TableName='UserDailyMetrics',
    BillingMode='PROVISIONED',
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 10000,  # Increase for bulk write
    }
)
# ... run pipeline ...
# Then scale back down
dynamodb.update_table(
    TableName='UserDailyMetrics',
    ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 100}
)
```

### Scenario D: Output looks wrong (silent corruption check)

```python
# Always run validate_pipeline.py after each run
# Set up a CloudWatch alarm if event count drops 30%+ vs 7-day average
```

---

## What's Next

After completing this project, you have a working production pipeline. Extend it with:

1. **Apache Iceberg:** Replace plain Parquet with Iceberg for ACID transactions, upserts, and schema evolution
2. **AWS Glue Data Catalog:** Register all layers as Glue tables for Athena querying
3. **Streaming Bronze ingestion:** Replace 5-minute batch JSON writes with Kinesis Firehose
4. **Data quality with Great Expectations:** Add automated data quality checks
5. **Cost optimization:** Implement lifecycle policies for Bronze archiving to Glacier
6. **Multi-region:** Add cross-region replication for DR

---

*Next: `11-glossary.md` →*
