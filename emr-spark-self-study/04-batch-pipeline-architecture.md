# Module 04: Batch Pipeline Architecture
## Designing End-to-End Data Pipelines

> **Difficulty:** 🟡 Intermediate
> **Prerequisite:** Modules 01–03
> **Est. time:** 4–5 hours

---

## 4.1 What Is a Batch Pipeline?

A **batch pipeline** processes a bounded, finite set of data and produces output. Contrast with streaming (processes unbounded, continuous data).

**Batch examples:**
- Every night at midnight, process all orders from the past day and update analytics tables
- Every hour, scan new log files and compute user engagement metrics
- Every Sunday, reprocess 6 months of transactions for regulatory reporting

**Your job spec:** Read data from S3 → process → write to S3 / DynamoDB. Long-running, periodic. Up to petabytes. This is a batch pipeline.

### The core batch pipeline loop

```
      ┌─────────────────────────────────────────────────────────┐
      │                   BATCH PIPELINE LOOP                    │
      │                                                          │
      │  Schedule Trigger (cron / EventBridge / Step Functions) │
      │           │                                             │
      │           ▼                                             │
      │  Determine what to process                              │
      │  (e.g., "date = yesterday")                             │
      │           │                                             │
      │           ▼                                             │
      │  Read source data from S3                               │
      │           │                                             │
      │           ▼                                             │
      │  Transform (filter, join, aggregate, enrich)            │
      │           │                                             │
      │           ▼                                             │
      │  Write output to S3 / DynamoDB                          │
      │           │                                             │
      │           ▼                                             │
      │  Signal success (update metadata, send notification)    │
      │           │                                             │
      │  Sleep until next trigger ──────────────────────────────┘
      └─────────────────────────────────────────────────────────┘
```

---

## 4.2 The Lambda Architecture (and Why You Might Not Need It)

You'll hear about "Lambda Architecture" in big data. Understand it so you can decide whether you need it.

### Lambda Architecture

Invented by Nathan Marz to solve the latency problem in large-scale batch systems:

```
Raw Data
   │
   ├──► Batch Layer ────────────────────► Batch Views (accurate, slow)
   │    (Hadoop/Spark, hours of latency)        │
   │                                            │
   └──► Speed Layer ─────────────────────► Real-time Views (fast, approximate)
        (Storm/Spark Streaming, seconds)         │
                                                 │
                                         Serving Layer merges both
```

**The problem it solves:** Batch jobs have high latency (hours). But users need recent data. Speed layer provides fast but potentially inaccurate recent data. Batch layer provides accurate historical data.

**The problems with Lambda Architecture:**
- You maintain TWO code paths for the same logic
- They can drift apart (bugs in one, not the other)
- Complex serving layer merges both views
- High operational burden

### Kappa Architecture (simpler alternative)

Proposed by Jay Kreps (Kafka creator): use only a streaming system. Reprocess historical data by replaying from Kafka topics.

### What most modern teams do

For most batch-primary pipelines: **neither**. Just run well-designed batch jobs on a tight schedule.

If your "batch" runs every 15 minutes, you effectively have near-real-time data with far less complexity than true streaming. For most business use cases, 15-minute latency is "real-time enough."

If you genuinely need second-level latency, then introduce streaming. But don't add it preemptively.

---

## 4.3 Medallion Architecture: Bronze, Silver, Gold

The **Medallion Architecture** is a widely-adopted pattern for organizing data in a data lake. It defines three layers:

```
Raw Sources          Bronze Layer         Silver Layer         Gold Layer
(S3 raw)            (s3://bucket/bronze/) (s3://bucket/silver/) (s3://bucket/gold/)

Kafka events ──────► Raw, unchanged       Cleaned, validated,   Business aggregates,
API responses        Copy of source        deduplicated,         metrics, ML features,
Database dumps       data                  joined with           serving-ready tables
Log files           Append-only           reference data
                    No transformations    Schema enforced
```

### Bronze layer

- **What:** Exact copy of source data, rarely modified
- **Format:** Often keeps source format (JSON, CSV) or converts to Parquet
- **Why:** Data lineage and auditing. If downstream layers have bugs, reprocess from Bronze.
- **Partitioned by:** Ingestion timestamp (when you received the data)

```
s3://my-bucket/bronze/events/
├── ingestion_date=2024-01-15/
│   ├── ingestion_hour=00/
│   │   └── events_00001.parquet
│   └── ingestion_hour=01/
│       └── events_00001.parquet
```

### Silver layer

- **What:** Cleaned, validated, standardized data
- **Format:** Parquet (always)
- **Transformations:** Schema validation, null handling, deduplication, type casting, PII masking, joining with dimension tables
- **Partitioned by:** Event timestamp (when the event happened), business keys

```
s3://my-bucket/silver/events/
├── event_date=2024-01-15/
│   ├── event_type=purchase/
│   │   └── part-00001.parquet
│   └── event_type=view/
│       └── part-00001.parquet
```

### Gold layer

- **What:** Business-ready aggregates and features
- **Format:** Parquet or DynamoDB / Redshift / Athena tables
- **Transformations:** Aggregations, metrics computation, ML feature generation
- **Partitioned by:** Business date, product, region — whatever your consumers query by

```
s3://my-bucket/gold/daily_user_metrics/
├── report_date=2024-01-15/
│   └── part-00001.parquet
```

### Why this pattern is good

1. **Each layer is independently queryable** — you can audit Bronze, validate Silver, serve from Gold
2. **Errors are isolated** — a bug in Silver doesn't corrupt Bronze. Fix and reprocess.
3. **Multiple consumers** — different teams can consume the layer appropriate to their needs
4. **Clear lineage** — you always know where data came from

---

## 4.4 Designing for Idempotency

This is one of the most important properties of a well-designed batch pipeline.

### What is idempotency?

An operation is **idempotent** if running it once produces the same result as running it N times.

```
Idempotent:     run("2024-01-15") → same output every time
Non-idempotent: run("2024-01-15") → different output each time (appends, duplicates)
```

### Why idempotency is critical for batch pipelines

In production, batch jobs fail. Networks blip. Spot instances get killed. Jobs time out. Your orchestrator retries the job.

If your job is not idempotent:
- First run: writes 1M rows to output
- Job crashes at 90%
- Retry: reads source, writes 1M rows AGAIN to output
- Result: 1.9M rows in output → **data corruption**

If your job is idempotent:
- First run: writes to temp location, then atomically moves to final
- Job crashes at 90%
- Retry: reads source, overwrites final location with clean 1M rows
- Result: exactly 1M rows → **correct**

### How to make your pipeline idempotent

**Rule 1: Overwrite, never append**

```python
# BAD — appending on retry creates duplicates
df.write.mode("append").parquet("s3://bucket/output/date=2024-01-15/")

# GOOD — overwriting is safe for retry
df.write.mode("overwrite").parquet("s3://bucket/output/date=2024-01-15/")
```

**Rule 2: Scope your overwrite precisely**

With dynamic partition overwrite, you overwrite exactly the partitions you're writing:

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite") \
        .partitionBy("date") \
        .parquet("s3://bucket/output/")
```

**Rule 3: Parameterize by time boundary**

Your job should accept a `--date` parameter and process exactly that date's data:

```python
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--date", required=True, help="Processing date: YYYY-MM-DD")
args = parser.parse_args()

# Read exactly this date's data
df = spark.read.parquet(f"s3://bucket/events/event_date={args.date}/")

# Process
result = transform(df)

# Write to exactly this date's output partition
result.write.mode("overwrite") \
             .parquet(f"s3://bucket/output/date={args.date}/")
```

This makes the job safe to retry: same input, same output, same date. Running it 5 times produces the same result.

**Rule 4: Deduplication**

If your source data might have duplicates (e.g., at-least-once delivery from Kafka), deduplicate at the Silver layer:

```python
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Keep only the latest event per event_id
window = Window.partitionBy("event_id").orderBy(F.desc("event_time"))
df_deduped = df.withColumn("rank", F.row_number().over(window)) \
               .filter(F.col("rank") == 1) \
               .drop("rank")
```

---

## 4.5 Pipeline Orchestration

Spark jobs don't run in isolation. They need to be scheduled, sequenced, and monitored.

### Orchestration options for EMR pipelines

**AWS Step Functions + EventBridge**
```
EventBridge (cron) → Step Functions State Machine → EMR Job
                              │
                    ┌─────────┴──────────┐
                    │  Success → Notify  │
                    │  Failure → Alert   │
                    │  Retry on failure  │
                    └────────────────────┘
```

Good for: AWS-native workflows, simple pipelines, no external tooling

**Apache Airflow (MWAA on AWS)**
```
Airflow DAG (Python)
  Task 1: Create EMR cluster
  Task 2: Submit Spark job
  Task 3: Wait for completion
  Task 4: Validate output
  Task 5: Terminate cluster
  On failure: Alert + mark for retry
```

Good for: Complex dependencies, many pipelines, data engineering teams

**AWS Glue Workflows**
Good for: Simple pipelines using Glue jobs (not EMR)

### Example: Step Functions + EMR

```json
{
  "Comment": "Daily Pipeline",
  "StartAt": "CreateCluster",
  "States": {
    "CreateCluster": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:createCluster.sync",
      "Parameters": {
        "Name": "daily-pipeline",
        "ReleaseLabel": "emr-7.2.0",
        "Applications": [{"Name": "Spark"}],
        "Instances": {
          "MasterInstanceType": "m5.xlarge",
          "SlaveInstanceType": "r5.4xlarge",
          "InstanceCount": 5
        },
        "AutoTerminate": false
      },
      "ResultPath": "$.cluster",
      "Next": "SubmitJob"
    },
    "SubmitJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:addStep.sync",
      "Parameters": {
        "ClusterId.$": "$.cluster.ClusterId",
        "Step": {
          "Name": "DailyPipeline",
          "ActionOnFailure": "CONTINUE",
          "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit", "--deploy-mode", "cluster",
                     "s3://bucket/jobs/pipeline.py", "--date.$", "$.date"]
          }
        }
      },
      "Next": "TerminateCluster"
    },
    "TerminateCluster": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:terminateCluster.sync",
      "Parameters": {"ClusterId.$": "$.cluster.ClusterId"},
      "End": true
    }
  }
}
```

### Example: Airflow DAG for EMR

```python
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from datetime import datetime, timedelta

CLUSTER_CONFIG = {
    "Name": "daily-pipeline",
    "ReleaseLabel": "emr-7.2.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {"InstanceRole": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1},
            {"InstanceRole": "CORE", "InstanceType": "r5.4xlarge", "InstanceCount": 4},
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
    },
    "LogUri": "s3://my-logs/emr/",
    "ServiceRole": "EMR_DefaultRole",
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "AutoTerminate": True,
}

SPARK_STEP = [
    {
        "Name": "DailyPipeline",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit", "--deploy-mode", "cluster",
                "--executor-memory", "8g", "--executor-cores", "4",
                "--conf", "spark.sql.adaptive.enabled=true",
                "s3://bucket/jobs/pipeline.py",
                "--date", "{{ ds }}"
            ]
        }
    }
]

with DAG(
    dag_id="daily_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",  # 2 AM daily
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
) as dag:

    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_cluster",
        job_flow_overrides=CLUSTER_CONFIG,
    )

    submit_step = EmrAddStepsOperator(
        task_id="submit_step",
        job_flow_id="{{ task_instance.xcom_pull('create_cluster', key='return_value') }}",
        steps=SPARK_STEP,
    )

    watch_step = EmrStepSensor(
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('create_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('submit_step', key='return_value')[0] }}",
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_cluster",
        job_flow_id="{{ task_instance.xcom_pull('create_cluster', key='return_value') }}",
        trigger_rule="all_done",  # Terminate even on failure
    )

    create_cluster >> submit_step >> watch_step >> terminate_cluster
```

---

## 4.6 Data Validation

Never trust your input data. Never ship output data you haven't validated.

### Input validation

```python
def validate_input(df, expected_date: str) -> None:
    """Validate input data before processing."""

    # Check row count is reasonable
    count = df.count()
    assert count > 0, f"No data found for date {expected_date}"
    assert count < 10_000_000_000, f"Suspiciously large dataset: {count} rows"

    # Check required columns exist
    required_cols = ["event_id", "user_id", "event_time", "amount"]
    missing = set(required_cols) - set(df.columns)
    assert not missing, f"Missing columns: {missing}"

    # Check null rates on critical columns
    null_counts = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in required_cols
    ]).collect()[0].asDict()

    for col, null_count in null_counts.items():
        null_rate = null_count / count
        assert null_rate < 0.01, f"Column {col} has {null_rate:.1%} nulls — expected < 1%"

    # Check date range is correct
    date_range = df.agg(
        F.min("event_time").alias("min_time"),
        F.max("event_time").alias("max_time")
    ).collect()[0]

    print(f"Input validation passed: {count:,} rows, time range: {date_range.min_time} - {date_range.max_time}")
```

### Output validation

```python
def validate_output(result_df, source_df) -> None:
    """Validate output before committing."""

    result_count = result_df.count()
    source_count = source_df.count()

    # Output shouldn't be empty
    assert result_count > 0, "Output is empty"

    # Result should be smaller than source (we're aggregating)
    assert result_count < source_count, \
        f"Result ({result_count}) >= source ({source_count}) — unexpected"

    # Check for null primary keys in output
    null_pk = result_df.filter(F.col("user_id").isNull()).count()
    assert null_pk == 0, f"Output has {null_pk} rows with null user_id"

    # Check amounts are non-negative
    negative_amounts = result_df.filter(F.col("total_amount") < 0).count()
    assert negative_amounts == 0, f"Output has {negative_amounts} negative amounts"

    print(f"Output validation passed: {result_count:,} rows")
```

---

## 4.7 Writing to DynamoDB

DynamoDB is a great target for pipeline outputs that need low-latency lookups (e.g., user metrics that an API serves).

### The naive approach (too slow)

```python
# DON'T do this — calls DynamoDB one row at a time
def write_row_to_dynamo(row):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('my-table')
    table.put_item(Item=row.asDict())

df.foreach(write_row_to_dynamo)  # Serial, very slow
```

### The correct approach: batch writes

```python
import boto3
from pyspark.sql import functions as F
from decimal import Decimal
import json

def write_partition_to_dynamo(rows):
    """Write a partition of rows to DynamoDB using batch_write_item."""
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('user_metrics')

    # DynamoDB batch_write_item supports max 25 items per call
    BATCH_SIZE = 25
    batch = []

    for row in rows:
        # Convert to DynamoDB-compatible format
        item = {
            'user_id': row.user_id,
            'date': row.date,
            'total_amount': Decimal(str(row.total_amount)),
            'event_count': int(row.event_count),
            'last_updated': row.last_updated.isoformat() if row.last_updated else None
        }
        # Remove None values (DynamoDB doesn't accept None)
        item = {k: v for k, v in item.items() if v is not None}
        batch.append({'PutRequest': {'Item': item}})

        if len(batch) == BATCH_SIZE:
            # Write batch with retry on throttle
            _write_batch_with_retry(dynamodb, 'user_metrics', batch)
            batch = []

    # Write remaining items
    if batch:
        _write_batch_with_retry(dynamodb, 'user_metrics', batch)


def _write_batch_with_retry(dynamodb, table_name, batch, max_retries=3):
    """Write a batch, retrying unprocessed items."""
    for attempt in range(max_retries):
        response = dynamodb.batch_write_item(
            RequestItems={table_name: batch}
        )
        unprocessed = response.get('UnprocessedItems', {}).get(table_name, [])
        if not unprocessed:
            return
        batch = unprocessed  # Retry unprocessed items

    if batch:
        raise RuntimeError(f"Failed to write {len(batch)} items after {max_retries} retries")


# Execute with foreachPartition (one boto3 client per partition, not per row)
df_result.foreachPartition(write_partition_to_dynamo)
```

### DynamoDB capacity planning

DynamoDB has two capacity modes:
- **On-demand:** Pay per request, auto-scales, no capacity planning
- **Provisioned:** Set read/write capacity units (RCU/WCU), cheaper for predictable load

For bulk pipeline writes, consider:
1. Temporarily increasing WCU before the pipeline runs
2. Using on-demand mode if writes are bursty
3. Distributing writes over time to avoid throttling

---

## 4.8 A Complete Pipeline Structure

Here's how a well-structured production pipeline looks:

```python
#!/usr/bin/env python3
"""
daily_pipeline.py — Daily event processing pipeline

Usage:
    spark-submit daily_pipeline.py --date 2024-01-15 [--dry-run]
"""

import argparse
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()


def read_events(spark: SparkSession, date: str, input_path: str):
    """Read events for a specific date."""
    logger.info(f"Reading events for date: {date}")

    schema = StructType([
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("event_time", TimestampType(), nullable=False),
        StructField("region", StringType(), nullable=True),
    ])

    df = spark.read \
        .schema(schema) \
        .parquet(f"{input_path}/event_date={date}/")

    count = df.count()
    logger.info(f"Loaded {count:,} events")
    assert count > 0, f"No events found for date={date}"
    return df


def transform(df):
    """Core business logic."""
    logger.info("Applying transformations")

    # Deduplicate by event_id (keep latest)
    window = Window.partitionBy("event_id").orderBy(F.desc("event_time"))
    df_deduped = df.withColumn("rn", F.row_number().over(window)) \
                   .filter(F.col("rn") == 1) \
                   .drop("rn")

    # Filter valid events
    df_clean = df_deduped.filter(
        F.col("user_id").isNotNull() &
        F.col("amount").isNotNull() &
        (F.col("amount") >= 0)
    )

    # Compute user daily metrics
    result = df_clean.groupBy("user_id", "region") \
        .agg(
            F.sum("amount").alias("total_amount"),
            F.count("*").alias("event_count"),
            F.countDistinct("event_type").alias("unique_event_types"),
            F.max("event_time").alias("last_event_time"),
        )

    return result


def write_output(df, output_path: str, date: str, dry_run: bool = False):
    """Write output to S3."""
    if dry_run:
        logger.info(f"[DRY RUN] Would write {df.count():,} rows to {output_path}")
        df.show(10)
        return

    logger.info(f"Writing output to {output_path}")
    df.withColumn("date", F.lit(date)) \
      .coalesce(20) \
      .write \
      .mode("overwrite") \
      .partitionBy("date") \
      .parquet(output_path)

    logger.info("Write complete")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--input-path", default="s3://my-bucket/bronze/events")
    parser.add_argument("--output-path", default="s3://my-bucket/gold/user_metrics")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logger.info(f"Starting pipeline for date={args.date}")

    spark = create_spark_session(f"daily-pipeline-{args.date}")

    try:
        events = read_events(spark, args.date, args.input_path)
        result = transform(events)
        write_output(result, args.output_path, args.date, args.dry_run)
        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

---

## 4.9 Module Summary

- **Medallion Architecture (Bronze → Silver → Gold)** is the standard data lake pattern
- **Idempotency is non-negotiable** — use overwrite, parameterize by date, deduplicate
- **Orchestrate with Step Functions or Airflow** — never run pipeline jobs manually in production
- **Validate input and output** — data quality issues surface as downstream corruption otherwise
- **Structure your pipeline code** — separate concerns: read, transform, write, validate
- **DynamoDB writes must use batch_write_item** — never write row-by-row

### ✅ Self-check

1. What are the three layers in Medallion Architecture and what does each contain?
2. What does "idempotent" mean for a batch pipeline? Give an example.
3. Why is `mode("overwrite")` safer than `mode("append")` for batch jobs?
4. What is dynamic partition overwrite and why is it critical?
5. Why use `foreachPartition` instead of `foreach` when writing to DynamoDB?
6. Why must you always call `terminate_cluster` even when a job fails?

---

*Next: `05-state-watermarks-checkpoints.md` →*
