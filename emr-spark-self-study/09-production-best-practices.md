# Module 09: Production Best Practices
## Operating Pipelines at Scale

> **Difficulty:** 🔴 Advanced
> **Prerequisite:** Modules 01–08
> **Est. time:** 4–5 hours

---

## 9.1 The Gap Between Working and Production-Ready

A Spark job that works in development and a production-grade pipeline are very different things.

**A working job:**
- Runs successfully on a known, clean dataset
- Produces correct output
- Doesn't crash

**A production-grade pipeline:**
- Handles input data that is wrong, late, or missing
- Continues working as schemas evolve
- Recovers from failures without human intervention
- Notifies the right people when something is wrong
- Can be backfilled for months of historical data
- Has monitoring that detects silent data corruption
- Costs predictable amounts of money
- Can be operated by engineers who didn't write it

This module covers the gap.

---

## 9.2 Schema Evolution

Your data sources will change over time. New fields are added. Old fields are renamed or removed. Types change. Your pipeline must handle these changes without breaking.

### Types of schema changes

| Change | Backward Compatible? | Forward Compatible? |
|--------|---------------------|---------------------|
| Add new column (nullable) | ✅ Yes | ✅ Yes |
| Add new column (required) | ❌ No (old data lacks it) | ✅ Yes |
| Remove column | ✅ Yes (code ignores it) | ❌ No (code expects it) |
| Rename column | ❌ No | ❌ No |
| Widen type (int → long) | ✅ Usually | ✅ Yes |
| Narrow type (double → int) | ❌ Data loss risk | ❌ No |

### The explicit schema strategy (recommended)

Always read with an explicit schema that represents what you **expect**, not what's actually there:

```python
# Define your expected schema in code — this is your contract
EVENTS_SCHEMA_V1 = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("event_time", TimestampType(), nullable=False),
])

# Read with this schema:
# - Columns in schema but not in file: filled with nulls
# - Columns in file but not in schema: ignored (safe backward compatibility)
df = spark.read.schema(EVENTS_SCHEMA_V1).parquet("s3://bucket/events/")
```

### Versioning schemas

When your schema changes, version it explicitly:

```python
# schemas.py — versioned schemas as code
from pyspark.sql.types import *

class EventSchemas:
    V1 = StructType([
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("amount", DoubleType(), nullable=True),
        StructField("event_time", TimestampType(), nullable=False),
    ])

    V2 = StructType([
        *V1.fields,  # inherit V1 fields
        StructField("region", StringType(), nullable=True),   # new in V2
        StructField("platform", StringType(), nullable=True),  # new in V2
    ])

    CURRENT = V2

# In your pipeline:
df = spark.read.schema(EventSchemas.CURRENT).parquet("s3://bucket/events/")
```

### Using mergeSchema for mixed-schema directories

```python
# When files in the same directory have different schemas:
df = spark.read \
    .option("mergeSchema", "true") \
    .parquet("s3://bucket/events/")
# Spark takes the union of all schemas, filling missing columns with null
```

**Cost:** mergeSchema reads metadata from all files to discover the union schema. Expensive at scale. Use only when needed, then migrate to a consistent schema.

### Schema evolution with Apache Iceberg

The cleanest solution is to use Apache Iceberg, which manages schema evolution explicitly:

```python
# Iceberg handles schema evolution atomically
spark.sql("""
    ALTER TABLE glue_catalog.events
    ADD COLUMNS (
        region STRING COMMENT 'User region, added 2024-01-15',
        platform STRING COMMENT 'Platform type: web/mobile/api'
    )
""")
# Historical data: region = null (no data loss)
# New data: region populated as normal
# Reads always work — Iceberg handles the null-filling automatically
```

---

## 9.3 Backfilling

Backfilling is reprocessing historical data. You'll need to backfill when:
- You fix a bug in transformation logic
- You add a new output field that requires reprocessing history
- You change partitioning strategy
- A data source retroactively corrects historical data

### Backfilling strategy

**Principle:** Backfills should use the same pipeline code as normal runs. If your code is idempotent, a backfill is just running the job for many dates.

```python
# backfill.py — run the pipeline for a range of dates
from datetime import datetime, timedelta
import subprocess

def backfill(
    start_date: str,
    end_date: str,
    job_script: str,
    parallelism: int = 3,
) -> None:
    """
    Backfill by running the pipeline for each date in range.

    Args:
        start_date: Inclusive start (YYYY-MM-DD)
        end_date: Inclusive end (YYYY-MM-DD)
        job_script: S3 path to the Spark job script
        parallelism: How many dates to process simultaneously
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    # Generate list of dates
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    print(f"Backfilling {len(dates)} dates from {start_date} to {end_date}")

    # Process in batches (don't create 365 clusters simultaneously)
    for i in range(0, len(dates), parallelism):
        batch = dates[i:i + parallelism]
        print(f"Processing batch: {batch}")

        # Submit one EMR job per date in the batch
        jobs = [submit_emr_job(date, job_script) for date in batch]

        # Wait for batch to complete before submitting next
        for job_id, date in zip(jobs, batch):
            wait_for_job(job_id, date)

    print("Backfill complete")
```

### Backfill considerations

**Resource isolation:** Run backfills on dedicated clusters, separate from production jobs. A slow backfill shouldn't block tonight's daily run.

**Cost estimation:**
```python
def estimate_backfill_cost(
    num_days: int,
    data_gb_per_day: float,
    instance_type: str = "r5.4xlarge",
    num_nodes: int = 20,
    job_hours: float = 2.0,
    parallelism: int = 5,
) -> dict:
    """Estimate backfill cost."""
    total_batches = num_days / parallelism
    total_hours = total_batches * job_hours

    # EMR + EC2 pricing (approximate)
    emr_cost_per_node_hour = 1.26  # r5.4xlarge On-Demand + EMR markup
    total_compute_cost = num_nodes * total_hours * emr_cost_per_node_hour

    return {
        "num_days": num_days,
        "parallelism": parallelism,
        "total_batches": int(total_batches),
        "estimated_wall_time_hours": total_hours,
        "estimated_cost_usd": round(total_compute_cost, 2),
    }

# Example: backfilling 1 year of data
print(estimate_backfill_cost(365, 500, parallelism=5))
# → wall time: ~73 × 2hr = ~146 hours ≈ 6 days, cost: ~$3,682
```

**Checkpoint your backfill progress:** Record which dates have been successfully processed so a failed backfill can resume from where it stopped.

```python
import boto3

def mark_date_complete(dynamo_table: str, pipeline: str, date: str) -> None:
    """Record successful completion of a date."""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamo_table)
    table.put_item(Item={
        'pk': f"BACKFILL#{pipeline}",
        'sk': date,
        'status': 'complete',
        'completed_at': str(datetime.now()),
    })

def is_date_complete(dynamo_table: str, pipeline: str, date: str) -> bool:
    """Check if a date has already been processed."""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamo_table)
    response = table.get_item(
        Key={'pk': f"BACKFILL#{pipeline}", 'sk': date}
    )
    return 'Item' in response and response['Item']['status'] == 'complete'
```

---

## 9.4 Monitoring

"It's not production if you can't tell when it's broken."

### What to monitor

**Job-level metrics:**

| Metric | Healthy Signal | Warning Signal |
|--------|---------------|----------------|
| Job duration | Consistent with baseline ±20% | > 2× baseline |
| Input row count | Consistent ±10% | 0 rows or 10× normal |
| Output row count | Consistent ±10% | 0 rows or 10× normal |
| Job success/failure | Always success | Any failure |
| Cost per run | Consistent | Significant increase |

**Infrastructure metrics (CloudWatch):**

| Metric | Threshold | Action |
|--------|-----------|--------|
| `YARNMemoryAvailablePcnt` | < 10% for > 5 min | Scale up / investigate OOM |
| `ContainerPendingRatio` | > 0.5 for > 10 min | Cluster undersized |
| `HDFSUtilization` | > 80% | Increase core node storage |

### Publishing custom metrics

```python
import boto3
from datetime import datetime

def publish_pipeline_metrics(
    pipeline_name: str,
    date: str,
    input_count: int,
    output_count: int,
    duration_seconds: float,
) -> None:
    """Publish custom metrics to CloudWatch."""
    cloudwatch = boto3.client('cloudwatch', region_name='us-east-1')

    cloudwatch.put_metric_data(
        Namespace='DataPlatform/Pipelines',
        MetricData=[
            {
                'MetricName': 'InputRowCount',
                'Dimensions': [
                    {'Name': 'Pipeline', 'Value': pipeline_name},
                    {'Name': 'Date', 'Value': date},
                ],
                'Value': input_count,
                'Unit': 'Count',
            },
            {
                'MetricName': 'OutputRowCount',
                'Dimensions': [
                    {'Name': 'Pipeline', 'Value': pipeline_name},
                    {'Name': 'Date', 'Value': date},
                ],
                'Value': output_count,
                'Unit': 'Count',
            },
            {
                'MetricName': 'DurationSeconds',
                'Dimensions': [{'Name': 'Pipeline', 'Value': pipeline_name}],
                'Value': duration_seconds,
                'Unit': 'Seconds',
            },
        ]
    )
```

### Alerting

```python
# CloudWatch Alarm: alert if pipeline hasn't succeeded by 6 AM
# (implies midnight daily job failed or is still running)
import boto3

cloudwatch = boto3.client('cloudwatch')

cloudwatch.put_metric_alarm(
    AlarmName='DailyPipeline-FailureAlert',
    AlarmDescription='Daily pipeline failed or did not run',
    MetricName='AppsFailed',
    Namespace='AWS/ElasticMapReduce',
    Statistic='Sum',
    Period=300,  # 5 minutes
    EvaluationPeriods=1,
    Threshold=1,
    ComparisonOperator='GreaterThanOrEqualToThreshold',
    AlarmActions=['arn:aws:sns:us-east-1:123456789:pipeline-alerts'],
    Dimensions=[
        {'Name': 'JobFlowId', 'Value': cluster_id},
    ]
)
```

### Data quality monitoring (Great Expectations)

For serious production pipelines, use **Great Expectations** — a data quality framework:

```python
import great_expectations as gx

# Define expectations about your data
context = gx.get_context()
datasource = context.sources.add_spark("my_spark_datasource", spark_session=spark)

batch = datasource.get_batch(
    batch_identifiers={"path": "s3://bucket/events/date=2024-01-15/"}
)

# Define expectations
batch.expect_column_to_exist("event_id")
batch.expect_column_values_to_not_be_null("event_id")
batch.expect_column_values_to_be_unique("event_id")
batch.expect_column_values_to_be_between("amount", min_value=0, max_value=1_000_000)
batch.expect_table_row_count_to_be_between(min_value=100_000, max_value=10_000_000)

# Run validation
result = batch.validate()
if not result.success:
    raise ValueError(f"Data quality check failed: {result}")
```

---

## 9.5 CI/CD for Data Pipelines

Your pipeline code needs the same quality guarantees as application code.

### Unit testing PySpark code

```python
# test_transforms.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing."""
    return SparkSession.builder \
        .master("local[2]") \
        .appName("tests") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def test_transform_deduplication(spark):
    """Test that duplicate events are removed."""
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("user_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("event_time", TimestampType()),
    ])

    data = [
        ("evt_001", "user_1", 100.0, datetime(2024, 1, 15, 10, 0)),
        ("evt_001", "user_1", 100.0, datetime(2024, 1, 15, 10, 1)),  # duplicate
        ("evt_002", "user_2", 200.0, datetime(2024, 1, 15, 11, 0)),
    ]

    df = spark.createDataFrame(data, schema)
    result = transform(df)  # your transform function

    # After dedup: should have 2 unique events
    assert result.count() == 2
    assert result.filter(result.event_id == "evt_001").count() == 1


def test_transform_handles_nulls(spark):
    """Test null handling in transform."""
    data = [
        ("evt_001", None, 100.0, datetime(2024, 1, 15, 10, 0)),  # null user_id
        ("evt_002", "user_2", None, datetime(2024, 1, 15, 11, 0)),  # null amount
        ("evt_003", "user_3", 300.0, datetime(2024, 1, 15, 12, 0)),  # valid
    ]

    df = spark.createDataFrame(data, schema)
    result = transform(df)

    # Null user_id events should be filtered out
    assert result.filter(result.user_id.isNull()).count() == 0

    # Valid events with null amount should have amount = 0 (or whatever your logic is)
    user3_row = result.filter(result.user_id == "user_3").collect()[0]
    assert user3_row.total_amount == 300.0
```

### Integration testing with S3

```python
# test_integration.py — runs against a real S3 bucket (test bucket)
import boto3
import pytest

TEST_BUCKET = "my-pipeline-test-bucket"
TEST_PREFIX = "integration-tests"

@pytest.fixture(autouse=True)
def cleanup(s3_client):
    """Clean up test data after each test."""
    yield
    # Delete all objects written during the test
    objects = s3_client.list_objects_v2(Bucket=TEST_BUCKET, Prefix=TEST_PREFIX)
    if 'Contents' in objects:
        s3_client.delete_objects(
            Bucket=TEST_BUCKET,
            Delete={'Objects': [{'Key': obj['Key']} for obj in objects['Contents']]}
        )

def test_full_pipeline_run(spark):
    """Test end-to-end pipeline with real S3 read/write."""
    input_path = f"s3://{TEST_BUCKET}/{TEST_PREFIX}/input/"
    output_path = f"s3://{TEST_BUCKET}/{TEST_PREFIX}/output/"

    # Write test data
    test_df = create_test_data(spark)
    test_df.write.parquet(input_path)

    # Run pipeline
    run_pipeline(spark, input_path, output_path, date="2024-01-15")

    # Verify output
    result = spark.read.parquet(output_path)
    assert result.count() > 0
    assert result.filter(result.date == "2024-01-15").count() == result.count()
```

### CD pipeline for EMR jobs

```yaml
# .github/workflows/deploy-pipeline.yml
name: Deploy Data Pipeline

on:
  push:
    branches: [main]
    paths: ['pipelines/**']

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with: {python-version: '3.11'}
      - run: pip install pyspark pytest great-expectations
      - run: pytest tests/unit/ -v

  deploy:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: aws-actions/configure-aws-credentials@v2
        with:
          role-to-assume: ${{ secrets.AWS_DEPLOY_ROLE }}

      - name: Upload job scripts to S3
        run: |
          aws s3 sync pipelines/ s3://my-bucket/jobs/ \
            --exclude "*.test.py" \
            --exclude "__pycache__/*"

      - name: Deploy dependencies
        run: |
          pip install -r requirements.txt -t ./deps
          zip -r deps.zip deps/
          aws s3 cp deps.zip s3://my-bucket/jobs/deps.zip

      - name: Notify deployment
        run: |
          aws sns publish \
            --topic-arn ${{ secrets.ALERTS_SNS_TOPIC }} \
            --message "Pipeline deployed: ${{ github.sha }}"
```

---

## 9.6 Configuration Management

Hardcoding S3 paths, table names, and configuration values in your job code is a maintenance problem.

### Externalize configuration

```python
# config.py
import os
import json
import boto3

def load_config(environment: str = None) -> dict:
    """Load configuration from environment variables or SSM Parameter Store."""
    env = environment or os.environ.get("ENVIRONMENT", "dev")

    if env == "local":
        return {
            "input_path": "s3://my-bucket-dev/bronze/events",
            "output_path": "s3://my-bucket-dev/gold/metrics",
            "dynamo_table": "user-metrics-dev",
        }

    # Load from SSM Parameter Store (secure, auditable)
    ssm = boto3.client('ssm')
    response = ssm.get_parameters_by_path(
        Path=f"/pipelines/{env}/daily-pipeline/",
        WithDecryption=True
    )
    return {
        param['Name'].split('/')[-1]: param['Value']
        for param in response['Parameters']
    }

# In your job:
config = load_config()
df = spark.read.parquet(config["input_path"])
```

### Using Spark configuration for EMR settings

```python
# conf/spark-defaults.conf — applied to all Spark sessions on the cluster
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.sources.partitionOverwriteMode=dynamic
spark.speculation=true

# Override per-job at submit time:
# --conf spark.sql.shuffle.partitions=400
```

---

## 9.7 Logging Best Practices

```python
import logging
import json
from datetime import datetime

# Structured logging — makes CloudWatch queries much easier
class StructuredLogger:
    def __init__(self, pipeline_name: str, date: str):
        self.pipeline = pipeline_name
        self.date = date
        self.logger = logging.getLogger(pipeline_name)

    def _log(self, level: str, message: str, **kwargs):
        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "pipeline": self.pipeline,
            "date": self.date,
            "message": message,
            **kwargs
        }
        self.logger.info(json.dumps(record))

    def info(self, msg: str, **kwargs):
        self._log("INFO", msg, **kwargs)

    def error(self, msg: str, **kwargs):
        self._log("ERROR", msg, **kwargs)

    def metric(self, name: str, value, **kwargs):
        self._log("METRIC", f"{name}={value}", metric_name=name, metric_value=value, **kwargs)


# Usage:
log = StructuredLogger("daily-pipeline", "2024-01-15")
log.info("Reading input data", s3_path="s3://bucket/events/date=2024-01-15/")
log.metric("input_row_count", 5_432_100)
log.metric("output_row_count", 1_234_567)
log.info("Pipeline complete", duration_seconds=1234)
```

---

## 9.8 Runbooks and Operational Documentation

Every production pipeline should have a runbook — a document describing what to do when things go wrong.

### Runbook template

```markdown
# Daily Pipeline Runbook

## Overview
- **Purpose:** Process daily events into user metrics
- **Schedule:** 02:00 UTC daily
- **SLA:** Complete by 06:00 UTC
- **Owner:** Data Platform team
- **Slack:** #data-platform-alerts

## Dependencies
- **Input:** s3://bucket/bronze/events/event_date=YYYY-MM-DD/
  - Written by: Kafka consumer pipeline (by 01:30 UTC)
- **Output:** s3://bucket/gold/user_metrics/date=YYYY-MM-DD/
  - Consumed by: Analytics dashboard (reads at 07:00 UTC)

## Common Failures and Remediation

### Pipeline fails with "No data found for date=X"
**Cause:** Input data not yet available (upstream pipeline delayed)
**Check:** `aws s3 ls s3://bucket/bronze/events/event_date=2024-01-15/`
**Fix:** Wait for upstream, then manually trigger retry via Step Functions

### Pipeline fails with OOM
**Cause:** Unusually large input (e.g., holiday traffic spike)
**Fix:** Re-run on larger cluster: increase to 30 r5.4xlarge nodes
**Command:** [see Ops Playbook #12]

### Pipeline succeeds but row counts look wrong
**Check:** Compare input vs output row counts in CloudWatch
**Fix:** If output < 80% of expected: investigate Silver layer job for that date

## Manual Retry
1. Navigate to Step Functions console → DailyPipelineStateMachine
2. Start new execution with input: `{"date": "2024-01-15"}`
3. Monitor execution in Step Functions console

## Escalation
- On-call engineer if failure detected by 04:00 UTC
- Pipeline lead if SLA breach (not complete by 06:00 UTC)
```

---

## 9.9 Module Summary

Production-grade pipelines require work beyond the core data processing logic:

1. **Schema evolution:** Version your schemas, use explicit definitions, plan for changes
2. **Backfilling:** Design pipelines to be backfillable from day one; idempotency makes this free
3. **Monitoring:** Publish custom metrics, set up CloudWatch alarms, use Great Expectations for data quality
4. **Testing:** Unit test transformations, integration test with real S3, deploy via CI/CD
5. **Configuration management:** Externalize all paths and settings; never hardcode
6. **Logging:** Use structured logging for CloudWatch queryability
7. **Runbooks:** Write operational documentation before you're under incident pressure

### ✅ Self-check

1. What is schema evolution and what type of changes are backward compatible?
2. What is the safest way to handle a column rename without breaking existing readers?
3. Why should backfills run on dedicated clusters, not sharing production resources?
4. What are the three key metrics to monitor for a batch pipeline?
5. Why is structured logging (JSON) better than plain text logging for CloudWatch?
6. What should a runbook contain for a production pipeline?

---

*Next: `10-hands-on-project.md` →*
