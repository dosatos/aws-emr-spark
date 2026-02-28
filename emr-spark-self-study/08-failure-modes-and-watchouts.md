# Module 08: Failure Modes and Watch-Outs
## What Breaks, How It Breaks, and How to Prevent It

> **Difficulty:** 🔴 Advanced
> **Prerequisite:** Modules 01–07
> **Est. time:** 3–4 hours

---

## 8.1 Why a Dedicated Failure Module?

Most tutorials teach you the happy path. Production doesn't have a happy path — it has a happy path 95% of the time, and then a catastrophic failure 5% of the time that destroys data, misses SLAs, or corrupts analytics.

This module covers the failure modes that actually happen to teams running Spark at scale. Some of these are one-time lessons that cost real incidents. Read this carefully.

---

## 8.2 The Most Common Beginner Mistakes

### Mistake 1: Using `.collect()` on a large dataset

```python
# DANGER: This tries to pull all data to the driver JVM
all_data = df.collect()  # 1 TB → driver gets 1 TB → OOM crash

# Safe alternatives:
df.write.parquet("s3://bucket/output/")      # Write to storage
df.show(100)                                  # Show first 100 rows
df.limit(1000).toPandas()                    # Bring small sample to driver
sample = df.sample(fraction=0.001).collect() # Collect tiny sample
```

**Rule:** Never `.collect()` a DataFrame unless you know it fits in driver memory (< a few GB). The driver typically has 8–16 GB of heap.

---

### Mistake 2: Mode("append") in batch pipelines

```python
# First run: writes 1M rows ✅
df.write.mode("append").parquet("s3://bucket/output/date=2024-01-15/")

# Pipeline fails, retried. Second run: appends ANOTHER 1M rows ❌
df.write.mode("append").parquet("s3://bucket/output/date=2024-01-15/")

# Result: 2M rows, all duplicated
```

**Always use `mode("overwrite")` for batch pipelines unless you explicitly need append semantics and have deduplication logic downstream.**

---

### Mistake 3: Default static partition overwrite

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")  # This is the DEFAULT!

# You process only 2024-01-15's data
df_jan15.write.mode("overwrite").partitionBy("date").parquet("s3://bucket/events/")

# WHAT HAPPENS:
# Static mode deletes ALL partitions first, then writes
# s3://bucket/events/date=2024-01-14/ ← DELETED
# s3://bucket/events/date=2024-01-13/ ← DELETED
# s3://bucket/events/date=2024-01-12/ ← DELETED (2 years of data gone!)
# s3://bucket/events/date=2024-01-15/ ← Written (only today's data remains)
```

**Set `spark.sql.sources.partitionOverwriteMode = dynamic` in ALL production configurations.**

---

### Mistake 4: Missing schema definition on reads

```python
# At small scale: works fine, fast enough
df = spark.read.parquet("s3://bucket/events/")

# At petabyte scale with 10 million files:
# Schema inference reads footers from ALL files → takes 30 minutes
# before a single row of data is processed
```

**Always define explicit schemas for production reads:**
```python
schema = StructType([...])
df = spark.read.schema(schema).parquet("s3://bucket/events/")
```

---

### Mistake 5: Forgetting that partitionBy creates many files

```python
# 200 Spark partitions × 20 storage partitions (regions) = 4,000 output files
df.write.partitionBy("region").parquet("s3://bucket/")

# After a week: 7 days × 4,000 files = 28,000 files
# After a month: 120,000 files → small files problem
```

**Always `repartition()` or `coalesce()` before `partitionBy()` writes.**

---

### Mistake 6: Not terminating clusters after job completion

```python
# Cluster left running "just in case" for debugging:
# r5.4xlarge × 20 nodes = $25.20/hour
# Left running over a weekend = $25.20 × 48 = $1,210 wasted
```

**Use `AutoTerminate: true` on EMR clusters. Always.**

---

### Mistake 7: Wide window functions without repartitioning

```python
# Window function without partitionBy → all data moves to ONE executor!
window_bad = Window.orderBy("event_time")  # GLOBAL sort → shuffle all data to 1 executor
df.withColumn("row_num", F.row_number().over(window_bad))
# → OOM on the single executor holding all data

# Correct: always use partitionBy in window specs
window_good = Window.partitionBy("user_id").orderBy("event_time")
df.withColumn("row_num", F.row_number().over(window_good))
# → Each user's data processed independently → parallelism preserved
```

---

## 8.3 Driver Out-Of-Memory (OOM)

### Symptoms
```
java.lang.OutOfMemoryError: Java heap space
  at java.util.Arrays.copyOf(Arrays.java:3210)
  at org.apache.spark.scheduler.DAGScheduler.handleTaskCompletion...

Or:
ERROR SparkContext: Error initializing SparkContext.
java.lang.OutOfMemoryError: GC overhead limit exceeded
```

### Causes and fixes

| Cause | Detection | Fix |
|-------|-----------|-----|
| `.collect()` on large data | Code review | Remove `.collect()`, use `.write()` |
| Large broadcast variable | `.explain()` shows BroadcastExchange with huge data | Don't broadcast large tables |
| Accumulating results in driver | Code loops building large lists | Process in Spark, not driver |
| Too many cached DataFrames | Spark UI > Storage tab shows full storage | Unpersist unused caches |
| Too many tasks tracked | > 100,000 tasks in history | Reduce partition count |

```bash
# Increase driver memory if needed (but fix the root cause first)
--driver-memory 16g
--driver-java-options "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35"
```

---

## 8.4 Executor Out-Of-Memory

### Symptoms
```
Container killed by YARN for exceeding memory limits.
X of Y GB physical memory used. Consider boosting spark.yarn.executor.memoryOverhead

Or:
ExecutorLostFailure: Slave lost: ... Container from a bad node
```

### Causes

**Cause 1: Task processes too much data**
- Large shuffle partition lands on one executor
- Executor doesn't have enough memory for the sort/aggregation

```bash
# Fix: reduce partition size
spark.conf.set("spark.sql.shuffle.partitions", "800")  # More partitions = less data per task
# Or enable AQE to auto-coalesce
```

**Cause 2: memoryOverhead too low**
- Python UDFs spawn separate Python processes
- Broadcast variables materialize in JVM native memory
- JVM internal overhead (JIT compiled code, thread stacks)

```bash
# Fix: increase overhead
--conf spark.executor.memoryOverhead=4g  # At least 20% of executor.memory
```

**Cause 3: Skewed partition with huge data**
- One partition has 100x the data of others
- That executor runs out of memory; others are idle

```bash
# Fix: enable AQE skew handling
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# Or apply salting (Module 07)
```

**Cause 4: Pandas UDFs materializing full DataFrame in Python**
```python
# This materializes the entire partition as a pandas DataFrame in Python memory
@F.pandas_udf("double")
def my_expensive_udf(values: pd.Series) -> pd.Series:
    return values.apply(lambda x: expensive_computation(x))

# Fix: if the computation can be done in Spark SQL, do it there
# If you must use pandas UDFs, ensure partitions are small enough to fit
```

---

## 8.5 Shuffle Failures

### Symptom
```
org.apache.spark.shuffle.FetchFailedException: Failed to connect to ...
or
Lost task ... in stage ... (ShuffleMapTask): org.apache.spark.shuffle.FetchFailedException
```

### What happened

During a shuffle, executor A wrote shuffle data to its local disk. Executor B needs to read that data. But executor A crashed (Spot termination, OOM, hardware failure). Executor B can't fetch the shuffle data → shuffle failure.

### Spark's recovery mechanism

Spark retries failed tasks up to `spark.task.maxFailures` times (default: 4). For shuffle failures, Spark resubmits the whole shuffle stage (re-runs all map tasks to regenerate the shuffle data) before retrying the reduce task.

### Prevention

1. **Don't use Spot for all workers.** Keep some core On-Demand nodes that hold shuffle data.
2. **Use external shuffle service.** This stores shuffle data in the NodeManager process, not in the executor. If the executor dies, shuffle data survives.

```bash
# External shuffle service (enabled by default in EMR)
spark.shuffle.service.enabled=true

# Tune task retry count
spark.task.maxFailures=4  # default; increase for flaky networks
```

3. **Enable speculative execution.** If a task is taking much longer than others, Spark launches a duplicate and uses whichever finishes first.

```bash
spark.speculation=true
spark.speculation.multiplier=2.0    # Task is "slow" if 2x the median
spark.speculation.quantile=0.75     # Wait until 75% of tasks finish before speculating
```

---

## 8.6 S3 Data Corruption Scenarios

### Scenario 1: Concurrent writers

Two Spark jobs write to the same S3 path simultaneously:
```
Job A: writes date=2024-01-15/part-00001.parquet
Job B: writes date=2024-01-15/part-00001.parquet (overwrites A!)
```

S3 is not transactional. Last writer wins. Partial data from both jobs = corruption.

**Prevention:** Orchestrate with a lock (Step Functions state machine, DynamoDB conditional writes) or ensure jobs are mutually exclusive.

```python
# DynamoDB-based lock before writing
import boto3

def acquire_lock(lock_table: str, lock_key: str) -> bool:
    """Returns True if lock acquired, False if already locked."""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(lock_table)
    try:
        table.put_item(
            Item={'lock_key': lock_key, 'acquired_at': str(datetime.now())},
            ConditionExpression='attribute_not_exists(lock_key)'
        )
        return True
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return False
```

### Scenario 2: Partial write failure with static partition overwrite

```
1. Pipeline starts writing date=2024-01-15
2. Spark deletes old date=2024-01-15 partition (static overwrite)
3. Writing new files...
4. Job crashes at 60%
5. date=2024-01-15 now has 60% of expected data
6. No recovery mechanism → corrupted partition
```

**Prevention:** Use dynamic partition overwrite + idempotent writes. The old partition is not deleted until new files are written.

### Scenario 3: Schema mismatch on read

```python
# Written with schema v1 (no 'region' column):
df.write.parquet("s3://bucket/events/date=2024-01-01/")

# Written with schema v2 (added 'region' column):
df.write.parquet("s3://bucket/events/date=2024-01-15/")

# Reading all dates:
df_all = spark.read.parquet("s3://bucket/events/")
# Spark infers schema from the FIRST file it reads
# If it reads an old file first: region column is missing → reads fail or nulls
```

**Prevention:**
- Use explicit schema on reads
- Add `mergeSchema=true` to tolerate schema evolution within Parquet
- Use Delta Lake or Iceberg which handle schema evolution explicitly

```python
df = spark.read \
    .option("mergeSchema", "true") \
    .parquet("s3://bucket/events/")
```

---

## 8.7 Spot Instance Termination

Spot instances can be terminated with 2-minute notice. At petabyte scale with 50+ spot nodes, expect terminations.

### What happens when a spot node is terminated

1. AWS sends a termination notice to the instance
2. YARN NodeManager starts decommissioning: marks running containers for migration
3. If the node has active shuffle data (running stage): shuffle failure
4. Spark retries the affected tasks on surviving nodes
5. If the job is mid-shuffle with no external shuffle service: shuffle stage is resubmitted

### Mitigation strategies

**Use task nodes (not core nodes) for spot:**
```json
{
  "InstanceRole": "TASK",
  "Market": "SPOT",
  "InstanceCount": 30
}
```
Task nodes don't hold HDFS data. Their termination only causes task retry, not data loss.

**Enable graceful decommission:**
```bash
yarn.resourcemanager.decommissioning.timeout=300  # 5 minutes
```
YARN waits up to 5 minutes for running tasks to complete before forcibly terminating.

**Mix Spot with On-Demand:**
Keep 20–30% of workers as On-Demand. Spot interruptions affect at most 70–80% of compute.

**Use EMR Managed Scaling with fallback:**
```json
{
  "SpotSpecification": {
    "TimeoutDurationMinutes": 10,
    "TimeoutAction": "SWITCH_TO_ON_DEMAND"
  }
}
```

---

## 8.8 Skew in Practice — Real Scenarios

### Scenario 1: The null key tsunami

```python
# You join events with users on user_id
result = df_events.join(df_users, on="user_id", how="left")

# Unknown to you: 30% of events have null user_id
# All nulls hash to the same partition → one executor gets 30% of all data
# Stage runs for 3 hours while other executors finish in 10 minutes
```

**Fix:**
```python
# Option A: Filter nulls before join (if nulls don't need user enrichment)
df_events_with_user = df_events.filter(F.col("user_id").isNotNull())
df_events_null = df_events.filter(F.col("user_id").isNull())
result_with_user = df_events_with_user.join(df_users, on="user_id", how="left")
result = result_with_user.union(df_events_null.withColumn("user_name", F.lit(None)))

# Option B: AQE skew handling (enable and let Spark handle it)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Scenario 2: The hot product key

```python
# Aggregating events by product_id
# product_id="IPHONE_15" has 50M events; average product has 1,000
result = df_events.groupBy("product_id").count()
# → The IPHONE_15 partition runs forever; others finish in seconds
```

**Fix: Salting**
(See Module 07 for full salting example)

### Scenario 3: The temporal skew

```python
# Daily batch: process events by date
# Black Friday has 100x normal volume → date=2024-11-29 partition is 100x normal size
df = spark.read.parquet("s3://bucket/events/")
result = df.groupBy("date", "product_id").count()
# → 2024-11-29 tasks take 100x longer
```

**Fix:** Run Black Friday as a separate job with more resources, or use AQE with higher skew thresholds.

---

## 8.9 Data Quality Failures

### Silent data corruption

The most dangerous type: your pipeline runs successfully, but produces wrong results. No exceptions, no OOM, just wrong answers.

**Common causes:**
1. **Timezone bugs:** `event_time` stored in UTC but processed as local time → wrong day attribution
2. **Integer overflow:** Summing billions of records with `int` (32-bit max: ~2.1 billion)
3. **Float precision:** Comparing floats with `==` → wrong filter results
4. **Null propagation:** `null + 5 = null` in SQL; `null || 'US' = null` → unexpected nulls in output
5. **Wrong join type:** `inner` when you needed `left` → missing rows (no error thrown)

**Prevention:**
```python
# 1. Always use UTC for timestamps
from pyspark.sql.functions import to_utc_timestamp, from_utc_timestamp
df = df.withColumn("event_time_utc", to_utc_timestamp(df.event_time, "UTC"))

# 2. Use LongType or DoubleType for large numeric aggregations
from pyspark.sql.types import LongType
df = df.withColumn("amount_cents", df.amount_cents.cast(LongType()))

# 3. Handle nulls explicitly
df = df.withColumn("region", F.coalesce(df.region, F.lit("UNKNOWN")))

# 4. Test your output counts
input_count = df_input.count()
output_count = df_result.count()
assert output_count > 0, "Empty output!"
assert output_count <= input_count, "Unexpected row multiplication!"
```

### The silent deduplication failure

```python
# You deduplicate by event_id, expecting exactly 1 row per event
window = Window.partitionBy("event_id").orderBy(F.desc("event_time"))
df_deduped = df.withColumn("rn", F.row_number().over(window)) \
               .filter(F.col("rn") == 1)

# Problem: if event_id is null, ALL null-event_id rows go to the same partition
# row_number() picks ONE of the null-event_id rows → others are lost!

# Fix: handle nulls before deduplication
df_with_valid_ids = df.filter(F.col("event_id").isNotNull())
df_null_ids = df.filter(F.col("event_id").isNull())

df_deduped = df_with_valid_ids \
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("event_id").orderBy(F.desc("event_time"))
    )) \
    .filter(F.col("rn") == 1) \
    .drop("rn") \
    .union(df_null_ids)  # Keep null-id rows as-is (they can't be deduped)
```

---

## 8.10 Glue Catalog Failures

### Missing partition metadata

You write new data to S3 but forget to update the Glue Catalog:
```python
# Write new partition
df.write.mode("overwrite").parquet("s3://bucket/events/date=2024-01-16/")

# Query via Athena or Spark SQL:
spark.sql("SELECT * FROM events WHERE date='2024-01-16'")
# Returns 0 rows! Catalog doesn't know about this partition.

# Fix: always update catalog after writing
spark.sql("ALTER TABLE events ADD IF NOT EXISTS PARTITION (date='2024-01-16')")
# Or repair all partitions:
spark.sql("MSCK REPAIR TABLE events")
```

### Catalog lock contention

Multiple Spark jobs updating the Glue Catalog simultaneously can fail with lock errors:
```
com.amazonaws.services.glue.model.ConcurrentModificationException:
  Table modification conflict
```

**Fix:** Serialize catalog updates. Only one job updates the catalog at a time. Or use `MSCK REPAIR TABLE` in a single dedicated step after all data is written.

---

## 8.11 Common EMR Failure Scenarios

### Cluster bootstrap failure

Your cluster fails to start because a bootstrap action (custom script that runs at startup) fails.

```bash
# Check bootstrap logs:
s3://my-logs/clusters/<cluster-id>/steps/s-XXXXXXXXXX/stderr

# Common causes:
# - Package installation fails (internet connectivity, wrong OS version)
# - Script syntax error
# - Missing permissions for bootstrap script's S3 path
```

### Step failure: job never starts

The EMR Step shows FAILED but Spark never ran:

```
ERROR: /usr/bin/spark-submit: command not found
```

Fix: Ensure `Name=Spark` is in the Applications list when creating the cluster.

### Job runs but writes to wrong S3 path

```python
# Bug: f-string formatting issues
date = "2024-01-15"
path = f"s3://bucket/events/date={date}"  # ✅ correct

# Common mistake: missing the f prefix
path = "s3://bucket/events/date={date}"   # ❌ writes literal "{date}" to S3
```

**Fix:** Always test pipeline locally with a small dataset before running on EMR.

---

## 8.12 A Failure Checklist for Production

Before deploying any pipeline, verify:

**Idempotency:**
- [ ] Job can be run twice for the same date without creating duplicates
- [ ] Uses `mode("overwrite")` not `mode("append")`
- [ ] Uses dynamic partition overwrite

**Schema:**
- [ ] Explicit schema defined for all reads
- [ ] Schema includes all expected columns with correct types
- [ ] `mergeSchema=true` if reading data written by different job versions

**Error handling:**
- [ ] Job logs exceptions with full stack trace
- [ ] Job fails loudly (raises exceptions) rather than silently producing wrong output
- [ ] Retry logic is configured in the orchestrator (not in the Spark job itself)

**Data quality:**
- [ ] Input row count check
- [ ] Output row count check
- [ ] Null check on primary keys
- [ ] Sanity check on numeric ranges

**Resource configuration:**
- [ ] `memoryOverhead` set to at least 20% of executor memory
- [ ] AQE enabled
- [ ] Explicit `spark.sql.shuffle.partitions` set (not default 200)
- [ ] Dynamic partition overwrite enabled

**S3:**
- [ ] EMR cluster and S3 bucket in the same region
- [ ] EC2 instance profile has S3 permissions
- [ ] Cluster has `AutoTerminate: true`

---

## 8.13 Module Summary

This module covered the most common real-world failure scenarios. The pattern:

1. **Prevention > Recovery:** Most failures are preventable with proper configuration
2. **Idempotency solves most retry problems:** If your job is safe to re-run, most failures become non-issues
3. **Data corruption is worse than job failure:** A job that fails loudly is better than one that silently produces wrong data
4. **Test locally before cluster deployment:** PySpark runs locally — test with small data before deploying

### ✅ Self-check

1. What is the danger of using `mode("append")` in a batch pipeline?
2. What is the danger of using static partition overwrite with `mode("overwrite")`?
3. What causes "Container killed by YARN for exceeding memory limits"?
4. What is a shuffle FetchFailedException and how does Spark recover from it?
5. Why is timezone handling important for event time processing?
6. What happens when two Spark jobs write to the same S3 path simultaneously?

---

*Next: `09-production-best-practices.md` →*
