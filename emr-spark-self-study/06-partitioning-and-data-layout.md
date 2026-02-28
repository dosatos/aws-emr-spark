# Module 06: Partitioning and Data Layout
## The Single Biggest Performance Lever

> **Difficulty:** 🟡 Intermediate → 🔴 Advanced
> **Prerequisite:** Modules 01–05
> **Est. time:** 3–4 hours

---

## 6.1 Why Data Layout Is Everything

In Spark on S3, **data layout is your most important architectural decision**. It determines:
- How much data gets read per query (affects cost and speed more than any tuning)
- How many files get listed (affects job startup time)
- How well compression works
- Whether Spark can push predicates into file scanning
- How easy it is to add new data incrementally

A well-partitioned petabyte dataset where your query needs 100 GB can run in minutes.
A poorly-partitioned petabyte dataset where your query reads the whole thing runs for hours.

**No amount of executor tuning compensates for wrong data layout.** Get the layout right first.

---

## 6.2 Two Types of Partitioning (Don't Confuse Them)

Spark has **two completely different partitioning concepts** that beginners constantly mix up.

### Type 1: Spark (In-Memory) Partitions

How data is divided inside Spark's memory during processing. This affects parallelism during computation.

```python
df.repartition(200)          # 200 in-memory partitions
df.coalesce(50)              # reduce to 50 in-memory partitions
spark.sql.shuffle.partitions # default 200, controls shuffle output
```

These exist only during job execution. They don't affect how files are laid out on S3.

### Type 2: Storage (S3/Hive) Partitions

How files are organized in directories on S3. This is what `partitionBy()` creates.

```python
df.write.partitionBy("date", "region").parquet("s3://bucket/events/")

# Creates:
s3://bucket/events/date=2024-01-15/region=US/part-00001.parquet
s3://bucket/events/date=2024-01-15/region=EU/part-00001.parquet
s3://bucket/events/date=2024-01-16/region=US/part-00001.parquet
```

These persist in S3 forever and affect all future reads.

**When you read with a filter:**
```python
df = spark.read.parquet("s3://bucket/events/")
df.filter(df.date == "2024-01-15")
```

Spark **only reads** `s3://bucket/events/date=2024-01-15/`. All other date directories are not listed or read. This is **partition pruning** — your most powerful performance tool.

---

## 6.3 Choosing Partition Columns

This is a design decision that's hard to change later (requires rewriting all data).

### The golden rules

**Rule 1: Partition by what you filter by most often**

If 90% of your queries filter by `date`, partition by `date`.
If your queries filter by `date` AND `region`, partition by both.

```python
# Your query:
SELECT * FROM events WHERE date = '2024-01-15' AND region = 'US'

# Good partition: partitionBy("date", "region")
# → reads only events/date=2024-01-15/region=US/ ← tiny data read

# Bad partition: partitionBy("user_id")
# → can't prune on date or region → reads everything
```

**Rule 2: Partition columns must have LOW cardinality**

Cardinality = number of distinct values.

| Column | Cardinality | Partition? |
|--------|-------------|------------|
| `date` | ~365–1825 (1–5 years) | ✅ Good |
| `region` | 5–20 regions | ✅ Good |
| `country` | ~200 countries | ⚠️ Be careful (consider grouping) |
| `hour` | 24 values | ✅ Good (if needed) |
| `user_id` | Millions | ❌ Way too high |
| `event_id` (UUID) | Billions | ❌ Never |
| `product_sku` | 100,000s | ❌ Too high |

**Why high cardinality is bad:**
- 1,000,000 distinct user_ids → 1,000,000 directories on S3
- Each directory has N files
- Listing ALL directories before reading = 10,000+ S3 API calls
- Even one query that doesn't filter on user_id reads all 1M directories

**Rule 3: Balance partition size**

Each partition directory should ideally contain 128 MB – 1 GB of data total (across all files).

```
Total data for one date: 100 GB
Partition by date only:  events/date=2024-01-15/ → 100 GB in one directory
  → One task reads 100 GB? No, Spark splits large files into 128 MB splits
  → 100GB / 128MB = ~780 tasks → good parallelism ✅

Total data for one (date, region) partition: 100GB / 10 regions = 10 GB
  → 10 GB / 128 MB = ~78 tasks per partition → still good ✅

Total data for one (date, hour) partition: 100GB / 24 hours = 4 GB
  → 4 GB / 128 MB = ~31 tasks per hour → acceptable ✅

Total data for one (date, hour, minute) partition: 100GB / 1440 minutes = 70 MB
  → 70 MB = less than one split = ONE task for the whole minute → bad ❌
  → Also: 1440 partition directories per day × 365 days = 525,600 directories
  → Listing these takes forever
```

**Rule 4: Date partitioning — use strings, not year/month/day nesting**

```python
# DON'T: year/month/day nested partitioning
df.write.partitionBy("year", "month", "day").parquet(...)
# Creates: events/year=2024/month=01/day=15/
# Glue/Athena: filter "date BETWEEN '2024-01-10' AND '2024-01-20'"
# → must enumerate all year=2024/month=01 combinations manually
# → SQL predicates like date BETWEEN don't work for nested partitions

# DO: single string date partition
df.write.partitionBy("date").parquet(...)
# Creates: events/date=2024-01-15/
# Supports: WHERE date BETWEEN '2024-01-10' AND '2024-01-20' ✅
# Supports: WHERE date LIKE '2024-01%' ✅
```

---

## 6.4 S3 Path Design

Your S3 path structure is part of your architecture. Design it intentionally.

### Recommended path template

```
s3://{bucket-name}/{layer}/{domain}/{entity}/
  {partition1}={value1}/{partition2}={value2}/
  {file}.parquet

Example:
s3://company-data/silver/ecommerce/events/
  date=2024-01-15/
  event_type=purchase/
  part-00001-a3f2d.snappy.parquet
```

### Layer organization

```
s3://company-data/
├── bronze/
│   ├── raw_kafka_events/       ← raw events, partitioned by ingestion_date
│   ├── raw_api_responses/
│   └── raw_db_exports/
│
├── silver/
│   ├── events/                 ← cleaned events, partitioned by event_date
│   ├── users/                  ← cleaned user records
│   └── products/               ← cleaned product catalog
│
├── gold/
│   ├── daily_user_metrics/     ← aggregates, partitioned by date
│   ├── product_performance/
│   └── regional_summaries/
│
├── warehouse/                  ← Iceberg/Delta tables (if using table formats)
│   └── events/                 ← Iceberg table data + metadata
│
├── checkpoints/                ← Spark streaming checkpoints (if any)
│   └── hourly_event_pipeline/
│
└── emr-logs/                   ← EMR cluster and step logs
    └── cluster-logs/
```

### Separate buckets for separate concerns

```
data-bronze-{account-id}     ← raw data (long retention, many writers)
data-silver-{account-id}     ← cleaned data
data-gold-{account-id}       ← aggregates (served to consumers)
data-archive-{account-id}    ← cold data (S3 Glacier)
emr-logs-{account-id}        ← cluster logs (short retention)
emr-checkpoints-{account-id} ← streaming checkpoints
```

**Why separate buckets:**
- Different retention policies (raw: keep 7 years; logs: keep 30 days)
- Different access patterns and IAM policies
- Different replication requirements
- Easier cost attribution

---

## 6.5 File Sizing and the Target Range

Target: **128 MB – 256 MB per Parquet file**.

Why this range?
- **128 MB:** Matches Spark's default split size for reads. One file = one task, perfect parallelism.
- **< 10 MB:** Small files problem (metadata overhead, too many tasks, poor compression)
- **> 1 GB:** Files become slow to write and slow to read if you only need part of the data

### Calculating target file count

```python
import math

def target_file_count(total_size_gb: float, target_file_size_mb: int = 128) -> int:
    """Calculate how many output files to create."""
    total_mb = total_size_gb * 1024
    return max(1, math.ceil(total_mb / target_file_size_mb))

# Example: 5 GB of output → 40 files of ~128 MB each
print(target_file_count(5.0))  # 40
```

### Controlling output file count

```python
# Method 1: coalesce (no shuffle, can only reduce)
df.coalesce(40).write.parquet(output_path)

# Method 2: repartition (shuffle, can increase or decrease)
df.repartition(40).write.parquet(output_path)

# Method 3: repartition by column (also partitions storage layout)
df.repartition(40, "region").write.partitionBy("region").parquet(output_path)

# Method 4: AQE coalescing (automatic — let Spark decide)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(128 * 1024 * 1024))
```

### The hidden trap: partitionBy creates files per partition

When you use `partitionBy()`, Spark creates files per Spark partition **within** each storage partition:

```
df has 200 Spark partitions
df.write.partitionBy("region").parquet(output_path)

→ For each of 200 Spark partitions:
     Write its US rows to output/region=US/
     Write its EU rows to output/region=EU/
     Write its APAC rows to output/region=APAC/

→ Result: 200 files in region=US/, 200 files in region=EU/, 200 files in region=APAC/
→ 600 total files (likely very small!)
```

**Fix:** Repartition by the partition column before writing:

```python
# Each Spark partition contains only one storage partition's data
df.repartition("region") \
  .coalesce(10) \  # 10 files per region
  .write.partitionBy("region") \
  .parquet(output_path)
```

---

## 6.6 Bucketing (Advanced)

**Bucketing** is a write-time optimization for joins and aggregations on high-cardinality columns. It's different from Hive-style storage partitioning.

### What bucketing does

When you bucket a DataFrame by `user_id` into 200 buckets:
- Spark computes `hash(user_id) % 200` for each row
- Rows with the same user_id always go to the same bucket
- Data is sorted within each bucket

```python
# Write bucketed table
df.write \
  .bucketBy(200, "user_id") \
  .sortBy("user_id") \
  .saveAsTable("events_bucketed")

# Read bucketed table and join
events = spark.table("events_bucketed")
users = spark.table("users_bucketed")  # also bucketed by user_id

# Spark detects matching bucket schemas → NO SHUFFLE for the join!
result = events.join(users, on="user_id", how="inner")
```

### When bucketing helps

✅ Repeated large joins on the same column
✅ Aggregations on the same column (no shuffle)
✅ When both tables are pre-bucketed on the join key

❌ Not useful when bucketing only one side of a join
❌ Requires the table to be registered in Hive/Glue metastore
❌ Bucket count must match between joined tables

**Important limitation on S3:** Bucketing works well with Hive Metastore (on EMR). With S3-only layout (no metastore), bucketing's benefits are limited because Spark must re-discover bucket info at read time.

---

## 6.7 File Compaction

Over time, even well-designed pipelines accumulate small files:
- Streaming jobs write many small files per micro-batch
- High-frequency batch writes one file per run per partition
- Schema migrations leave old files alongside new files

**Compaction** is the process of reading many small files and rewriting as fewer large files.

### Simple compaction job

```python
def compact_s3_path(
    spark: SparkSession,
    source_path: str,
    target_file_size_mb: int = 128
) -> None:
    """
    Read all files at source_path and rewrite them as larger, fewer files.
    This is an in-place compaction — same path, overwritten.
    """
    # Read all files (discovery may take time for many small files)
    df = spark.read.parquet(source_path)

    # Estimate total size (requires AWS SDK call)
    total_bytes = estimate_s3_size(source_path)
    target_partitions = max(1, total_bytes // (target_file_size_mb * 1024 * 1024))

    print(f"Compacting {source_path}: ~{total_bytes/1e9:.1f} GB → {target_partitions} files")

    # Rewrite with optimal file count
    df.coalesce(target_partitions) \
      .write.mode("overwrite") \
      .parquet(source_path)


def estimate_s3_size(s3_path: str) -> int:
    """Estimate total bytes at an S3 prefix using boto3."""
    import boto3
    from urllib.parse import urlparse

    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip('/')

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')

    total_bytes = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            total_bytes += obj['Size']
    return total_bytes
```

### Compaction schedule

```python
# Compact daily — run after each day's data accumulates
# Example: compact previous day's hourly writes
def daily_compaction_job(spark, date: str, base_path: str):
    """Compact all hourly partitions for a given date into fewer large files."""
    from datetime import datetime, timedelta

    # Read all data for this date (all hours combined)
    date_path = f"{base_path}/event_date={date}/"
    df = spark.read.parquet(date_path)

    count = df.count()
    total_mb = estimate_s3_size(date_path) / (1024 * 1024)
    target_files = max(1, int(total_mb / 128))

    # Rewrite: the sub-hour partitions are collapsed into fewer files
    df.coalesce(target_files) \
      .write.mode("overwrite") \
      .parquet(date_path)

    print(f"Compacted {date}: {count:,} rows, {total_mb:.0f} MB → {target_files} files")
```

---

## 6.8 Partition Evolution: Adding New Partitions

Over time, you'll want to add new partition columns. This requires care.

### The problem

Your data for 2022–2023 is partitioned only by `date`:
```
events/date=2022-01-01/part-00001.parquet
events/date=2022-01-02/part-00001.parquet
```

You want to add `region` as a second partition column:
```
events/date=2022-01-01/region=US/part-00001.parquet
events/date=2022-01-01/region=EU/part-00001.parquet
```

**The old files don't have the `region` partition.** If you add new data with `date/region` partitioning while old data only has `date`, Spark will have trouble reading the mixed schema.

### Safe approach: migration

1. Write new data to a new path with the new partition scheme
2. Rewrite old data to the new path (backfill job)
3. Switch readers to the new path
4. Clean up old path

Or use Apache Iceberg — it handles partition evolution natively:
```sql
ALTER TABLE events ADD PARTITION FIELD region;
-- New data uses the new partition scheme
-- Old data is still readable
-- Iceberg's metadata handles the mixed partition history
```

---

## 6.9 Practical Data Layout Examples

### Example 1: User event tracking pipeline

```
Data characteristics:
  - 500M events/day
  - ~500 GB/day
  - Queries primarily filter by date, user_id, event_type
  - Aggregations are by date and region
  - Retention: 3 years

Recommended layout:
  Bronze: partitioned by ingestion_date
    s3://bucket/bronze/events/ingestion_date=2024-01-15/

  Silver: partitioned by event_date
    s3://bucket/silver/events/event_date=2024-01-15/
    (about 500 GB per date partition, ~3,900 files of 128 MB)

  Gold (daily metrics): partitioned by date
    s3://bucket/gold/user_daily_metrics/date=2024-01-15/
    (smaller: ~1M users × a few bytes = ~50 MB, 1 file)

  Gold (regional metrics): partitioned by date + region
    s3://bucket/gold/regional_metrics/date=2024-01-15/region=US/
    (~10 regions × ~5 MB per region = tiny, compacted to 1 file each)
```

### Example 2: E-commerce order pipeline

```
Data characteristics:
  - 10M orders/day globally
  - 5 regions: US, EU, APAC, LATAM, ME
  - Queries filter by date, region, order_status
  - Aggregations by date, region, product_category
  - Need point lookups by order_id (→ DynamoDB)

Silver layer:
  partitionBy("date", "region")
  s3://bucket/silver/orders/date=2024-01-15/region=US/
  → 2M US orders × ~1 KB = ~2 GB per (date, region) → 16 files of 128 MB ✅

Gold layer:
  Daily regional product metrics:
    partitionBy("date", "region")
    s3://bucket/gold/product_metrics/date=2024-01-15/region=US/
    → ~1000 product categories per region → ~100 MB total → 1 file ✅

DynamoDB table (order lookups):
  PK: ORDER#<order_id>
  Attributes: status, amount, user_id, date
  Written in batch by pipeline
```

---

## 6.10 Module Summary

The most important lessons from this module:

1. **Two partitioning concepts:** Spark in-memory partitions (temporary) and S3/Hive storage partitions (permanent). Don't confuse them.
2. **Choose partition columns based on query filters.** The most common filter = the partition column.
3. **Low cardinality partition columns only.** Thousands of distinct values max, not millions.
4. **Use string date format `YYYY-MM-DD`** for date partitions, not nested year/month/day.
5. **Target 128–256 MB per file.** Control with `coalesce()` or `repartition()` before write.
6. **`partitionBy()` + many Spark partitions = many small files.** Repartition by the column before writing.
7. **Compaction is maintenance, not optional.** Especially for streaming or high-frequency batch.
8. **Design your S3 path structure as architecture.** Bronze/Silver/Gold with clear partition schemes.

### ✅ Self-check

1. What is the difference between Spark in-memory partitions and S3 storage partitions?
2. Why is partitioning by `user_id` dangerous for a dataset with 10M distinct users?
3. Why is `partitionBy("year", "month", "day")` worse than `partitionBy("date")`?
4. You have 200 Spark partitions and you write with `partitionBy("region")` (10 regions). How many files will you get, and is that a problem?
5. What is compaction and when should you schedule it?
6. You need to add a new partition column to an existing 2-year dataset. What's the safe way to do it?

---

*Next: `07-performance-and-cost.md` →*
