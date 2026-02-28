# Module 03: Spark on S3
## The Most Important Thing Nobody Told You

> **Difficulty:** 🟡 Intermediate
> **Prerequisite:** Modules 01 & 02
> **Est. time:** 3–4 hours

---

## 3.1 The Big Lie: S3 Is Not a Filesystem

This is the single most important thing to understand about Spark on S3. Most Spark documentation, tutorials, and examples assume HDFS-style semantics. S3 is fundamentally different. Getting this wrong causes data corruption, missing data, and very confusing bugs.

### What you think S3 is

You think S3 works like a filesystem:
```
bucket/
├── data/
│   ├── file1.parquet
│   └── file2.parquet
└── output/
    └── result.parquet
```

- `mkdir` creates a directory
- `rename` is atomic
- `delete` is immediate and consistent
- You can list a directory and get all its contents immediately

### What S3 actually is

S3 is an **object store**. Key differences:

1. **No real directories.** "Folders" are just prefixes in object keys. `data/file1.parquet` is an object with the key `data/file1.parquet`. There is no folder object called `data/`.

2. **No atomic rename.** There is no `rename` operation. What looks like a rename is actually: `CopyObject` (read all bytes, write to new key) + `DeleteObject` (delete old key). For large files, this takes time and is not atomic.

3. **Eventual consistency... mostly gone now.** AWS made S3 strongly consistent for most operations in December 2020. But certain edge cases remain (see below).

4. **List operations are eventually consistent in some edge cases.** Newly written objects may not immediately appear in list results in all scenarios, especially immediately after a write.

### Why this matters for Spark

Spark's output commit protocol (determining when a job's output is "committed" and visible) was designed for HDFS, which has atomic renames. The naive protocol is:

```
Stage data in a temp directory → When job succeeds, rename temp/ to final/
```

On HDFS, the rename is **atomic** (< 1ms). Success or failure. Consistent.
On S3, the "rename" is: copy all bytes (slow) + delete old (eventually consistent). During this window, partial data is visible. If a job crashes mid-rename, you get corrupted output with some files from the old job and some from the new.

**This is a real production problem.** It has bitten many teams.

---

## 3.2 The EMRFS S3-Optimized Committer

AWS built a solution: the **EMRFS S3-optimized committer**. This is enabled by default on EMR 5.19+ and replaces the naive rename-based protocol.

### How it works

Instead of staging in a temp dir and renaming, the optimized committer:

1. Tasks write data directly to the final S3 location with a staging suffix
2. On task success, a manifest of successfully written files is recorded
3. On job commit, the driver atomically renames only the file metadata (S3 multipart upload completes)
4. If a task fails, its files are simply abandoned (not referenced in the manifest)

```
Traditional protocol:                EMRFS optimized:

Task 1 → temp/_task1/file.parquet    Task 1 → final/._task1_staging.parquet
Task 2 → temp/_task2/file.parquet    Task 2 → final/._task2_staging.parquet
         ↓ (slow copy + delete)              ↓ (instant completion)
final/file1.parquet                  final/file1.parquet
final/file2.parquet                  final/file2.parquet
```

### How to verify it's enabled

The EMRFS S3-optimized committer is **automatically used** for Parquet writes on EMR when:
- Using `spark.write.parquet()` API
- Writing with dynamic partition overwrite enabled

Check your Spark logs for: `Using EMRFS S3-optimized committer`

**For CSV, JSON, text files:** The standard FileOutputCommitter is used. For these, use Algorithm Version 2:
```
spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2
```

### Enabling dynamic partition overwrite

When you write to partitioned data and only want to overwrite the partitions you're writing (not all partitions), use dynamic partition overwrite:

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df.write.mode("overwrite") \
        .partitionBy("date") \
        .parquet("s3://bucket/output/")
```

With `partitionOverwriteMode = dynamic`:
- You write `date=2024-01-15` data
- Only `s3://bucket/output/date=2024-01-15/` is overwritten
- All other date partitions are untouched

Without it (`static` mode, the default):
- Writing **any** partition overwrites **all** partitions
- `s3://bucket/output/` is completely deleted and rewritten
- If your job fails mid-write, you've lost all existing data

**This is a critical footgun.** Always use dynamic partition overwrite for incremental pipelines.

---

## 3.3 Reading from S3

### Basic patterns

```python
# Read a single file
df = spark.read.parquet("s3://bucket/path/to/file.parquet")

# Read a directory (all files in the directory)
df = spark.read.parquet("s3://bucket/path/to/directory/")

# Read with glob pattern
df = spark.read.parquet("s3://bucket/path/date=2024-01-*/")

# Read specific partitions (partition pruning)
df = spark.read.parquet("s3://bucket/events/") \
               .filter("date = '2024-01-15'")
# Spark reads only s3://bucket/events/date=2024-01-15/ if partition discovery is enabled
```

### Partition discovery and pruning

When you write partitioned data with `partitionBy("date")`, Spark creates a Hive-style directory structure:
```
s3://bucket/events/
├── date=2024-01-13/
│   ├── part-00001.parquet
│   └── part-00002.parquet
├── date=2024-01-14/
│   ├── part-00001.parquet
│   └── part-00002.parquet
└── date=2024-01-15/
    ├── part-00001.parquet
    └── part-00002.parquet
```

When you read with a filter on the partition column:
```python
df = spark.read.parquet("s3://bucket/events/")
result = df.filter(df.date == "2024-01-15")
```

Spark does **partition pruning** — it skips `date=2024-01-13/` and `date=2024-01-14/` entirely. It only lists and reads `date=2024-01-15/`. This is what makes partitioning powerful for performance.

**Important:** Partition pruning only works if you filter on the partition column. If you filter on a non-partition column, Spark must read all partitions.

### Schema inference vs explicit schema

```python
# Don't do this in production:
df = spark.read.parquet("s3://bucket/events/")  # Spark infers schema from file metadata

# Do this instead:
from pyspark.sql.types import *
schema = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("event_time", TimestampType(), nullable=False),
])
df = spark.read.schema(schema).parquet("s3://bucket/events/")
```

**Why:** Schema inference reads the footer of all Parquet files to discover the schema. At petabyte scale with millions of files, this takes minutes or hours. Always specify schemas explicitly in production.

---

## 3.4 The Small Files Problem

This is the #1 operational problem for Spark on S3 teams.

### What it is

A "small file" is a file much smaller than the HDFS block size (~128 MB). When your S3 data lake accumulates thousands or millions of small files (1 KB – 10 MB each), several bad things happen:

**Problem 1: Metadata overhead**

S3 lists objects at ~1000 objects per API call. Reading a directory with 100,000 files requires 100 API calls (each ~10–50 ms). That's 1–5 seconds of listing before reading a single byte. At millions of files, this becomes minutes.

```
1,000,000 files ÷ 1,000 per S3 ListObjects call = 1,000 API calls
1,000 × 50ms = 50 seconds just for listing
And S3 throttles aggressive listing → retries → even slower
```

**Problem 2: Spark task overhead**

Spark creates one task per input file (up to the configured minimum partition size). 1,000,000 small files → 1,000,000 tasks. Each task has JVM initialization overhead (~50–100 ms). Task scheduling overhead. Result collection overhead. The job spends more time on overhead than on actual computation.

**Problem 3: Hive Metastore / Glue Catalog explosion**

Each partition has many files. More files = more metadata. Glue Catalog can get slow with very large numbers of files.

### How small files happen

1. **Low parallelism writes:** Too few partitions → few files, but may still be small
2. **High parallelism writes:** Too many Spark partitions → too many output files
3. **Streaming micro-batches:** Kafka → S3 at 1-minute intervals creates many small files per partition
4. **Filter + write:** After filtering, data might be very sparse

```python
# This creates 200 files (default spark.sql.shuffle.partitions = 200)
# Even if the data is only 1 GB → 200 × 5 MB files = fine
df.write.parquet("s3://bucket/output/")

# After filtering, data might be very small
# But still creates 200 files → 200 × 100 KB files = small files problem
df.filter(df.country == "LU") \
  .write.parquet("s3://bucket/output/lu_only/")
```

### How to fix/prevent small files

**Strategy 1: Coalesce before writing**
```python
# Estimate target: 128 MB per file
# If your output is ~1 GB, coalesce to ~8 files
df.coalesce(8).write.parquet("s3://bucket/output/")
```

**Strategy 2: Repartition to control output**
```python
# More precise control — triggers a shuffle
# Use when you also need to repartition by a column
df.repartition(20, "region") \
  .write.partitionBy("region") \
  .parquet("s3://bucket/output/")
```

**Strategy 3: Adaptive Query Execution (AQE)**
```python
# Let Spark automatically coalesce partitions after shuffles
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")  # 128 MB
```

AQE is available in Spark 3.0+ and is **one of the most impactful free optimizations.** Enable it always.

**Strategy 4: Compaction jobs**
For streaming or high-frequency batch, run a periodic compaction job that reads many small files and rewrites as fewer large files:

```python
def compact_partition(spark, s3_path, target_file_size_mb=128):
    """Read all files in a path and rewrite as optimally-sized files."""
    df = spark.read.parquet(s3_path)
    total_size_bytes = get_total_size(s3_path)  # custom function using boto3
    target_files = max(1, total_size_bytes // (target_file_size_mb * 1024 * 1024))

    df.coalesce(target_files) \
      .write.mode("overwrite") \
      .parquet(s3_path)
```

**Strategy 5: Modern table formats (Delta Lake, Apache Iceberg)**

These maintain file manifests and support `OPTIMIZE` / `COMPACT` operations that merge small files without rewriting the whole table. If you're building a new system, consider using Apache Iceberg with EMR — it has excellent AWS support.

---

## 3.5 File Formats: What to Use and Why

### Parquet — Your default choice

Parquet is a **columnar** binary file format designed for analytics.

```
Row-based (CSV, JSON):           Columnar (Parquet):
Row 1: [id, name, amount, date]  Column id:     [1, 2, 3, ...]
Row 2: [id, name, amount, date]  Column name:   ["Alice", "Bob", ...]
Row 3: [id, name, amount, date]  Column amount: [100.0, 200.0, ...]
...                              Column date:   ["2024-01-01", ...]
```

**Why columnar is better for analytics:**
- If you only need `id` and `amount`, Parquet reads only those two columns. CSV reads every column.
- Columns of the same type compress much better (repeated patterns)
- Enables vectorized reading (process multiple rows in one CPU instruction)

**Parquet features:**
- **Column statistics:** Each row group stores min/max per column. Spark can skip entire row groups that don't match your filter.
- **Dictionary encoding:** Common values are encoded as integers. High repetition = high compression.
- **Nested types:** Supports arrays, maps, structs
- **Schema embedded in file:** File is self-describing

**Read performance:** Parquet reads are 10–100x faster than CSV for typical analytics queries.

> **Go deeper:** Module 12 (`12-parquet-deep-dive.md`) covers Parquet's internal structure (row groups, pages, encodings, Bloom filters) and all the tuning options in detail. Read it after this module.

### ORC — Similar to Parquet

ORC (Optimized Row Columnar) is Hive's native columnar format. Similar performance to Parquet. Use Parquet unless you have a specific reason to use ORC (e.g., Hive compatibility).

### Avro — For streaming schemas

Avro is a row-based format with schema evolution support. Better for streaming (row by row) and schema migrations. Not great for analytics.

### Delta Lake / Apache Iceberg — Table formats (not file formats)

These sit on top of Parquet files and add:
- ACID transactions
- Schema evolution
- Time travel (query historical versions)
- Efficient upserts (merge operations)
- Automatic file compaction
- Improved query planning via table statistics

For new petabyte-scale pipelines, Apache Iceberg on EMR is increasingly the right answer. It solves many of the problems (small files, overwrites, schema evolution) described in this module.

```python
# Apache Iceberg on EMR
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://my-bucket/warehouse/") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .getOrCreate()

# UPSERT (merge) operation — not possible with plain Parquet
spark.sql("""
    MERGE INTO glue_catalog.events e
    USING new_events n ON e.event_id = n.event_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

---

## 3.6 S3 Request Costs and Throttling

S3 charges for API requests, not just storage. At scale, this matters.

### Request pricing (approximate)

| Operation | Cost |
|-----------|------|
| GET / SELECT | $0.0004 per 1,000 requests |
| PUT / COPY / POST | $0.005 per 1,000 requests |
| LIST | $0.005 per 1,000 requests |
| DELETE | Free |

**Small files amplify costs.** 1,000,000 small files vs 100 large files:
- Read all: 1,000,000 GET requests vs 100 GET requests (10,000x more expensive)
- List: many more LIST requests for small-file prefix

### S3 request rate limits

S3 supports high request rates but throttles at extremes:
- Per prefix: 5,500 GET/HEAD requests per second, 3,500 PUT/COPY/POST/DELETE per second
- Throttling returns HTTP 503 "Slow Down" responses

**The prefix problem:** If all your S3 paths start with `s3://bucket/events/`, all requests go to the same prefix. AWS partitions S3 internally by prefix hash. Too many requests to the same prefix = throttling.

**Solution:** Add randomization or use many distinct partition columns to spread requests across prefixes.

Old advice (pre-2018): Add random prefixes to object keys. **This is now outdated.** S3 automatically scales to support high request rates on well-partitioned namespaces as of 2018. If you see throttling, it's usually because you have genuinely too many requests, not a prefix issue.

---

## 3.7 S3 Consistency Model

AWS made S3 strongly consistent for most operations in December 2020. But understand what this means exactly.

### What's strongly consistent now

- Read-after-write: After a successful `PutObject`, any subsequent `GetObject` or `HeadObject` will see the new data
- List consistency: After writing objects, `ListObjects` will include the new objects

### What's still eventually consistent (edge cases)

- **S3 Replication:** Cross-region or cross-account replication is eventually consistent. If you read from a replica bucket, you may see stale data.
- **Delete + immediate list:** In some race conditions with concurrent operations, deletions may not be immediately reflected.
- **Extremely high concurrent write scenarios:** Edge cases in partition discovery can occasionally miss files written by concurrent jobs.

### Practical implication for Spark

For most Spark workloads, S3 strong consistency is sufficient. The main remaining concern is:

1. **Concurrent writers:** Two Spark jobs writing to the same path simultaneously can corrupt data. Never allow concurrent writes to the same output path.
2. **Glue Catalog updates:** After writing data, updating the Glue catalog is a separate API call. If your consumer queries the catalog before you update it, they see the old data.

---

## 3.8 S3 and EMRFS Configuration

Key S3-related configurations for EMR Spark jobs:

```python
# In your SparkSession configuration or spark-submit args

# The EMRFS S3-optimized committer is on by default, but let's be explicit
spark.conf.set("spark.sql.sources.commitProtocolClass",
               "com.amazon.emr.committer.EmrOptimizedSparkSqlParquetCommitter")

# Dynamic partition overwrite (CRITICAL for incremental pipelines)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Adaptive Query Execution (enable this — it's free performance)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Retry configuration for S3 (default is usually fine but can tune)
spark.conf.set("spark.hadoop.fs.s3.maxRetries", "10")

# Enable speculative execution (re-run slow tasks on another executor)
spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.speculation.multiplier", "3.0")
spark.conf.set("spark.speculation.quantile", "0.9")

# Prevent empty partitions from creating 0-byte files
# (useful for partitioned writes where some partitions have no data)
```

---

## 3.9 Reading and Writing with Schema Registry / Glue Catalog

On AWS, the **AWS Glue Data Catalog** acts as your metadata store — it remembers what tables exist, where their data lives (S3 paths), and what their schemas look like.

```python
# Configure Spark to use Glue Catalog as Hive Metastore
spark = SparkSession.builder \
    .config("hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .enableHiveSupport() \
    .getOrCreate()

# Now you can query tables registered in Glue
df = spark.sql("SELECT * FROM mydb.events WHERE date = '2024-01-15'")

# Write and register a new table
df.write \
  .mode("overwrite") \
  .partitionBy("date") \
  .parquet("s3://my-bucket/warehouse/events/")

spark.sql("""
    CREATE TABLE IF NOT EXISTS mydb.events
    USING PARQUET
    LOCATION 's3://my-bucket/warehouse/events/'
    PARTITIONED BY (date STRING)
""")

# After writing new partitions, update the catalog
spark.sql("MSCK REPAIR TABLE mydb.events")
# Or for a specific partition:
spark.sql("ALTER TABLE mydb.events ADD PARTITION (date='2024-01-15')")
```

---

## 3.10 Module Summary

The critical lessons from this module:

1. **S3 is not HDFS.** No atomic rename. Different consistency model. Design accordingly.
2. **Always use the EMRFS S3-optimized committer.** It's default on EMR, but know why it exists.
3. **Dynamic partition overwrite is critical.** Without it, you risk losing all existing data on a failed write.
4. **Small files will kill you.** Target 128–256 MB per file. Use coalesce + AQE.
5. **Parquet is your default format.** Columnar, compressed, predicate-pushdown capable.
6. **Consider Apache Iceberg** for complex pipelines needing upserts, schema evolution, and file management.
7. **Schema inference is too slow at scale.** Always define schemas explicitly.
8. **Enable AQE** (`spark.sql.adaptive.enabled = true`). Free performance improvement.

### ✅ Self-check

1. Why can't Spark use the HDFS rename trick on S3?
2. What does the EMRFS S3-optimized committer do differently?
3. What is `partitionOverwriteMode = dynamic` and why do you need it?
4. Name three causes of the small files problem.
5. Why is Parquet better than CSV for analytics?
6. What happens if you don't define an explicit schema when reading Parquet at petabyte scale?

---

*Next: `04-batch-pipeline-architecture.md` →*
