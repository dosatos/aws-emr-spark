# Module 07: Performance and Cost Optimization
## Making Petabyte Pipelines Fast and Cheap

> **Difficulty:** 🔴 Advanced
> **Prerequisite:** Modules 01–06
> **Est. time:** 5–6 hours

---

## 7.1 The Performance Mental Model

Before tuning anything, internalize this hierarchy:

```
Performance Improvement Priority (top = biggest impact):

1. Data layout & partitioning  ←── 10x–1000x impact
   (read less data, skip irrelevant partitions)

2. File format & compression   ←── 5x–20x impact
   (Parquet + Snappy vs CSV)

3. Join strategy               ←── 5x–50x impact
   (broadcast vs shuffle join)

4. Shuffle minimization        ←── 2x–10x impact
   (reduce wide transformations)

5. Adaptive Query Execution    ←── 2x–5x impact (free!)
   (enable spark.sql.adaptive.enabled)

6. Executor configuration      ←── 1.2x–3x impact
   (memory, cores, parallelism)

7. Instance type selection     ←── 1.5x–3x impact
   (memory-optimized for Spark)
```

**Most beginners start at #6 or #7 and wonder why tuning doesn't help.** Always start at the top.

---

## 7.2 Cluster Sizing for EMR

### Instance type selection

Spark is memory-hungry. For most Spark workloads:

**Recommended instance families:**
| Family | vCPU:Memory | Best for |
|--------|-------------|----------|
| `r5` / `r6g` | 1:8 (memory-optimized) | Spark joins, large shuffles |
| `m5` / `m6g` | 1:4 (general purpose) | Balanced workloads |
| `c5` / `c6g` | 1:2 (compute-optimized) | CPU-bound transformations |

**For most Spark ETL workloads: `r5.4xlarge` or `r5.8xlarge`.**

`r5.4xlarge`: 16 vCPU, 128 GB RAM
`r5.8xlarge`: 32 vCPU, 256 GB RAM

The `r6g` (Graviton2) variants are 10–15% cheaper for the same performance. Consider them.

### The executor sizing formula

**General rule:** Leave resources for YARN, OS, and driver overhead.

For `r5.4xlarge` (16 vCPU, 128 GB RAM):

```
Reserved for OS + YARN:         ~4 GB, ~1 vCPU
Available for Spark:           ~124 GB, 15 vCPU

Option A: 1 fat executor per node
  executor memory: 110 GB
  executor cores:  15
  overhead:        14 GB (memoryOverhead)
  → Pros: max memory per task, fewer JVMs
  → Cons: GC pauses, poor parallelism within node

Option B: Multiple executors per node (recommended for most workloads)
  # Rule of thumb: 4-5 cores per executor
  Number of executors per node: 15 / 5 = 3 executors
  executor cores: 5
  executor memory: 124 GB / 3 ≈ 40 GB
  overhead per executor: 40 * 0.1875 ≈ 7.5 GB
  effective heap: 40 - 7.5 = 32.5 GB
  → Pros: better parallelism, more GC threads, resilience
  → Cons: more JVM overhead
```

**Recommended config for `r5.4xlarge` cluster:**
```bash
spark-submit \
  --executor-memory 40g \
  --executor-cores 5 \
  --driver-memory 8g \
  --driver-cores 4 \
  --conf spark.executor.memoryOverhead=8g \
  --conf spark.yarn.executor.memoryOverheadFactor=0.1875
```

### Cluster sizing (total nodes)

How many nodes do you need?

```
Target parallelism:
  How much data? → 1 TB
  Target partition size: 128 MB → 1 TB / 128 MB = 8,192 partitions = 8,192 concurrent tasks needed

Available parallelism per node (r5.4xlarge, 3 executors × 5 cores):
  15 cores per node

Nodes needed:
  8,192 tasks / 15 cores = ~546 nodes (this is your peak parallelism need)

But: not all tasks run simultaneously (multi-stage DAG)
Practical rule: start with 1/3 of theoretical need
  546 / 3 ≈ 180 nodes → probably too many for most ETL

Real-world sizing:
  For a 1 TB job with reasonable shuffle and 4-hour SLA:
  30–50 r5.4xlarge nodes is usually right
  Use Managed Scaling to auto-adjust
```

**The right approach:** Start with a reasonable estimate, use Managed Scaling, observe job duration and resource utilization in the Spark History Server, then adjust.

---

## 7.3 Memory Configuration Deep Dive

### The complete memory stack

```
Physical Node: 128 GB RAM
│
├── OS + System: ~4 GB
├── YARN NodeManager: ~1 GB
│
└── YARN Containers (Spark executors):
    ┌─────────────────────────────────┐
    │ Executor Container: 48 GB       │
    │                                 │
    │  JVM Heap (executor.memory):    │
    │  40 GB                          │
    │  ┌───────────────────────────┐  │
    │  │ Reserved: ~300 MB         │  │
    │  │ User Memory: ~16 GB (40%) │  │
    │  │ Execution Memory: ~14 GB  │  │  ← shuffle, sort, aggregation
    │  │ Storage Memory: ~10 GB    │  │  ← cached data (flexible)
    │  └───────────────────────────┘  │
    │                                 │
    │  Off-heap (memoryOverhead):     │
    │  8 GB                           │
    │  ┌───────────────────────────┐  │
    │  │ Python worker processes   │  │
    │  │ JVM native memory         │  │
    │  │ Thread stacks             │  │
    │  │ OS buffers                │  │
    │  └───────────────────────────┘  │
    └─────────────────────────────────┘
```

### When to increase memoryOverhead

Default `memoryOverhead` = 18.75% of executor memory (but minimum 384 MB).

Increase it when:
- Using PySpark with heavy UDFs (Python workers consume off-heap memory)
- Large broadcast variables
- Heavy use of Pandas UDFs (Arrow-based)
- Getting "Container killed by YARN for exceeding memory limits" despite JVM heap being OK

```bash
# Conservative: 20% overhead
--conf spark.executor.memoryOverhead=8g  # if executor.memory=40g

# Heavy Python UDFs: 30–40% overhead
--conf spark.executor.memoryOverhead=12g
```

### Tuning Spark's unified memory

```bash
# Default: 60% of (heap - reserved) for Spark
spark.memory.fraction=0.6

# Default: 50% of unified memory for storage cache
spark.memory.storageFraction=0.5

# For heavy shuffle workloads: increase execution fraction
spark.memory.fraction=0.7
spark.memory.storageFraction=0.3  # less for storage, more for execution

# For heavy caching: inverse
spark.memory.fraction=0.7
spark.memory.storageFraction=0.7  # more for storage
```

---

## 7.4 Shuffle Optimization

Shuffles are the primary cause of slowness in Spark jobs. Here's how to minimize their cost.

### Measure first: find your shuffles

In the Spark History Server:
1. Go to the job → Stages tab
2. Look for stages with large "Shuffle Read/Write" columns
3. A shuffle stage with 500 GB read indicates a very expensive operation

### Technique 1: Reduce shuffle partitions

The default `spark.sql.shuffle.partitions = 200`. For large datasets, increase it. For small datasets, decrease it.

```python
# Rule: target 128 MB per shuffle partition
# If total shuffle data = 10 GB: 10,000 MB / 128 = 78 partitions
spark.conf.set("spark.sql.shuffle.partitions", "80")

# Or let AQE handle it automatically:
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(128 * 1024 * 1024))
```

### Technique 2: Broadcast joins

If one side of a join is small (< a few hundred MB), **broadcast** it to every executor instead of shuffling both sides:

```python
from pyspark.sql import functions as F

# Automatic broadcast (Spark decides based on table size)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024))  # 100 MB

# Manual broadcast hint (forces broadcast regardless of threshold)
result = df_large.join(
    F.broadcast(df_small),
    on="user_id",
    how="inner"
)
```

**When to use broadcast:**
- Joining large events table with a small dimension table (e.g., product catalog, country codes)
- One side is < 100–200 MB uncompressed
- The small side fits comfortably in executor memory

**Don't broadcast when:**
- The "small" side is actually large (> 500 MB) — causes OOM
- Both sides are large — you need the shuffle join

**Check if a broadcast join is being used:**
```python
result.explain()
# Look for: BroadcastHashJoin or BroadcastExchange in the plan
# vs: SortMergeJoin (shuffle-based)
```

### Technique 3: Pre-aggregation before joins

If you need to join two large tables and then aggregate, aggregate one side first to reduce its size:

```python
# SLOW: shuffle join on full data, then aggregate
result = df_events.join(df_users, on="user_id") \
                   .groupBy("user_segment") \
                   .sum("amount")

# FASTER: aggregate events by user_id first (reduce size), then join
events_by_user = df_events.groupBy("user_id").agg(F.sum("amount").alias("total"))
result = events_by_user.join(df_users, on="user_id") \
                        .groupBy("user_segment") \
                        .sum("total")
# events_by_user is much smaller than df_events → smaller shuffle/join
```

### Technique 4: Avoid Cartesian products

A cross join (no join condition or `how="cross"`) creates O(N×M) output. 1M rows × 1M rows = 1 trillion rows. This will destroy your cluster.

```python
# DANGER: Spark may silently do a cross join if you join on a column with different names
# and forget to specify the condition correctly
result = df_a.join(df_b)  # CrossJoin! Both DataFrames with no condition

# Safe: always specify the join condition
result = df_a.join(df_b, df_a.id == df_b.user_id, how="inner")
```

Enable cross join protection:
```python
spark.conf.set("spark.sql.crossJoin.enabled", "false")  # default in Spark 3
# This will raise an error if an accidental cross join is detected
```

### Technique 5: Adaptive Query Execution (AQE)

AQE is Spark 3.0+'s runtime optimization framework. Enable it and let Spark self-tune:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")

# AQE features:
# 1. Coalesce small partitions after shuffle (reduces small files)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# 2. Handle skewed partitions automatically (see section 7.6)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# 3. Switch join strategies at runtime (e.g., switch to broadcast if one side is small)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

**AQE is one of the most impactful free improvements in Spark 3. Always enable it.**

---

## 7.5 Reading Performance

### Predicate pushdown

Parquet files store column statistics (min/max per row group). When you filter on a column, Spark can skip entire row groups that don't match:

```python
# Spark pushes this filter into Parquet scan:
# If the row group's max amount < 100, skip the whole row group
df = spark.read.parquet("s3://bucket/events/") \
               .filter(df.amount > 100)

# Check if pushdown is happening:
df.explain()
# Look for: PushedFilters in the plan
```

Pushdown works best with:
- Numeric comparisons (`>`, `<`, `=`)
- String equality (with dictionary encoding)
- The column must be in the Parquet schema (not a partition column — those are pruned, not pushed down)

### Projection pushdown

Parquet reads only the columns you select:

```python
# Reads all 50 columns — slow and wastes I/O
df = spark.read.parquet("s3://bucket/events/")
result = df.groupBy("user_id").sum("amount")

# Reads only user_id and amount — much faster (Catalyst does this automatically)
df = spark.read.parquet("s3://bucket/events/").select("user_id", "amount")
result = df.groupBy("user_id").sum("amount")

# Catalyst usually does this automatically, but being explicit helps with schema inference
```

### S3 read optimization

```bash
# Increase read buffer size for large sequential reads
spark.hadoop.fs.s3a.readahead.range=1048576  # 1 MB
spark.hadoop.fs.s3a.fast.upload=true

# Parallelize S3 listings for large directories
spark.hadoop.fs.s3a.list.version=2

# EMRFS-specific: increase block size for large files
spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=134217728  # 128 MB
```

---

## 7.6 Skew: The Hidden Performance Killer

**Skew** occurs when data is unevenly distributed across partitions. One partition has 10x more data than others → one task runs 10x longer → everything waits for it.

### Detecting skew

In the Spark History Server:
1. Go to a slow Stage
2. Look at the "Task Duration" distribution
3. If the median is 30 seconds but the max is 30 minutes → skew

Or in code:
```python
# Check partition sizes
df.groupBy(F.spark_partition_id()).count().orderBy(F.desc("count")).show(20)

# Check data distribution for join keys
df.groupBy("user_id").count().orderBy(F.desc("count")).show(20)
# If top user has 10M rows and average is 100 → severe skew
```

### Causes of skew

1. **Hot keys:** Some user_ids have millions of events; others have 10
2. **Null keys:** Joining on a column with many nulls — all nulls go to one partition
3. **Uneven partitioning:** Data was never repartitioned after filtering
4. **Holiday effect:** Black Friday has 100x normal volume → date=2024-11-29 is 100x larger than average

### Fixing skew: Salting

Salting adds a random suffix to hot keys to distribute them:

```python
import random
from pyspark.sql import functions as F

NUM_SALTS = 10

# Salt the larger table's keys
events_salted = df_events.withColumn(
    "user_id_salted",
    F.concat(
        F.col("user_id"),
        F.lit("_"),
        (F.rand() * NUM_SALTS).cast("int").cast("string")
    )
)

# Explode the small table to match all salts
users_exploded = df_users.withColumn(
    "salt", F.explode(F.array([F.lit(i) for i in range(NUM_SALTS)]))
).withColumn(
    "user_id_salted",
    F.concat(F.col("user_id"), F.lit("_"), F.col("salt").cast("string"))
)

# Join on salted key — no more skew
result = events_salted.join(users_exploded, on="user_id_salted", how="inner") \
                       .drop("user_id_salted", "salt")
```

### Fixing skew: AQE skew handling

Spark 3.x AQE can automatically detect and split skewed partitions:

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
# A partition is skewed if it's 5x the median size

spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
               str(256 * 1024 * 1024))  # 256 MB
# Only split partitions larger than 256 MB
```

AQE splits skewed partitions into smaller sub-tasks automatically. Enable this — it handles most common skew without code changes.

### Fixing skew: Filter nulls before joining

```python
# Remove null join keys before joining (they can't match anyway)
df_events_clean = df_events.filter(F.col("user_id").isNotNull())
df_users_clean = df_users.filter(F.col("user_id").isNotNull())

result = df_events_clean.join(df_users_clean, on="user_id", how="left")
```

---

## 7.7 Spark UI and History Server: Debugging Performance

The Spark History Server is your primary performance debugging tool. Learn to read it.

### Key views

**Jobs Tab:**
- Shows all jobs in your application
- Duration, number of tasks, stages
- Look for unexpected job duration

**Stages Tab:**
- Shows all stages and their metrics
- Sort by "Duration" to find bottlenecks
- Columns to check: Input, Output, Shuffle Read, Shuffle Write, Duration

**Stage Details:**
- Shows task-level metrics
- Look for: task duration distribution (skew visible here), GC time, shuffle spill to disk

**SQL Tab:**
- Shows the executed SQL/DataFrame plans
- Links to the physical plan
- Shows row counts, bytes at each step

### What to look for

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| One task 10x slower than others | Skew | Salting, AQE skew, or filter nulls |
| High "Shuffle Spill (Disk)" | Not enough execution memory | Increase executor memory |
| High GC time (> 20% of task time) | Too much garbage in JVM | Use off-heap storage, reduce caching |
| Many 0-byte shuffle partitions | Over-partitioned | Reduce shuffle partitions |
| Driver OOM | Collecting too much data to driver | Remove .collect(), use write() instead |
| Long "Getting result" duration | Large result being sent from executors to driver | Same — don't collect large results |

---

## 7.8 Cost Optimization

EMR costs = EC2 instance cost + EMR service markup (~25% on top of EC2).

### The cost breakdown

For a 20-node `r5.4xlarge` cluster (On-Demand):
- EC2 `r5.4xlarge` On-Demand: ~$1.01/hr per instance
- EMR markup: ~25%
- Total per instance: ~$1.26/hr
- 20 instances for 4 hours: 20 × $1.26 × 4 = **$100.80 per job run**

Monthly cost (daily run): ~$100.80 × 30 = **~$3,024/month**

### Cost optimization techniques

**Strategy 1: Spot instances for task nodes (60–90% savings)**

```bash
# Add task nodes as Spot with fallback to On-Demand
# Replace 15 of 20 nodes with Spot → 75% of compute at 60-90% discount
# Effective savings: 75% × 75% discount ≈ 56% overall cost reduction
```

**Strategy 2: Right-sizing (biggest non-Spot lever)**

Most new EMR jobs are over-provisioned. Start with a reasonable cluster, measure utilization, shrink.

If YARN memory utilization averages 40% → you're using half your cluster. Reduce by 50%.

**Strategy 3: Transient clusters (pay only when running)**

```python
# Stop leaving clusters running idle
# Each idle r5.4xlarge costs $1.26/hr
# 20 idle nodes for 20 idle hours/day = 20 × $1.26 × 20 = $504/day wasted

# Use transient clusters: create → run → auto-terminate
# Pay only for the 4 hours the job actually runs
```

**Strategy 4: EMR Serverless for low-utilization jobs**

If your cluster utilization is < 70% (On-Demand), EMR Serverless is cheaper (per-second billing):
- Serverless charges ~$0.052224/vCPU-hour and ~$0.0057785/GB-hour
- For a job that actually uses 10 vCPU × 40 GB for 4 hours:
  - CPU: 10 × $0.052224 × 4 = $2.09
  - Memory: 40 × $0.0057785 × 4 = $0.92
  - Total: **~$3.01** vs potentially much more for a provisioned cluster

**Strategy 5: Graviton instances (10–15% better price/performance)**

```bash
# Replace r5.4xlarge ($1.01/hr) with r6g.4xlarge ($0.82/hr)
# Same memory, similar or better performance, 19% cheaper
# Ensure your Spark version supports Graviton: EMR 6.1+ ✅
```

**Strategy 6: S3 cost optimization**

```bash
# Avoid unnecessary S3 LIST operations (costly at scale)
# Use explicit paths rather than glob patterns where possible

# Use S3 Intelligent-Tiering for data accessed infrequently
# After 30 days of no access → automatically moved to cheaper tier
# No retrieval fee (unlike Glacier)

# Use S3 Lifecycle policies to archive old data
aws s3api put-bucket-lifecycle-configuration \
  --bucket my-data-bucket \
  --lifecycle-configuration '{
    "Rules": [{
      "ID": "archive-bronze-after-90-days",
      "Filter": {"Prefix": "bronze/"},
      "Status": "Enabled",
      "Transitions": [
        {"Days": 90, "StorageClass": "STANDARD_IA"},
        {"Days": 365, "StorageClass": "GLACIER"}
      ]
    }]
  }'
```

### EMR cost gotchas

**Gotcha 1: Minimum billing is 1 minute, but clusters take 5–10 minutes to start**

A 15-minute job costs: cluster startup (~8 min) + job (~15 min) + shutdown (~2 min) = ~25 minutes.
Use EMR Serverless for short jobs to avoid cluster startup overhead.

**Gotcha 2: Large master nodes are wasted money**

The master runs: YARN ResourceManager, Spark History Server, coordination. It doesn't run tasks.
Use a `m5.xlarge` for master (4 vCPU, 16 GB RAM), not `r5.4xlarge`.

**Gotcha 3: EBS volumes are not included in instance pricing**

EMR attaches EBS volumes for local storage (HDFS, shuffle data). These cost extra.
At heavy shuffle workloads, local SSD instances (`r5d.4xlarge`) can be more cost-effective.

**Gotcha 4: Data transfer costs**

Reading from S3 in the same region as EMR: **free**.
Reading from S3 in a different region: **$0.02/GB**. Always run EMR in the same region as your S3 data.

---

## 7.9 Module Summary

Performance and cost are deeply connected — fast jobs are usually cheap jobs.

**Top performance levers (in order):**
1. Get your data layout and partitioning right (Module 06)
2. Enable AQE (`spark.sql.adaptive.enabled=true`)
3. Use broadcast joins for small tables
4. Fix skew with salting or AQE skew handling
5. Right-size executor memory and cores
6. Enable speculative execution for straggler mitigation

**Top cost levers:**
1. Use Spot instances for task nodes (60–90% savings)
2. Use transient clusters (pay only for run time)
3. Right-size your cluster (don't over-provision)
4. Use Graviton instances (10–15% better price/performance)
5. Lifecycle policies for S3 data (archive old Bronze data)
6. EMR Serverless for intermittent low-utilization jobs

### ✅ Self-check

1. What is the recommended executor core count? What happens with too many cores per executor?
2. What is `memoryOverhead` and when do you need to increase it?
3. What is AQE and what three things does it automatically optimize?
4. What is a broadcast join? When is it faster than a shuffle join?
5. What is skew? How do you detect it in the Spark History Server?
6. What is salting and how does it fix skew?
7. Name three EMR cost optimization strategies.

---

*Next: `08-failure-modes-and-watchouts.md` →*
