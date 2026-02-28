# Module 01: Apache Spark Fundamentals
## From Zero to Understanding the Engine

> **Difficulty:** 🟢 Beginner → 🟡 Intermediate
> **Prerequisite:** None
> **Est. time:** 4–6 hours

---

## 1.1 Why Spark Exists — The Problem It Solves

### Start here: What happens when your data outgrows one machine?

Imagine you have a log file. 1 GB. You write a Python script. It reads the file, filters lines, counts events. Done in 3 seconds. Easy.

Now the file is 10 GB. Still works, maybe 30 seconds.

Now it's 1 TB. Your script runs out of RAM. You swap to disk. It takes 8 hours. Maybe it crashes.

Now it's 1 PB. You cannot run this on one machine. Period. You need a fleet of machines working together.

**This is the distributed computing problem.** And it's not just about scale — it's about time. A petabyte-scale pipeline that runs in 4 hours is useful. One that runs in 4 days is not.

### The MapReduce era (2003–2013)

Google published the MapReduce paper in 2003. Hadoop brought it to open source. The idea: split your data across many machines, run a `map` function on each chunk in parallel, then a `reduce` function to combine results.

It worked. But it had deep problems:

1. **Disk I/O between every step.** After each MapReduce phase, results were written to HDFS (distributed disk), then read back for the next phase. Multi-stage pipelines = massive disk I/O overhead.
2. **No concept of memory reuse.** If you needed to use the same dataset multiple times (e.g., iterative ML algorithms), you read it from disk every single time.
3. **Inflexible programming model.** Everything had to be expressed as map → shuffle → reduce. Complex logic was awkward.
4. **Slow iteration speed.** Running a 10-stage pipeline could take hours just from I/O.

### Spark's breakthrough (2009, open sourced 2010)

Researchers at UC Berkeley's AMPLab asked: *What if we kept data in memory across operations?*

Spark's answer: **Resilient Distributed Datasets (RDDs)** — a distributed collection of data that lives in memory across a cluster, can be reused across operations, and knows how to recompute itself if a partition is lost.

The result:
- **10x–100x faster** than MapReduce for iterative workloads (ML, graph processing)
- **Much faster** for multi-stage pipelines because intermediate results stay in memory
- **More expressive** — you write functional transformations, not map/reduce boilerplate

Today, Spark is the dominant engine for large-scale data processing. MapReduce is effectively deprecated for new work.

---

## 1.2 The Mental Model: Spark in Three Concepts

Before diving into architecture, burn these three concepts into your brain.

### Concept 1: Your data is a distributed collection

In regular Python, `data = [1, 2, 3, 4, 5]` is a list that lives on your machine.

In Spark, `data = spark.createDataFrame(...)` is a distributed collection — a `DataFrame` — that is logically one thing but physically split into **partitions** spread across many machines.

```
Your view:        DataFrame (one logical collection)
                  [row1, row2, row3, row4, row5, row6, ...]

Physical reality: Partition 1     Partition 2     Partition 3
                  [row1, row2]    [row3, row4]    [row5, row6]
                  on Machine A    on Machine B    on Machine C
```

Each machine processes its own partition in parallel. That's where the speed comes from.

### Concept 2: Operations are lazy (nothing runs until you force it)

In Python, `result = data.filter(lambda x: x > 2)` executes immediately.

In Spark, `result = df.filter(df.value > 2)` does **nothing**. It records your intent. It builds a plan.

This is **lazy evaluation**. Spark collects a chain of transformations and only executes when you demand a result (called an **action**).

Why? Because Spark can optimize the full plan before running anything. If you filter a billion rows and then count, Spark can push the filter before the shuffle, dramatically reducing work.

### Concept 3: Failures are expected and handled automatically

In a cluster of 100 machines, expect 1–2 to fail on any given day. Hardware fails. Networks partition. Spot instances get killed.

Spark doesn't crash when a machine fails. It knows how to recompute lost partitions. This is the **Resilient** part of RDD. Spark tracks the lineage of how each partition was derived and can replay the computation from the last stable point.

---

## 1.3 Spark Architecture

### The cluster topology

```
                    ┌─────────────────────────────────┐
                    │            DRIVER               │
                    │  (SparkContext / SparkSession)   │
                    │  - Your main() function runs here│
                    │  - Builds the execution plan     │
                    │  - Coordinates executors         │
                    │  - Collects final results        │
                    └──────────────┬──────────────────┘
                                   │
                                   │ submits tasks via
                                   │ Cluster Manager
                                   │
                    ┌──────────────▼──────────────────┐
                    │         CLUSTER MANAGER          │
                    │    (YARN on EMR / Kubernetes /   │
                    │       Standalone / Mesos)        │
                    │  - Allocates resources           │
                    │  - Schedules tasks on workers    │
                    └──────┬──────────────────┬────────┘
                           │                  │
              ┌────────────▼──┐          ┌────▼────────────┐
              │   EXECUTOR    │          │    EXECUTOR     │
              │  (Worker Node)│          │  (Worker Node)  │
              │               │          │                 │
              │  Task  Task   │          │  Task  Task     │
              │  [Part1][Part2]│         │  [Part3][Part4] │
              │               │          │                 │
              │  JVM Process  │          │  JVM Process    │
              └───────────────┘          └─────────────────┘
```

**Driver:**
- One per Spark application
- Runs your `main()` function
- Holds the `SparkSession` / `SparkContext`
- Builds the logical and physical execution plan
- Tracks task progress
- Collects small result sets (`.collect()`, `.show()`)
- **Critical pitfall:** The driver has limited memory. Never `.collect()` a large dataset to the driver. This is how you get Driver OOM errors.

**Executor:**
- Multiple per cluster (one per JVM, typically one per node or a few per node)
- Actually processes data
- Each executor has a fixed amount of CPU cores and memory
- Runs tasks assigned by the driver
- Communicates results back to driver or writes to storage

**Cluster Manager:**
- On AWS EMR: **YARN** (Yet Another Resource Negotiator)
- Negotiates resources between the driver's requests and available workers
- Schedules tasks
- On EMR Serverless: managed by AWS, invisible to you

### The SparkSession

Everything in modern Spark starts with a `SparkSession`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyPipeline") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()
```

This is your entry point to everything: reading data, writing data, running SQL, configuring the cluster.

---

## 1.4 DataFrames vs RDDs vs Datasets

Spark has three primary APIs. Understanding when to use each matters.

### RDD (Resilient Distributed Dataset) — 🔴 Rarely use directly

The original Spark API. A distributed collection of JVM objects.

```python
# RDD API (low-level, avoid for new code)
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
result = rdd.filter(lambda x: x > 2).map(lambda x: x * 2).collect()
```

**When it appears:** Legacy code, very custom operations that don't fit DataFrame API
**Problem:** No query optimization, no column-level type information, harder to read, often slower

### DataFrame — 🟢 Your primary tool

Structured data with named columns and types, like a distributed SQL table. Uses Spark's Catalyst optimizer for automatic query optimization.

```python
# DataFrame API (what you'll use 95% of the time)
df = spark.read.parquet("s3://my-bucket/data/")
result = df.filter(df.amount > 100) \
           .groupBy("region") \
           .agg({"amount": "sum"})
result.write.parquet("s3://my-bucket/output/")
```

**Use this for:** Essentially everything. ETL, aggregations, joins, transformations.

### Dataset — 🟡 Scala-specific (Python = DataFrame)

Typed DataFrames, available in Scala/Java. In Python, DataFrames are effectively the same thing. When you see "Dataset" in Scala Spark code, just think "strongly-typed DataFrame."

**Practical rule:** In Python (PySpark), always use DataFrames.

---

## 1.5 Lazy Evaluation and the DAG

This is one of the most important concepts in Spark. Understand it deeply.

### What lazy evaluation means

```python
df = spark.read.parquet("s3://bucket/logs/")           # 1. No data read yet
filtered = df.filter(df.event_type == "purchase")       # 2. No filtering yet
grouped = filtered.groupBy("user_id").count()           # 3. No grouping yet
grouped.write.parquet("s3://bucket/output/")            # 4. NOW it runs, ALL at once
```

Steps 1–3 build a plan. Step 4 (or any **action**) triggers execution. Spark's optimizer can look at the entire plan and reorder, combine, or skip operations.

### Transformations vs Actions

**Transformations** — lazy, return a new DataFrame:
```
filter(), select(), withColumn(), join(), groupBy(),
orderBy(), distinct(), union(), repartition(), ...
```

**Actions** — trigger execution, return a result:
```
collect(), show(), count(), write.*(), take(n),
first(), head(), foreach(), save(), ...
```

**Mental model:** Transformations are like writing a recipe. Actions are like actually cooking. You don't do any cooking until someone says "make this dish."

### The DAG (Directed Acyclic Graph)

When you chain transformations, Spark builds a **DAG** — a graph of operations where:
- Each **node** is an operation (filter, join, aggregate)
- Each **edge** represents data flowing from one operation to the next
- It's **directed** (data flows one way)
- It's **acyclic** (no cycles, data doesn't loop back)

```
              Read S3
                 │
             Filter(type='purchase')
                 │
             Select(user_id, amount)
                 │
             GroupBy(user_id)
                 │
             Count()
                 │
             Write S3
```

Spark's Catalyst optimizer analyzes this DAG and:
1. Pushes filters as early as possible (reads less data)
2. Eliminates unused columns early (projection pushdown)
3. Reorders joins for efficiency
4. Combines multiple operations into single stages

### Stages and Tasks

When Spark executes a DAG, it splits it into **Stages**:

- A **Stage** is a sequence of operations that can run without shuffling data between machines
- A **Stage boundary** occurs wherever data needs to cross machine boundaries (shuffle)
- Each Stage contains many **Tasks** — one task per partition

```
Stage 1: Read → Filter → Select          (no shuffle, runs locally on each partition)
         Task1  Task2  Task3  Task4       (one task per partition, runs in parallel)
         [P1]   [P2]   [P3]   [P4]
           |      |      |      |
           └──────┴──────┴──────┘
                      │
              SHUFFLE (data crosses network)
                      │
Stage 2: GroupBy → Count → Write         (aggregation after shuffle)
         Task1  Task2  Task3  Task4
         [P1']  [P2']  [P3']  [P4']
```

Understanding stages is critical for performance. Every stage boundary = network I/O = slowdown.

---

## 1.6 The Shuffle — The Most Expensive Operation

The **shuffle** is when Spark needs to redistribute data across executors. It happens when:

- `groupBy()` — all rows with the same key must be on the same machine
- `join()` — matching rows from two datasets must meet
- `orderBy()` / `sort()` — global sort requires all data to be seen
- `repartition()` — explicit redistribution

### What physically happens during a shuffle

```
Before Shuffle:
  Executor A: [(user_1, $50), (user_2, $30), (user_1, $20)]
  Executor B: [(user_3, $10), (user_2, $60), (user_1, $15)]
  Executor C: [(user_2, $40), (user_3, $25), (user_3, $35)]

Operation: groupBy(user_id).sum(amount)
→ All rows for user_1 must be on ONE executor
→ All rows for user_2 must be on ONE executor
→ etc.

Shuffle:
  Each executor writes shuffle files to disk (shuffle write)
  Each executor reads the partitions it needs from other executors (shuffle read)
  This involves NETWORK I/O and DISK I/O

After Shuffle:
  Executor A: [(user_1, $50+$20+$15)]    → computes sum = $85
  Executor B: [(user_2, $30+$60+$40)]    → computes sum = $130
  Executor C: [(user_3, $10+$25+$35)]    → computes sum = $70
```

### Why shuffles are expensive

1. **Network I/O:** Data physically moves between machines
2. **Disk I/O:** Shuffle data is written to disk before transfer (in case of failure)
3. **Memory pressure:** Shuffle buffers consume executor memory
4. **Coordination overhead:** Executors must wait for all shuffle writes before reading
5. **Skew amplification:** If one key has many more rows than others, one executor gets overloaded

**The rule:** Minimize shuffles. Every `groupBy`, `join`, or `repartition` is a potential performance bottleneck. When you see a slow Spark job, look for unexpected shuffles first.

---

## 1.7 Partitions Deep Dive

Understanding partitions is fundamental. Everything in Spark optimization comes back to partitions.

### What is a partition?

A partition is a chunk of your data — specifically, a subset of rows that are processed together on one executor at one time.

```
DataFrame:  1,000,000,000 rows
           ÷ 200 partitions
           = 5,000,000 rows per partition
```

One task processes one partition. Partitions run in parallel across executors.

### Why partition count matters

**Too few partitions:**
```
10 executors × 4 cores = 40 parallel tasks possible
But only 5 partitions → only 5 tasks run → 35 cores sit idle
All 5 partitions are huge (slow per-task, risk OOM)
```

**Too many partitions:**
```
10,000 partitions → 10,000 tasks
Spark scheduler overhead is high
Each partition is tiny (2 KB)
More time spent scheduling than computing
```

**The goldilocks zone:** Target **128 MB – 256 MB per partition**. This maximizes throughput while keeping task overhead manageable.

### How to control partition count

```python
# Increase partitions (e.g., after reading small data or before big operations)
df = df.repartition(200)

# Decrease partitions (e.g., before writing to avoid small files)
df = df.coalesce(20)   # coalesce is more efficient — avoids a full shuffle
                        # but can only decrease, not increase

# Repartition by column (useful for partition-aware writes)
df = df.repartition("date", "region")

# Configure default shuffle partitions (default is 200, often too low or too high)
spark.conf.set("spark.sql.shuffle.partitions", "400")
```

### Narrow vs Wide dependencies

**Narrow transformation** — each output partition depends on one input partition:
```
filter(), select(), map(), withColumn()
→ No shuffle needed
→ Fast, pipelined within a stage
```

**Wide transformation** — each output partition depends on multiple input partitions:
```
groupBy(), join(), orderBy(), distinct(), repartition()
→ Shuffle required
→ Stage boundary
→ Slower
```

Knowing this distinction helps you predict when your code will shuffle.

---

## 1.8 Spark SQL and the Catalyst Optimizer

Modern Spark strongly encourages using the **DataFrame API** or **Spark SQL** (SQL queries) rather than low-level RDD operations. The reason: the **Catalyst optimizer**.

### What Catalyst does

Catalyst is Spark's query optimizer. When you write a DataFrame transformation or SQL query, Catalyst:

1. **Parses** your query into a logical plan
2. **Analyzes** it (resolves column names, checks types)
3. **Optimizes** the logical plan (pushes predicates down, eliminates unnecessary work)
4. **Plans** physical execution (chooses join strategy, partition count)
5. **Generates** bytecode (via Tungsten codegen) for efficient execution

The result: your high-level DataFrame code often runs as fast as hand-optimized code.

### Practical implications

```python
# Catalyst can optimize this:
df.filter(df.date > "2024-01-01") \
  .select("user_id", "amount") \
  .groupBy("user_id") \
  .sum("amount")

# Into an efficient plan like:
# - Read only "user_id", "amount", "date" columns (projection pushdown)
# - Filter as early as possible (predicate pushdown)
# - Push filter into Parquet file scanning (skip entire files/row groups)

# You can inspect the plan:
df.explain(mode="extended")
# Shows: parsed, analyzed, optimized, physical plans
```

### Spark SQL

You can also write plain SQL:

```python
df.createOrReplaceTempView("events")
result = spark.sql("""
    SELECT user_id, SUM(amount) as total
    FROM events
    WHERE date > '2024-01-01'
    GROUP BY user_id
""")
```

Same Catalyst optimization applies. Choose whichever is more readable for your team.

---

## 1.9 Memory Management

Spark executors manage memory in distinct regions:

```
Executor JVM Memory (e.g., 8 GB)
┌─────────────────────────────────────────────┐
│                                             │
│  Reserved Memory (~300 MB)                  │
│  (for Spark internal objects)               │
│                                             │
│  User Memory (40% of remaining)             │
│  (for your data structures, UDFs,           │
│   custom objects)                           │
│                                             │
│  Unified Memory (60% of remaining)          │
│  ┌─────────────────────────────────────┐   │
│  │  Execution Memory (dynamic)         │   │
│  │  (for shuffles, sorts, aggregations │   │
│  │   during task execution)            │   │
│  │                                     │   │
│  │  Storage Memory (dynamic)           │   │
│  │  (for cached/persisted DataFrames)  │   │
│  └─────────────────────────────────────┘   │
│                                             │
└─────────────────────────────────────────────┘
```

**Key config parameters:**
```
spark.executor.memory         = 8g   (total executor JVM heap)
spark.executor.memoryOverhead = 2g   (off-heap: Python worker, native libs, JVM overhead)
spark.memory.fraction         = 0.6  (fraction of heap for Spark's unified memory)
spark.memory.storageFraction  = 0.5  (within unified memory, reserved for storage)
```

**The overhead trap:** Many beginners forget `memoryOverhead`. If you run PySpark with Python UDFs, Python processes run separately from the JVM. They need their own memory. `memoryOverhead` should be at least 10% of executor memory, and more if you use Python UDFs heavily.

### When memory goes wrong

**OOM in executor:**
```
Symptom: "ExecutorLostFailure: ... Container killed by YARN for exceeding memory limits"
Cause: Task is processing more data than the executor can hold
Fix: Increase executor memory, reduce partition size, avoid wide aggregations
```

**OOM in driver:**
```
Symptom: "java.lang.OutOfMemoryError: Java heap space" on the driver
Cause: Calling .collect() on a large dataset, or building large broadcast variables
Fix: Never collect large datasets to driver; use write() instead
```

---

## 1.10 Caching and Persistence

When you use a DataFrame multiple times, recompute its lineage each time — or cache it.

```python
# Cache in memory (default: MEMORY_AND_DISK)
df_filtered = df.filter(df.active == True).cache()

# Explicitly specify storage level
from pyspark import StorageLevel
df_filtered.persist(StorageLevel.MEMORY_AND_DISK)

# Always unpersist when done
df_filtered.unpersist()
```

### Storage levels

| Level | Memory | Disk | Notes |
|-------|--------|------|-------|
| `MEMORY_ONLY` | ✓ | ✗ | Fastest, recomputes if evicted |
| `MEMORY_AND_DISK` | ✓ | ✓ | Spills to disk if evicted |
| `DISK_ONLY` | ✗ | ✓ | Slow, avoids recompute |
| `MEMORY_ONLY_SER` | ✓ | ✗ | Serialized (smaller, CPU cost) |

### When to cache

✅ Cache when:
- The same DataFrame is used in 2+ downstream operations
- The DataFrame is expensive to compute (complex joins, aggregations)
- You're doing iterative processing (ML, graph algorithms)

❌ Don't cache when:
- The DataFrame is used only once
- The DataFrame is very large and you have limited memory
- You're writing a single-pass pipeline

**Caching does not persist between Spark sessions.** It's an in-memory optimization within a job run.

---

## 1.11 Key Spark APIs Cheat Sheet

### Reading data

```python
# Parquet (preferred format)
df = spark.read.parquet("s3://bucket/path/")

# JSON
df = spark.read.json("s3://bucket/path/")

# CSV
df = spark.read.option("header", "true") \
               .option("inferSchema", "true") \
               .csv("s3://bucket/path/")

# With explicit schema (always preferred for production)
from pyspark.sql.types import *
schema = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=True),
    StructField("event_time", TimestampType(), nullable=False),
])
df = spark.read.schema(schema).parquet("s3://bucket/path/")
```

### Common transformations

```python
# Filter
df.filter(df.amount > 100)
df.filter("amount > 100")  # SQL string also works

# Select columns
df.select("user_id", "amount", "date")

# Add/modify columns
df.withColumn("amount_usd", df.amount * 0.01)

# Drop columns
df.drop("internal_id")

# Rename
df.withColumnRenamed("amt", "amount")

# Type casting
df.withColumn("amount", df.amount.cast("double"))

# Null handling
df.dropna(subset=["user_id", "amount"])
df.fillna({"amount": 0.0, "region": "UNKNOWN"})

# Aggregations
from pyspark.sql import functions as F
df.groupBy("region") \
  .agg(
      F.count("*").alias("event_count"),
      F.sum("amount").alias("total_amount"),
      F.avg("amount").alias("avg_amount"),
      F.max("amount").alias("max_amount")
  )

# Joins
df_users.join(df_events, on="user_id", how="inner")
df_users.join(df_events, on="user_id", how="left")

# Window functions
from pyspark.sql.window import Window
window = Window.partitionBy("user_id").orderBy("event_time")
df.withColumn("row_num", F.row_number().over(window))
```

### Writing data

```python
# Parquet (default mode: append; be careful!)
df.write.mode("overwrite").parquet("s3://bucket/output/")

# With partitioning
df.write.mode("overwrite") \
        .partitionBy("date", "region") \
        .parquet("s3://bucket/output/")

# Coalesce before writing to control file count
df.coalesce(10).write.mode("overwrite").parquet("s3://bucket/output/")
```

---

## 1.12 Module Summary

You've covered:
- **Why Spark exists** — replaces MapReduce, keeps data in memory
- **Core architecture** — Driver, Executors, Cluster Manager
- **Lazy evaluation** — transformations build a plan, actions execute it
- **The DAG** — Spark's optimization graph
- **The shuffle** — the expensive operation to minimize
- **Partitions** — the unit of parallelism, 128–256 MB is the target
- **DataFrames** — your primary API, Catalyst-optimized
- **Memory management** — executor heap, overhead, unified memory
- **Caching** — when and how to cache DataFrames

### ✅ Self-check before moving on

Can you answer these without looking?

1. What is the difference between a transformation and an action?
2. What causes a shuffle? Name three operations that trigger one.
3. What is the ideal partition size in MB?
4. What is the difference between `repartition()` and `coalesce()`?
5. Why does Spark use lazy evaluation?
6. What is the driver? What happens if the driver runs out of memory?
7. What is the Catalyst optimizer and what does it do?

If you're shaky on any of these, re-read the relevant sections. These are foundations that Module 02 and beyond assume you know cold.

---

*Next: `02-emr-fundamentals.md` →*
