# Module 12: Apache Parquet Deep Dive
## The File Format That Powers the Data Lake

> **Difficulty:** 🟢 Beginner → 🔴 Advanced
> **Prerequisite:** Module 01 (Spark Fundamentals), Module 03 (Spark on S3)
> **Est. time:** 3–5 hours

---

## 12.1 Why Parquet Exists — The Problem It Solves

To understand why Parquet matters, start with what it replaced.

### The world before columnar formats

The default file formats for data — CSV, JSON, plain text — are **row-oriented**. Every row is stored contiguously on disk:

```
CSV file (row-oriented):
Row 1: user_id,event_type,amount,region,timestamp
Row 2: u001,purchase,99.99,US,2024-01-15T10:00:00
Row 3: u002,view,0.00,EU,2024-01-15T10:01:00
Row 4: u001,add_to_cart,0.00,US,2024-01-15T10:02:00
...
100,000,000 more rows
```

**The analytics problem:** The query `SELECT SUM(amount) FROM events WHERE region = 'US'` needs only two columns — `amount` and `region`. But reading a CSV forces you to read **every byte of every column** for every row. At 10 columns and 100M rows, you read 10x more data than needed.

For transactional workloads (insert one row, read one row), row-oriented formats are perfect. For analytics (read one column across many rows), they're wasteful.

### Parquet's solution: columnar storage

Parquet stores all values of the same column together:

```
Parquet file (columnar):
Column user_id:    [u001, u002, u001, u003, ...]   ← all user IDs together
Column event_type: [purchase, view, add_to_cart, ...] ← all event types together
Column amount:     [99.99, 0.00, 0.00, 149.00, ...]  ← all amounts together
Column region:     [US, EU, US, US, ...]             ← all regions together
Column timestamp:  [2024-01-15T10:00, ...]           ← all timestamps together
```

Now `SELECT SUM(amount) WHERE region = 'US'` reads only two columns. The other 8 columns are not touched. At 10 columns: **10x less I/O**. At 100 columns (common in real datasets): **100x less I/O**.

**Plus compression:** Columnar data compresses dramatically better. A column of `region` values `[US, US, EU, US, US, EU, US, ...]` compresses to nearly nothing with run-length encoding. Mixed-column rows don't compress as efficiently.

Parquet was created by Twitter and Cloudera in 2013, became an Apache top-level project in 2015, and is now the de facto standard for big data analytics storage.

---

## 12.2 Parquet File Structure — From Top to Bottom

Understanding Parquet's internal structure is the key to tuning it correctly. Every performance decision traces back to this hierarchy.

### The four-level hierarchy

```
┌──────────────────────────────────────────────────┐
│                  PARQUET FILE                     │
│                                                  │
│  ┌────────────────────────────────────────────┐  │
│  │              ROW GROUP 1 (~128 MB)          │  │
│  │                                            │  │
│  │  ┌──────────────┐  ┌──────────────┐        │  │
│  │  │ Column Chunk  │  │ Column Chunk  │  ...   │  │
│  │  │  (user_id)   │  │  (amount)    │        │  │
│  │  │              │  │              │        │  │
│  │  │  ┌────────┐  │  │  ┌────────┐  │        │  │
│  │  │  │ Page 1 │  │  │  │ Page 1 │  │        │  │
│  │  │  │ (1 MB) │  │  │  │ (1 MB) │  │        │  │
│  │  │  ├────────┤  │  │  ├────────┤  │        │  │
│  │  │  │ Page 2 │  │  │  │ Page 2 │  │        │  │
│  │  │  │ (1 MB) │  │  │  │ (1 MB) │  │        │  │
│  │  │  └────────┘  │  │  └────────┘  │        │  │
│  │  └──────────────┘  └──────────────┘        │  │
│  └────────────────────────────────────────────┘  │
│                                                  │
│  ┌────────────────────────────────────────────┐  │
│  │              ROW GROUP 2 (~128 MB)          │  │
│  │  ...                                       │  │
│  └────────────────────────────────────────────┘  │
│                                                  │
│  FILE FOOTER (metadata for ALL row groups)       │
│  - Schema                                        │
│  - Row group offsets                             │
│  - Column statistics (min/max/null count)        │
│  - Bloom filter data (optional)                  │
└──────────────────────────────────────────────────┘
```

### Level 1: The File

A single `.parquet` file. Contains one or more row groups plus a file footer. The footer is written last and contains all metadata — schema, row group locations, statistics.

**Magic bytes:** Every Parquet file starts and ends with `PAR1` (4 bytes). A reader can verify a file is valid Parquet before reading any data.

### Level 2: Row Groups

A row group is a horizontal partition of rows — a contiguous block of rows stored in columnar form within the file. Think of it as a "mini-table" inside the file.

```
File with 10M rows and 128-row-group-size:
Row Group 1: rows 1–1,000,000
Row Group 2: rows 1,000,001–2,000,000
...
Row Group 10: rows 9,000,001–10,000,000
```

**Why row groups matter for skipping:**
Each row group has its own column statistics (min, max, null count). If you query `WHERE amount > 1000`, a reader can check each row group's `amount` statistics and **skip the entire row group** if its max amount < 1000. This is called **row group level skipping**.

**Recommended row group size:** 128 MB – 512 MB. Larger row groups:
- ✅ Better compression (more data to find patterns)
- ✅ Better statistics (more rows = more representative min/max)
- ✅ Lower metadata overhead
- ❌ Less granular skipping (you skip a larger or smaller chunk)
- ❌ Less parallelism at read time (fewer row groups to split into tasks)

**Spark's default:** `parquet.block.size = 128 MB` (matches HDFS block size).

### Level 3: Column Chunks

Within each row group, each column has its own **column chunk** — a contiguous sequence of data for that column within that row group.

Column chunks are the unit of compression and encoding. Each column chunk uses a single compression codec (Snappy, ZSTD, etc.) and can use multiple encodings for its pages.

Column chunks also have their own statistics: min, max, null count for that column within that row group. These are what Spark uses for predicate pushdown.

### Level 4: Pages

Each column chunk is divided into **pages** — the smallest unit of data in Parquet. A page is the minimum unit for decompression and encoding.

**Page types:**
- **Data page:** Contains the actual column values
- **Dictionary page:** Contains the dictionary (unique values) when dictionary encoding is used (always the first page in a column chunk, if present)
- **Index page:** Optional, used for page-level index and skipping

**Recommended page size:** 1 MB (default). Smaller pages = finer-grained skipping but more overhead. Larger pages = better compression but coarser granularity.

**Why pages matter:** In Parquet 2.0+, column indexes allow readers to skip individual pages within a row group, not just entire row groups. This is especially powerful for sorted data.

---

## 12.3 Encodings: How Values Are Stored

Parquet doesn't just store raw bytes. It applies encodings to reduce size and speed up reads.

### Dictionary Encoding

The most impactful encoding for repeated values.

```
Raw values: [US, EU, US, US, APAC, US, EU, US, APAC, US, ...]

Dictionary page (written first):
  0 → US
  1 → EU
  2 → APAC

Data page (encoded):
  [0, 1, 0, 0, 2, 0, 1, 0, 2, 0, ...]
```

Instead of storing the full string "US" repeatedly, Parquet stores a tiny integer (often 1–2 bytes). For high-repetition columns, this can achieve **50–100x compression**.

**Dictionary encoding is applied automatically when the number of distinct values fits within the dictionary page size.** If a column has too many distinct values (e.g., UUIDs), dictionary encoding falls back to plain encoding.

**When dictionary encoding is most effective:**
- `region`, `country`, `status`, `event_type` — low cardinality strings
- `date`, `hour` — repeated temporal values
- `category_id` — repeated integer keys

**When it's NOT effective (and falls back to plain encoding):**
- `user_id` (millions of distinct values)
- `event_id` (UUID, completely unique)
- Continuous float values (every value different)

### Run-Length Encoding (RLE) / Bit Packing

Used for repetition and definition levels (Parquet's null/nesting encoding), and optionally for data pages.

```
Values: [1, 1, 1, 1, 2, 2, 3, 3, 3, 3, 3]
RLE:    [(1, 4 times), (2, 2 times), (3, 5 times)]
```

Very effective for sorted or near-sorted data. Combined with dictionary encoding:

```
Sorted region column: [APAC, APAC, APAC, EU, EU, US, US, US, US, US]
Dictionary:  {APAC=0, EU=1, US=2}
RLE on dict: [(0, 3), (1, 2), (2, 5)]
→ 10 values encoded in 3 entries
```

**This is why sorting data before writing Parquet dramatically improves compression and read performance.**

### Delta Encoding

For integer sequences, store only the differences between consecutive values:

```
Timestamps: [1000, 1060, 1120, 1180, 1240]
Deltas:     [1000, +60, +60, +60, +60]  ← much smaller integers
```

Effective for monotonically increasing values like timestamps, IDs, sequence numbers.

### Plain Encoding

Fallback when other encodings don't apply: raw bytes. Used for high-cardinality columns (UUIDs, free text).

---

## 12.4 Compression Codecs

Compression operates on pages (after encoding). Each column chunk uses one codec.

### Codec comparison

| Codec | Compression Ratio | Compression Speed | Decompression Speed | Best For |
|-------|------------------|-------------------|---------------------|----------|
| **Snappy** | Medium (~2–3x) | Very fast | Very fast | General purpose, EMR default |
| **ZSTD** | High (~3–5x) | Fast | Fast | Production (better than Snappy, similar speed) |
| **GZIP/Deflate** | High (~3–5x) | Slow | Medium | Cold storage, rarely read |
| **LZ4** | Low-medium (~1.5–2x) | Very fast | Very fast | Extremely latency-sensitive reads |
| **Brotli** | Very high | Very slow | Medium | Long-term cold archives |
| **Uncompressed** | None (1x) | — | Fastest | Testing, very hot data |

### The practical choice: ZSTD over Snappy

For new production workloads, **ZSTD** is the modern recommendation:
- Compresses 10–15% better than Snappy (real-world, not theoretical)
- Decompression speed comparable to Snappy
- Configurable compression level (1=fast, 22=max compression; level 3–5 is the sweet spot)
- Supported in Spark 3.0+, EMR 6.x+

```python
# Configure ZSTD compression in Spark
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")

# Optional: set ZSTD compression level (default is 3)
spark.conf.set("parquet.zstd.bufferpool.enabled", "true")

# Write with explicit codec:
df.write.option("compression", "zstd").parquet("s3://bucket/output/")

# Or globally set:
spark = SparkSession.builder \
    .config("spark.sql.parquet.compression.codec", "zstd") \
    .getOrCreate()
```

**Don't mix codecs across files in the same dataset.** Parquet stores the codec per column chunk, so readers handle mixed files — but consistency makes debugging simpler.

### How much compression to expect

| Column Type | Encoding | Codec | Typical Reduction |
|-------------|----------|-------|-------------------|
| Low-cardinality string (region, status) | Dictionary + RLE | Snappy | 20–50x |
| High-cardinality string (user_id) | Plain | Snappy | 2–4x |
| UUID/GUID | Plain | Snappy | ~1.5x |
| Timestamps (sorted) | Delta + RLE | ZSTD | 10–20x |
| Float amounts | Plain | Snappy | 2–3x |
| Boolean flags | RLE | Snappy | 100x+ |
| Free text | Plain | ZSTD | 2–5x |

Real-world datasets: CSV at 100 GB → Parquet + ZSTD at 8–15 GB. **6–10x is typical.**

---

## 12.5 Column Statistics and Predicate Pushdown

This is the feature that makes Parquet dramatically faster for filtered queries.

### What statistics are stored

Every column chunk (per row group) stores in its metadata:
- **min value** — the smallest value in this column chunk
- **max value** — the largest value in this column chunk
- **null count** — how many nulls
- **distinct count** (optional) — approximate cardinality

These are stored in the **file footer** — no data pages need to be read to access statistics.

### How predicate pushdown works

```python
df = spark.read.parquet("s3://bucket/events/")
result = df.filter(df.amount > 1000)
```

Before reading any data, Spark reads the file footer and checks each row group's `amount` statistics:

```
Row Group 1: amount min=0.01, max=500.00  → max < 1000 → SKIP ENTIRE ROW GROUP
Row Group 2: amount min=0.50, max=2500.00 → max > 1000 → MUST READ (might have matches)
Row Group 3: amount min=1500.0, max=9999.0 → min > 1000 → ALL ROWS MATCH (still read, but no filter needed)
```

Row Group 1 is never read at all. At petabyte scale, this can reduce I/O from 1 PB to 10 GB for a selective query.

### Predicate pushdown in the Spark explain plan

```python
df.filter(df.amount > 1000).explain()

# Look for:
# == Physical Plan ==
# *(1) Filter (isnotnull(amount#12) AND (amount#12 > 1000.0))
# +- *(1) ColumnarToRow
#    +- FileScan parquet [user_id#10, amount#12, ...]
#       Batched: true, DataFilters: [isnotnull(amount#12), (amount#12 > 1000.0)],
#       Format: Parquet,
#       PartitionFilters: [],
#       PushedFilters: [IsNotNull(amount), GreaterThan(amount,1000.0)],  ← PUSHED DOWN
#       ReadSchema: struct<user_id:string, amount:double>
```

`PushedFilters` shows which predicates are evaluated at the Parquet reader level (before data reaches Spark). This is what you want to see.

### When predicate pushdown does NOT work

- **Column is computed, not raw:** `filter(df.amount * 1.1 > 1000)` — Spark can't push down a computed expression
- **Wrong data type comparison:** `filter(df.amount_str > "1000")` — string comparison on what should be a numeric column
- **OR conditions with non-pushdown predicates:** `filter((df.amount > 1000) | (df.user_id.like('%test%')))` — `like` with leading wildcard can't be pushed down
- **Non-native UDFs:** Python UDFs run after data is read, not during scanning

```python
# DOES push down (native comparison):
df.filter(df.amount > 1000)
df.filter(df.region == "US")
df.filter(df.event_date.between("2024-01-01", "2024-01-31"))

# Does NOT push down (computed):
df.filter(df.amount * 1.1 > 1000)   # arithmetic first
df.filter(F.upper(df.region) == "US")  # function applied first
```

---

## 12.6 Bloom Filters

Column statistics (min/max) work well for range queries but fail for equality checks on high-cardinality columns:

```
Row Group 1: user_id min="user_000001", max="user_999999"
Query: WHERE user_id = 'user_500000'
→ min ≤ 'user_500000' ≤ max → can't skip → MUST READ
```

For high-cardinality columns, **Bloom filters** solve the "definitely not here" problem.

### How a Bloom filter works

A Bloom filter is a compact probabilistic data structure that answers: "Is this value in the set?"
- **"No" answer is definitive** — the value is definitely not in the row group
- **"Yes" answer is probabilistic** — the value is *probably* in the row group (may be a false positive)

```
Bloom filter for user_id column in Row Group 1:
  Contains user_000001? → YES (actually there)
  Contains user_500000? → NO  (definitely not here, skip this row group!)
  Contains user_987654? → YES (probabilistic — might be a false positive)
```

For a selective query like `WHERE user_id = 'some_specific_user'`, a Bloom filter can skip most row groups that don't contain that user, even though min/max can't help.

### Enabling Bloom filters in Spark

```python
# Write with Bloom filter on specific columns
df.write \
  .option("parquet.bloom.filter.enabled", "true") \
  .option("parquet.bloom.filter.enabled#user_id", "true") \
  .option("parquet.bloom.filter.expected.ndv#user_id", "1000000")  # expected distinct values
  .option("parquet.bloom.filter.enabled#product_id", "true") \
  .option("parquet.bloom.filter.expected.ndv#product_id", "50000") \
  .parquet("s3://bucket/events/")

# Enable reading with Bloom filter pushdown
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
spark.conf.set("spark.sql.parquet.bloomFilter.enabled", "true")
```

### Bloom filter cost/benefit

| Factor | Value |
|--------|-------|
| Storage overhead | 2–8 KB per column per row group (tiny) |
| Write time overhead | Small (<5%) |
| Query speedup (equality on high-cardinality col) | Up to 30x |
| False positive rate (default 1%) | Configurable |

**When to use Bloom filters:**
- ✅ Equality queries on high-cardinality columns (`user_id`, `order_id`, `device_id`)
- ✅ Columns frequently used in point lookups
- ✅ `IN` predicates with small lists

**When NOT to use:**
- ❌ Range queries (min/max statistics handle this better)
- ❌ Very low cardinality columns (region, status — min/max and dictionaries suffice)
- ❌ Columns never used in filters

---

## 12.7 Schema and Nested Types

Parquet handles more than flat tables. It supports complex nested data natively.

### Parquet's type system

**Primitive types:**
```
BOOLEAN, INT32, INT64, INT96 (legacy timestamp), FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY
```

**Logical types (annotations on primitives):**
```
STRING         → BYTE_ARRAY with UTF8 annotation
TIMESTAMP_MILLIS → INT64 with timestamp annotation
DECIMAL(p, s)  → INT32/INT64/BYTE_ARRAY with decimal annotation
DATE           → INT32 with date annotation
UUID           → FIXED_LEN_BYTE_ARRAY(16)
```

**Repetition levels (how nullability and nesting work):**
- `REQUIRED` — not null, must be present (maps to Spark `nullable=False`)
- `OPTIONAL` — may be null (maps to Spark `nullable=True`)
- `REPEATED` — may appear zero or more times (maps to Spark arrays)

### Nested structures in PySpark → Parquet

```python
from pyspark.sql.types import *

# Complex schema with nested types
schema = StructType([
    StructField("event_id", StringType(), nullable=False),   # REQUIRED
    StructField("user_id", StringType(), nullable=True),     # OPTIONAL
    StructField("amount", DoubleType(), nullable=True),      # OPTIONAL

    # Array: zero or more items
    StructField("tags", ArrayType(StringType()), nullable=True),

    # Struct: nested object
    StructField("device", StructType([
        StructField("os", StringType(), nullable=True),
        StructField("version", StringType(), nullable=True),
        StructField("screen_width", IntegerType(), nullable=True),
    ]), nullable=True),

    # Map: key-value pairs
    StructField("attributes", MapType(StringType(), StringType()), nullable=True),
])

# Working with nested types
df.select(
    "event_id",
    "device.os",           # access struct field
    "device.version",
    F.explode("tags"),     # unnest array into rows
    F.col("attributes")["utm_source"],  # access map value
)
```

### The nested types performance trap

Nested types are convenient but have a cost: Parquet stores them using **Dremel encoding** (repetition and definition levels), which adds encoding overhead and can make some operations slower.

```python
# SLOW: Spark must decode the full nested struct even to access one field
df.select("device.os")  # still reads all device sub-columns

# WORKAROUND: For frequently-accessed nested fields, denormalize at write time
df.withColumn("device_os", F.col("device.os")) \
  .withColumn("device_version", F.col("device.version")) \
  .drop("device") \
  .write.parquet(...)
# Now "device_os" is a top-level column → full pushdown and projection support
```

**Rule:** If you repeatedly access specific nested fields in queries, flatten them to top-level columns. Keep nested types only for rarely-accessed structured payloads.

---

## 12.8 Row Group Sizing and Sort Order

These two decisions determine how effective Parquet's data skipping is.

### Optimal row group size

```
Too small (e.g., 1 MB row groups):
  1 TB file → 1,000,000 row groups → 1,000,000 footer metadata entries
  → Reading the footer takes minutes
  → Statistics too fine-grained, overhead overwhelms benefit

Too large (e.g., 2 GB row groups):
  1 TB file → 500 row groups
  → Very coarse skipping (skip 2 GB at a time)
  → Fewer opportunities to skip

Goldilocks: 128 MB – 512 MB row groups
  1 TB file → 2,000–8,000 row groups
  → Good skipping granularity
  → Manageable metadata

Spark writes: controlled by parquet.block.size (default: 128 MB)
```

```python
# Set row group size
spark.conf.set("parquet.block.size", str(256 * 1024 * 1024))  # 256 MB row groups
```

### Sort order: the secret multiplier for compression and skipping

Sorting data before writing Parquet has two massive effects:

**Effect 1: Better compression**

```
Unsorted region column: [EU, US, APAC, US, US, EU, APAC, US, EU, ...]
RLE doesn't help much: many short runs

Sorted region column: [APAC, APAC, APAC, ..., EU, EU, ..., US, US, US, ...]
RLE compresses dramatically: few long runs
```

**Effect 2: Better statistics (min/max skipping)**

```
Unsorted timestamp column per row group:
  Row Group 1: min=2024-01-01, max=2024-12-31  ← year-wide range, can't skip much
  Row Group 2: min=2024-01-01, max=2024-12-31  ← same
  → Query: WHERE timestamp > '2024-11-01' → MUST READ ALL ROW GROUPS

Sorted by timestamp:
  Row Group 1: min=2024-01-01, max=2024-03-31  ← can skip for late-year queries
  Row Group 2: min=2024-04-01, max=2024-06-30  ← can skip
  Row Group 3: min=2024-07-01, max=2024-09-30  ← can skip
  Row Group 4: min=2024-10-01, max=2024-12-31  ← read this one
  → Query: WHERE timestamp > '2024-11-01' → reads only 25% of data
```

**How to sort before writing in Spark:**

```python
# Sort by the column most commonly used in range filters
df.sortWithinPartitions("event_time") \
  .write.parquet("s3://bucket/events/")

# Or globally sort (triggers a shuffle — expensive but best statistics)
df.sort("event_time") \
  .write.parquet("s3://bucket/events/")

# Sort by multiple columns for multi-dimensional skipping
df.sortWithinPartitions("date", "user_id") \
  .write.parquet(...)
```

**`sortWithinPartitions` vs `sort`:**
- `sortWithinPartitions`: sorts data within each Spark partition independently. No shuffle. Statistics are good within each output file but not globally across files.
- `sort`: global sort across all data. Triggers a full shuffle. Statistics are globally optimal.

For most use cases, `sortWithinPartitions` is the right tradeoff — avoids the shuffle cost while significantly improving statistics within each file.

---

## 12.9 Reading Parquet Efficiently with Spark

### Vectorized reading

Spark 2.0+ includes a **vectorized Parquet reader** that processes multiple values in one CPU instruction (SIMD). This is dramatically faster than the scalar reader.

```python
# Vectorized reader is enabled by default in Spark 3.x
# Verify:
spark.conf.get("spark.sql.parquet.enableVectorizedReader")  # should be "true"

# If disabled, enable:
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")

# Control batch size (rows read per SIMD batch):
spark.conf.set("spark.sql.parquet.columnarReaderBatchSize", "4096")  # default
```

**Limitation:** The vectorized reader only works with certain data types. Complex nested types (arrays, maps, structs) fall back to the non-vectorized reader. Another reason to flatten nested types.

### Column pruning (projection pushdown)

Spark only reads the columns you request:

```python
# Reads ALL columns from Parquet (wastes I/O)
df = spark.read.parquet("s3://bucket/events/")
result = df.select("user_id", "amount")

# Reads ONLY user_id and amount (same result, much less I/O)
# Catalyst does this automatically — it's called projection pushdown
# The .select() is pushed into the Parquet scanner

# Verify with explain:
df.select("user_id", "amount").explain()
# Look for: ReadSchema: struct<user_id:string,amount:double>
# (not the full schema → projection pushdown is working)
```

### Reading with schema merging

When files in the same directory have different schemas (e.g., after adding a column):

```python
# Read and automatically merge schemas
df = spark.read \
    .option("mergeSchema", "true") \
    .parquet("s3://bucket/events/")
# Columns missing in older files are filled with null
```

**Cost:** Merging schemas requires reading the footer of every file to discover the union schema. At millions of files, this is slow. Use only when necessary, then migrate to a consistent schema.

### Footer caching

Reading the footer of millions of Parquet files is expensive (one I/O per file). Some tools (Trino, Athena) cache footers. Spark doesn't cache footers natively across jobs, but within a job, the Parquet metadata is cached in the executor.

At petabyte scale with millions of files, footer reading is a real bottleneck. This is one of the strongest arguments for:
1. Fewer, larger files (fewer footers to read)
2. Apache Iceberg / Delta Lake (maintain a manifest that avoids listing + footer-reading entirely)

---

## 12.10 Writing Parquet Correctly

### The full write configuration checklist

```python
# Comprehensive Parquet write configuration
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")       # Better than Snappy
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")   # Vectorized reads
spark.conf.set("spark.sql.parquet.filterPushdown", "true")           # Predicate pushdown
spark.conf.set("spark.sql.parquet.bloomFilter.enabled", "true")      # Bloom filter reads
spark.conf.set("parquet.block.size", str(256 * 1024 * 1024))         # 256 MB row groups
spark.conf.set("parquet.page.size", str(1 * 1024 * 1024))            # 1 MB pages (default)
spark.conf.set("parquet.enable.dictionary", "true")                  # Dict encoding (default)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") # Safe overwrites

# Write
df.sortWithinPartitions("event_time") \
  .coalesce(20) \
  .write \
  .mode("overwrite") \
  .option("compression", "zstd") \
  .option("parquet.bloom.filter.enabled#user_id", "true") \
  .option("parquet.bloom.filter.expected.ndv#user_id", "5000000") \
  .partitionBy("event_date") \
  .parquet("s3://bucket/events/")
```

### Controlling the number of output files

```python
# Problem: Spark's default shuffle partitions (200) creates 200 files per storage partition
# After partitionBy("date"), you get 200 files per date → small files over time

# Solution A: coalesce before write (no shuffle)
df.coalesce(10).write.partitionBy("date").parquet(output_path)

# Solution B: repartition by the partition column (shuffle, but perfectly balanced files)
df.repartition("event_date") \
  .coalesce(8) \
  .write.partitionBy("event_date") \
  .parquet(output_path)

# Solution C: AQE automatic coalescing (let Spark decide)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(256 * 1024 * 1024))
```

### Verifying your Parquet files

```python
# Read just the metadata without reading data
import pyarrow.parquet as pq

# Read Parquet metadata from a local file (or download first)
schema = pq.read_schema("path/to/file.parquet")
print(schema)

metadata = pq.read_metadata("path/to/file.parquet")
print(f"Row groups: {metadata.num_row_groups}")
print(f"Rows: {metadata.num_rows}")
for i in range(metadata.num_row_groups):
    rg = metadata.row_group(i)
    print(f"Row group {i}: {rg.num_rows} rows, {rg.total_byte_size / 1e6:.1f} MB")
    for j in range(rg.num_columns):
        col = rg.column(j)
        stats = col.statistics
        if stats:
            print(f"  Column {col.path_in_schema}: min={stats.min}, max={stats.max}, nulls={stats.null_count}")
```

---

## 12.11 Parquet vs ORC vs Avro — Definitive Comparison

Understanding when NOT to use Parquet is as important as knowing when to use it.

### Head-to-head comparison

| Dimension | Parquet | ORC | Avro |
|-----------|---------|-----|------|
| **Storage model** | Columnar | Columnar | Row-based |
| **Best for** | Analytics reads | Hive/analytics | Streaming writes |
| **Spark integration** | Native default | Supported | Supported |
| **Compression** | Excellent | Excellent | Good |
| **Schema evolution** | Limited | Limited | Excellent |
| **Read performance** | Excellent | Excellent | Poor for analytics |
| **Write performance** | Good | Good | Excellent |
| **Nested types** | Good | Good | Excellent |
| **Predicate pushdown** | ✅ Min/max + Bloom | ✅ Min/max + Bloom | ❌ None |
| **Splittable** | ✅ (per row group) | ✅ (per stripe) | ✅ (with sync markers) |
| **Human readable** | ❌ Binary | ❌ Binary | ❌ Binary |
| **Kafka integration** | ❌ Not typical | ❌ Not typical | ✅ Standard |
| **ACID support** | ❌ (need Iceberg/Delta) | ✅ (native in Hive) | ❌ |

### When to use each

**Use Parquet when:**
- Analytics, reporting, ML feature computation
- Your primary access pattern is read-heavy with column selection
- Using Spark, Athena, Presto/Trino, or any modern query engine
- Building Silver and Gold layers in your data lake
- Default choice for 90% of big data use cases

```python
# ✅ Parquet: analytics, batch output
df.write.parquet("s3://bucket/silver/events/")
df.write.parquet("s3://bucket/gold/metrics/")
```

**Use ORC when:**
- You use Apache Hive heavily and need Hive's native ACID transactions
- Your team's existing tooling is ORC-centric
- You're migrating from a Hive warehouse and ORC is already in use

```python
# ORC: Hive compatibility
df.write.orc("s3://bucket/hive_warehouse/events/")
```

**Use Avro when:**
- Event streaming with Kafka (Avro + Schema Registry is the Kafka standard)
- You need robust schema evolution (add fields, rename, change defaults) without breaking consumers
- Write-throughput is critical (row-based = faster writes)
- Raw Bronze ingestion where downstream schema is uncertain

```python
# Avro: Kafka consumer output, schema-flexible Bronze
df.write.format("avro").save("s3://bucket/bronze/raw_events/")
```

### The "just use Parquet" rule

For a data engineer building batch pipelines on S3 with Spark:
- **Bronze layer:** Parquet (or Avro if coming directly from Kafka with schema evolution)
- **Silver layer:** Parquet (always)
- **Gold layer:** Parquet (always)
- **DynamoDB:** Not Parquet (it's a database, not a file)

You'll interact with Avro mainly when reading from Kafka topics. Convert to Parquet as early as possible in your pipeline.

---

## 12.12 Common Parquet Mistakes

### Mistake 1: Not compressing (or using GZIP)

```python
# BAD: no compression → 10x more storage, 10x more I/O
df.write.parquet(output_path)

# BAD: GZIP → good compression, but slow decompression, not splittable at page level
df.write.option("compression", "gzip").parquet(output_path)

# GOOD: ZSTD for new work, Snappy for compatibility
df.write.option("compression", "zstd").parquet(output_path)
```

### Mistake 2: Too many columns in schema

Reading a Parquet file with 500 columns and only needing 5 is wasteful — but less wasteful than CSV (columnar projection still saves most of the I/O). However, defining 500 columns in an explicit schema makes your code hard to maintain. Group rarely-used columns into a `Map<String, String>` attributes column.

### Mistake 3: Storing timestamps as strings

```python
# BAD: stores as string → no statistics, no range pushdown, large size
df.withColumn("event_time_str", F.col("event_time").cast("string")) \
  .write.parquet(...)

# GOOD: store as TimestampType → statistics, range pushdown, compact storage
df.write.parquet(...)  # keep event_time as TimestampType
```

### Mistake 4: Ignoring the dictionary fallback

If a column exceeds `parquet.dictionary.page.size` (default: 2 MB) of distinct values during writing, Parquet falls back to plain encoding **for the rest of that column chunk**. With UUIDs in a large row group, this means losing encoding efficiency mid-file.

```python
# If you have a UUID column that kills dictionary encoding:
# Option A: increase dictionary page size
spark.conf.set("parquet.dictionary.page.size", str(4 * 1024 * 1024))  # 4 MB

# Option B: hash the UUID into a fixed number of buckets (lose exact lookup but gain encoding)
df.withColumn("user_bucket", F.hash("user_id") % 10000)

# Option C: accept plain encoding for that column — it's OK if you don't filter by it
```

### Mistake 5: Not verifying statistics exist

Column statistics are only written when `parquet.writer.version=PARQUET_2_0` or when using certain Spark configurations. In Spark 2.x, some configurations disabled statistics. In Spark 3.x, statistics are on by default.

```python
# Verify statistics are being written
spark.conf.set("spark.sql.parquet.writeLegacyFormat", "false")  # use Parquet 2.0 format
```

### Mistake 6: Reading with inferSchema on a massive dataset

```python
# CATASTROPHIC at scale: reads footers from EVERY file to infer schema
df = spark.read.parquet("s3://bucket/events/")  # 10 million files → hours of footer reading

# ALWAYS use explicit schema
schema = StructType([...])
df = spark.read.schema(schema).parquet("s3://bucket/events/")
```

---

## 12.13 Parquet with Apache Iceberg (The Future)

Plain Parquet files work well for immutable batch output. But for mutable data lakes (upserts, deletes, schema evolution), plain Parquet has limitations.

**Apache Iceberg** wraps Parquet files with a transaction log, enabling:

```
Plain Parquet:                       Iceberg on Parquet:
  - Immutable files                    - ACID transactions
  - No atomic updates                  - Upserts (MERGE INTO)
  - Schema changes break readers       - Schema evolution
  - Full table scan for partition      - Advanced partition pruning
    metadata                             via manifest files
  - No time travel                     - Time travel / rollback
  - Small files accumulate             - OPTIMIZE command (compaction)
```

```python
# Writing to an Iceberg table (uses Parquet files underneath)
df.writeTo("glue_catalog.mydb.events") \
  .option("write.parquet.compression-codec", "zstd") \
  .option("write.target-file-size-bytes", str(256 * 1024 * 1024)) \
  .partitionedBy(F.days("event_time")) \
  .createOrReplace()

# Compaction (merge small files into large ones)
spark.sql("CALL glue_catalog.system.rewrite_data_files('mydb.events')")
```

For new systems where you need upserts, schema evolution, or fine-grained deletes: **use Iceberg with Parquet**. For simple append-only analytics tables: **plain Parquet is sufficient and simpler**.

---

## 12.14 Module Summary

### The mental model for Parquet

```
Parquet file
  = Columnar storage (read only the columns you need)
  + Dictionary/RLE encoding (repeated values compress heavily)
  + Row group statistics (skip entire blocks via predicate pushdown)
  + Bloom filters (skip blocks for equality queries on high-cardinality cols)
  + Nested type support (arrays, maps, structs)
  + ZSTD compression (smallest files, fast reads)
  + Schema embedded (self-describing)
```

### The decision checklist

| Question | Answer → Use |
|----------|-------------|
| Analytics / read-heavy? | Parquet |
| Streaming / write-heavy? | Avro (then convert to Parquet) |
| Hive ACID tables? | ORC |
| Need upserts / deletes? | Iceberg (on Parquet) |
| Need schema evolution? | Iceberg, or Avro for Bronze |
| Default for data lake Silver/Gold? | **Always Parquet** |

### Key tuning knobs

| Setting | Recommended Value | Why |
|---------|-----------------|-----|
| `spark.sql.parquet.compression.codec` | `zstd` | Better compression than Snappy |
| `parquet.block.size` | `256 MB` | Larger row groups = better statistics |
| `spark.sql.parquet.enableVectorizedReader` | `true` | Faster SIMD reads |
| `spark.sql.parquet.filterPushdown` | `true` | Enable predicate pushdown (default) |
| `spark.sql.parquet.bloomFilter.enabled` | `true` | Enable Bloom filter reads |
| Sort before write | `sortWithinPartitions("filter_col")` | Better statistics, better compression |

### ✅ Self-check

1. What is a row group? What is a page? How do they relate?
2. Why is columnar storage better for analytics than row-based storage?
3. What is dictionary encoding and what kind of columns benefit most from it?
4. What is predicate pushdown? Give an example of a filter that does push down and one that does not.
5. What is a Bloom filter and when should you use one in Parquet?
6. Why is sorting data before writing Parquet beneficial?
7. When would you choose Avro over Parquet?
8. What happens if you don't specify an explicit schema when reading millions of Parquet files?

---

**Sources consulted:**
- [Apache Parquet Official Docs — Concepts](https://parquet.apache.org/docs/concepts/)
- [Apache Parquet — Bloom Filter Spec](https://parquet.apache.org/docs/file-format/bloomfilter/)
- [Apache Parquet — Configurations](https://parquet.apache.org/docs/file-format/configurations/)
- [All About Parquet Part 10 — Performance Tuning](https://dev.to/alexmercedcoder/all-about-parquet-part-10-performance-tuning-and-best-practices-with-parquet-1ib1)
- [ClickHouse Deep Dive into Parquet Internals](https://clickhouse.com/blog/apache-parquet-clickhouse-local-querying-writing-internals-row-groups)

---

*Return to `00-learning-roadmap.md` for curriculum navigation*
