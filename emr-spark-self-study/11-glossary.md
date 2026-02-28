# Module 11: Glossary
## Big Data Terminology Reference

> **Difficulty:** All levels
> **Use:** Keep this open as a companion to all other modules

---

## A

**Action (Spark)**
An operation that triggers actual computation in Spark. Unlike transformations (which are lazy), actions force Spark to execute the DAG and produce a result. Examples: `count()`, `collect()`, `show()`, `write.*()`, `first()`. Every job begins from an action and executes backward through the DAG.

**Adaptive Query Execution (AQE)**
A Spark 3.0+ feature that re-optimizes query plans at runtime based on observed statistics. AQE can: (1) coalesce small shuffle partitions, (2) split skewed partitions, (3) switch from shuffle join to broadcast join when a table turns out to be small. Enable with `spark.sql.adaptive.enabled=true`. One of the highest-value free improvements in modern Spark.

**Append Mode**
A Spark write mode that adds new data to an existing output path. Dangerous for batch pipelines because retries create duplicates. Use `overwrite` mode instead, except when you explicitly need append semantics and have downstream deduplication.

**At-Least-Once**
A delivery semantic where each message/record is processed one or more times. Guarantees no data loss but may create duplicates. Common in distributed systems. Requires idempotent processing to avoid double-counting.

**At-Most-Once**
A delivery semantic where each message/record is processed zero or one time. Data may be lost on failure. Only appropriate for non-critical workloads where occasional data loss is acceptable.

**AWS Glue Data Catalog**
AWS's managed metadata store (Hive-compatible Metastore). Stores table definitions: schema, S3 location, partition information, and file format details. Used by Spark (via Hive integration), Athena, and Glue ETL. Free for up to 1 million objects; nominal charge beyond that. Works as the "brain" that maps logical tables to physical S3 paths.

---

## B

**Backfill**
Reprocessing historical data. Necessary when you fix a pipeline bug, change transformation logic, add new output columns, or receive corrected source data. Requires idempotent pipelines (safe to re-run). Typically done with the same job code parameterized over a range of historical dates.

**Bloom Filter**
A probabilistic data structure that can definitively say "X is NOT in the set" or "X is PROBABLY in the set." Used in Parquet file statistics to skip files that can't possibly contain a matching row. Spark 3.0+ supports Bloom filter generation on write.

**Bootstrap Action (EMR)**
A shell script that runs on every EMR cluster node during startup (before applications like Spark are installed). Used to: install custom software, configure environment variables, set up monitoring agents. Failure in a bootstrap action causes cluster creation to fail.

**Broadcast Join**
A join strategy where Spark sends the smaller table to every executor, avoiding a shuffle. Much faster than shuffle join (SortMergeJoin) when one side is small (< 100–200 MB). Configure with `spark.sql.autoBroadcastJoinThreshold` or force with `F.broadcast(df_small)`.

**Bronze Layer**
The first layer in the Medallion Architecture. Contains raw, unmodified data exactly as received from the source. May retain original format (JSON, CSV) or convert to Parquet. Append-only by nature. Data lineage anchor — if downstream layers have bugs, you can always reprocess from Bronze. Typically partitioned by ingestion date, not event date.

**Bucketing**
A write-time optimization where rows are distributed into a fixed number of "buckets" based on a column's hash. Rows with the same key always go to the same bucket. When two tables are bucketed by the same column with the same bucket count, Spark can join them without a shuffle. Requires Hive Metastore registration.

---

## C

**Cache (Spark)**
Storing a DataFrame in memory (or memory+disk) across multiple uses within a single Spark job. Avoids recomputing the lineage from scratch. Use `df.cache()` or `df.persist()`. Always `unpersist()` when done to free memory. Caches do not persist between Spark sessions.

**Catalyst Optimizer**
Spark SQL's query optimizer. Analyzes your DataFrame operations and rewrites them for efficiency: pushes filters early (predicate pushdown), eliminates unused columns (projection pushdown), reorders joins, and generates optimized bytecode (via Tungsten). Applies to all DataFrame and Spark SQL operations.

**Checkpoint (Batch)**
In batch Spark, saving an RDD/DataFrame to durable storage (S3, HDFS) to break long lineage chains. After checkpointing, Spark treats the checkpointed data as the source rather than recomputing from the original lineage. Used in iterative algorithms (ML) or very long transformation chains.

**Checkpoint (Streaming)**
In Structured Streaming, a directory on durable storage that stores: (1) offset log — which source messages have been processed, (2) state store snapshots — intermediate aggregation results, (3) metadata about the query. Mandatory for fault tolerance and exactly-once semantics. Set with `.option("checkpointLocation", "s3://bucket/checkpoints/my-query/")`.

**Cluster Manager**
The component that allocates resources to Spark applications and schedules tasks on worker nodes. On EMR: YARN. On EMR Serverless: managed by AWS. On standalone Spark: Spark's built-in cluster manager. On Kubernetes: the K8s scheduler.

**Coalesce**
A Spark transformation that reduces the number of partitions by combining existing partitions without a full shuffle. More efficient than `repartition()` when reducing partition count. Cannot increase partitions (use `repartition()` for that). Commonly used before writing to reduce output file count: `df.coalesce(20).write.parquet(...)`.

**Column Statistics (Parquet)**
Metadata stored in each Parquet row group's footer: min value, max value, null count per column. Spark uses these to skip row groups entirely if they can't contain matching rows (predicate pushdown). Dramatically reduces I/O for selective queries.

**Commit Protocol**
The mechanism Spark uses to safely write output to storage. The naive protocol: write to temp location, then rename to final (atomic on HDFS, not atomic on S3). The EMRFS S3-optimized committer replaces rename-based commits with a multipart-upload-based approach that's safe for S3.

**Compaction**
The process of merging many small files into fewer large files. Necessary for streaming pipelines or high-frequency batch writes that accumulate small files. Typically a separate Spark job that reads all files in a partition and rewrites as a smaller number of larger files. Apache Iceberg supports compaction via `OPTIMIZE` command.

**Core Node (EMR)**
Worker nodes that run both YARN NodeManager (Spark executors) and HDFS DataNode (distributed filesystem). Core nodes should be On-Demand because they hold HDFS data — losing them means data loss. On S3-only architectures (EMRFS), core nodes can be Spot but this is riskier.

**Catalyst** — see *Catalyst Optimizer*

---

## D

**DAG (Directed Acyclic Graph)**
The execution plan Spark builds from your transformations. A graph of operations (nodes) connected by data flow (edges). Directed = data flows one way. Acyclic = no cycles. Spark analyzes and optimizes the full DAG before execution, then splits it into Stages for parallel execution.

**DataFrame**
Spark's primary data structure — a distributed collection of rows organized into named, typed columns. Like a distributed SQL table. Processed by Spark's Catalyst optimizer for efficiency. The API you'll use for 95% of Spark work.

**Data Lake**
A storage system that holds raw data in its native format (files on S3) without requiring a predefined schema. Data is schema-on-read — you define the schema when reading, not when writing. Contrasted with Data Warehouses (schema-on-write, structured, SQL). S3 is the storage layer; Spark is the processing engine; Glue Catalog is the metadata layer.

**Data Skew** — see *Skew*

**Delta Lake**
An open-source table format (originally by Databricks) that adds ACID transactions, schema enforcement, and time travel to Parquet files. Files are standard Parquet; a `_delta_log/` directory stores transaction logs. Alternatives: Apache Iceberg (open, AWS-preferred), Apache Hudi.

**Driver (Spark)**
The JVM process that runs your `main()` function, holds the `SparkSession`, builds the execution plan, and coordinates executors. There's one driver per Spark application. It tracks all tasks and collects small results. Never call `.collect()` on large DataFrames — data goes to the driver and can cause OOM.

**Dynamic Partition Overwrite**
A Spark write behavior where `mode("overwrite")` with `partitionBy()` only overwrites partitions that are present in the new data, leaving other partitions untouched. Set with `spark.sql.sources.partitionOverwriteMode=dynamic`. **Critically important for incremental batch pipelines.** Without it, all partitions are deleted and only the current run's data survives.

---

## E

**EMR (Elastic MapReduce)**
AWS's managed platform for running distributed data processing frameworks (primarily Apache Spark). Handles cluster provisioning, configuration, monitoring, and termination. Available as EMR on EC2, EMR on EKS, or EMR Serverless.

**EMR Serverless**
A deployment model for EMR where AWS fully manages the compute infrastructure. No cluster to provision or manage. Auto-scales based on job demand. Per-second billing for actual vCPU and memory consumed. Suitable for intermittent workloads. Not suitable when sustained high utilization makes provisioned clusters cheaper, or when you need Spot pricing.

**EMRFS (EMR File System)**
AWS's custom Hadoop-compatible filesystem implementation for S3. Used when accessing `s3://` paths on EMR. Provides S3-aware optimizations: retry logic, consistent view, and the S3-optimized commit protocol. Always use `s3://` prefix on EMR (not `s3a://` which uses the open-source Hadoop S3A connector instead).

**EMRFS S3-Optimized Committer**
AWS's replacement for Hadoop's `FileOutputCommitter` on EMR. Avoids the expensive rename-based commit protocol. Uses S3 multipart upload completion as the atomic commit operation. Default for Parquet writes on EMR 5.19+. Results in faster job commits and correct behavior on S3.

**Event Time**
The timestamp recorded when an event actually occurred (from the source system's clock). Contrast with processing time (when the event arrived at the processing system). Event-time-based processing correctly handles out-of-order and late-arriving data. Always prefer event time for business logic when available.

**Exactly-Once**
A delivery semantic where each record is processed exactly once, even in the presence of failures. Requires: (1) a replayable source with stable message IDs/offsets, (2) an idempotent or transactional sink, (3) a checkpoint to track which messages have been processed. Hardest guarantee to achieve in distributed systems.

**Executor (Spark)**
A JVM process running on a worker node. Each executor runs multiple tasks in parallel (one per core). Executors hold cached data in memory, write shuffle data, and report task status to the driver. There are typically multiple executors per cluster, and each executor has a fixed amount of memory and CPU cores.

---

## F

**FetchFailedException**
A Spark exception thrown when an executor can't retrieve shuffle data from another executor (usually because that executor died). Spark's recovery: resubmit the shuffle map stage to regenerate the lost shuffle data, then retry the failing task. Common with Spot instance terminations.

**File Format**
How data is serialized on disk. Common big data formats:
- **Parquet**: Columnar, compressed, supports predicate pushdown. Best for analytics.
- **ORC**: Columnar, similar to Parquet, Hive's native format.
- **Avro**: Row-based, schema evolution support. Good for streaming.
- **JSON/JSONL**: Human-readable, verbose, slow for analytics. OK for Bronze.
- **CSV**: Human-readable, no types, no compression advantage. Avoid for production.
- **Delta/Iceberg**: Table formats that wrap Parquet files with transaction metadata.

**Funnel Analysis**
Analyzing user progression through a sequence of events (e.g., view → add_to_cart → purchase). A common analytics pattern. Spark window functions make this tractable at scale.

---

## G

**Gold Layer**
The third layer in the Medallion Architecture. Contains business-ready aggregates, metrics, and ML features. Optimized for consumption: dashboards, APIs, ML training. Schema designed for consumers, not for raw data fidelity. Typically the smallest layer (highly aggregated). Partitioned by business dimensions (date, region, product).

**Graviton**
AWS's custom ARM-based processors (Graviton2, Graviton3). Available as `r6g`, `m6g`, `c6g`, etc. instances. Typically 10–20% better price/performance than x86 equivalents for workloads that don't require x86-specific libraries. Supported on EMR 6.1+. Consider for cost optimization.

---

## H

**HDFS (Hadoop Distributed File System)**
Hadoop's distributed storage system, designed for large-file sequential reads. Unlike S3, HDFS supports atomic rename operations. On EMR with S3 as primary storage (EMRFS), HDFS is only used for temporary shuffle data. On EMR with HDFS as primary storage, Core nodes run HDFS DataNodes.

**Hive Metastore**
A service that stores metadata about tables: schema, partition information, S3 locations, file formats. Spark uses Hive Metastore to enable SQL queries against files stored in S3. On AWS, the Glue Data Catalog serves as a Hive-compatible metastore.

**Hive-Style Partitioning**
A directory naming convention where partition columns are reflected in the path as `column=value` pairs:
```
events/date=2024-01-15/region=US/file.parquet
```
Spark reads this structure and automatically makes `date` and `region` available as columns, and can prune directories based on filter predicates.

**Hot Key** — see *Skew*

---

## I

**Iceberg** — see *Apache Iceberg*

**Apache Iceberg**
An open-source table format designed for large-scale data lakes. Sits on top of Parquet files. Features: ACID transactions, schema evolution, partition evolution, time travel (query historical snapshots), automatic file management. AWS-preferred table format for EMR. EMR 6.5+ has native Iceberg integration. Solves many S3 data consistency and management problems.

**Idempotency**
A property of an operation where running it multiple times produces the same result as running it once. Essential for batch pipelines: if a job fails and retries, idempotent behavior prevents data duplication or corruption. Achieved with `mode("overwrite")`, deterministic output, and parameterized time windows.

**Ingestion Time**
The timestamp when data arrived at your system (e.g., when it was written to S3, or when Kafka received it). Contrast with event time. Use ingestion time for Bronze layer partitioning — it guarantees every event appears in exactly one partition, with no late arrivals.

**Instance Fleet**
An EMR configuration where you specify multiple EC2 instance types, and EMR picks whichever types are available (especially useful for Spot). Better Spot availability than single-instance-type "Instance Groups" because AWS can source capacity from multiple instance pools.

---

## J

**Job (Spark)**
The unit of work triggered by an action. One action = one job. A job is divided into stages, which are further divided into tasks. The Spark UI shows jobs, stages, and tasks separately.

**Join Strategies (Spark)**
- **Broadcast Hash Join (BHJ)**: Broadcasts the small table to all executors, no shuffle. Best when one side < autoBroadcastJoinThreshold.
- **Sort Merge Join (SMJ)**: Both sides are shuffled, sorted, and merged. Default for large-large joins. Expensive.
- **Shuffle Hash Join**: One side is hashed after shuffle. Used when one side fits in executor memory but is too large to broadcast.
- **Nested Loop Join**: Cartesian product — O(N×M). Only for cross joins or non-equi joins.

---

## K

**Kappa Architecture**
An alternative to Lambda Architecture that uses a single streaming pipeline for both real-time processing and historical reprocessing (by replaying from Kafka). Simpler operationally but requires a replayable event log.

**Key (DynamoDB)**
DynamoDB primary key is composed of:
- **Partition key (PK)**: Determines which physical partition stores the item. Must be present.
- **Sort key (SK)**: Optional. Enables range queries within a partition.

Design your keys for your access patterns. Common pattern for pipeline output: `PK = "USER#<user_id>"`, `SK = "METRICS#<date>"`.

---

## L

**Lambda Architecture**
An architecture with two parallel layers: a batch layer (accurate, high latency) and a speed layer (approximate, low latency). A serving layer merges both. Now largely replaced by simpler "Kappa" or near-real-time batch approaches. Mentioned here because you'll see it referenced frequently in big data literature.

**Lazy Evaluation**
Spark's execution model: transformations build a computation plan but don't execute. Only when an action is called does Spark execute the full plan. Enables end-to-end optimization (filter pushdown, join reordering) before any computation happens.

**Lineage**
Spark's record of how each partition was derived, expressed as a chain of transformations from the original source. Used for fault tolerance: if a partition is lost (executor failure), Spark replays the lineage to recompute it. Very long lineage chains can be expensive to replay — use `.checkpoint()` to truncate them.

---

## M

**MapReduce**
Google's original distributed computation model (2003): a `map` phase applies a function to each input record in parallel, then a `reduce` phase aggregates results. Hadoop's open-source implementation dominated big data from 2006–2014. Now largely replaced by Spark, which keeps intermediate data in memory instead of writing to disk between phases.

**Master Node (EMR)**
The primary node in an EMR cluster. Runs: YARN ResourceManager, Spark History Server, Hive Metastore, and cluster management services. Does not run Spark tasks. Should always be On-Demand (never Spot) — if it fails, the entire cluster fails.

**Medallion Architecture**
A data lake organization pattern with three layers:
- **Bronze**: Raw data, as-received
- **Silver**: Cleaned, validated, enriched
- **Gold**: Business aggregates, serving-ready

**Memory Overhead (spark.executor.memoryOverhead)**
Memory allocated outside the JVM heap for each executor container. Used by Python worker processes (PySpark UDFs), JVM native memory, thread stacks, and OS buffers. Default: 18.75% of executor memory, minimum 384 MB. Common source of OOM when using Python UDFs if set too low.

**Metastore** — see *Hive Metastore*

**Microbatch**
In Structured Streaming, the smallest unit of work: Spark processes a small batch of new records (from Kafka, S3, etc.) in each microbatch. Microbatch interval is configurable (e.g., every 1 minute). Between microbatches, Spark updates the checkpoint and state store.

---

## N

**Narrow Transformation**
A Spark transformation where each output partition depends on only one input partition. No data movement between executors required. Examples: `filter()`, `select()`, `withColumn()`, `map()`. Narrow transformations are fast because they don't trigger shuffles.

---

## O

**Object Store**
A storage system that stores data as objects with keys (like S3), rather than as files in a directory hierarchy. Key differences from filesystems: no atomic rename, "directories" are just key prefixes, eventual consistency in some edge cases. Spark was designed for HDFS (filesystem); adapting it for S3 (object store) requires the EMRFS S3 committer.

**OOM (Out of Memory)**
When a JVM process runs out of heap space. In Spark: driver OOM from `.collect()` on large datasets; executor OOM from large partitions, unbound aggregations, or insufficient memory overhead. Manifests as: `java.lang.OutOfMemoryError: Java heap space` or `Container killed by YARN for exceeding memory limits`.

**ORC (Optimized Row Columnar)**
Hive's native columnar file format. Similar performance to Parquet. Choose Parquet unless you have specific Hive compatibility requirements.

---

## P

**Parquet**
A columnar, binary file format. The default choice for analytics workloads. Features: column-level compression, row group statistics (min/max per column), nested data types, schema embedded in the file footer. Spark reads only the columns you need (projection pushdown) and skips row groups that don't match your filter (predicate pushdown). Typical compression ratio: 5–10x vs raw CSV. See `12-parquet-deep-dive.md` for full internals coverage: row groups, pages, encodings, Bloom filters, and tuning.

**Partition (S3/Storage)**
A directory on S3 that stores a subset of a dataset, created by `partitionBy("column")`. Spark uses partition directories for partition pruning: only directories matching your filter are read. The naming convention `column=value` is Hive-style partitioning. Persists permanently in S3.

**Partition (Spark/In-Memory)**
A chunk of data held in one executor's memory at one time. Each partition is processed by one task. The number of in-memory partitions controls parallelism. Distinct from storage partitions (S3 directories). Changed with `repartition()` or `coalesce()`.

**Partition Pruning**
Spark's ability to skip reading entire S3 directories (storage partitions) when a query's filter excludes those partitions. For example, filtering `WHERE date = '2024-01-15'` on a `date`-partitioned dataset reads only the `date=2024-01-15/` directory. The most impactful performance optimization for large datasets.

**PySpark**
Apache Spark's Python API. Provides a Python interface to all Spark functionality. Internally, PySpark communicates with the Spark JVM via Py4J. For performance-critical operations, avoid Python UDFs (which serialize data between JVM and Python) and prefer built-in Spark SQL functions.

---

## R

**RDD (Resilient Distributed Dataset)**
Spark's original low-level distributed collection API. Replaced by DataFrames for most use cases. The "Resilient" part: Spark tracks lineage to recompute lost partitions. Modern Spark code should use DataFrames/Datasets instead of RDDs directly.

**Repartition**
A Spark transformation that redistributes data across a new number of partitions using a full shuffle. Can increase or decrease partition count. Can partition by a column (useful before `partitionBy()` writes). Contrast with `coalesce()` (no shuffle, can only decrease).

---

## S

**S3 (Simple Storage Service)**
AWS's object storage service. The de facto storage layer for big data on AWS. Characteristics: durable (11 nines), scalable, cheap (~$0.023/GB-month), strongly consistent (since Dec 2020), but NOT a filesystem (no atomic rename). Spark accesses S3 via EMRFS on EMR.

**S3 Glacier**
AWS's archival storage tier. Much cheaper ($0.004/GB-month) but retrieval takes hours and costs money. Use for Bronze data older than 1–3 years via S3 Lifecycle policies.

**Salting**
A technique to fix data skew in joins/aggregations: append a random number to skewed keys, then explode the small table to match all possible salted keys. Distributes the hot key's work across many executors instead of one. See Module 07 for a full example.

**Shuffle**
The redistribution of data across executors. Triggered by: `groupBy()`, `join()`, `orderBy()`, `distinct()`, `repartition()`. Involves: writing shuffle data to local disk (shuffle write), transferring data over the network (shuffle read). Most expensive Spark operation. Minimize shuffles for performance.

**Silver Layer**
The second layer in the Medallion Architecture. Contains cleaned, validated, deduplicated, and enriched data. Always Parquet. Partitioned by event date (business time). The source of truth for business metrics. Multiple Gold layer pipelines read from Silver.

**Skew**
Uneven distribution of data across partitions. One partition has 10× more rows than the median → one task takes 10× longer. The entire stage waits for the slowest task. Common causes: null keys in joins, hot product/user IDs, temporal events (holidays). Fixed with: salting, AQE skew handling, or pre-filtering nulls.

**Spark History Server**
A web UI that shows historical Spark job execution: stages, tasks, shuffle sizes, executor metrics, timing. Available on EMR via SSH tunnel or the EMR console. Primary tool for performance debugging. Access at port 18080 on the EMR master node.

**SparkSession**
The entry point to Spark in modern PySpark. Replaces `SparkContext` + `HiveContext` + `SQLContext` from older APIs. `spark = SparkSession.builder.appName("...").getOrCreate()`. Everything — reads, writes, SQL, configuration — goes through the SparkSession.

**Speculation (Spark)**
When a task is running much slower than its peers, Spark launches a duplicate task on another executor. Whichever finishes first "wins"; the slower one is killed. Enables automatic mitigation of stragglers. Enable with `spark.speculation=true`. Useful when occasional hardware degradation or resource contention causes single tasks to be very slow.

**Stage (Spark)**
A unit of execution in Spark's DAG that runs without requiring a shuffle. A DAG is split into stages at shuffle boundaries. All tasks in a stage can run in parallel. Stages run sequentially (each stage waits for all tasks in the previous stage to complete before starting).

**State Store**
In Structured Streaming, an in-memory key-value store per partition that accumulates aggregation state across microbatches. Checkpointed to durable storage after each batch. Grows unboundedly without watermarks. Use RocksDB state store for large state.

**Step (EMR)**
A unit of work submitted to an EMR cluster. Typically a `spark-submit` command. Steps run sequentially on a cluster. `ActionOnFailure` controls what happens when a step fails: `TERMINATE_CLUSTER`, `CONTINUE`, or `CANCEL_AND_WAIT`.

**Structured Streaming**
Spark's streaming API, built on DataFrames. Processes continuous streams of data using the same DataFrame API as batch. Supports event-time windows, watermarks, stateful aggregations, and exactly-once semantics with appropriate sources and sinks.

---

## T

**Task (Spark)**
The smallest unit of work in Spark — processes one partition in one stage. Each task runs on one CPU core of one executor. Task duration is what you see in the Stage Details view. Skew manifests as some tasks taking much longer than others.

**Task Node (EMR)**
Worker nodes that run YARN NodeManager and Spark executors, but NOT HDFS DataNode. Safe to use as Spot instances because they hold no HDFS data. Removing them only causes task retry, not data loss.

**Transient Cluster**
An EMR cluster created for a single job (or pipeline run) and auto-terminated after completion. Best practice for periodic batch pipelines. Avoids paying for idle cluster time. Opposite of "long-running cluster" which stays up between jobs.

**Transformation (Spark)**
A lazy operation on a DataFrame that produces a new DataFrame. Builds the DAG but doesn't execute. Examples: `filter()`, `select()`, `join()`, `groupBy()`. Execution only happens when an action is called.

**Tungsten**
Spark's code generation engine. Generates optimized JVM bytecode for Spark operations, enabling CPU efficiency improvements. Part of Catalyst/the physical planning phase. Operates automatically — you don't configure it directly.

---

## U

**Unified Memory (spark.memory.fraction)**
Spark's memory management zone that serves both execution (shuffles, sorts) and storage (caching). Unified means execution and storage memory can borrow from each other dynamically. Configured as a fraction of executor heap minus reserved memory. Default: 60%.

**UDF (User-Defined Function)**
Custom code written in Python/Scala/Java that extends Spark's built-in functions. Python UDFs serialize rows through a Python process, which is slow. Prefer Pandas UDFs (Apache Arrow-based, batch processing) or built-in Spark SQL functions. Scala UDFs run in the JVM directly and have much less overhead.

---

## W

**Watermark**
In Structured Streaming, a declaration of the maximum tolerable event lateness. `df.withWatermark("event_time", "30 minutes")` tells Spark to wait up to 30 minutes after the current watermark before closing time windows. Events older than the watermark are discarded. Necessary to bound state size and enable output in append mode.

**Wide Transformation**
A Spark transformation where each output partition depends on multiple input partitions. Triggers a shuffle. Examples: `groupBy()`, `join()`, `orderBy()`, `distinct()`. Stage boundaries occur at wide transformations.

---

## Y

**YARN (Yet Another Resource Negotiator)**
Hadoop's cluster resource manager. On EMR on EC2, YARN manages resource allocation: your Spark application's driver requests containers (CPU + memory), YARN allocates them from available worker nodes. Spark's cluster manager on EMR is YARN.

**YARN ResourceManager**
The YARN master process, running on the EMR master node. Schedules containers across cluster nodes based on resource requests. Monitors NodeManagers on worker nodes. Exposed via the YARN Web UI (port 8088 on master, accessible via SSH tunnel).

---

## Z

**Z-Ordering**
A data skipping technique used by Delta Lake and Iceberg: data is sorted along multiple columns simultaneously using a Z-order space-filling curve. Enables effective multi-dimensional filtering. For example, if you frequently filter by `date AND user_id`, Z-ordering on both columns enables reading a fraction of data even though you can only physically sort by one column at a time.

---

## Abbreviations Quick Reference

| Abbreviation | Full Name |
|-------------|-----------|
| AQE | Adaptive Query Execution |
| DAG | Directed Acyclic Graph |
| EMRFS | EMR File System |
| ETL | Extract, Transform, Load |
| GC | Garbage Collection |
| HDFS | Hadoop Distributed File System |
| IOC | Input/Output Commit |
| JVM | Java Virtual Machine |
| OOM | Out of Memory |
| ORC | Optimized Row Columnar |
| PK | Partition Key (DynamoDB) |
| RDD | Resilient Distributed Dataset |
| SK | Sort Key (DynamoDB) |
| SLA | Service Level Agreement |
| SSM | AWS Systems Manager (Parameter Store) |
| WCU | Write Capacity Unit (DynamoDB) |
| YARN | Yet Another Resource Negotiator |

---

*End of Glossary — Return to `00-learning-roadmap.md` for curriculum navigation*
