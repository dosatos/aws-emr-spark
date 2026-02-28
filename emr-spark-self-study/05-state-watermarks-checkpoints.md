# Module 05: State, Watermarks, and Checkpoints
## Understanding Fault Tolerance and Temporal Reasoning

> **Difficulty:** 🟡 Intermediate → 🔴 Advanced
> **Prerequisite:** Modules 01–04
> **Est. time:** 3–4 hours

---

## 5.1 Why These Concepts Matter for Batch Engineers

You might be thinking: "I'm building batch jobs, not streaming. Why do I need watermarks and checkpoints?"

Two reasons:

1. **Batch jobs checkpoint too.** Understanding Spark's fault tolerance — how it saves progress and recovers from failures — applies directly to long-running batch jobs. A 4-hour batch job that crashes at 3h50m needs to recover, not restart from scratch.

2. **Your batch pipeline will eventually become semi-streaming.** As business requirements evolve, "run once a day" becomes "run every hour," then "every 15 minutes," then "near-real-time." Building mental models now prevents expensive rewrites later.

3. **Late data is a batch problem too.** If your midnight batch job processes "today's data," but some events from today arrive tomorrow due to network delays, you have a late data problem — even in a batch context.

---

## 5.2 Checkpoints: Fault Tolerance for Long Jobs

### What is a checkpoint?

A checkpoint is a snapshot of Spark's execution state saved to durable storage (S3, HDFS). If a job crashes, it can recover from the latest checkpoint rather than starting from the beginning.

Think of it like a save point in a video game — crash recovery resumes from the last save, not from the beginning.

### Checkpoints in Batch Spark

For batch jobs, Spark has RDD checkpointing:

```python
# Set checkpoint directory (use S3 on EMR)
spark.sparkContext.setCheckpointDir("s3://my-bucket/checkpoints/job-name/")

# Checkpoint a DataFrame to break long lineage chains
df_complex = (
    df.join(df_large, "key")
      .groupBy("key")
      .agg(F.sum("value").alias("total"))
      .join(df_another, "key")
)

# If the lineage of df_complex is very long, checkpointing saves the data
# to disk and severs the lineage chain — recomputation starts from here
df_complex.checkpoint()
```

**When to checkpoint in batch:**
- Very long lineage chains (50+ transformations)
- Iterative algorithms (ML training loops)
- After expensive operations you want to cache reliably across failures

**For most standard ETL batch jobs, you don't need explicit checkpointing** because the job is structured as read → transform → write. If it fails, just re-run it (idempotent).

### Checkpoints in Structured Streaming

This is where checkpoints become absolutely mandatory.

Spark Structured Streaming maintains **state** — information about what has been processed and the results of ongoing aggregations. This state must be saved somewhere durable. That's what the streaming checkpoint does.

```python
query = df_stream \
    .groupBy("user_id", F.window("event_time", "1 hour")) \
    .sum("amount") \
    .writeStream \
    .format("parquet") \
    .option("path", "s3://bucket/output/") \
    .option("checkpointLocation", "s3://bucket/checkpoints/hourly-sum/") \
    .outputMode("append") \
    .start()
```

The checkpoint location stores:
1. **Offset log:** Which messages from the source have been processed (e.g., Kafka offsets)
2. **State store:** The intermediate aggregation state (e.g., running sums per user per window)
3. **Metadata:** Schema, configuration, streaming query ID

**Without a checkpoint location, the streaming query:**
- Cannot recover from failure
- Cannot guarantee exactly-once or at-least-once semantics
- Will reprocess ALL historical data on restart

**Always specify a unique checkpoint location per streaming query.** Two queries sharing a checkpoint location will corrupt each other.

---

## 5.3 Exactly-Once vs At-Least-Once vs At-Most-Once

These are fundamental guarantees in distributed systems. Understanding them prevents subtle bugs.

### At-most-once (fire and forget)

Data is processed **zero or one time**. If processing fails, the data is dropped.

```
Message sent → Processing attempt:
  Success → ✅ Processed
  Failure → 🚫 Dropped (lost forever)
```

Use when: Losing some data is acceptable (e.g., non-critical metrics)
Avoid when: Missing data is unacceptable (financial transactions, audit logs)

### At-least-once

Data is processed **one or more times**. If processing fails, it retries — which means it might process the same message multiple times.

```
Message sent → Processing attempt:
  Success → ✅ Processed
  Failure → 🔄 Retry → might process again
             → 🔄 Retry again → might process again
             → Eventually succeeds
```

Common result: **duplicates** in your output if not handled explicitly.

Use when: You can deduplicate downstream, or the operation is naturally idempotent (e.g., `set value = X` not `increment value by 1`).

### Exactly-once

Data is processed **exactly one time**, even in the presence of failures.

```
Message sent → Processing attempt:
  Success → ✅ Processed exactly once
  Failure → 🔄 Retry → system ensures no duplicate
```

This is the hardest guarantee to achieve in distributed systems. It requires coordination between the source, the processing system, and the sink.

### Exactly-once in Spark

Spark Structured Streaming achieves exactly-once end-to-end with:
1. **Replayable sources** (Kafka, Kinesis — each message has an offset/sequence number)
2. **Idempotent sinks** (writes that are safe to repeat) or **transactional sinks** (Delta Lake)
3. **Checkpoint** to track exactly which offsets have been processed

```
Kafka                Spark                    Delta Lake
[msg1, offset=0] ──► Process msg1 ──────────► Write row (transactional)
[msg2, offset=1]     Checkpoint: offset=0
    ↑                    (crash!)
    │                    ↓
    └────────────────── Restart from offset=0
                         Process msg1 again ──► Delta sees duplicate,
                                                deduplicates via txn log
```

### Approximating exactly-once in batch

For batch jobs, exactly-once is typically achieved by:
1. **Idempotent writes:** Overwrite the output partition rather than append
2. **Transaction-like commit:** Write to temp location, validate, then atomically move

```python
def process_with_exactly_once_semantics(spark, date, input_path, output_path):
    """Process data with exactly-once guarantees via idempotent overwrite."""

    # If this job has been run before for this date, this overwrites it
    # If this job is a retry after failure, this overwrites any partial output
    df = spark.read.parquet(f"{input_path}/date={date}/")
    result = transform(df)

    # Write to a staging path first
    staging_path = f"{output_path}/_staging_{date}/"
    result.write.mode("overwrite").parquet(staging_path)

    # Validate the staging output
    validate_output(spark.read.parquet(staging_path), date)

    # Atomically move staging to final (on HDFS this is atomic;
    # on S3 with EMRFS committer, this is handled by the committer)
    final_path = f"{output_path}/date={date}/"
    move_s3_prefix(staging_path, final_path)  # using boto3 copy + delete
```

---

## 5.4 Watermarks: Handling Time in Streaming

Watermarks are a streaming concept, but understanding them builds critical intuition for temporal data in any context.

### The problem: event time vs processing time

In a perfect world, events arrive at your processing system in the order they occurred:

```
Events in the world:    event1@10:00 → event2@10:01 → event3@10:02
Events in the system:   event1@10:00 → event2@10:01 → event3@10:02
                        (perfect order, no delay)
```

In reality, events arrive late and out of order:

```
Events in the world:  event1@10:00 → event2@10:01 → event3@10:02
                         ↑                ↑                ↑
Events arrive:        @10:01            @10:01           @10:15
                                        (late!)           (very late!)

Events in system:     event1 → event2 → [gap] → event3@10:02
                       10:01    10:01             10:15 (14 min late!)
```

**Event time:** When the event actually happened (user's clock)
**Processing time:** When the event arrived at your system
**Ingestion time:** When the event entered your Kafka/queue

The gap between event time and processing time is **event latency**. Mobile apps, international traffic, and network issues cause events to arrive minutes or hours late.

### The aggregation problem

Suppose you want to compute "total purchases per hour." With event-time windows:

```
Window [10:00 - 11:00]: should contain all purchases with event_time in this range

Events arriving:
  10:01 → purchase $50  → goes into window [10:00-11:00] ✅
  10:02 → purchase $30  → goes into window [10:00-11:00] ✅
  11:15 → purchase $20 (event_time=10:45, 30 min late!) → should go into [10:00-11:00] ✅

  But by 11:15, have you already "closed" the [10:00-11:00] window and emitted results?
```

This is the core tension: you want to emit results quickly, but you must wait for late arrivals.

### What a watermark is

A **watermark** is a declaration: "I believe I have seen all events up to time T - latency_threshold. Any event with event_time < T - latency_threshold is so late that I'll discard it."

```
Max event time seen so far: 11:30
Watermark threshold: 30 minutes
Current watermark: 11:30 - 30min = 11:00

→ Window [10:00 - 11:00] is now safe to close and emit results
→ Any event with event_time < 11:00 arriving now will be discarded

A new event arrives with event_time = 10:55:
  10:55 < 11:00 (watermark) → DISCARDED (too late)

A new event arrives with event_time = 11:15:
  11:15 > 11:00 (watermark) → ACCEPTED, goes into [11:00 - 12:00] window
```

### Watermarks in code

```python
from pyspark.sql import functions as F

# Define a watermark: wait up to 30 minutes for late data
df_with_watermark = df_stream.withWatermark("event_time", "30 minutes")

# Now aggregate over time windows
result = df_with_watermark \
    .groupBy(
        "user_id",
        F.window("event_time", "1 hour")  # 1-hour tumbling windows
    ) \
    .agg(F.sum("amount").alias("hourly_total"))

# Write in append mode (only emit when window is closed by watermark)
query = result.writeStream \
    .format("parquet") \
    .option("path", "s3://bucket/hourly-totals/") \
    .option("checkpointLocation", "s3://bucket/checkpoints/hourly-totals/") \
    .outputMode("append") \
    .start()
```

### Watermarks and state cleanup

Without watermarks, Spark keeps state for every window indefinitely (infinite memory growth).

With watermarks, Spark knows when a window is "finalized" (no more late arrivals expected) and can clean up that state.

```
Without watermark:
  State grows forever: [window1_state, window2_state, window3_state, ...]
  Eventually OOM.

With watermark:
  After watermark passes window1's end time:
    → Emit window1 results
    → Discard window1 state
    → Memory is bounded
```

**For long-running streaming jobs, always set watermarks.** A streaming job without watermarks will eventually run out of memory (hours, days, or weeks — but it will happen).

---

## 5.5 Late Data in Batch Pipelines

Even without streaming, late data is a real problem.

### The scenario

Your daily batch job runs at midnight and processes "today's data." It reads:
```
s3://bucket/events/event_date=2024-01-15/
```

But some events from 2024-01-15 arrive at 1 AM on 2024-01-16, after your batch already ran. Those events are now orphaned — they exist in the source but were missed by the batch.

### Strategies for handling late batch data

**Strategy 1: Reprocess window**

Instead of processing only yesterday's data, always reprocess the last N days:

```python
from datetime import datetime, timedelta

def get_dates_to_process(target_date: str, lookback_days: int = 3) -> list:
    """Return list of dates to process, including lookback for late arrivals."""
    target = datetime.strptime(target_date, "%Y-%m-%d")
    return [
        (target - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(lookback_days)
    ]

dates = get_dates_to_process("2024-01-18", lookback_days=3)
# → ["2024-01-18", "2024-01-17", "2024-01-16"]
```

With idempotent writes, reprocessing older dates is safe — it just overwrites the same output partitions with (slightly) updated data.

**Tradeoff:** More processing cost, but catches late arrivals.

**Strategy 2: Separate late-data partition**

Treat late data as a first-class concern. The pipeline that ingests events stamps events with both event_time and ingestion_time. Late events are written to a separate partition:

```
s3://bucket/events/
├── type=ontime/event_date=2024-01-15/   ← events ingested on time
└── type=late/event_date=2024-01-15/     ← events ingested late
```

A separate reconciliation job runs daily and incorporates late events:

```python
def reconcile_late_data(spark, date: str):
    """Incorporate late events into the gold layer."""
    late_events = spark.read.parquet(f"s3://bucket/events/type=late/event_date={date}/")

    if late_events.count() == 0:
        return

    # Read current gold output for this date
    current_output = spark.read.parquet(f"s3://bucket/gold/date={date}/")

    # Re-aggregate including late events
    all_events_for_date = spark.read.parquet(f"s3://bucket/events/event_date={date}/")
    recomputed = transform(all_events_for_date)

    # Overwrite the output partition (idempotent)
    recomputed.write.mode("overwrite").parquet(f"s3://bucket/gold/date={date}/")
```

**Strategy 3: Use ingestion time for partitioning**

Partition raw/Bronze data by ingestion_date, not event_date. This ensures every event is captured in exactly one partition, with no late arrivals:

```python
# Bronze layer: partition by when data was ingested
df.withColumn("ingestion_date", F.current_date()) \
  .write.partitionBy("ingestion_date") \
  .parquet("s3://bucket/bronze/events/")

# Silver/Gold layer: partition by event_date (may require reprocessing)
```

---

## 5.6 Exactly-Once Delivery to DynamoDB

When your pipeline writes to DynamoDB, how do you ensure exactly-once writes during retries?

### The idempotency key pattern

Use a deterministic, unique key for each output record. If a retry writes the same record again, it overwrites itself (PutItem is idempotent when the primary key doesn't change):

```python
# Each (user_id, date) pair has exactly one row in DynamoDB
# Retrying the pipeline overwrites the same row with the same values
item = {
    'PK': f"USER#{row.user_id}",    # Primary key
    'SK': f"METRICS#{row.date}",    # Sort key
    'total_amount': Decimal(str(row.total_amount)),
    'event_count': int(row.event_count),
    # DynamoDB PutItem: if this PK+SK exists, replace it
    # Same values on retry → safe idempotent operation
}
table.put_item(Item=item)
```

**This works because PutItem is idempotent** — writing the same item twice has the same effect as writing it once. The key is that your keys are deterministic (computed from input, not from random UUIDs or timestamps).

### Conditional writes for atomic operations

For non-idempotent operations (like incrementing counters), use DynamoDB's conditional expressions:

```python
# Idempotency token ensures this write only happens once
# even if the pipeline retries
try:
    table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(PK)",  # Only write if new
    )
except table.meta.client.exceptions.ConditionalCheckFailedException:
    # Item already exists — a retry wrote it — this is fine
    logger.info(f"Item {item['PK']} already exists — skipping (idempotent retry)")
```

---

## 5.7 State Store in Structured Streaming

For completeness, understand Spark's state store mechanism.

### What the state store is

During stateful streaming aggregations, Spark maintains an in-memory **state store** — a key-value map per partition that accumulates state across microbatches.

```
Microbatch 1:
  user_1 had $50 in events → State: {user_1: 50}

Microbatch 2:
  user_1 had $30 more events → State: {user_1: 80}

Microbatch 3:
  user_1 had $20 more events → State: {user_1: 100}
```

The state store is checkpointed to durable storage after each microbatch, so it survives failures.

### State store backends on EMR

By default, Spark uses an in-memory state store backed by local disk for checkpointing. For large state (many keys, long windows), you may need a more scalable state store backend.

**RocksDB state store (Spark 3.2+):**
```python
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
```

RocksDB stores state on disk rather than in JVM heap — much more efficient for large state, avoids GC pressure.

---

## 5.8 Module Summary

- **Checkpoints for batch:** RDD checkpoints break long lineage chains and save recompute work
- **Checkpoints for streaming:** Mandatory. Store offset log + state. Always use unique checkpoint locations.
- **Exactly-once:** Requires replayable source + idempotent or transactional sink + checkpoint
- **Watermarks:** Define how long to wait for late data before closing a time window
- **Late data in batch:** Reprocess recent N days, or use ingestion-time partitioning for Bronze
- **Idempotent DynamoDB writes:** Use deterministic primary keys so retries are safe

### ✅ Self-check

1. What does a streaming checkpoint store? What happens if you don't set a checkpoint location?
2. What is the difference between event time and processing time?
3. What is a watermark and what problem does it solve?
4. What happens to a streaming job's state if no watermark is set?
5. What does "exactly once" require from the source, processor, and sink?
6. How do you handle events that arrive 2 days late in a daily batch pipeline?

---

*Next: `06-partitioning-and-data-layout.md` →*
