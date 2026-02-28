# AWS EMR + Spark Self-Study Curriculum

A structured curriculum for engineers who are strong at programming but new to distributed data systems. Covers Apache Spark, AWS EMR, S3 batch pipelines, and production operations at petabyte scale.

**Target stack:** Scala/PySpark · AWS EMR (EC2 + Serverless) · S3 · Parquet

---

## Modules

| # | File | Topic |
|---|------|--------|
| 00 | [Learning Roadmap](emr-spark-self-study/00-learning-roadmap.md) | 5-phase study plan, skill benchmarks |
| 01 | [Spark Fundamentals](emr-spark-self-study/01-spark-fundamentals.md) | DAG, lazy evaluation, shuffles, partitions |
| 02 | [EMR Fundamentals](emr-spark-self-study/02-emr-fundamentals.md) | EC2 vs Serverless, YARN, Spot, cost comparison |
| 03 | [Spark on S3](emr-spark-self-study/03-spark-on-s3.md) | Commit protocol, partition overwrite, small files |
| 04 | [Batch Pipeline Architecture](emr-spark-self-study/04-batch-pipeline-architecture.md) | Medallion pattern, idempotency, orchestration |
| 05 | [State, Watermarks, Checkpoints](emr-spark-self-study/05-state-watermarks-checkpoints.md) | Exactly-once, late data, checkpoint strategies |
| 06 | [Partitioning & Data Layout](emr-spark-self-study/06-partitioning-and-data-layout.md) | S3 path design, file sizing, compaction |
| 07 | [Performance & Cost](emr-spark-self-study/07-performance-and-cost.md) | Skew, broadcast joins, AQE, cluster sizing |
| 08 | [Failure Modes & Watch-outs](emr-spark-self-study/08-failure-modes-and-watchouts.md) | OOM, silent data corruption, Spot termination |
| 09 | [Production Best Practices](emr-spark-self-study/09-production-best-practices.md) | Schema evolution, backfills, monitoring, CI/CD |
| 10 | [Hands-On Project](emr-spark-self-study/10-hands-on-project.md) | Full Bronze→Silver→Gold pipeline with EMR deploy |
| 11 | [Glossary](emr-spark-self-study/11-glossary.md) | ~60 term reference |
| 12 | [Parquet Deep Dive](emr-spark-self-study/12-parquet-deep-dive.md) | Encodings, statistics, Bloom filters, tuning |

Start with `00-learning-roadmap.md`.

---

## Agent Prompt

[`AGENT.md`](AGENT.md) contains a system prompt for an AI coding agent scoped to this stack — Scala, EMR, S3, Parquet — with explicit defaults, optimization priorities, and anti-patterns to avoid.
