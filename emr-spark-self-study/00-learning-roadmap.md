# EMR & Spark Self-Study Curriculum
## Master Learning Roadmap

> **Who this is for:** A software engineer who is strong at programming but has never touched distributed data systems, Apache Spark, or AWS EMR.
>
> **Where this takes you:** Practical working proficiency — you can architect, build, deploy, operate, and debug production-grade batch pipelines that process petabyte-scale data on AWS EMR.

---

## 🗺️ The Big Picture

Before you read a single page of documentation, understand what you're getting into.

### What problem does all this solve?

You have a lot of data. More data than fits on one machine. More data than one machine can process in any reasonable time. You need to:

1. **Store** it somewhere durable and cheap
2. **Process** it (transform, aggregate, filter, join) reliably
3. **Write results** somewhere useful
4. Do all of this **repeatedly**, on a **schedule**, **without data loss**, and **without paying a fortune**

That's the problem. Apache Spark is the processing engine. AWS EMR is the managed platform that runs Spark. S3 is your storage layer. This curriculum teaches you to wire them together correctly.

---

## 📁 Curriculum Files

| File | Topic | Difficulty | Est. Study Time |
|------|--------|------------|-----------------|
| `01-spark-fundamentals.md` | What Spark is, how it works, core concepts | 🟢 Beginner | 4–6 hours |
| `02-emr-fundamentals.md` | AWS EMR architecture, deployment models | 🟢 Beginner | 3–4 hours |
| `03-spark-on-s3.md` | Reading/writing S3, commit protocols, pitfalls | 🟡 Intermediate | 3–4 hours |
| `04-batch-pipeline-architecture.md` | Designing end-to-end batch pipelines | 🟡 Intermediate | 4–5 hours |
| `05-state-watermarks-checkpoints.md` | Checkpoints, watermarks, idempotency | 🟡 Intermediate | 3–4 hours |
| `06-partitioning-and-data-layout.md` | Partition strategies, S3 path design | 🟡 Intermediate | 3–4 hours |
| `07-performance-and-cost.md` | Tuning, sizing, cost optimization | 🔴 Advanced | 5–6 hours |
| `08-failure-modes-and-watchouts.md` | What breaks, how it breaks, how to fix it | 🔴 Advanced | 3–4 hours |
| `09-production-best-practices.md` | Schema evolution, backfills, monitoring | 🔴 Advanced | 4–5 hours |
| `10-hands-on-project.md` | Full end-to-end project with real code | 🔴 Advanced | 8–12 hours |
| `11-glossary.md` | Reference glossary for all terminology | 🟢–🔴 All levels | Reference |
| `12-parquet-deep-dive.md` | Parquet internals, encodings, statistics, Bloom filters | 🟢–🔴 All levels | 3–5 hours |

**Total:** ~45–55 hours of focused study + hands-on project

---

## 🧭 Recommended Learning Path

### Phase 1 — Build Mental Models (Week 1)
```
01-spark-fundamentals.md  →  02-emr-fundamentals.md
```
Do not skip this. Every advanced concept builds on these foundations. If you have shaky mental models here, everything downstream is harder.

**Goal after Phase 1:** You can explain what Spark is, how data flows through a cluster, and what EMR does for you. You can draw the architecture on a whiteboard.

---

### Phase 2 — S3 + Pipeline Architecture (Week 2)
```
03-spark-on-s3.md  →  12-parquet-deep-dive.md  →  04-batch-pipeline-architecture.md
```
This is where you start thinking about your actual job. How does data flow from S3 into Spark and back out? What does a real pipeline look like? Module 12 covers Parquet deeply — its internal structure, how compression and statistics work, and why it's the right format for almost everything you'll write.

**Goal after Phase 2:** You can design a basic batch pipeline on paper. You understand why S3 is not a regular filesystem, why Parquet is the correct storage format, and why that matters.

---

### Phase 3 — State, Partitioning, Layout (Week 3)
```
05-state-watermarks-checkpoints.md  →  06-partitioning-and-data-layout.md
```
This phase builds the concepts that separate "it works on my laptop" from "it works in production." Checkpoints protect you from failures. Partitioning determines whether your job runs in 10 minutes or 10 hours.

**Goal after Phase 3:** You can design a partition strategy for a given dataset. You understand idempotency and why it matters for repeated runs.

---

### Phase 4 — Production Reality (Week 4)
```
07-performance-and-cost.md  →  08-failure-modes-and-watchouts.md  →  09-production-best-practices.md
```
This is where most tutorials stop, but where real engineers earn their salary. This phase teaches you what goes wrong, how to detect it, and how to build systems that survive.

**Goal after Phase 4:** You can size a cluster, estimate costs, identify a shuffle skew problem, and build a pipeline that handles failures gracefully.

---

### Phase 5 — Build It (Week 5–6)
```
10-hands-on-project.md
```
Apply everything. Build the full petabyte-scale periodic S3 pipeline from scratch, end to end.

**Goal after Phase 5:** You have a working, production-quality pipeline. You've encountered real problems and solved them.

---

## 🧠 How to Use This Curriculum

### Don't just read — do

Every module has exercises. Do them. Spark knowledge that lives only in your head is not real knowledge. Run the code. Break things intentionally. Fix them.

### Build a reference environment

As you go, keep a running `notes.md` file in this directory. Write down:
- Things you found confusing and later understood
- Commands you always forget
- Configurations that took time to figure out
- Cost surprises

### Use the glossary as a companion

Keep `11-glossary.md` open as you read. New terminology appears constantly. Look things up immediately rather than letting confusion accumulate.

### Expected frustration points

Most learners get stuck in one of three places:
1. **Understanding lazy evaluation** (Module 01) — keep at it, the "aha" moment is worth it
2. **Grasping why S3 is different from a filesystem** (Module 03) — critical, must internalize
3. **Partition tuning** (Module 06/07) — experience is the only real teacher here

---

## 🎯 Skill Benchmarks

After completing this curriculum, you should be able to answer these questions:

### Fundamentals
- [ ] What is a DAG in Spark? Why does Spark use lazy evaluation?
- [ ] What is a shuffle? Why is it expensive?
- [ ] What is the difference between a transformation and an action?
- [ ] What is the difference between narrow and wide dependencies?

### EMR
- [ ] When would you use EMR Serverless vs EMR on EC2?
- [ ] What is the driver node? What is the executor? What is the cluster manager?
- [ ] How does YARN schedule work on EMR?
- [ ] What are Spot instances and when is it safe to use them?

### S3 + Storage
- [ ] Why can't Spark treat S3 like HDFS? What is the commit problem?
- [ ] What is the EMRFS S3-optimized committer and why does it exist?
- [ ] What is the small files problem? How do you fix it?
- [ ] What is Parquet? Why use it instead of CSV?

### Pipelines
- [ ] What is idempotency and why must batch jobs be idempotent?
- [ ] What is a checkpoint? What is a watermark?
- [ ] What does "exactly once" mean and how do you approximate it?
- [ ] How do you handle late-arriving data in a batch job?

### Performance
- [ ] What does executor memory configuration look like?
- [ ] What is skew and how do you detect it?
- [ ] What is broadcast join and when do you use it?
- [ ] How do you size a cluster for a given job?

### Production
- [ ] What is schema evolution and how does it break pipelines?
- [ ] How do you backfill historical data safely?
- [ ] What does a healthy pipeline monitoring setup look like?

---

## 🛠️ Prerequisites

Before starting, make sure you have:

- [ ] An AWS account (free tier is fine to start)
- [ ] AWS CLI installed and configured
- [ ] Python 3.8+ installed
- [ ] PySpark installed locally (`pip install pyspark`)
- [ ] Basic familiarity with SQL
- [ ] Basic familiarity with Python

You don't need a cluster to learn Spark fundamentals — PySpark runs locally in standalone mode. You'll need an AWS account for the EMR and S3 hands-on work.

---

## 📚 Supplemental Reading

These are not required but will accelerate your learning:

**Books:**
- *High Performance Spark* by Holden Karau & Rachel Warren — best deep-dive on Spark internals
- *Learning Spark, 2nd Edition* by Databricks team — good broad introduction

**Documentation:**
- [Apache Spark Official Docs](https://spark.apache.org/docs/latest/) — always authoritative
- [AWS EMR Best Practices Guide](https://aws.github.io/aws-emr-best-practices/) — AWS-maintained, practical
- [AWS EMR Release Guide](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/) — version-specific config

**Engineering Blogs:**
- Databricks Engineering Blog — regularly publishes deep Spark internals content
- Netflix Tech Blog — excellent real-world batch pipeline articles
- Airbnb Engineering Blog — strong Spark+S3 partitioning content

---

## ⚡ Quick Reference: Difficulty Legend

Throughout all modules, sections are marked:

- 🟢 **Beginner** — core concepts, must understand before moving on
- 🟡 **Intermediate** — working knowledge, important for real jobs
- 🔴 **Advanced** — production-grade, requires hands-on experience

> **Note:** Advanced sections are not optional. At petabyte scale, ignoring them causes real production incidents.

---

*Start with `01-spark-fundamentals.md` →*
