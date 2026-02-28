# Module 02: AWS EMR Fundamentals
## Running Spark at Scale on AWS

> **Difficulty:** 🟢 Beginner → 🟡 Intermediate
> **Prerequisite:** Module 01 (Spark Fundamentals)
> **Est. time:** 3–4 hours

---

## 2.1 What is Amazon EMR?

**EMR** stands for **Elastic MapReduce** — a name from 2009 that's now misleading. Today EMR is a managed platform for running distributed data processing frameworks, primarily Apache Spark, but also Hive, Presto/Trino, HBase, and others.

The short version: **EMR handles the infrastructure so you can focus on your Spark code.**

Without EMR, running Spark at scale means:
- Provisioning EC2 instances
- Installing Java, Scala, Python, Spark
- Configuring YARN or another cluster manager
- Setting up HDFS or S3 integration
- Configuring network security (VPC, security groups, IAM)
- Managing Spark upgrades
- Setting up monitoring and logging
- Handling node failures and replacements
- Building auto-scaling logic

EMR does all of this. You specify cluster size, Spark version, and your job. AWS handles the rest.

---

## 2.2 EMR Deployment Models

AWS offers three ways to run EMR. Understanding the tradeoffs is critical because the choice affects architecture, cost, and operational burden.

```
EMR Deployment Options:
┌────────────────────┬────────────────────┬────────────────────┐
│   EMR on EC2       │   EMR on EKS       │  EMR Serverless    │
│                    │                    │                    │
│ Traditional        │ Run Spark inside   │ No cluster         │
│ cluster model.     │ Kubernetes pods.   │ management at all. │
│ Max control.       │ Multi-tenant.      │ Auto-scales.       │
│ Full config.       │ Container-native.  │ Pay per use.       │
└────────────────────┴────────────────────┴────────────────────┘
```

### EMR on EC2 — The Traditional Model

You provision a cluster of EC2 instances. AWS installs Spark, YARN, and supporting services. You SSH in if needed, submit jobs, and terminate the cluster when done.

**Architecture:**
```
                    ┌─────────────────────┐
                    │    Master Node       │
                    │  - YARN ResourceMgr  │
                    │  - Spark Driver      │
                    │  - Spark History Svr │
                    │  - Hive Metastore    │
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
    ┌─────────▼──────┐ ┌───────▼───────┐ ┌─────▼──────────┐
    │   Core Node 1  │ │  Core Node 2  │ │  Core Node 3   │
    │  - YARN NodeMgr│ │ - YARN NodeMgr│ │ - YARN NodeMgr │
    │  - Executors   │ │ - Executors   │ │ - Executors    │
    │  - HDFS DataN. │ │ - HDFS DataN. │ │ - HDFS DataN.  │
    └────────────────┘ └───────────────┘ └────────────────┘
              │                │                │
    ┌─────────▼──────┐ ┌───────▼───────┐
    │  Task Node 1   │ │  Task Node 2  │
    │  (Spot, no     │ │  (Spot, no    │
    │   HDFS)        │ │   HDFS)       │
    └────────────────┘ └───────────────┘
```

**Node types:**
- **Master (Primary):** Runs YARN ResourceManager, Spark driver, cluster coordination. One per cluster. Don't run on Spot — if it dies, your cluster dies.
- **Core:** Run YARN NodeManager + executors. Also run HDFS DataNode (for HDFS users). Should be On-Demand for stability.
- **Task:** Run YARN NodeManager + executors only. No HDFS. Safe for Spot instances — killing them doesn't lose data.

**Best for:**
- Jobs that need specific EC2 instance types
- Custom software installations alongside Spark
- Long-running clusters with sustained workload (> 70% cluster utilization)
- Workloads using Spot instances for cost savings
- Maximum control and configurability

### EMR Serverless — The Modern Default

No cluster to manage. You create an **EMR Serverless Application**, submit jobs to it, and AWS automatically provisions, scales, and terminates compute as needed. You pay only for vCPU-seconds and GB-seconds consumed.

```
You Submit Job
      │
      ▼
EMR Serverless Application
      │
      ├── Spins up pre-initialized workers (warm pool)
      ├── Scales workers up as job demands
      ├── Scales workers down when idle
      └── No cluster visible to you
```

**Best for:**
- Intermittent workloads (scheduled batch jobs, periodic pipelines)
- Teams that don't want to manage cluster configuration
- Variable workloads with unpredictable resource needs
- Cluster utilization < 70% (vs On-Demand) or < 50% (vs Savings Plans)

**Not best for:**
- Very short jobs (< 1 minute) — warm pool startup adds latency
- Workloads that benefit heavily from Spot pricing
- Jobs needing custom AMIs or software installation

### EMR on EKS — The Container Model

Run Spark inside Kubernetes pods on Amazon EKS. Useful for organizations already invested in Kubernetes for orchestration.

**Best for:**
- Multi-tenant environments where Spark shares a K8s cluster with other services
- Container-native workflows
- Teams with strong Kubernetes expertise

**Generally not recommended for beginners** — adds Kubernetes complexity on top of Spark complexity.

---

## 2.3 EMR on EC2: Cluster Lifecycle

### Cluster types: Long-running vs Transient

**Long-running cluster:**
```
Create cluster → Submit jobs all day → Keep running → Pay hourly
```
Use when: Multiple jobs run continuously, cluster setup time is expensive, you need a persistent Hive Metastore on the cluster.

**Transient (ephemeral) cluster:**
```
Create cluster → Submit one job → Auto-terminate → Pay only for job duration
```
Use when: You have a single periodic job, want to pay only for compute time, or want clean state for each run.

**For periodic batch pipelines:** Transient clusters are usually correct. Create a cluster, run your job, terminate. Repeat on schedule. This avoids paying for an idle cluster between runs.

### Cluster creation example (AWS CLI)

```bash
aws emr create-cluster \
  --name "daily-pipeline-2024-01-15" \
  --release-label emr-7.2.0 \
  --applications Name=Spark \
  --instance-groups '[
    {
      "InstanceRole": "MASTER",
      "InstanceType": "m5.xlarge",
      "InstanceCount": 1
    },
    {
      "InstanceRole": "CORE",
      "InstanceType": "r5.4xlarge",
      "InstanceCount": 4
    },
    {
      "InstanceRole": "TASK",
      "InstanceType": "r5.4xlarge",
      "InstanceCount": 8,
      "Market": "SPOT",
      "BidPrice": "OnDemandPrice"
    }
  ]' \
  --log-uri "s3://my-emr-logs/cluster-logs/" \
  --service-role "EMR_DefaultRole" \
  --ec2-attributes '{"InstanceProfile": "EMR_EC2_DefaultRole"}' \
  --auto-terminate
```

### Job submission with spark-submit

```bash
# Submit from the cluster master node or via EMR step
spark-submit \
  --deploy-mode cluster \
  --master yarn \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 20 \
  --conf spark.executor.memoryOverhead=2g \
  --conf spark.sql.shuffle.partitions=400 \
  --py-files s3://my-bucket/dependencies.zip \
  s3://my-bucket/jobs/daily_pipeline.py \
  --date 2024-01-15 \
  --input s3://my-bucket/raw/ \
  --output s3://my-bucket/processed/
```

### EMR Steps

Rather than SSHing to submit jobs manually, use **EMR Steps** — jobs submitted through the EMR API that run in sequence:

```python
import boto3

emr = boto3.client('emr', region_name='us-east-1')

# Add a step to an existing cluster
response = emr.add_job_flow_steps(
    JobFlowId='j-XXXXXXXXXXXXX',
    Steps=[{
        'Name': 'Daily Pipeline',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--executor-memory', '8g',
                '--executor-cores', '4',
                's3://my-bucket/jobs/daily_pipeline.py',
                '--date', '2024-01-15'
            ]
        }
    }]
)
```

---

## 2.4 EMR Versions and Spark Versions

EMR uses **release labels** like `emr-7.2.0` that bundle specific versions of Spark, Hadoop, and other components.

**Current guidance (2024–2025):**
- Use **EMR 7.x** for new work — bundles Spark 3.5.x, Java 17
- Use **EMR 6.x** if you need Spark 3.3/3.4 compatibility with existing code

```bash
# List available EMR releases
aws emr list-release-labels
```

Key Spark versions you'll encounter:
- **Spark 3.5** (current stable) — use for new work
- **Spark 3.3/3.4** — still widely deployed, very stable
- **Spark 2.x** — legacy, avoid for new work

**Important:** Always pin your EMR release label in production. Upgrades can break pipelines in subtle ways.

---

## 2.5 IAM Roles and Security

EMR requires specific IAM roles. Understand these — misconfigured IAM is a common source of failures.

### Required roles

**EMR Service Role (`EMR_DefaultRole`):**
- Allows EMR control plane to manage EC2 instances, networks, etc.
- Attached to the EMR service, not to the cluster nodes

**EC2 Instance Profile (`EMR_EC2_DefaultRole`):**
- Allows cluster nodes (EC2 instances) to access AWS services
- **This is where you add S3 bucket access, DynamoDB access, etc.**
- Attached to the EC2 instances in the cluster

```json
// Example EC2 Instance Profile Policy — S3 access
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::my-data-bucket",
        "arn:aws:s3:::my-data-bucket/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["dynamodb:PutItem", "dynamodb:BatchWriteItem"],
      "Resource": "arn:aws:dynamodb:us-east-1:*:table/my-table"
    }
  ]
}
```

### S3 access patterns

Your Spark code running on EMR nodes accesses S3 via the **EMRFS** (EMR File System). EMRFS uses the EC2 Instance Profile credentials automatically.

```python
# In your PySpark code, just use the S3 path — EMRFS handles auth
df = spark.read.parquet("s3://my-bucket/data/")
```

The `s3://` scheme on EMR uses EMRFS. Don't use `s3a://` on EMR — that's for non-EMR Spark. Using `s3a://` on EMR will work in some cases but skips EMRFS's optimizations and commit protocol.

---

## 2.6 EMR Serverless: How to Use It

### Setup

```python
import boto3

emr_serverless = boto3.client('emr-serverless', region_name='us-east-1')

# Create an application (one-time setup)
response = emr_serverless.create_application(
    name='my-spark-app',
    type='SPARK',
    releaseLabel='emr-7.2.0',
    initialCapacity={
        'DRIVER': {
            'workerCount': 1,
            'workerConfiguration': {
                'cpu': '2vCPU',
                'memory': '4GB'
            }
        },
        'EXECUTOR': {
            'workerCount': 5,
            'workerConfiguration': {
                'cpu': '4vCPU',
                'memory': '16GB'
            }
        }
    },
    maximumCapacity={
        'cpu': '200vCPU',
        'memory': '800GB'
    }
)
application_id = response['applicationId']
```

### Submitting a job

```python
# Submit a Spark job
response = emr_serverless.start_job_run(
    applicationId=application_id,
    executionRoleArn='arn:aws:iam::123456789:role/MyEMRServerlessRole',
    jobDriver={
        'sparkSubmit': {
            'entryPoint': 's3://my-bucket/jobs/daily_pipeline.py',
            'entryPointArguments': ['--date', '2024-01-15'],
            'sparkSubmitParameters': '--conf spark.executor.cores=4 --conf spark.executor.memory=16g'
        }
    },
    configurationOverrides={
        'monitoringConfiguration': {
            's3MonitoringConfiguration': {
                'logUri': 's3://my-emr-logs/serverless/'
            }
        }
    }
)
job_run_id = response['jobRunId']
```

### Monitoring job status

```python
import time

while True:
    response = emr_serverless.get_job_run(
        applicationId=application_id,
        jobRunId=job_run_id
    )
    state = response['jobRun']['state']
    print(f"Job state: {state}")

    if state in ['SUCCESS', 'FAILED', 'CANCELLED']:
        break
    time.sleep(30)
```

---

## 2.7 Spot Instances on EMR

Spot instances are unused EC2 capacity that AWS sells at a discount (60–90% off On-Demand). The catch: AWS can reclaim them with a 2-minute notice.

### When Spot instances are safe on EMR

✅ **Task nodes** — No HDFS, no shuffle data ownership. Spark can reschedule their tasks.
✅ **Core nodes in YARN** — Manageable with careful configuration, but risky
✅ **When your job is idempotent** — If it fails, just re-run it

❌ **Master node** — Never Spot. If it dies, cluster dies.
❌ **Core nodes if you use HDFS** — HDFS data lives on core nodes; losing them loses data

### Instance fleets vs Instance groups

**Instance groups:** Fixed instance type, Spot or On-Demand.

**Instance fleets:** Specify multiple instance types; EMR picks whichever Spot pool has available capacity. Much better Spot availability.

```json
// Instance fleet configuration — better Spot availability
{
  "InstanceFleetType": "TASK",
  "TargetOnDemandCapacity": 0,
  "TargetSpotCapacity": 40,
  "InstanceTypeConfigs": [
    {"InstanceType": "r5.4xlarge"},
    {"InstanceType": "r5a.4xlarge"},
    {"InstanceType": "r5d.4xlarge"},
    {"InstanceType": "r4.4xlarge"}
  ],
  "LaunchSpecifications": {
    "SpotSpecification": {
      "TimeoutDurationMinutes": 10,
      "TimeoutAction": "SWITCH_TO_ON_DEMAND"
    }
  }
}
```

**Spot tip:** Set `TimeoutAction: SWITCH_TO_ON_DEMAND` so that if Spot isn't available, EMR falls back to On-Demand rather than failing cluster creation.

---

## 2.8 Monitoring EMR Jobs

### CloudWatch Metrics

EMR automatically publishes metrics to CloudWatch:

| Metric | What it tells you |
|--------|-------------------|
| `YARNMemoryAvailablePcnt` | % of YARN memory unused — low = cluster is full |
| `ContainerPendingRatio` | Ratio of pending containers to running — high = cluster too small |
| `HDFSUtilization` | HDFS disk usage (if using HDFS) |
| `IsIdle` | Whether cluster has been idle for 5+ minutes |
| `AppsCompleted` | Count of completed YARN apps |
| `AppsFailed` | Count of failed YARN apps |

### Spark History Server

Every EMR cluster runs a **Spark History Server** — a web UI that shows completed job execution details: stages, tasks, shuffle reads/writes, task duration distributions, executor metrics.

Access it via:
1. SSH tunnel to master node + port 18080
2. EMR console → Cluster → Application UIs → Spark History Server (requires AWS credentials)

This is your primary debugging tool for performance issues. When a job is slow, the History Server shows you exactly which stage and which task is the bottleneck.

### CloudWatch Logs

With proper log configuration, Spark logs (both driver and executor) stream to CloudWatch Logs. This is invaluable for debugging failures.

```bash
# In your cluster config, set log URI
--log-uri s3://my-emr-logs/clusters/

# Logs are also available at:
s3://my-emr-logs/clusters/<cluster-id>/steps/<step-id>/stderr
s3://my-emr-logs/clusters/<cluster-id>/steps/<step-id>/stdout
s3://my-emr-logs/clusters/<cluster-id>/containers/  (YARN container logs)
```

---

## 2.9 EMR vs The Alternatives

When should you use EMR? When should you not?

### Use EMR when:
- Large-scale batch Spark jobs (100s of GBs to PBs)
- You need fine control over cluster configuration
- Cost optimization is important (Spot instances)
- You're already on AWS
- You process data from S3 (EMRFS integration is seamless)

### Don't use EMR when:
- **Data volume < 50 GB:** Just use a single powerful EC2 instance with local Spark. Much simpler.
- **Streaming real-time:** Consider Amazon Kinesis Data Analytics or Flink on MSK
- **SQL analytics only:** Consider Amazon Athena (serverless Presto, much simpler, pay per query)
- **Truly simple, small-scale ETL (< 100 GB, no skew, no custom tuning):** Consider AWS Glue — but read the cost comparison below first
- **Already on GCP/Azure:** BigQuery or Synapse Analytics might be better fits

### EMR vs AWS Glue

| | EMR | AWS Glue |
|-|-----|----------|
| Complexity | Higher — you manage config | Lower — fully managed |
| Control | Full — executor tuning, AQE, skew, custom libs | Limited — DPU sizing only |
| Custom code | Any Python/Scala/R, any library | Python/Scala, restricted environment |
| Cost at scale | **Much cheaper** — Spot = 80%+ less than Glue DPUs | **Expensive** — $0.44/DPU-hr adds up fast |
| Cost at small scale | Over-provisioned clusters waste money | Reasonable |
| JSON parsing tuning | Full control | None |
| Skew handling | AQE skew join, salting | Not configurable |
| Spark version | Latest (EMR tracks Spark releases) | Lags 1–2 versions behind |
| Best for | Any serious production pipeline above a few hundred GB | Quick-and-dirty small ETL jobs |

**Be direct about the cost trap.** At 1 TB per run, 6 runs/day:

```
Glue (100 DPUs × $0.44 × 1hr):     $44/run  →  $264/day  →  ~$8,000/month
EMR Serverless (equivalent):        $30/run  →  $180/day  →  ~$5,400/month
EMR on EC2 with Spot:               ~$8/run  →  ~$48/day  →  ~$1,450/month
```

Glue's "simplicity" costs roughly 5× more than EMR Spot at this scale. At PB-scale daily workloads, that gap is tens of thousands of dollars per month.

**When Glue is genuinely the right answer:**
- Dataset < 100 GB per job
- No custom executor tuning needed (no skew, no heavy JSON parsing, no broadcast joins)
- Team has zero infrastructure experience and wants zero ops burden
- Ad-hoc or exploratory jobs, not production SLA-bound pipelines

**For anything that matches your real workload** — PB-scale, JSON parsing, aggregations, periodic windows, cost sensitivity — **EMR is the correct choice**, full stop. EMR Serverless closes the operational gap if you want managed infrastructure without the DPU pricing penalty.

---

## 2.10 Auto-Scaling on EMR

EMR supports two forms of auto-scaling:

### Managed Scaling (recommended)

AWS automatically scales your cluster based on YARN metrics. Set minimum and maximum:

```json
{
  "ComputeLimits": {
    "UnitType": "Instances",
    "MinimumCapacityUnits": 2,
    "MaximumCapacityUnits": 20,
    "MaximumOnDemandCapacityUnits": 6
  }
}
```

EMR Managed Scaling considers YARN memory metrics, pending containers, and — importantly — the location of shuffle data on specific nodes before deciding which nodes to decommission. This prevents data loss during scale-down.

### Custom Auto-Scaling (legacy, avoid for new work)

Define scale-out/scale-in rules based on CloudWatch metrics. More control but more complex. Managed Scaling replaced this for most use cases.

---

## 2.11 EMR on EC2 vs EMR Serverless — Head-to-Head

The two models you'll actually choose between for production batch pipelines. EKS is excluded here — it adds Kubernetes complexity with no advantage over Serverless for most teams.

### Architecture difference in one diagram

```
EMR on EC2 (you see the machines)          EMR Serverless (you don't)
─────────────────────────────────          ──────────────────────────
You: create-cluster (5–10 min)             You: start-job-run (30–120 sec)
       │                                          │
       ▼                                          ▼
┌──────────────────────┐               ┌──────────────────────────┐
│  Master node (yours) │               │  AWS-managed compute     │
│  Core nodes  (yours) │               │  pool — invisible to you │
│  Task nodes  (yours) │               │                          │
│  YARN running        │               │  Workers spin up, run    │
│  Billed per hour     │               │  your job, scale down    │
└──────────────────────┘               │  Billed per second       │
                                       └──────────────────────────┘
```

### Full comparison

| Dimension | EMR on EC2 | EMR Serverless |
|-----------|-----------|----------------|
| **Startup time** | 5–10 min (cluster bootstrap) | 30–120 sec (warm pool) |
| **Billing unit** | Per-hour per instance (even when idle) | Per-second of actual vCPU + GB used |
| **Spot instances** | ✅ Yes — 60–90% cheaper than On-Demand | ❌ No Spot option |
| **Instance type control** | ✅ Exact instance (r5.4xlarge, c6g, etc.) | ❌ Only vCPU + GB sizing |
| **Max executor config** | Full YARN: cores, memory, overhead, GC flags | sparkSubmitParameters only |
| **Custom AMI / bootstrap** | ✅ Yes — install anything at cluster start | ❌ No (use custom images or `--py-files`) |
| **SSH / direct access** | ✅ SSH to master, YARN UI, Spark UI live | ❌ No SSH — logs only via CloudWatch |
| **Spark History Server** | ✅ Always running on master node | ✅ Available via EMR console |
| **Managed Scaling** | ✅ YARN-aware autoscale (Managed Scaling) | ✅ Automatic (no config needed) |
| **Cluster utilization crossover** | Cheaper than Serverless above ~70% utilization | Cheaper than On-Demand below ~70% |
| **Cold start penalty** | High (full cluster boot every run) | Low (warm pool pre-initialises workers) |
| **Ops burden** | High — you size, configure, and monitor the cluster | Low — AWS handles everything |
| **Debugging failed jobs** | SSH + YARN logs + Spark History Server | CloudWatch Logs + EMR console only |
| **Best fit** | High-throughput, cost-critical, tuning-heavy workloads | Periodic batch, variable load, teams avoiding infra ops |

---

### Cost model side by side

**EMR on EC2** bills for the full cluster lifetime, whether tasks are running or not:

```
Cluster: 1 master (m5.xlarge) + 10 core (r5.4xlarge On-Demand) + 15 task (r5.4xlarge Spot ~70% disc.)

Per hour:
  Master:     1  × $0.19/hr              =   $0.19
  Core:      10  × $1.01/hr              =  $10.10
  Task (Spot):15 × $1.01 × 0.30          =   $4.55
  EMR markup: ~25% on EC2 cost           =   $3.71
  ─────────────────────────────────────────────────
  Total while running:                   ~$18.55/hr

  Job runs for 1 hr, cluster up 1.25 hr (boot + shutdown):
  Cost per run: ~$23
```

**EMR Serverless** bills only for vCPU-seconds and GB-seconds tasks actually consume:

```
Equivalent job (400 vCPU, 1,600 GB peak, ~1 hr actual compute):
  vCPU:   400 × $0.052224/hr × 1hr  =  $20.89
  Memory: 1600 × $0.0057785/hr × 1hr =  $9.25
  ─────────────────────────────────────────────
  Cost per run:                      ~$30.14

  No idle charge. No startup charge.
```

**Conclusion for a 1-hour job running 6×/day:**

```
                     Per run    Per day    Per month
EMR on EC2 (Spot):    ~$23       ~$138      ~$4,150
EMR Serverless:       ~$30       ~$180      ~$5,400
EMR on EC2 (OD):      ~$62       ~$372     ~$11,200
```

EMR on EC2 with Spot is cheapest at consistent high load. Serverless wins when jobs are short, infrequent, or cluster utilization is low (you avoid paying for idle).

---

### Operational difference that actually matters day-to-day

**On EC2 you are responsible for:**
- Choosing the right instance type (wrong choice = wasted money or OOM)
- Setting `--executor-memory`, `--executor-cores`, `memoryOverhead` correctly
- Deciding how many core vs task nodes
- Monitoring YARN memory pressure and adjusting cluster size
- Ensuring the master node doesn't get OOM (it's also running your driver)

**On Serverless you specify:**
```python
'sparkSubmitParameters': (
    '--conf spark.executor.cores=4 '
    '--conf spark.executor.memory=16g '
    '--conf spark.executor.memoryOverhead=4g'
)
```
AWS handles node count, instance placement, and scaling. You still tune executor size but not cluster topology.

**The debugging gap.** On a failed EC2 cluster job you can:
- SSH to master and tail driver logs in real time
- Open the YARN UI to see which container died
- Open Spark History Server at `master:18080`

On a failed Serverless job you must:
- Wait for logs to propagate to CloudWatch (30–60 sec delay)
- Find the right log stream across driver + executor streams
- No live UI during the job (Spark UI is not exposed)

For iterative debugging during development, EC2 is significantly faster to work with. For stable production jobs where you rarely need to inspect internals, Serverless is fine.

---

### Decision rule for your workload

```
                        Is the job > 30 min?
                        AND runs > 4×/day?
                        AND team can manage Spark config?
                               │
                    ┌──────────┴──────────┐
                   YES                    NO
                    │                     │
          Do you need Spot         Is ops burden a
          (60-90% savings)?        hard constraint?
                    │                     │
              ┌─────┴──────┐         ┌────┴─────┐
             YES            NO       YES         NO
              │              │        │           │
         EMR on EC2    EMR on EC2  EMR          Either;
         with Spot     On-Demand   Serverless   start with
                       or Server-              Serverless
                       less
```

**For a PB-scale, 4-hour or 24-hour window job running multiple times per day:**
- If cost is the top constraint → **EMR on EC2 with Spot task nodes**
- If operational simplicity is the top constraint → **EMR Serverless**
- Both are correct choices. The gap is roughly 25–30% cost difference, not an order of magnitude like Glue.

---

## 2.12 Module Summary

You've covered:
- **What EMR is** — managed Spark/Hadoop platform, handles all infrastructure
- **Three deployment models** — EC2, EKS, Serverless
- **Cluster architecture** — Master, Core, Task nodes
- **Transient vs long-running clusters** — transient is right for periodic jobs
- **IAM roles** — Service Role vs EC2 Instance Profile
- **EMRFS** — the S3 connector, always use `s3://` on EMR (not `s3a://`)
- **Spot instances** — 60–90% cheaper, safe on task nodes
- **Monitoring** — CloudWatch, Spark History Server, S3 logs
- **EC2 vs Serverless** — Spot on EC2 is cheapest at scale; Serverless removes ops burden at ~25–30% cost premium vs EC2 Spot

### ✅ Self-check

1. What is the difference between Core and Task nodes?
2. Why should you never run the master node on a Spot instance?
3. What is the billing model difference between EMR on EC2 and EMR Serverless?
4. You have a 2-hour Spark job that runs 8 times per day and your team wants to avoid managing clusters. Which EMR model do you choose and why?
5. What IAM role controls your cluster's access to S3?
6. What is EMRFS and why does EMR use `s3://` instead of `s3a://`?
7. What is the main debugging disadvantage of EMR Serverless?
8. At what utilization level does EMR Serverless become cheaper than EMR on EC2 On-Demand?

---

*Next: `03-spark-on-s3.md` →*
