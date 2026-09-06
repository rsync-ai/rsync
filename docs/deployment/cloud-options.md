# Cloud Provider Comparison

Choosing the right cloud for rsync-ai depends on where you are in the lifecycle:
demo → MVP → production. This document compares all viable options.

---

## TL;DR Recommendations

| Stage | Provider | Why |
|---|---|---|
| **Demo / prototype** | Oracle Cloud Always Free | 4 OCPU + 24 GB RAM, free forever, no expiry |
| **MVP / early prod** | AWS EC2 t3.2xlarge + ALB | Familiar tooling, easy to scale, ~$290/mo |
| **Scale / production** | AWS ECS Fargate + managed services | Auto-scaling, no servers to manage, ~$685/mo |

---

## Provider Breakdown

### 1. Oracle Cloud (OCI)

**Best for: demo, prototyping, cost-zero long-term**

#### Free Tier structure
Oracle's free tier has two layers:

**Layer 1 — 30-day trial ($300 credit)**
- Full access to all OCI services for 30 days
- $300 credit usable on anything (GPU instances, more RAM, databases, etc.)
- If you do nothing after 30 days: automatically drops to Always Free
- If you upgrade to Pay As You Go: credit applies first, then billed

**Layer 2 — Always Free (no expiry)**
- `VM.Standard.A1.Flex` — **up to 4 OCPU + 24 GB RAM** across all A1 instances (ARM64 Ampere)
- `VM.Standard.E2.1.Micro` × 2 — 1 OCPU, 1 GB RAM each (x86)
- 200 GB block storage total
- 10 GB object storage
- 1 load balancer (10 Mbps)
- Always Free has no 12-month expiry — it's genuinely free indefinitely

#### ARM64 compatibility

<!-- published-platforms: linux/amd64 -->
<!-- The prose below was written against that platform set, which is computed from
     .github/workflows/docker-publish.yml, not asserted here. If the workflow starts
     building another platform, test_published_image_platforms_match_the_docs.py goes
     red and points at this block. -->
> [!WARNING]
> **rsync.ai does not publish `linux/arm64` images.** Every `ghcr.io/rsync-ai/*`
> image is built by `docker-publish.yml` on `runs-on: ubuntu-latest` with no
> `platforms:` key on either `docker/build-push-action` step, so buildx tags each
> one for the runner's own platform and nothing else. All 14 images the
> quickstart compose pulls carry a single `linux/amd64` entry and no arm64 entry.
> Both documented install paths — `install.sh` and the Helm chart — **pull** those
> images rather than building them, so an A1.Flex VM is not a supported target
> today without one of the workarounds below.

This section predates published images and used to read as a green light. It is
not one. What is true, and what is not:

| | Multi-arch `linux/arm64`? |
|---|---|
| `postgres:16-alpine`, `redis:7-alpine`, `confluentinc/cp-kafka:7.6.1`, `temporalio/auto-setup`, `minio/minio` | yes — the third-party dependencies are fine |
| `ghcr.io/rsync-ai/*` (api-gateway, orchestrator, frontend, llm-service-oss, temporal-adapter, and the connector images) | **no** — `linux/amd64` only |

The failure is not a clean one. `docker compose pull` reports
`no matching manifest for linux/arm64/v8 in the manifest list entries`; on
Kubernetes the same gap surfaces as `ImagePullBackOff` with no arm64 candidate.
Worse, if a qemu handler *is* registered the pull succeeds and the containers
then die with `exec format error`, which reads like an application bug.
`install.sh` now checks the daemon's architecture up front and says so before it
writes anything.

Two ways to run on A1.Flex today:

1. **Emulate.** Register the x86-64 binfmt handler, then install normally.
   Expect a substantial slowdown, especially on the Python services:
   ```bash
   docker run --privileged --rm tonistiigi/binfmt --install amd64
   ```
2. **Build natively from source** on the VM, which produces real arm64 images:
   ```bash
   git clone https://github.com/rsync-ai/rsync.git && cd rsync
   docker compose -f docker-compose.yml build
   ```

If you want a $0 VM that needs neither, take the two `VM.Standard.E2.1.Micro`
x86 instances instead — but 1 GB of RAM each is far below the 6 GB the stack
wants, so they suit a single external component, not the whole stack.

#### Cost for demo stack
| Resource | Cost |
|---|---|
| VM.Standard.A1.Flex (4 OCPU, 24 GB) | $0/mo |
| 100 GB block storage | $0/mo |
| Object storage 10 GB | $0/mo |
| **Total** | **$0/mo** |

Only external cost: OpenAI API calls (~$1–5/month for demo traffic). You can eliminate even this by running Ollama on the same VM.

#### Limitations
- No managed Kafka (MSK equivalent) in Always Free
- No managed PostgreSQL (RDS equivalent) in Always Free
- **ARM64 is not a published target** — `ghcr.io/rsync-ai/*` is `linux/amd64` only; emulate or build from source (see above)
- Single VM — no HA for demo purposes

---

### 2. AWS (Amazon Web Services)

**Best for: production, familiar tooling, managed services**

#### Free Tier structure
AWS Free Tier has three categories:

**12-month free (after signup)**
- `t2.micro` / `t3.micro` — 750 hours/month (1 OCPU, 1 GB RAM) — **too small for rsync-ai**
- RDS `db.t3.micro` — 750 hours/month
- S3 — 5 GB standard storage
- ELB — 750 hours

**Always Free**
- Lambda — 1M requests/month
- DynamoDB — 25 GB
- CloudWatch — 10 metrics, 1M API requests
- ECR — 500 MB/month storage

**Free trials (short-term)**
- Fargate — 2-month trial for new accounts

#### Why AWS t2.micro is not enough
The full rsync-ai stack needs at minimum 8 GB RAM (trimmed demo) to 16–32 GB (full stack). A `t2.micro` (1 GB) cannot run even a single service. Use AWS for production Phase 1+ only.

#### Cost estimates

**Phase 1 — EC2 MVP** (~$290/month)
| Resource | Type | Cost |
|---|---|---|
| EC2 | t3.2xlarge (8 vCPU, 32 GB) | ~$240/mo |
| ALB | 1 ALB + minimal LCUs | ~$20/mo |
| EBS | 200 GB gp3 | ~$18/mo |
| Route 53 | 1 hosted zone | ~$2/mo |
| Data transfer | ~100 GB outbound | ~$9/mo |
| ACM certificate | Public cert | $0 |
| **Total** | | **~$290/mo** |

**Phase 2 — ECS Fargate + managed services** (~$685/month)
| Resource | Type | Cost |
|---|---|---|
| ECS Fargate | 5 services avg 1 vCPU / 2 GB | ~$180/mo |
| RDS PostgreSQL | db.t3.medium Multi-AZ | ~$120/mo |
| MSK Kafka | 2× kafka.t3.small | ~$140/mo |
| ElastiCache Redis | cache.t3.medium | ~$60/mo |
| Temporal Cloud | Developer plan | ~$100/mo |
| ALB | | ~$25/mo |
| ECR + S3 | Images + data | ~$20/mo |
| Secrets Manager | ~20 secrets | ~$8/mo |
| CloudWatch | Logs + metrics | ~$30/mo |
| Route 53 | | ~$2/mo |
| **Total** | | **~$685/mo** |

#### AWS regions
Use `us-east-1` (N. Virginia) — cheapest EC2 pricing and closest to most free-tier services.

---

### 3. Google Cloud Platform (GCP)

**Best for: 90-day sprint with generous credit**

#### Free Tier structure

**$300 credit — 90 days**
- Available on signup for new accounts
- Works on all GCP services
- Expires after 90 days regardless of usage
- After expiry: drops to Always Free tier (very limited)

**Always Free (permanent)**
- `e2-micro` VM — 2 vCPU (shared), 1 GB RAM — **too small for rsync-ai**
- 30 GB HDD on e2-micro
- Cloud Storage — 5 GB
- Cloud Run — 2M requests/month

#### Good for rsync-ai demo?
With $300 credit you can run a `n2-standard-4` (4 vCPU, 16 GB) for about 60 days, or `n2-standard-8` (8 vCPU, 32 GB) for ~30 days. After 90 days you lose the VM unless you upgrade to paid.

**Verdict**: Good for a time-boxed sprint but not sustainable without payment after 90 days.

#### GCP-specific notes
- Artifact Registry is the ECR equivalent — $0.10/GB/month
- Cloud SQL (managed Postgres) is comparable to RDS
- Pub/Sub is a Kafka alternative but requires significant rework
- GKE Autopilot can replace ECS Fargate

---

### 4. Azure

**Best for: Microsoft-heavy teams**

#### Free Tier structure
- **$200 credit — 30 days** (new accounts)
- 12-month free services: `B1s` VM (1 vCPU, 1 GB RAM), Azure SQL 250 GB, Blob Storage 5 GB
- Always Free: Azure Functions 1M executions, Cosmos DB 1000 RU/s

**Verdict**: $200/30 days is the least useful option. B1s VM (1 GB RAM) is too small. Not recommended for rsync-ai.

---

### 5. Render / Railway / Fly.io

**Best for: simple Node/Python apps — not suitable for rsync-ai**

These platforms are great for simple web apps but have hard limits that make them unsuitable:

| Platform | Free RAM | Kafka | PostgreSQL | Verdict |
|---|---|---|---|---|
| Render | 512 MB/service | No | 90-day free then $7/mo | Not viable |
| Railway | 512 MB/service | No | $5/mo | Not viable |
| Fly.io | 256 MB/VM | No | $0.15/GB/mo | Not viable |

rsync-ai requires Kafka and ~8 GB RAM minimum — none of these platforms support that on free tiers.

---

### 6. Hetzner Cloud

**Best for: cost-efficient paid hosting in EU/US**

Not free, but extremely cost-effective:
- `CX31` (2 vCPU, 8 GB RAM) — **€5.83/month**
- `CX41` (4 vCPU, 16 GB RAM) — **€11.67/month**
- `CX51` (8 vCPU, 32 GB RAM) — **€21.33/month**

No free tier, but if Oracle Always Free isn't enough for your demo and you want to pay a small amount, Hetzner `CX41` at ~$12/month is the best value anywhere.

---

## Summary Table

| Provider | Free RAM | Duration | ARM64 | Managed Kafka | Managed PG | Verdict |
|---|---|---|---|---|---|---|
| **Oracle Cloud** | **24 GB** | **Forever** | Yes (required) | No | No | **Best for demo** |
| AWS | 1 GB (t2.micro) | 12 months | No | MSK ($140+/mo) | RDS ($60+/mo) | Best for production |
| GCP | 1 GB (e2-micro) | 90 days trial | No | Pub/Sub (different) | Cloud SQL | Good for sprint |
| Azure | 1 GB (B1s) | 30 days trial | No | Event Hubs | Azure SQL | Not recommended |
| Hetzner | N/A (paid) | Paid | No | No | No | Best paid value |
| Render/Railway | 512 MB | Forever | No | No | Limited | Not viable |
