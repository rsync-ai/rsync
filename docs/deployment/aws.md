# AWS Production Deployment

Two-phase approach:
- **Phase 1 (MVP)** — Single EC2 box + docker-compose + ALB. Fast to set up, ~$290/month.
- **Phase 2 (Production)** — ECS Fargate + managed AWS services. Auto-scaling, ~$685/month.

For TLS/HTTPS setup, see [aws-acm.md](aws-acm.md).
For CI/CD automation, see [ci-cd.md](ci-cd.md).

---

## Phase 1 — EC2 MVP

### 1.1 EC2 Instance

| Setting | Value |
|---|---|
| Type | `t3.2xlarge` (8 vCPU, 32 GB RAM) — minimum for full stack |
| AMI | Amazon Linux 2023 or Ubuntu 22.04 |
| Storage | 200 GB gp3 |
| Region | `us-east-1` (cheapest) |
| Elastic IP | Yes — assign a fixed public IP |

**Security group** (apply to EC2):
| Type | Port | Source |
|---|---|---|
| SSH | 22 | Your IP only |
| Custom TCP | 80 | ALB security group |
| Custom TCP | 443 | ALB security group |
| All egress | All | `0.0.0.0/0` |

Do NOT expose application ports (5001, 3000, 8081) directly to the internet. All public traffic goes through the ALB.

### 1.2 Install Docker

```bash
# Amazon Linux 2023
sudo dnf install -y docker
sudo systemctl enable docker && sudo systemctl start docker
sudo usermod -aG docker ec2-user

# Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
  -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

### 1.3 Application Load Balancer

Create ALB in same VPC as EC2:

**Listeners**:
- HTTP `:80` → Redirect to HTTPS `:443` (status 301)
- HTTPS `:443` → Default forward to `rsync-frontend` target group

**Target groups**:
| Name | Protocol | Port | Health check |
|---|---|---|---|
| `rsync-frontend` | HTTP | 3000 | `GET /` → 200 |
| `rsync-api` | HTTP | 5001 | `GET /health` → 200 |

**HTTPS listener rules** (evaluated top to bottom):
| Priority | Condition | Action |
|---|---|---|
| 1 | Path is `/api/*` OR `/ws` OR `/oauth/callback/*` | Forward to `rsync-api` |
| 2 | Default | Forward to `rsync-frontend` |

**HTTPS certificate**: Attach ACM certificate — see [aws-acm.md](aws-acm.md).

### 1.4 Route 53

```
Hosted zone: yourdomain.com
Record: A, yourdomain.com → Alias → ALB DNS name
Record: A, www.yourdomain.com → Alias → ALB DNS name
```

### 1.5 Environment file on EC2

```bash
git clone https://github.com/<org>/rsync-ai /opt/rsync-ai
cd /opt/rsync-ai
cp .env.prod.example .env.prod
nano .env.prod
```

Required values in `.env.prod`:

```bash
DOMAIN=yourdomain.com

# Generate with: openssl rand -base64 32
# CRITICAL: Back up ENCRYPTION_KEY — losing it breaks all stored credentials
ENCRYPTION_KEY=<generated>
JWT_SECRET=<generated>
POSTGRES_PASSWORD=<generated>
REDIS_PASSWORD=<generated>

OPENAI_API_KEY=sk-...

GITHUB_CLIENT_ID=<from GitHub OAuth app>
GITHUB_CLIENT_SECRET=<from GitHub OAuth app>
GOOGLE_CLIENT_ID=<from Google Cloud Console>
GOOGLE_CLIENT_SECRET=<from Google Cloud Console>

RSYNC_ADMIN_EMAILS=your@email.com
ACME_EMAIL=your@email.com

NEXT_PUBLIC_API_URL=https://yourdomain.com
```

### 1.6 OAuth callback URLs

Update OAuth app settings to use the production domain:

**GitHub OAuth App** (github.com → Settings → Developer settings → OAuth Apps):
- Homepage URL: `https://yourdomain.com`
- Callback URL: `https://yourdomain.com/oauth/callback/github`

**Google OAuth** (console.cloud.google.com → APIs → Credentials):
- Authorized redirect URI: `https://yourdomain.com/oauth/callback/google`

### 1.7 Deploy

Since ALB handles TLS, disable the built-in Traefik in `docker-compose.prod.yml`:

```yaml
# In docker-compose.prod.yml or an override file, disable Traefik:
  traefik:
    profiles: ["traefik"]  # won't start by default
```

Then start the stack:

```bash
cd /opt/rsync-ai
docker compose \
  -f docker-compose.yml \
  -f docker-compose.prod.yml \
  --env-file .env.prod \
  up -d
```

### 1.8 Verify

```bash
curl https://yourdomain.com/health
curl https://yourdomain.com/api/v1/health
docker compose logs api-gateway | grep -i migration
```

---

## Phase 2 — ECS Fargate + Managed Services

### 2.1 Replace docker-compose with AWS managed services

| docker-compose service | AWS replacement |
|---|---|
| `postgres:16-alpine` | RDS PostgreSQL 16 (Multi-AZ) |
| `cp-kafka:7.6.1` | MSK (Amazon Managed Kafka) |
| `redis:7-alpine` | ElastiCache Redis 7 |
| `temporalio/auto-setup` | Temporal Cloud (recommended) |
| Local volumes | EFS for connector bind-mounts |
| Fluent Bit + OTEL | CloudWatch Logs + X-Ray |

### 2.2 VPC Layout

```
VPC: 10.0.0.0/16
  Public subnets:   10.0.1.0/24, 10.0.2.0/24   (ALB, NAT Gateway)
  Private subnets:  10.0.10.0/24, 10.0.11.0/24  (ECS, RDS, MSK, ElastiCache)
```

**Security groups**:
| Group | Inbound |
|---|---|
| `alb-sg` | 80, 443 from `0.0.0.0/0` |
| `app-sg` | 5001, 3000 from `alb-sg` only |
| `internal-sg` | All traffic from `app-sg` |
| `rds-sg` | 5432 from `app-sg` + `internal-sg` |
| `msk-sg` | 9092, 9094 from `internal-sg` |
| `redis-sg` | 6379 from `internal-sg` |

### 2.3 ECR Repositories

```bash
# Create one repo per service
aws ecr create-repository --repository-name rsync-ai/api-gateway
aws ecr create-repository --repository-name rsync-ai/orchestrator
aws ecr create-repository --repository-name rsync-ai/temporal-adapter
aws ecr create-repository --repository-name rsync-ai/llm-service
aws ecr create-repository --repository-name rsync-ai/frontend
```

### 2.4 ECS Cluster + Services

Create cluster: `rsync-ai-prod`

**Task definitions** (approximate sizing):

| Service | CPU units | Memory | Port | Min replicas |
|---|---|---|---|---|
| api-gateway | 1024 (1 vCPU) | 2048 MB | 5001 | 2 (multi-AZ) |
| orchestrator | 2048 | 4096 MB | 8081 | 2 |
| temporal-adapter | 512 | 1024 MB | 8082 | 1 |
| llm-service | 2048 | 4096 MB | 5000 | 1 |
| frontend | 512 | 1024 MB | 3000 | 2 (multi-AZ) |

**Service discovery** (AWS Cloud Map):
- Internal DNS: `rsync.local`
- `api-gateway.rsync.local:5001`
- `orchestrator.rsync.local:8081`
- `llm-service.rsync.local:5000`

### 2.5 Secrets Manager

Store each secret individually:

```bash
aws secretsmanager create-secret --name rsync-ai/prod/encryption-key \
  --secret-string "$(openssl rand -base64 32)"

aws secretsmanager create-secret --name rsync-ai/prod/jwt-secret \
  --secret-string "$(openssl rand -base64 32)"

# Service-to-service auth secret (api-gateway → orchestrator). MUST be injected
# into api-gateway, orchestrator, AND frontend from this SAME value — a mismatch
# (or an empty value) makes the orchestrator fail requirePrincipal closed → 401.
aws secretsmanager create-secret --name rsync-ai/prod/internal-service-secret \
  --secret-string "$(openssl rand -hex 32)"

aws secretsmanager create-secret --name rsync-ai/prod/postgres-password \
  --secret-string "$(openssl rand -base64 24)"

# ... repeat for redis-password, openai-api-key, github-client-secret, google-client-secret
```

In ECS task definitions, inject via `secrets:` section:
```json
"secrets": [
  {
    "name": "ENCRYPTION_KEY",
    "valueFrom": "arn:aws:secretsmanager:us-east-1:ACCOUNT:secret:rsync-ai/prod/encryption-key"
  }
]
```

### 2.6 Database migration in ECS

Migrations run automatically at api-gateway startup (`db.Migrate()` is embedded).
For controlled migration, run as a one-off ECS task before deploying the new service version:

```bash
aws ecs run-task \
  --cluster rsync-ai-prod \
  --task-definition rsync-api-gateway-migrate \
  --overrides '{"containerOverrides":[{"name":"api-gateway","command":["migrate-only"]}]}'
```

---

## Phase 1 Cost Estimate

| Resource | Type | Monthly cost |
|---|---|---|
| EC2 | t3.2xlarge on-demand | ~$240 |
| ALB | 1 ALB + minimal traffic | ~$20 |
| EBS | 200 GB gp3 | ~$18 |
| Route 53 | 1 hosted zone | ~$2 |
| Data transfer | ~100 GB outbound | ~$9 |
| ACM cert | Public cert | $0 |
| **Total** | | **~$290/mo** |

## Phase 2 Cost Estimate

| Resource | Type | Monthly cost |
|---|---|---|
| ECS Fargate | 5 services avg 1 vCPU/2 GB | ~$180 |
| RDS PostgreSQL | db.t3.medium Multi-AZ | ~$120 |
| MSK Kafka | 2× kafka.t3.small | ~$140 |
| ElastiCache Redis | cache.t3.medium | ~$60 |
| Temporal Cloud | Developer plan | ~$100 |
| ALB | | ~$25 |
| ECR + S3 | | ~$20 |
| Secrets Manager | ~20 secrets | ~$8 |
| CloudWatch | | ~$30 |
| Route 53 | | ~$2 |
| **Total** | | **~$685/mo** |

---

## Pre-launch checklist

- [ ] Domain registered + Route 53 hosted zone created
- [ ] ACM certificate issued (`yourdomain.com` + `*.yourdomain.com`)
- [ ] EC2 or ECS running all services healthy
- [ ] ALB: HTTP → HTTPS redirect + ACM cert on HTTPS listener
- [ ] Route 53 A record pointing to ALB
- [ ] All secrets populated (ENCRYPTION_KEY backed up in Secrets Manager)
- [ ] DB migrations ran successfully
- [ ] OAuth apps configured with production callback URL
- [ ] Health checks passing: `curl https://yourdomain.com/health`

## Post-launch monitoring

- CloudWatch alarm: API 5xx rate > 1%
- CloudWatch alarm: ECS CPU > 80%
- RDS automated backups: 7-day retention
- MSK broker storage alarm: 80% threshold
- ACM cert: auto-renews if DNS validation CNAME stays in Route 53
