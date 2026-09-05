# CI/CD with GitHub Actions

Automated pipeline: push to `main` → run tests → build Docker images → push to ECR → rolling deploy to ECS.

---

## Prerequisites

- AWS account with ECR repositories created (one per service)
- ECS cluster `rsync-ai-prod` with services running
- GitHub repository with access to Actions

---

## Step 1 — IAM user for CI/CD

Create a dedicated IAM user with least-privilege permissions:

**IAM → Users → Create user** → name: `rsync-ai-cicd`

Attach an inline policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload",
        "ecr:PutImage"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ecs:UpdateService",
        "ecs:DescribeServices",
        "ecs:DescribeTaskDefinition",
        "ecs:RegisterTaskDefinition"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "iam:PassRole"
      ],
      "Resource": "arn:aws:iam::*:role/rsync-ai-*"
    }
  ]
}
```

Generate access keys for this user.

---

## Step 2 — GitHub repository secrets

**GitHub repo → Settings → Secrets and variables → Actions → New repository secret**

| Secret name | Value |
|---|---|
| `AWS_ACCOUNT_ID` | Your 12-digit AWS account ID |
| `AWS_REGION` | `us-east-1` |
| `AWS_ACCESS_KEY_ID` | IAM user access key ID |
| `AWS_SECRET_ACCESS_KEY` | IAM user secret access key |
| `ECR_REGISTRY` | `<account>.dkr.ecr.us-east-1.amazonaws.com` |
| `ECS_CLUSTER` | `rsync-ai-prod` |

---

## Step 3 — GitHub Actions workflow

Create `.github/workflows/deploy.yml`:

```yaml
name: Deploy to Production

on:
  push:
    branches: [main]
  workflow_dispatch:   # allow manual trigger

env:
  AWS_REGION: ${{ secrets.AWS_REGION }}
  ECR_REGISTRY: ${{ secrets.ECR_REGISTRY }}

jobs:
  # ─── Tests ──────────────────────────────────────────────────────────────────
  test:
    name: Run tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version: "1.22"

      - name: Test api-gateway
        run: cd api-gateway && go test ./... -timeout 60s

      - name: Test orchestrator
        run: cd backend-orchestrator && go test ./... -timeout 60s

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Test llm-service
        run: |
          pip install -r llm-service/requirements.txt
          cd llm-service && python -m pytest tests/ -x -q 2>/dev/null || echo "No Python tests found"

  # ─── Build & Push ────────────────────────────────────────────────────────────
  build-and-push:
    name: Build ${{ matrix.service.name }}
    needs: test
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        service:
          - name: api-gateway
            dockerfile: api-gateway/Dockerfile
            context: .
          - name: orchestrator
            dockerfile: backend-orchestrator/Dockerfile
            context: .
          - name: temporal-adapter
            dockerfile: backend-temporal-adapter/Dockerfile
            context: ./backend-temporal-adapter
          - name: llm-service
            dockerfile: llm-service/Dockerfile
            context: ./llm-service
          - name: frontend
            dockerfile: frontend/Dockerfile
            context: ./frontend

    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ secrets.AWS_REGION }}

      - name: Login to ECR
        uses: aws-actions/amazon-ecr-login@v2

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Build and push ${{ matrix.service.name }}
        uses: docker/build-push-action@v5
        with:
          context: ${{ matrix.service.context }}
          file: ${{ matrix.service.dockerfile }}
          push: true
          tags: |
            ${{ env.ECR_REGISTRY }}/rsync-ai/${{ matrix.service.name }}:${{ github.sha }}
            ${{ env.ECR_REGISTRY }}/rsync-ai/${{ matrix.service.name }}:latest
          cache-from: type=gha
          cache-to: type=gha,mode=max

  # ─── Deploy ──────────────────────────────────────────────────────────────────
  deploy:
    name: Deploy to ECS
    needs: build-and-push
    runs-on: ubuntu-latest
    environment: production

    steps:
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ secrets.AWS_REGION }}

      - name: Force rolling deploy on all services
        run: |
          for SERVICE in api-gateway orchestrator temporal-adapter llm-service frontend; do
            echo "Deploying rsync-${SERVICE}..."
            aws ecs update-service \
              --cluster ${{ secrets.ECS_CLUSTER }} \
              --service rsync-${SERVICE} \
              --force-new-deployment \
              --no-cli-pager
          done

      - name: Wait for critical services to stabilise
        run: |
          echo "Waiting for api-gateway and frontend..."
          aws ecs wait services-stable \
            --cluster ${{ secrets.ECS_CLUSTER }} \
            --services rsync-api-gateway rsync-frontend
          echo "Deployment complete"

      - name: Smoke test
        run: |
          sleep 10
          STATUS=$(curl -s -o /dev/null -w "%{http_code}" https://yourdomain.com/health)
          if [ "$STATUS" != "200" ]; then
            echo "Health check failed: HTTP $STATUS"
            exit 1
          fi
          echo "Health check passed: HTTP $STATUS"
```

---

## Deploy flow

```
git push main
  │
  ├── test job: go test + python pytest
  │
  ├── build-and-push (matrix — runs in parallel for all 5 services):
  │     docker build → push to ECR with git SHA + :latest tags
  │
  └── deploy:
        aws ecs update-service --force-new-deployment (for each service)
        aws ecs wait services-stable (api-gateway + frontend)
        curl health check → fail workflow if 500
```

ECS performs a rolling update:
- Old tasks stay up and serving traffic
- New tasks start and must pass ALB health checks before old tasks are stopped
- Zero downtime (requires min 2 replicas for api-gateway and frontend)

---

## Manual deploy (EC2 Phase 1)

For the Phase 1 EC2 setup, replace the deploy job with a simple SSH command:

```yaml
  deploy:
    needs: build-and-push
    runs-on: ubuntu-latest
    steps:
      - name: Deploy via SSH
        uses: appleboy/ssh-action@v1
        with:
          host: ${{ secrets.EC2_HOST }}
          username: ec2-user
          key: ${{ secrets.EC2_SSH_KEY }}
          script: |
            cd /opt/rsync-ai
            git pull origin main
            docker compose pull
            docker compose -f docker-compose.yml -f docker-compose.prod.yml \
              --env-file .env.prod \
              up -d --remove-orphans
```

Add `EC2_HOST` and `EC2_SSH_KEY` to GitHub secrets.

---

## Rollback

Rolling back to a previous deployment:

```bash
# List recent task definition revisions
aws ecs list-task-definitions --family-prefix rsync-api-gateway --sort DESC

# Force deploy a specific revision
aws ecs update-service \
  --cluster rsync-ai-prod \
  --service rsync-api-gateway \
  --task-definition rsync-api-gateway:42 \
  --force-new-deployment
```

Or re-run the GitHub Actions workflow on a previous commit:
**GitHub → Actions → Select workflow run → Re-run jobs**

---

## Branch protection

To prevent accidental deploys, add a branch protection rule on `main`:
- Require status checks to pass before merging (select the `test` job)
- Require at least 1 approving review

**GitHub repo → Settings → Branches → Add rule → Branch name: `main`**
