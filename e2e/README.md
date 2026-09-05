# E2E Testing Guide

## Overview
This directory contains **end-to-end tests** that validate the entire rsync-ai pipeline execution flow with **real database connections**.

> **Canonical entrypoint = the gate, not individual scripts.** The merge/CI E2E gate is
> `make e2e-gate` (→ `E2E_BUILD=1 bash e2e/run_gate.sh`) — it brings up the stack, runs the
> batch + CDC correctness suite (the `test_*.{sh,py}` in this directory), self-locks, and
> self-cleans. CI wires it via `.github/workflows/ci.yml`. Run **that**, not the individual
> `test_*.py` by hand — the single-script instructions further down are for debugging one
> case in isolation only.

## Architecture

```
┌───────────────────────────────────────────────┐
│  E2E Test Databases (Docker)                  │
├───────────────────────────────────────────────┤
│  mysql-e2e:3307                               │
│  ├─ big_table (100k rows seeded)             │
│  └─ e2e_db database                           │
│                                               │
│  postgres-e2e:5433                            │
│  ├─ big_table_copy (destination)             │
│  └─ e2e_db database                           │
│                                               │
│  minio:9000/9001                              │
│  ├─ S3-compatible object storage             │
│  └─ e2e-bucket/test-output/                   │
└───────────────────────────────────────────────┘
         ↓ (connected via rsync-ai_default network)
┌───────────────────────────────────────────────┐
│  Main rsync-ai Stack                          │
│  - API Gateway (port 8080)                    │
│  - Orchestrator (port 8081)                   │
│  - Temporal (port 7233)                       │
│  - MCP Connectors (stdio from shared/)        │
└───────────────────────────────────────────────┘
```

## Quick Start

### Step 1: Start E2E Databases

```bash
# From project root
docker compose -f docker-compose.e2e.yml -p e2e-databases up -d

# Verify MySQL is seeded
mysql -h 127.0.0.1 -P 3307 -u e2e_user -pe2e_password -e "SELECT COUNT(*) FROM e2e_db.big_table"
# Expected: 100000

# Verify Postgres is ready
psql -h 127.0.0.1 -p 5433 -U e2e_user -d e2e_db -c "\dt"
# Expected: big_table_copy, test_destination

# Verify MinIO is accessible
curl -s http://localhost:9001  # MinIO console UI (minioadmin/minioadmin)
```

### Step 2: Ensure Main Stack is Running

```bash
docker compose -f docker-compose.yml up -d
```

### Step 3: Run E2E Test Suite

```bash
cd e2e
pip install requests boto3 psycopg2-binary  # Install dependencies
python3 test_pipeline_full.py
```

**Expected Output:**
```
================================================================================
🚀 E2E Pipeline Execution Test Suite
================================================================================

================================================================================
🧪 TEST 1: MySQL → S3 (CSV + Gzip)
================================================================================
✅ Connection 'E2E MySQL Source' created (ID: abc-123)
✅ Connection 'E2E S3 Destination' created (ID: def-456)
✅ Pipeline created (ID: 5ed37c39)
✅ Pipeline 5ed37c39 started
⏳ Stage: intent | Status: processing | Progress: 10%
⏳ Stage: schema_discovery | Status: processing | Progress: 30%
📋 HITL: Selecting tables: ['big_table']
✅ Tables selected: ['big_table']
⏳ Stage: execution | Status: processing | Progress: 80%
✅ Pipeline completed successfully!
📊 Rows processed: 100000
✅ S3 object exists: s3://e2e-bucket/test-output/big_table.csv.gz (15234567 bytes)
✅ S3 row count verified: 100000

================================================================================
🧪 TEST 2: MySQL → Postgres (Direct DB-to-DB)
================================================================================
✅ Connection 'E2E MySQL Source 2' created (ID: ghi-789)
✅ Connection 'E2E Postgres Dest' created (ID: jkl-012)
✅ Pipeline created (ID: 7fa48d12)
✅ Pipeline 7fa48d12 started
⏳ Stage: execution | Status: processing | Progress: 90%
✅ Pipeline completed successfully!
📊 Rows processed: 100000
✅ Postgres row count verified: 100000

================================================================================
📊 TEST SUMMARY
================================================================================
  MySQL → S3: ✅ PASS
  MySQL → Postgres: ✅ PASS

🎯 Result: 2/2 tests passed

🎉 ALL TESTS PASSED! System is production-ready.
```

## Test Files

### 1. `test_pipeline_full.py`
**Purpose**: End-to-end pipeline execution tests  
**What it tests**:
- ✅ Creating connections via API
- ✅ Creating pipelines from natural language
- ✅ Running pipelines and handling HITL (table selection)
- ✅ Data movement (MySQL → S3 and MySQL → Postgres)
- ✅ Row count verification (source vs destination)

**Test Scenarios**:
1. **MySQL → S3 (CSV + Gzip)**: Exports 100k rows to MinIO (S3-compatible)
2. **MySQL → Postgres**: Direct database-to-database transfer

### 2. `mysql/init/001_schema.sql`
Seeds the `big_table` with 100,000 rows (~150MB total) using cross joins.

### 3. `postgres/init/001_schema.sql`
Creates destination tables for testing:
- `big_table_copy`: Receives data from MySQL
- `test_destination`: Generic destination for other tests

## Cleanup

```bash
# Stop and remove e2e databases (preserves volumes for faster restarts)
docker compose -p e2e-databases down

# Full cleanup (removes volumes too)
docker compose -p e2e-databases down -v
```

## Troubleshooting

### Test fails with "Connection refused"
**Cause**: E2E databases not running or not on correct network  
**Fix**:
```bash
docker compose -p e2e-databases up -d
docker network inspect rsync-ai_default  # Verify e2e containers are on this network
```

### Test hangs at "HITL: Selecting tables"
**Cause**: Table selection signal not reaching Temporal workflow  
**Fix**:
- Check Temporal UI: http://localhost:8233
- Check orchestrator logs: `docker logs rsync-ai-orchestrator`
- Verify API Gateway health: `curl http://localhost:8080/health`

### S3 verification fails
**Cause**: MinIO not initialized or bucket missing  
**Fix**:
```bash
docker logs rsync-ai-minio-init  # Check init container logs
docker exec -it rsync-ai-minio mc ls rsync/  # Verify buckets exist
```

### Postgres verification fails
**Cause**: Destination table not created or permission issues  
**Fix**:
```bash
psql -h 127.0.0.1 -p 5433 -U e2e_user -d e2e_db -c "SELECT * FROM big_table_copy LIMIT 5;"
# If table doesn't exist, check postgres-e2e logs
docker logs rsync-ai-postgres-e2e
```

## CI/CD Integration

To run these tests in GitHub Actions:

```yaml
name: E2E Tests
on: [push, pull_request]

jobs:
  e2e:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Start main stack
        run: docker compose up -d
      
      - name: Start e2e databases
        run: docker compose -f docker-compose.e2e.yml up -d
      
      - name: Wait for services
        run: |
          docker compose exec -T mysql-e2e mysqladmin ping -h 127.0.0.1 --wait=30
          docker compose exec -T postgres-e2e pg_isready -U e2e_user --timeout=30
      
      - name: Run e2e tests
        run: |
          pip install requests boto3 psycopg2-binary
          cd e2e && python3 test_pipeline_full.py
      
      - name: Cleanup
        if: always()
        run: |
          docker compose down
          docker compose -f docker-compose.e2e.yml down
```

## Future Enhancements

- [ ] **CDC Pipeline Tests**: Test Debezium + Kafka → Postgres real-time sync
- [ ] **Transformation Tests**: Test filters, column mapping, and data transformations
- [ ] **Multi-table Tests**: Test pipelines with 10+ tables selected
- [ ] **Scheduled Pipeline Tests**: Test cron-triggered execution
- [ ] **Error Scenario Tests**: Test network failures, invalid credentials, schema drift
- [ ] **Performance Benchmarks**: Track throughput (rows/sec) over time
- [ ] **Connector Generation Tests**: Test tool-generator accuracy with real data

---

**Last Updated**: 2025-12-29  
**Maintainer**: Tool Generator Team
