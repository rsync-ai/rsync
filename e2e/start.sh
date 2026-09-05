#!/bin/bash
# Quick start script for E2E testing

set -e

echo "=================================================="
echo "🚀 Starting E2E Test Databases"
echo "=================================================="

# Start e2e databases as separate project
echo "Starting mysql-e2e, postgres-e2e, minio..."
docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml up -d mysql-e2e postgres-e2e minio minio-init mock-github

# Wait for services to be healthy
echo ""
echo "⏳ Waiting for services to be healthy..."
sleep 5

echo ""
echo "✅ Verifying MySQL seed data..."
docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml exec -T mysql-e2e \
  mysqladmin ping -h 127.0.0.1 -uroot -prootpassword --silent || echo "⚠️  MySQL not ready yet"

echo ""
echo "✅ Verifying Postgres..."
docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml exec -T postgres-e2e \
  pg_isready -U e2e_user -d e2e_db || echo "⚠️  Postgres not ready yet"

echo ""
echo "=================================================="
echo "✅ E2E Databases are starting!"
echo "=================================================="
echo ""
echo "📍 Connection Details:"
echo "   MySQL:    mysql://e2e_user:e2e_password@localhost:3307/e2e_db"
echo "   Postgres: postgres://e2e_user:e2e_password@localhost:5433/e2e_db"
echo "   MinIO:    http://localhost:9001 (minioadmin/minioadmin)"
echo ""
echo "🧪 To run tests:"
echo "   cd e2e && python3 test_pipeline_full.py"
echo ""
echo "🛑 To stop:"
echo "   docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml down"
echo ""

