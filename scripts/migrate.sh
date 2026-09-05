#!/bin/bash
# Rsync AI - Database Migration Script
# Runs all SQL migrations in order

set -e

echo "📊 Running Database Migrations"
echo "==============================="

# Database connection details
DB_HOST="${POSTGRES_HOST:-localhost}"
DB_PORT="${POSTGRES_PORT:-5432}"
DB_NAME="${POSTGRES_DB:-rsyncdb}"
DB_USER="${POSTGRES_USER:-rsync}"
export PGPASSWORD="${POSTGRES_PASSWORD:-rsyncpass}"

# Wait for PostgreSQL to be ready
echo ""
echo "⏳ Waiting for PostgreSQL..."
MAX_RETRIES=30
RETRY_COUNT=0

while ! docker-compose exec -T postgres pg_isready -h localhost -U "$DB_USER" > /dev/null 2>&1; do
  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
    echo "❌ PostgreSQL did not become ready in time"
    exit 1
  fi
  echo "  Waiting... ($RETRY_COUNT/$MAX_RETRIES)"
  sleep 2
done

echo "✅ PostgreSQL is ready"

# Function to run a migration file
run_migration() {
  local file=$1
  local filename=$(basename "$file")
  
  echo ""
  echo "📄 Running migration: $filename"
  
  if docker-compose exec -T postgres psql -U "$DB_USER" -d "$DB_NAME" -f - < "$file" > /dev/null 2>&1; then
    echo "   ✅ $filename completed"
  else
    echo "   ❌ $filename failed"
    return 1
  fi
}

# Run API Gateway migrations
echo ""
echo "🔧 API Gateway Migrations"
echo "-------------------------"

if [ -d "api-gateway/migrations" ]; then
  for file in api-gateway/migrations/*.sql; do
    if [ -f "$file" ]; then
      run_migration "$file"
    fi
  done
else
  echo "⚠️  No API Gateway migrations found"
fi

# Run Orchestrator migrations (if they exist)
echo ""
echo "🔧 Orchestrator Migrations"
echo "--------------------------"

if [ -d "backend-orchestrator/internal/db/migrations" ]; then
  for file in backend-orchestrator/internal/db/migrations/*.sql; do
    if [ -f "$file" ]; then
      run_migration "$file"
    fi
  done
else
  echo "ℹ️  No Orchestrator migrations found"
fi

# Verify migrations
echo ""
echo "🔍 Verifying Database State"
echo "---------------------------"

# Check tables exist
echo ""
echo "📋 Tables:"
docker-compose exec -T postgres psql -U "$DB_USER" -d "$DB_NAME" -c "\dt" 2>/dev/null | grep -E "users|sessions|connections|pipelines|executions" || echo "⚠️  Some tables may not exist yet"

# Check users
echo ""
echo "👥 Users:"
USER_COUNT=$(docker-compose exec -T postgres psql -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM users" 2>/dev/null | tr -d ' \n')
if [ "$USER_COUNT" -gt 0 ]; then
  echo "   ✅ $USER_COUNT user(s) found"
  docker-compose exec -T postgres psql -U "$DB_USER" -d "$DB_NAME" -c "SELECT email, role FROM users" 2>/dev/null
else
  echo "   ⚠️  No users found"
fi

echo ""
echo "✅ All Migrations Complete!"
echo ""

unset PGPASSWORD
