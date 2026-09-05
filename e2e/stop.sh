#!/bin/bash
# Stop e2e test databases

echo "🛑 Stopping E2E Test Databases..."
docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml down --remove-orphans

echo ""
echo "To remove volumes (full cleanup):"
echo "  docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml down -v --remove-orphans"

