#!/bin/bash

# Setup SigNoz for Observability
# Based on: https://signoz.io/docs/install/docker/

set -e

echo "=========================================="
echo "📊 Setting up SigNoz for RSYNC AI"
echo "=========================================="
echo ""

# Navigate to project directory
cd "$(dirname "$0")/.."

# Create signoz directory if it doesn't exist
if [ ! -d "signoz" ]; then
    echo "📦 Cloning SigNoz repository..."
    git clone -b main https://github.com/SigNoz/signoz.git
    echo "✅ SigNoz repository cloned"
else
    echo "✅ SigNoz repository already exists"
fi

# Navigate to deploy directory
cd signoz/deploy

echo ""
echo "🚀 Starting SigNoz..."
echo "This will start:"
echo "  - ClickHouse (data storage)"
echo "  - Query Service (backend)"
echo "  - Frontend UI (typically http://localhost:8080)"
echo "  - OTel Collector (ports 4317, 4318)"
echo ""

cd docker
docker compose -f docker-compose.yaml up -d --remove-orphans

echo ""
echo "⏳ Waiting for services to be healthy (30 seconds)..."
sleep 30

echo ""
echo "📊 Checking SigNoz status..."
docker compose -f docker-compose.yaml ps

echo ""
echo "=========================================="
echo "✅ SigNoz is running!"
echo "=========================================="
echo ""
echo "📝 SigNoz URLs:"
echo "  - UI Dashboard:      http://localhost:8080"
echo "  - OTel Collector:    http://localhost:4317 (gRPC)"
echo "  - OTel Collector:    http://localhost:4318 (HTTP)"
echo ""
echo "🔍 View logs:  cd signoz/deploy/docker && docker compose logs -f"
echo "🛑 Stop:       cd signoz/deploy/docker && docker compose down"
echo ""
echo "⚠️  Note: First time setup will take 2-3 minutes to initialize"
echo "         Visit http://localhost:3301 to create your account"
echo ""

