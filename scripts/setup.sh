#!/bin/bash
# Rsync AI - Complete Setup Script
# Combines: init_project.sh, setup_environment.sh

set -e

echo "🚀 Rsync AI - Complete Setup"
echo "=============================="

# Check prerequisites
echo ""
echo "📋 Checking prerequisites..."

if ! command -v docker &> /dev/null; then
    echo "❌ Docker not found. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose not found. Please install Docker Compose first."
    exit 1
fi

echo "✅ Docker and Docker Compose found"

# Check if .env exists
if [ ! -f .env ]; then
    echo ""
    echo "📝 Creating .env file from .env.example..."
    cp .env.example .env
    echo "✅ .env file created"
    echo "⚠️  Please update .env with your actual values"
else
    echo "✅ .env file already exists"
fi

# Create necessary directories
echo ""
echo "📁 Creating directories..."
mkdir -p shared/mcp-connectors
mkdir -p docs
mkdir -p logs
echo "✅ Directories created"

# Make scripts executable
echo ""
echo "🔧 Making scripts executable..."
chmod +x scripts/*.sh
echo "✅ Scripts are now executable"

# Pull Docker images
echo ""
echo "🐳 Pulling Docker images..."
docker-compose pull
echo "✅ Docker images pulled"

# Build services
echo ""
echo "🏗️  Building services..."
docker-compose build
echo "✅ Services built"

# Start database and Kafka first
echo ""
echo "🗄️  Starting database and Kafka..."
docker-compose up -d postgres kafka
echo "⏳ Waiting for services to be ready..."
sleep 10

# Run migrations
echo ""
echo "📊 Running database migrations..."
./scripts/migrate.sh
echo "✅ Migrations completed"

# Create Kafka topics
echo ""
echo "📨 Creating Kafka topics..."
# No `|| echo` here. --if-not-exists already makes "topic exists" a success, so a
# non-zero exit means the broker rejected a create -- swallowing it leaves the
# stack up with an empty data plane and every consumer blocking on topics that do
# not exist.
./scripts/create_kafka_topics.sh
echo "✅ Kafka topics ready"

echo ""
echo "✅ Setup Complete!"
echo ""
echo "Next steps:"
echo "  1. Update .env with your API keys and configuration"
echo "  2. Start all services: docker-compose up -d"
echo "  3. Open http://localhost:3000"
echo "  4. Login with:"
echo "     Email: admin@rsync.ai"
echo "     Password: admin123"
echo ""
