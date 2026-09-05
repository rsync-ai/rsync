#!/bin/bash
# Stop All Services Script for RSYNC AI
# Stops all Docker containers and application processes

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🛑 RSYNC AI - Stop All Services${NC}"
echo "================================"
echo ""

# Stop Docker containers
echo "1️⃣  Stopping Docker containers..."
if docker-compose ps -q 2>/dev/null | grep -q .; then
    docker-compose down -v
    echo -e "${GREEN}✓ Docker containers stopped${NC}"
else
    echo -e "${YELLOW}No Docker containers running${NC}"
fi
echo ""

# Stop application processes
echo "2️⃣  Stopping application processes..."

# Frontend (Next.js)
if pgrep -f "next" > /dev/null; then
    pkill -f "next"
    echo -e "${GREEN}✓ Frontend (Next.js) stopped${NC}"
else
    echo -e "${YELLOW}Frontend not running${NC}"
fi

# LLM Service (uvicorn)
if pgrep -f "uvicorn" > /dev/null; then
    pkill -f "uvicorn"
    echo -e "${GREEN}✓ LLM Service stopped${NC}"
else
    echo -e "${YELLOW}LLM Service not running${NC}"
fi

# API Gateway
if pgrep -f "api-gateway" > /dev/null; then
    pkill -f "api-gateway"
    echo -e "${GREEN}✓ API Gateway stopped${NC}"
else
    echo -e "${YELLOW}API Gateway not running${NC}"
fi

# Backend Orchestrator
if pgrep -f "backend-orchestrator" > /dev/null; then
    pkill -f "backend-orchestrator"
    echo -e "${GREEN}✓ Backend Orchestrator stopped${NC}"
else
    echo -e "${YELLOW}Backend Orchestrator not running${NC}"
fi

# Agent Orchestrator
if pgrep -f "orchestrator.py" > /dev/null; then
    pkill -f "orchestrator.py"
    echo -e "${GREEN}✓ Agent Orchestrator stopped${NC}"
else
    echo -e "${YELLOW}Agent Orchestrator not running${NC}"
fi

echo ""
echo "3️⃣  Verifying all ports are free..."
sleep 2

# Check critical ports
PORTS=(3000 5000 5432 8080 8081 8083 9000 9001 9092)
ALL_FREE=true

for port in "${PORTS[@]}"; do
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        PID=$(lsof -Pi :$port -sTCP:LISTEN -t)
        echo -e "${RED}❌ Port $port still in use (PID: $PID)${NC}"
        ALL_FREE=false
        # Try to kill it
        kill -9 $PID 2>/dev/null || true
    fi
done

if [ "$ALL_FREE" = true ]; then
    echo -e "${GREEN}✅ All ports are free!${NC}"
else
    echo -e "${YELLOW}⚠️  Some ports were still in use, attempted to force kill${NC}"
fi

echo ""
echo -e "${GREEN}✅ All services stopped!${NC}"
echo ""
echo "To start again:"
echo "  make quick-start"
echo ""

