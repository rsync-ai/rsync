#!/bin/bash
# Quick Frontend Smoke Test Script
# Usage: ./scripts/quick-test.sh

set -e

echo "🔍 RSYNC-AI Frontend Quick Test"
echo "================================"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
API_GATEWAY=${API_GATEWAY_URL:-http://localhost:5001}
ORCHESTRATOR=${ORCHESTRATOR_URL:-http://localhost:8081}
FRONTEND=${FRONTEND_URL:-http://localhost:3000}

check_service() {
    local name=$1
    local url=$2
    local endpoint=${3:-/health}
    
    echo -n "  Checking $name... "
    if curl -sf "${url}${endpoint}" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ OK${NC}"
        return 0
    else
        echo -e "${RED}✗ FAILED${NC}"
        return 1
    fi
}

echo ""
echo "📡 Service Health Checks"
echo "------------------------"

SERVICES_OK=true
check_service "API Gateway" "$API_GATEWAY" || SERVICES_OK=false
check_service "Orchestrator" "$ORCHESTRATOR" || SERVICES_OK=false
check_service "Frontend" "$FRONTEND" "/" || SERVICES_OK=false

if [ "$SERVICES_OK" = false ]; then
    echo ""
    echo -e "${RED}⚠️  Some services are not running!${NC}"
    echo "Start with: docker-compose up -d && cd frontend && npm run dev"
    exit 1
fi

echo ""
echo "🔌 API Endpoint Tests"
echo "---------------------"

# Test Connectors API
echo -n "  GET /api/v1/connectors... "
CONNECTORS=$(curl -sf "${API_GATEWAY}/api/v1/connectors" 2>/dev/null)
if [ $? -eq 0 ]; then
    COUNT=$(echo "$CONNECTORS" | grep -o '"total":[0-9]*' | cut -d: -f2)
    echo -e "${GREEN}✓ OK${NC} ($COUNT connectors)"
else
    echo -e "${RED}✗ FAILED${NC}"
fi

# Test Connections API
echo -n "  GET /api/v1/connections... "
if curl -sf "${API_GATEWAY}/api/v1/connections" -H "X-User-ID: test-user" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ OK${NC}"
else
    echo -e "${YELLOW}⚠ Requires auth${NC}"
fi

# Test Pipelines API
echo -n "  GET /api/v1/pipelines... "
if curl -sf "${API_GATEWAY}/api/v1/pipelines" -H "X-User-ID: test-user" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ OK${NC}"
else
    echo -e "${YELLOW}⚠ Requires auth${NC}"
fi

# Test Decisions API
echo -n "  GET /api/v1/decisions... "
if curl -sf "${API_GATEWAY}/api/v1/decisions" -H "X-User-ID: test-user" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ OK${NC}"
else
    echo -e "${YELLOW}⚠ Not implemented or requires auth${NC}"
fi

echo ""
echo "🖥️  Frontend Pages to Test Manually"
echo "------------------------------------"
echo "  1. ${FRONTEND}/connectors"
echo "     - Connectors load in grid"
echo "     - Category filters work"
echo "     - Click card opens config modal"
echo ""
echo "  2. ${FRONTEND}/connections/new"
echo "     - Source/Destination buttons work"
echo "     - Connectors populate"
echo "     - Form validation works"
echo "     - Test/Save buttons enable correctly"
echo ""
echo "  3. ${FRONTEND}/pipelines"
echo "     - Pipeline list loads"
echo "     - Click pipeline shows detail view"
echo ""
echo "  4. ${FRONTEND}/chat"
echo "     - Can type and submit query"
echo "     - Agent thinking panel shows activity"
echo ""

echo "🧪 Run Playwright Tests"
echo "-----------------------"
echo "  npx playwright test"
echo "  npx playwright test --ui"
echo ""

echo -e "${GREEN}✅ Quick test complete!${NC}"
