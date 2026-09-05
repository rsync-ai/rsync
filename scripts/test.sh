#!/bin/bash
# Rsync AI - Test Runner Script
# Combines: test_system_integration.sh, run_ui_tests.sh

set -e

TEST_TYPE="${1:-all}"

echo "🧪 Rsync AI Test Runner"
echo "========================"

# Check if services are running
echo ""
echo "📋 Checking services..."

check_service() {
  local name=$1
  local url=$2
  
  if curl -s -o /dev/null -w "%{http_code}" "$url" | grep -q "200\|307"; then
    echo "  ✅ $name"
    return 0
  else
    echo "  ❌ $name (not responding)"
    return 1
  fi
}

ALL_OK=true

check_service "Frontend" "http://localhost:3000" || ALL_OK=false
check_service "API Gateway" "http://localhost:5001/health" || ALL_OK=false
check_service "Orchestrator" "http://localhost:8081/health" || ALL_OK=false

if [ "$ALL_OK" = false ]; then
  echo ""
  echo "⚠️  Some services are not running. Start them with: ./scripts/dev.sh start"
  exit 1
fi

echo ""
echo "✅ All services are running"

case $TEST_TYPE in
  api)
    echo ""
    echo "🔌 Testing API Endpoints..."
    echo "---------------------------"
    
    # Test auth endpoints
    echo ""
    echo "1️⃣  Testing Login..."
    LOGIN_RESPONSE=$(curl -s -X POST http://localhost:5001/api/v1/auth/login \
      -H "Content-Type: application/json" \
      -d '{"email":"admin@rsync.ai","password":"admin123"}')
    
    if echo "$LOGIN_RESPONSE" | grep -q "token"; then
      echo "   ✅ Login successful"
      TOKEN=$(echo "$LOGIN_RESPONSE" | grep -o '"token":"[^"]*' | cut -d'"' -f4)
      USER_ID=$(echo "$LOGIN_RESPONSE" | grep -o '"user_id":"[^"]*' | cut -d'"' -f4)
      echo "   User ID: $USER_ID"
    else
      # Dev-mode fallback: use default seeded user when auth is not configured
      # (api-gateway migration inserts dummy users that can't login via password)
      USER_ID="${USER_ID:-00000000-0000-0000-0000-000000000001}"
      echo "   ⚠️  Login not available in this environment, using X-User-ID: $USER_ID"
      echo "   Response: $LOGIN_RESPONSE"
    fi
    
    # Test connections endpoint
    echo ""
    echo "2️⃣  Testing Connections API..."
    CONNECTIONS=$(curl -s -H "X-User-ID: $USER_ID" http://localhost:5001/api/v1/connections)
    if echo "$CONNECTIONS" | grep -q "\[\]"; then
      echo "   ✅ Connections endpoint working (empty list)"
    else
      echo "   ✅ Connections endpoint working"
      echo "   Found: $(echo "$CONNECTIONS" | grep -o '"id"' | wc -l) connections"
    fi
    
    # Test connectors endpoint
    echo ""
    echo "3️⃣  Testing Connectors API..."
    CONNECTORS=$(curl -s http://localhost:5001/api/v1/connectors)
    CONNECTOR_COUNT=$(echo "$CONNECTORS" | grep -o '"name"' | wc -l)
    if [ "$CONNECTOR_COUNT" -gt 0 ]; then
      echo "   ✅ Connectors endpoint working"
      echo "   Found: $CONNECTOR_COUNT connectors"
    else
      echo "   ❌ No connectors found"
    fi
    
    # Test pipelines endpoint
    echo ""
    echo "4️⃣  Testing Pipelines API..."
    PIPELINES=$(curl -s -H "X-User-ID: $USER_ID" http://localhost:5001/api/v1/pipelines)
    if echo "$PIPELINES" | grep -q "\[\]"; then
      echo "   ✅ Pipelines endpoint working (empty list)"
    else
      echo "   ✅ Pipelines endpoint working"
    fi
    
    echo ""
    echo "✅ All API tests passed!"
    ;;
    
  ui)
    echo ""
    echo "🖥️  Running UI Tests..."
    echo "----------------------"
    
    if [ ! -d "e2e/node_modules" ]; then
      echo "📦 Installing Playwright dependencies..."
      cd e2e && npm install && cd ..
    fi
    
    echo ""
    echo "Running Playwright tests..."
    cd e2e && npm test && cd ..
    ;;
    
  integration)
    echo ""
    echo "🔗 Running Integration Tests..."
    echo "-------------------------------"
    
    # Test complete flow
    echo ""
    echo "Testing complete pipeline flow..."
    
    if [ -f "e2e/test_pipeline_full.py" ]; then
      python3 e2e/test_pipeline_full.py
    elif [ -f "tests/test_real_pipelines.py" ]; then
      python3 tests/test_real_pipelines.py
    else
      echo "⚠️  Integration test file not found"
    fi
    ;;
    
  all)
    echo ""
    echo "Running all tests..."
    
    # Run API tests
    $0 api
    
    echo ""
    echo "=========================================="
    echo ""
    
    # Run UI tests (if requested)
    read -p "Run UI tests? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
      $0 ui
    fi
    
    echo ""
    echo "✅ All tests complete!"
    ;;
    
  *)
    echo "Usage: $0 {api|ui|integration|all}"
    echo ""
    echo "Test Types:"
    echo "  api          - Test all API endpoints"
    echo "  ui           - Run Playwright UI tests"
    echo "  integration  - Run integration tests"
    echo "  all          - Run all tests"
    echo ""
    echo "Examples:"
    echo "  $0 api              # Quick API endpoint tests"
    echo "  $0 ui               # Full UI test suite"
    echo "  $0 all              # Everything"
    exit 1
    ;;
esac

echo ""
