# UI Testing Guide

This guide covers testing strategies and best practices for the Rsync-AI frontend application.

## Table of Contents

1. [Overview](#overview)
2. [Test Types](#test-types)
3. [Running Tests](#running-tests)
4. [Test File Structure](#test-file-structure)
5. [Testing Real-Time Features](#testing-real-time-features)
6. [Response Accuracy Testing](#response-accuracy-testing)
7. [Performance Testing](#performance-testing)
8. [CI/CD Integration](#cicd-integration)

---

## Overview

The UI testing strategy ensures accuracy and reliability across all frontend features, with special focus on:

- **Real-time WebSocket updates** for pipeline status, agent activity, and topology
- **Response accuracy** comparing UI state against backend truth
- **Performance benchmarks** for load times and API latency
- **Visual regression** for topology map and charts

### Test Stack

| Tool | Purpose |
|------|---------|
| **Playwright** | E2E browser testing |
| **Vitest** | Unit/component testing |
| **@testing-library/react** | React component testing |
| **Python (pytest)** | Backend integration tests |

---

## Test Types

### 1. End-to-End (E2E) Tests

Browser E2E runs through the frontend Playwright config (`frontend/playwright.a11y.config.ts`, suite under `frontend/e2e/`).

> The legacy top-level `e2e/*.spec.ts` suite (`test_pipeline_status`, `test_agent_activity`, `test_topology_map`) and `e2e/playwright.config.ts` were removed — they were orphaned and never run by CI.

### 2. Component Tests

Located in `/frontend/src/**/__tests__/*.test.tsx`

Tests individual React components in isolation:

```typescript
// frontend/src/components/__tests__/log-viewer.test.tsx
import { render, screen } from '@testing-library/react';
import { LogViewer } from '../log-viewer';

test('renders log entries', () => {
  const logs = [{ id: '1', timestamp: '...', level: 'INFO', message: 'Test' }];
  render(<LogViewer logs={logs} />);
  expect(screen.getByText('Test')).toBeInTheDocument();
});
```

### 3. Response Accuracy Tests

Located in `/e2e/test_ui_response_accuracy.py`

Compares UI state against backend API responses:

```python
# Verify pipeline count matches backend
def test_pipeline_count_accuracy(self):
    backend_data = self._api_get("/api/v1/pipelines")
    backend_count = len(backend_data.get("pipelines", []))
    # Compare with UI count via Playwright
```

### 4. WebSocket Tests

Located in `/e2e/test_websocket_realtime.py`

Tests real-time features and WebSocket connections:

```python
async def test_pipeline_status_event(self):
    ws = await self.connect()
    await ws.send(json.dumps({"type": "subscribe", "channel": "pipelines"}))
    messages = await self.receive_messages(ws, filter_type="pipeline_status")
```

### 5. Performance Benchmarks

Located in `/e2e/test_performance_benchmarks.py`

Measures load times and API latency:

```python
def benchmark_dashboard_page(self):
    times = self._measure_page_load("/dashboard")
    self._record_benchmark("Page: Dashboard", times, threshold_ms=1000)
```

---

## Running Tests

### Prerequisites

```bash
# Install frontend dependencies
cd frontend
npm install

# Install E2E dependencies
cd e2e
pip install -r requirements.txt
npx playwright install
```

### Running E2E Tests (Playwright)

```bash
# Run all Playwright tests
cd frontend
npx playwright test

# Run specific test file
npx playwright test <spec-name>.spec.ts

# Run in headed mode (see browser)
npx playwright test --headed

# Run in UI mode
npx playwright test --ui

# Generate report
npx playwright show-report
```

### Running Python Tests

```bash
# Run response accuracy tests
cd e2e
python test_ui_response_accuracy.py

# Run WebSocket tests
python test_websocket_realtime.py

# Run performance benchmarks
python test_performance_benchmarks.py
```

### Running Component Tests

```bash
cd frontend
npm run test        # Run all tests
npm run test:watch  # Watch mode
npm run test:cov    # With coverage
```

---

## Test File Structure

```
rsync-ai/
├── e2e/
│   ├── test_ui_response_accuracy.py
│   ├── test_websocket_realtime.py
│   └── test_performance_benchmarks.py
│
├── frontend/
│   └── src/
│       ├── components/
│       │   ├── log-viewer.tsx
│       │   ├── agent-activity-feed.tsx
│       │   ├── topology-map.tsx
│       │   └── __tests__/
│       │       ├── log-viewer.test.tsx
│       │       ├── agent-activity-feed.test.tsx
│       │       └── topology-map.test.tsx
│       └── lib/
│           └── __tests__/
│               └── websocket-manager.test.ts
```

---

## Testing Real-Time Features

### WebSocket Connection Testing

```typescript
// Mock WebSocket in tests
class MockWebSocket {
  onmessage: ((event: MessageEvent) => void) | null = null;
  
  simulateMessage(data: object) {
    this.onmessage?.(new MessageEvent('message', { data: JSON.stringify(data) }));
  }
}

test('handles pipeline status updates', () => {
  const mockWs = new MockWebSocket();
  // Inject mock and test event handling
});
```

### Testing Log Streaming

```typescript
test('streams logs in real-time', async ({ page }) => {
  await page.goto('/dashboard/pipelines/123');
  
  // Wait for initial logs
  await expect(page.getByTestId('log-viewer')).toBeVisible();
  
  // Simulate new log via WebSocket
  await page.evaluate(() => {
    window.dispatchEvent(new CustomEvent('ws:log', {
      detail: { level: 'INFO', message: 'New log entry' }
    }));
  });
  
  await expect(page.getByText('New log entry')).toBeVisible();
});
```

### Testing Topology Updates

```typescript
test('updates topology on connection change', async ({ page }) => {
  await page.goto('/dashboard/topology');
  
  // Get initial node count
  const initialNodes = await page.locator('[data-testid="topology-node"]').count();
  
  // Trigger topology update
  await page.evaluate(() => {
    window.dispatchEvent(new CustomEvent('ws:topology', {
      detail: { nodes: [...], edges: [...] }
    }));
  });
  
  // Verify update
  const newNodes = await page.locator('[data-testid="topology-node"]').count();
  expect(newNodes).toBeGreaterThan(initialNodes);
});
```

---

## Response Accuracy Testing

### Strategy

1. **Fetch backend truth** via API
2. **Capture UI state** via Playwright
3. **Compare values** with tolerance for timing
4. **Report discrepancies**

### Example: Pipeline Status Accuracy

```python
def test_pipeline_status_accuracy(self):
    # Get backend truth
    backend_data = self._api_get("/api/v1/pipelines")
    
    # Get UI state via Playwright
    page = self.browser.new_page()
    page.goto(f"{FRONTEND_URL}/dashboard/pipelines")
    
    # Extract UI pipeline count
    ui_count = page.locator('[data-testid="pipeline-card"]').count()
    
    # Compare
    backend_count = len(backend_data.get("pipelines", []))
    assert ui_count == backend_count, f"Count mismatch: UI={ui_count}, Backend={backend_count}"
```

### Handling Timing Differences

```python
# Allow for propagation delay
async def wait_for_sync(self, max_wait=5):
    start = time.time()
    while time.time() - start < max_wait:
        if await self.is_synced():
            return True
        await asyncio.sleep(0.5)
    return False
```

---

## Performance Testing

### Key Metrics

| Metric | Target | Critical |
|--------|--------|----------|
| Dashboard Load | < 1s | < 2s |
| API Response | < 200ms | < 500ms |
| WebSocket Latency | < 100ms | < 300ms |
| TTFB | < 500ms | < 1s |

### Benchmark Example

```python
def benchmark_api_latency(self, endpoint: str, threshold_ms: float):
    times = []
    for _ in range(5):
        start = time.time()
        response = requests.get(f"{API_URL}{endpoint}")
        elapsed = (time.time() - start) * 1000
        if response.status_code == 200:
            times.append(elapsed)
    
    avg_time = statistics.mean(times)
    passed = avg_time <= threshold_ms
    
    return {
        "passed": passed,
        "avg_time_ms": avg_time,
        "threshold_ms": threshold_ms
    }
```

### Running Performance Tests

```bash
# Run benchmarks
python e2e/test_performance_benchmarks.py

# View results
cat performance_benchmark_results.json

# Generate report
cat performance_benchmark_report.md
```

---

## CI/CD Integration

### GitHub Actions Workflow

```yaml
# .github/workflows/ui-tests.yml
name: UI Tests

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  e2e-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Node
        uses: actions/setup-node@v4
        with:
          # Track the major in frontend/Dockerfile — the real workflow does.
          # Do not drop this to 20: jsdom 29 pulls undici 7, which calls
          # worker_threads.markAsUncloneable at import time, and that API does
          # not exist on Node 20 — every vitest file dies at worker startup.
          node-version: '26'
      
      - name: Install dependencies
        run: |
          cd frontend && npm ci
          npx playwright install --with-deps
      
      - name: Start services
        run: docker-compose up -d
      
      - name: Wait for services
        run: |
          timeout 60 bash -c 'until curl -s http://localhost:5001/health; do sleep 2; done'
          timeout 60 bash -c 'until curl -s http://localhost:3000; do sleep 2; done'
      
      - name: Run E2E tests
        run: cd frontend && npx playwright test
      
      - name: Upload report
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: playwright-report
          path: frontend/playwright-report/

  accuracy-tests:
    runs-on: ubuntu-latest
    needs: e2e-tests
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: pip install -r e2e/requirements.txt
      
      - name: Start services
        run: docker-compose up -d
      
      - name: Run accuracy tests
        run: python e2e/test_ui_response_accuracy.py
      
      - name: Run performance benchmarks
        run: python e2e/test_performance_benchmarks.py

  performance-gate:
    runs-on: ubuntu-latest
    needs: accuracy-tests
    steps:
      - name: Check performance thresholds
        run: |
          # Parse results and fail if thresholds exceeded
          pass_rate=$(jq '.pass_rate' performance_benchmark_results.json)
          if (( $(echo "$pass_rate < 80" | bc -l) )); then
            echo "Performance test pass rate below 80%"
            exit 1
          fi
```

### Pre-commit Hook

```bash
# .husky/pre-commit
#!/bin/sh
. "$(dirname "$0")/_/husky.sh"

# Run component tests
cd frontend && npm run test -- --run

# Quick E2E smoke test
npx playwright test --grep "@smoke"
```

---

## Best Practices

### 1. Use Data Attributes for Test Selectors

```tsx
// ✅ Good
<div data-testid="log-viewer">

// ❌ Bad (fragile)
<div className="log-viewer">
```

### 2. Isolate Tests

```typescript
// Each test should be independent
test.beforeEach(async ({ page }) => {
  // Reset state
  await page.evaluate(() => localStorage.clear());
});
```

### 3. Mock External Dependencies

```typescript
// Mock WebSocket for deterministic tests
await page.route('**/ws', route => {
  route.fulfill({
    status: 200,
    body: JSON.stringify({ type: 'connected' })
  });
});
```

### 4. Test Edge Cases

```typescript
test('handles empty pipeline list', async ({ page }) => {
  await page.route('**/api/v1/pipelines', route => {
    route.fulfill({ json: { pipelines: [] } });
  });
  await page.goto('/dashboard/pipelines');
  await expect(page.getByText('No pipelines found')).toBeVisible();
});
```

### 5. Use Meaningful Assertions

```typescript
// ✅ Good - specific assertion
await expect(page.getByRole('button', { name: 'Start Pipeline' })).toBeEnabled();

// ❌ Bad - too generic
await expect(page.locator('button')).toBeVisible();
```

---

## Troubleshooting

### Common Issues

**Tests timeout waiting for WebSocket**
```typescript
// Increase timeout for WebSocket tests
test.setTimeout(30000);
```

**Flaky tests due to animations**
```typescript
// Disable animations in tests
await page.addStyleTag({
  content: '*, *::before, *::after { animation: none !important; transition: none !important; }'
});
```

**Tests fail in CI but pass locally**
```bash
# Run with same config as CI
npx playwright test --config=playwright.ci.config.ts
```

---

## References

- [Playwright Documentation](https://playwright.dev/docs/intro)
- [Testing Library Docs](https://testing-library.com/docs/)
- [Vitest Documentation](https://vitest.dev/)
- [Response Accuracy Strategy](strategy.md)

