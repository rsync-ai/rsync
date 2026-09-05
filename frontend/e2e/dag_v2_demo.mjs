#!/usr/bin/env node
/**
 * Open a real Chromium window pointed at the running app, with rich Phase 1+2+3
 * mock data injected. Lets you click around the new DAG surfaces interactively.
 *
 * Run:
 *   cd frontend
 *   node e2e/dag_v2_demo.mjs
 * Closes when you close the window.
 */
import { chromium } from '@playwright/test'

const HOST_GATEWAY = 'http://localhost:5001'
const FRONTEND = 'http://localhost:3000'
const TEST_EMAIL = `demo-${Date.now()}@example.com`
const TEST_PASSWORD = 'DemoUser!2024'

async function api(path, opts = {}) {
  const res = await fetch(`${HOST_GATEWAY}${path}`, {
    ...opts,
    headers: { 'Content-Type': 'application/json', ...(opts.headers ?? {}) },
  })
  if (!res.ok) throw new Error(`${path}: ${res.status} ${await res.text()}`)
  return res.json()
}

console.log('1/4  registering demo user…')
const reg = await api('/api/v1/auth/register', {
  method: 'POST',
  body: JSON.stringify({ email: TEST_EMAIL, password: TEST_PASSWORD, name: 'Demo' }),
})
const token = reg.token

console.log('2/4  creating demo pipeline…')
const pipe = await api('/api/v1/pipelines', {
  method: 'POST',
  headers: { Authorization: token },
  body: JSON.stringify({
    name: 'Customer Sync (Demo)',
    request: 'sync mysql customers to s3',
    source: 'mysql',
    destination: 'aws-s3',
  }),
})
const pipelineId = pipe.id

const STATE = {
  execution_id: 'exec-demo',
  current_state: 'COMPLETED',
  execution_plan: {
    pipeline_id: pipelineId,
    mode: 'dag',
    stages: [
      {
        id: 'src',
        display_name: 'Extract from MySQL',
        description: 'Pull customer rows',
        status: 'complete',
        node_kind: 'source',
        dependencies: [],
        actual_duration: 1200,
        result_summary: '12,450 customers extracted',
        metadata: { node_config: { connector_type: 'mysql' } },
      },
      {
        id: 'tx_pii',
        display_name: 'Mask PII',
        description: 'Redact email + phone + SSN',
        status: 'complete',
        node_kind: 'transform',
        dependencies: ['src'],
        actual_duration: 1500,
        result_summary: 'Processed 12,450 records',
        metadata: { pii_fields: ['email', 'phone', 'ssn'] },
      },
      {
        id: 'tx_slow',
        display_name: 'Aggregate Stats',
        description: 'Compute customer aggregations',
        status: 'complete',
        node_kind: 'transform',
        dependencies: ['tx_pii'],
        actual_duration: 9000,
        result_summary: 'Aggregated 12,450 records',
      },
      {
        id: 'dst',
        display_name: 'Load to S3',
        description: 'Write parquet to S3',
        status: 'complete',
        node_kind: 'destination',
        dependencies: ['tx_slow'],
        actual_duration: 1800,
        result_summary: '1.2M rows loaded',
        metadata: { node_config: { connector_type: 'aws-s3' } },
      },
    ],
    metadata: { is_dag: true, node_count: 4, edge_count: 3 },
  },
}

console.log('3/4  launching Chromium…')
const browser = await chromium.launch({ headless: false, args: ['--window-size=1440,900'] })
const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } })

await ctx.addCookies([{ name: 'auth_token', value: token, url: FRONTEND }])
await ctx.addInitScript((t) => localStorage.setItem('auth_token', t), token)

await ctx.route(`**/api/v1/pipelines/${pipelineId}/state`, (route) =>
  route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify(STATE),
  }),
)
await ctx.route('**/api/v1/pipelines/*/events**', (route) =>
  route.fulfill({ status: 200, contentType: 'application/json', body: '{"events":[]}' }),
)

const page = await ctx.newPage()
await page.goto(`${FRONTEND}/pipelines/${pipelineId}?tab=steps`)

console.log('4/4  ✅ ready — click around. Close the window to exit.\n')
console.log(`     pipeline url:  ${FRONTEND}/pipelines/${pipelineId}?tab=steps`)
console.log(`     demo creds:    ${TEST_EMAIL} / ${TEST_PASSWORD}`)

// Keep alive until window closes
await new Promise((resolve) => browser.on('disconnected', resolve))
console.log('\nbye 👋')
