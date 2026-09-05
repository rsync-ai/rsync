import { defineConfig, devices } from '@playwright/test'
import path from 'node:path'
import fs from 'node:fs'

/**
 * Playwright config for accessibility (axe-core) tests.
 *
 * Three projects:
 *   - `setup`             — logs in once and persists storageState. Skips
 *                           when no backend is reachable, so PR CI stays
 *                           green and authenticated specs simply don't run.
 *   - `public-a11y`       — unauthenticated routes (login, signup). Always
 *                           runs; no backend needed.
 *   - `authenticated-a11y`— routes behind the middleware auth guard. Depends
 *                           on `setup`, loads its storageState, and only
 *                           runs when the setup project succeeded.
 */

const AUTH_STATE = path.join(__dirname, '.auth', 'user.json')
// Only pass storageState if the file actually exists; otherwise Playwright
// would throw at context creation. The authenticated spec self-skips when
// the file is missing.
const authStorageState = fs.existsSync(AUTH_STATE) ? AUTH_STATE : undefined

export default defineConfig({
  testDir: './e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: process.env.CI ? 2 : undefined,
  reporter: [['list'], ['html', { outputFolder: 'playwright-a11y-report', open: 'never' }]],
  use: {
    baseURL: process.env.FRONTEND_URL || 'http://localhost:3100',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  projects: [
    {
      name: 'setup',
      testMatch: /a11y\/.*\.setup\.ts$|fixtures\/auth\.setup\.ts$/,
    },
    {
      name: 'public-a11y',
      testMatch: /a11y\/public_pages\.spec\.ts$/,
      use: { ...devices['Desktop Chrome'] },
    },
    {
      name: 'authenticated-a11y',
      testMatch: /a11y\/authenticated_pages\.spec\.ts$/,
      use: {
        ...devices['Desktop Chrome'],
        storageState: authStorageState,
      },
      dependencies: ['setup'],
    },
  ],
  webServer: {
    // `next start` against a prebuilt app. CI runs `next build` before this.
    command: 'PORT=3100 npm run start',
    url: 'http://localhost:3100/login',
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
    env: {
      // Public env vars must be present at build time, so these only matter
      // if a contributor runs locally without a prior build.
      NEXT_PUBLIC_API_URL: process.env.E2E_API_URL || 'http://localhost:5001',
      NEXT_PUBLIC_WS_URL: 'ws://localhost:5001/ws',
      // Server-side fetches in dashboard layout (getCurrentUser, etc.) default
      // to the docker-compose hostname. Point them at the host-reachable URL
      // so authenticated specs can complete the SSR auth check.
      API_GATEWAY_INTERNAL_URL: process.env.E2E_API_URL || 'http://localhost:5001',
      ORCHESTRATOR_INTERNAL_URL: process.env.E2E_ORCHESTRATOR_URL || 'http://localhost:8000',
    },
  },
  timeout: 30_000,
  expect: { timeout: 5_000 },
})
