import { test as setup, expect } from '@playwright/test'
import path from 'node:path'
import fs from 'node:fs'

/**
 * Logs in once and persists the resulting cookie/localStorage state to disk
 * so authenticated specs can load it via `storageState`.
 *
 * Skips (without failing the run) when no backend is reachable — that way
 * the PR-blocking CI lane stays green; the test only exercises authenticated
 * flows when an api-gateway is actually available (locally or in a future
 * docker-compose-backed workflow).
 *
 * Credentials are read from env so this same fixture works against any
 * environment. Defaults match the seeded dev user.
 */

const API_URL = process.env.E2E_API_URL || process.env.NEXT_PUBLIC_API_URL || 'http://localhost:5001'
const EMAIL = process.env.E2E_USER_EMAIL || 'default@rsync-ai.local'
const PASSWORD = process.env.E2E_USER_PASSWORD || 'password123'

export const AUTH_STATE_PATH = path.join(__dirname, '..', '..', '.auth', 'user.json')

setup('authenticate test user', async ({ request, page }) => {
  // Probe backend before attempting the real login. We never want a missing
  // backend to look like an auth failure in the report.
  let reachable = false
  try {
    const probe = await request.get(`${API_URL}/health`, { timeout: 3_000 })
    reachable = probe.ok()
  } catch {
    reachable = false
  }
  setup.skip(!reachable, `Skipping authenticated suite — backend at ${API_URL} not reachable`)

  const loginResponse = await request.post(`${API_URL}/api/v1/auth/login`, {
    data: { email: EMAIL, password: PASSWORD },
    headers: { 'Content-Type': 'application/json' },
  })
  expect(loginResponse.ok(), `login failed: ${loginResponse.status()} ${await loginResponse.text()}`)
    .toBeTruthy()

  // Some auth flows return a token in the body that the SPA stores in
  // localStorage and mirrors into a cookie. Capture both so either consumer
  // path works after restore.
  const body = await loginResponse.json().catch(() => ({}))
  const token: string | undefined = body?.token || body?.access_token || body?.auth_token

  // Carry the Set-Cookie response into the page's context, then mirror the
  // token into localStorage before saving state. We do this by navigating
  // to the app origin and seeding storage.
  await page.goto('/login')
  if (token) {
    await page.evaluate((t) => {
      window.localStorage.setItem('auth_token', t)
    }, token)
    await page.context().addCookies([
      {
        name: 'auth_token',
        value: token,
        url: page.url(),
        httpOnly: false,
        sameSite: 'Lax',
      },
    ])
  }

  // Ensure the .auth/ directory exists and persist.
  fs.mkdirSync(path.dirname(AUTH_STATE_PATH), { recursive: true })
  await page.context().storageState({ path: AUTH_STATE_PATH })
})
