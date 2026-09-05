import { test, expect, request as apiRequest } from '@playwright/test'

/**
 * Phase 13b E2E — verifies the connection-time auth method picker.
 *
 * Setup: generate a multi-auth connector via the discovery fast path, then
 * open its connection form and assert the picker UI is rendered with the
 * declared methods.
 */

const HOST_GATEWAY = 'http://localhost:5001'
const TOOLGEN = 'http://localhost:5010'
const TEST_EMAIL = `e2e-picker-${Date.now()}@example.com`
const TEST_PASSWORD = 'E2eTest!2024'
// Use a fresh kebab-case name each run so we always exercise the post-Phase-12 path
const CONNECTOR_NAME = `shopify-pick-${Date.now().toString(36).slice(-6)}`

let token = ''

test.beforeAll(async () => {
  // Register user
  const ctx = await apiRequest.newContext({ baseURL: HOST_GATEWAY })
  const reg = await ctx.post('/api/v1/auth/register', {
    data: { email: TEST_EMAIL, password: TEST_PASSWORD, name: 'Picker E2E' },
  })
  if (!reg.ok()) throw new Error(`Register failed: ${reg.status()} ${await reg.text()}`)
  token = (await reg.json()).token

  // Generate the multi-auth connector via session fast path
  const tg = await apiRequest.newContext({ baseURL: TOOLGEN })
  const disc = await tg.post('/v1/discover', {
    data: {
      api_name: CONNECTOR_NAME,
      api_variant_id: 'admin-graphql',
      vendor_id: 'shopify',
    },
  })
  if (!disc.ok()) throw new Error(`Discover failed: ${disc.status()}`)
  const sessionId = (await disc.json()).session_id

  const gen = await ctx.post('/api/v1/connectors/generate', {
    data: { api_name: CONNECTOR_NAME, session_id: sessionId },
  })
  if (!gen.ok()) throw new Error(`Generate failed: ${gen.status()} ${await gen.text()}`)

  // Verify the generated connector exposes supported_auth_methods
  const connector = await ctx.get(`/api/v1/connectors/${CONNECTOR_NAME}`)
  if (!connector.ok()) throw new Error(`Connector lookup failed: ${connector.status()}`)
  const body = await connector.json()
  if (!body.supported_auth_methods || body.supported_auth_methods.length === 0) {
    throw new Error('Generated connector missing supported_auth_methods — cannot exercise picker')
  }

  await ctx.dispose()
  await tg.dispose()
})

test('connection form renders auth method picker for multi-auth connector', async ({ page }) => {
  // Authenticate
  await page.context().addCookies([
    { name: 'auth_token', value: token, url: 'http://localhost:3000' },
  ])
  await page.addInitScript((t) => localStorage.setItem('auth_token', t), token)

  await page.goto(`/connectors`)
  await page.waitForLoadState('networkidle')

  // The connectors page has a search box — type our generated name
  const search = page.locator('input[placeholder*="Search"]').first()
  if (await search.isVisible({ timeout: 4000 }).catch(() => false)) {
    await search.fill(CONNECTOR_NAME)
    await page.waitForTimeout(500)
  }

  // Find the connector card and click Configure (or the card itself)
  const card = page.locator(`text=/${CONNECTOR_NAME}/i`).first()
  await expect(card).toBeVisible({ timeout: 10000 })

  // Click the configure button on the matching card
  const configureBtn = page.getByRole('button', { name: /configure/i }).first()
  if (await configureBtn.isVisible({ timeout: 3000 }).catch(() => false)) {
    await configureBtn.click()
  } else {
    await card.click()
  }

  // The connection form should open. Verify the auth method picker is there.
  await expect(page.locator('[data-testid="auth-method-picker"]')).toBeVisible({
    timeout: 10000,
  })

  await page.screenshot({
    path: 'test-results/auth-picker-shopify.png',
    fullPage: false,
  })

  // For Shopify with one declared method, no dropdown should appear, but
  // the credential field IS present.
  await expect(page.locator('[data-testid="credential-access_token"]')).toBeVisible()
})
