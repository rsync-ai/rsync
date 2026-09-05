import { test, expect, request as apiRequest, type Page } from '@playwright/test'

/**
 * Phase 8 E2E — exercises the new discovery flow against the live tool-generator
 * + api-gateway stack (must be up via `docker compose up -d`).
 *
 * Auth strategy: register a fresh user via api-gateway, drop the returned token
 * as a cookie + localStorage. Server-side auth then succeeds.
 *
 * Backend: tool-generator on host port 5010 (see docker-compose.override.yml).
 */

const HOST_GATEWAY = 'http://localhost:5001'
const TOOLGEN = 'http://localhost:5010'
const TEST_EMAIL = `e2e-discover-${Date.now()}@example.com`
const TEST_PASSWORD = 'E2eTest!2024'

let token = ''

test.beforeAll(async () => {
  const ctx = await apiRequest.newContext({ baseURL: HOST_GATEWAY })
  const reg = await ctx.post('/api/v1/auth/register', {
    data: { email: TEST_EMAIL, password: TEST_PASSWORD, name: 'Discover E2E' },
  })
  if (!reg.ok()) {
    throw new Error(`Register failed: ${reg.status()} ${await reg.text()}`)
  }
  const json = await reg.json()
  token = json.token
  await ctx.dispose()

  // Sanity: tool-generator must be up
  const tg = await apiRequest.newContext({ baseURL: TOOLGEN })
  const health = await tg.get('/health')
  if (!health.ok()) {
    throw new Error(`tool-generator not reachable on ${TOOLGEN}`)
  }
  await tg.dispose()
})

async function authenticate(page: Page) {
  await page.context().addCookies([
    { name: 'auth_token', value: token, url: 'http://localhost:3000' },
  ])
  await page.addInitScript((t) => {
    localStorage.setItem('auth_token', t)
  }, token)
}

test.describe('Discovery flow (Phase 8)', () => {
  test('curated vendor → variant pick → contract preview → confirm', async ({ page }) => {
    await authenticate(page)

    await page.goto('/connectors/generate')
    await page.waitForLoadState('networkidle')

    // Page heading rendered (Phase 13c: discovery is the only flow now)
    await expect(page.locator('text=/Generate a connector/i').first()).toBeVisible({ timeout: 8000 })
    await expect(page.locator('text=/no hallucinated connectors/i').first()).toBeVisible()

    // Type "shopify"
    const apiNameInput = page.locator('input[placeholder*="shopify"]').first()
    await apiNameInput.click()
    await apiNameInput.fill('shopify')

    // Type-ahead suggestion appears
    await expect(page.locator('text=/Curated/i').first()).toBeVisible({ timeout: 5000 })
    await page.locator('text=/Shopify/i').first().click()

    // Variant cards render
    await expect(page.locator('text=/Admin GraphQL/i').first()).toBeVisible({ timeout: 5000 })
    await expect(page.locator('text=/Admin REST/i').first()).toBeVisible()

    // Pick Admin GraphQL
    await page.locator('text=/Admin GraphQL/i').first().click()

    await page.screenshot({ path: 'test-results/discovery-01-variant-picked.png', fullPage: false })

    // Click Discover
    await page.getByRole('button', { name: /Discover/i }).click()

    // Contract preview renders with green facts
    await expect(page.locator('text=/Generation contract/i').first()).toBeVisible({ timeout: 10000 })
    await expect(page.locator('text=/Endpoint/i').first()).toBeVisible()
    await expect(page.locator('text=/myshopify\\.com/i').first()).toBeVisible()

    // Connection-time fields informational section
    await expect(page.locator('text=/Connection-time fields/i').first()).toBeVisible()
    await expect(page.locator('code:has-text("shop")').first()).toBeVisible()
    await expect(page.locator('code:has-text("access_token")').first()).toBeVisible()

    await page.screenshot({ path: 'test-results/discovery-02-contract-preview.png', fullPage: false })

    // Phase 12: contract reaches READY immediately. Operations are
    // auto-discovered from docs; auth is satisfied via supported_auth_methods.
    // No user input required.
    await expect(page.locator('text=/Ready/i').first()).toBeVisible({ timeout: 10000 })
    const generateBtn = page.getByRole('button', { name: /Generate connector/i })
    await expect(generateBtn).toBeEnabled()

    // Auth question must NOT appear — supported_auth_methods is declared
    await expect(page.locator('text=/How does this API authenticate/i')).not.toBeVisible()

    await page.screenshot({ path: 'test-results/discovery-03-ready.png', fullPage: false })

    // Phase 13a: clicking Generate actually produces a connector via the
    // session fast path. Should land in <30s for the GraphQL fast path.
    await generateBtn.click()
    const successCard = page.locator('[data-testid="generation-success"]')
    await expect(successCard).toBeVisible({ timeout: 30000 })

    // The card carries the protocol + operation count — assert via the
    // card's textContent rather than the document so we don't accidentally
    // match other "graphql" references on the page.
    const cardText = (await successCard.textContent()) || ''
    expect(cardText.toLowerCase()).toContain('graphql')
    expect(cardText.toLowerCase()).toMatch(/operations?/)

    await page.screenshot({ path: 'test-results/discovery-04-generated.png', fullPage: false })
  })

  test('unknown vendor → questions surface', async ({ page }) => {
    await authenticate(page)
    await page.goto('/connectors/generate')
    await page.waitForLoadState('networkidle')

    const apiNameInput = page.locator('input[placeholder*="shopify"]').first()
    await apiNameInput.fill('totally-unknown-vendor-xyz')

    // Generic inputs (docs URL + protocol dropdown) surface
    await expect(page.locator('text=/Documentation URL/i').first()).toBeVisible()
    await expect(page.locator('text=/API type/i').first()).toBeVisible()

    // Click Discover (no vendor match → all critical questions surface)
    await page.getByRole('button', { name: /Discover/i }).click()

    await expect(page.locator('text=/Generation contract/i').first()).toBeVisible({ timeout: 10000 })
    await expect(page.locator('text=/Needs input/i').first()).toBeVisible()

    // All four critical questions present
    await expect(page.locator('text=/What kind of API/i').first()).toBeVisible()
    await expect(page.locator('text=/What.s the API endpoint/i').first()).toBeVisible()
    await expect(page.locator('text=/How does this API authenticate/i').first()).toBeVisible()
    await expect(page.locator('text=/Which operations/i').first()).toBeVisible()

    await page.screenshot({ path: 'test-results/discovery-04-unknown-vendor.png', fullPage: false })
  })
})
