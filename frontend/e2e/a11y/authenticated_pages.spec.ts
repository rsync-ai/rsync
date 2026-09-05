import { test, expect } from '@playwright/test'
import AxeBuilder from '@axe-core/playwright'
import fs from 'node:fs'
import path from 'node:path'

// Playwright's project `dependencies` mechanism does not gate on skipped
// setup — only on failed. When the auth setup skips (no backend reachable),
// these tests would otherwise run with no storageState and chase redirects
// to /login. Guard the whole file on the presence of the auth artifact.
const AUTH_STATE = path.join(__dirname, '..', '..', '.auth', 'user.json')
const HAS_AUTH = fs.existsSync(AUTH_STATE)

test.beforeEach(() => {
  test.skip(
    !HAS_AUTH,
    `No auth state at ${AUTH_STATE} — backend likely unreachable; setup skipped.`,
  )
})

/**
 * Accessibility checks for authenticated routes.
 *
 * Same blocking impact bar as public_pages.spec.ts (zero serious/critical
 * WCAG 2.1 A/AA violations). The `authenticated-a11y` project loads the
 * storageState produced by `fixtures/auth.setup.ts`, so these tests only
 * run when a backend was reachable at setup time.
 *
 * Keep this list short and stable: pages that render with no run-specific
 * state. Dashboard list views, settings index, etc.
 */

type Impact = 'minor' | 'moderate' | 'serious' | 'critical'
const BLOCKING: Impact[] = ['serious', 'critical']

const ROUTES = [
  { path: '/dashboard', label: 'Dashboard' },
  { path: '/connections', label: 'Connections' },
  { path: '/pipelines', label: 'Pipelines' },
  { path: '/settings', label: 'Settings' },
]

for (const { path: route, label } of ROUTES) {
  test(`${label} (${route}) has no serious/critical axe violations`, async ({ page }, testInfo) => {
    const response = await page.goto(route)
    // Middleware redirects to /login if storageState is invalid — fail loudly
    // rather than silently asserting against the login page.
    expect(page.url(), `expected to land on ${route}, got ${page.url()}`).toContain(route)
    expect(response?.ok() ?? false, `non-OK response on ${route}: ${response?.status()}`).toBeTruthy()

    await page.waitForLoadState('networkidle')

    const results = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze()

    const blocking = results.violations.filter((v) => BLOCKING.includes(v.impact as Impact))

    await testInfo.attach('axe-report.json', {
      body: JSON.stringify(results, null, 2),
      contentType: 'application/json',
    })

    expect(
      blocking,
      `axe found ${blocking.length} blocking violation(s) on ${route}: ` +
        blocking.map((v) => `${v.id} (${v.impact})`).join(', '),
    ).toEqual([])
  })
}
