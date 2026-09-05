import { defineConfig, devices } from '@playwright/test'

/**
 * Playwright E2E Test Configuration for RSYNC-AI Frontend
 * 
 * Prerequisites:
 *   - Start backend services: docker-compose up -d
 *   - Start frontend: npm run dev
 * 
 * Run:
 *   - All tests: npm run test:e2e
 *   - With UI: npm run test:e2e:ui
 *   - Single file: npx playwright test e2e/agentic-ui.spec.ts
 *   - Debug: npx playwright test --debug
 */

export default defineConfig({
  testDir: './e2e',
  
  // Run tests sequentially for now (can parallelize later)
  fullyParallel: false,
  
  // Fail the build on CI if test.only is left in source code
  forbidOnly: !!process.env.CI,
  
  // Retry on CI only
  retries: process.env.CI ? 2 : 0,
  
  // Run one worker at a time for stability
  workers: 1,
  
  // Reporter configuration
  reporter: [
    ['html', { outputFolder: 'playwright-report' }],
    ['list'],
  ],
  
  // Global settings for all tests
  use: {
    // Frontend URL
    baseURL: process.env.FRONTEND_URL || 'http://localhost:3000',
    
    // Collect trace on first retry
    trace: 'on-first-retry',
    
    // Screenshot on failure
    screenshot: 'only-on-failure',
    
    // Video on failure
    video: 'retain-on-failure',
    
    // Browser context options
    viewport: { width: 1280, height: 720 },
    
    // Default timeout for actions
    actionTimeout: 10000,
    
    // Navigation timeout
    navigationTimeout: 30000,
  },

  // Test projects (browsers to test on)
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
    // Uncomment to test on more browsers
    // {
    //   name: 'firefox',
    //   use: { ...devices['Desktop Firefox'] },
    // },
    // {
    //   name: 'webkit',
    //   use: { ...devices['Desktop Safari'] },
    // },
    // {
    //   name: 'Mobile Chrome',
    //   use: { ...devices['Pixel 5'] },
    // },
  ],

  // Web server configuration
  // Note: We assume services are already running
  webServer: {
    command: 'echo "Frontend should already be running on port 3000"',
    url: 'http://localhost:3000',
    reuseExistingServer: true,
    timeout: 5000,
  },

  // Global timeout for test file
  timeout: 60000,

  // Expect timeout
  expect: {
    timeout: 10000,
  },
})
