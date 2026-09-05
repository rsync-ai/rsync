import { defineConfig } from "vitest/config"
import react from "@vitejs/plugin-react"
import { fileURLToPath } from "node:url"

// Vitest harness for the frontend. Scoped to the Data Explorer modules
// for now (the only suite with unit/component tests today); broaden the
// `include` globs as coverage grows. E2E lives in Playwright (test:e2e).
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./vitest.setup.ts"],
    // vitest's 5s default is tuned for unit tests. These are jsdom component
    // tests, and CI runs them on a self-hosted Mac that hosts four runners at
    // once — a test measured at 168ms locally has been observed timing out at
    // 5000ms there, a >13x contention factor that no per-test optimisation can
    // absorb. 30s still fails a genuinely hung test; it just stops reporting
    // "the machine was busy" as "your dependency bump is broken".
    testTimeout: 30_000,
    hookTimeout: 30_000,
    include: [
      "src/__tests__/**/*.{test,spec}.{ts,tsx}",
      "src/lib/events/**/*.{test,spec}.{ts,tsx}",
      "src/lib/explorer/**/*.{test,spec}.{ts,tsx}",
      "src/lib/pipeline/**/*.{test,spec}.{ts,tsx}",
      "src/lib/types/**/*.{test,spec}.{ts,tsx}",
      "src/lib/workspace/**/*.{test,spec}.{ts,tsx}",
      "src/components/explorer/**/*.{test,spec}.{ts,tsx}",
      "src/components/connectors/**/*.{test,spec}.{ts,tsx}",
      "src/components/pipeline/**/*.{test,spec}.{ts,tsx}",
    ],
    css: false,
  },
})
