import * as Sentry from "@sentry/nextjs"

const SENTRY_DSN = process.env.NEXT_PUBLIC_SENTRY_DSN

if (SENTRY_DSN) {
  Sentry.init({
    dsn: SENTRY_DSN,
    environment: process.env.NEXT_PUBLIC_APP_ENV ?? "development",
    release: process.env.NEXT_PUBLIC_APP_VERSION,

    // Capture 10% of transactions in prod; 100% in staging/dev
    tracesSampleRate: process.env.NEXT_PUBLIC_APP_ENV === "production" ? 0.1 : 1.0,

    // Session replay: 1% of normal sessions, 100% on errors
    replaysSessionSampleRate: 0.01,
    replaysOnErrorSampleRate: 1.0,

    // Propagate trace context to API calls so frontend spans link to backend traces
    tracePropagationTargets: [
      "localhost",
      /^https:\/\/.*\.rsync\.ai/,
      process.env.NEXT_PUBLIC_API_URL ?? "",
    ].filter(Boolean) as (string | RegExp)[],

    integrations: [
      Sentry.replayIntegration({
        maskAllText: false,
        blockAllMedia: false,
      }),
      Sentry.browserTracingIntegration(),
    ],

    // Strip PII from breadcrumbs
    beforeBreadcrumb(breadcrumb) {
      if (breadcrumb.category === "xhr" || breadcrumb.category === "fetch") {
        // Remove auth tokens from request URLs
        if (breadcrumb.data?.url) {
          breadcrumb.data.url = breadcrumb.data.url.replace(/token=[^&]+/, "token=[REDACTED]")
        }
      }
      return breadcrumb
    },
  })
}
