import * as Sentry from "@sentry/nextjs"

const SENTRY_DSN = process.env.SENTRY_DSN ?? process.env.NEXT_PUBLIC_SENTRY_DSN

if (SENTRY_DSN) {
  Sentry.init({
    dsn: SENTRY_DSN,
    environment: process.env.APP_ENV ?? process.env.NEXT_PUBLIC_APP_ENV ?? "development",
    release: process.env.APP_VERSION ?? process.env.NEXT_PUBLIC_APP_VERSION,

    tracesSampleRate: process.env.APP_ENV === "production" ? 0.1 : 1.0,

    // Avoid capturing health check noise
    ignoreErrors: [
      "ECONNREFUSED",
      "ENOTFOUND",
    ],
  })
}
