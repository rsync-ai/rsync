import * as Sentry from "@sentry/nextjs"

import { SENTRY_DATA_COLLECTION } from "./sentry.data-collection"

const SENTRY_DSN = process.env.SENTRY_DSN ?? process.env.NEXT_PUBLIC_SENTRY_DSN

if (SENTRY_DSN) {
  Sentry.init({
    dsn: SENTRY_DSN,
    dataCollection: SENTRY_DATA_COLLECTION,
    environment: process.env.APP_ENV ?? "development",
    tracesSampleRate: process.env.APP_ENV === "production" ? 0.1 : 1.0,
  })
}
