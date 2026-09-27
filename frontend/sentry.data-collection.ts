import type * as Sentry from "@sentry/nextjs"

type DataCollection = NonNullable<NonNullable<Parameters<typeof Sentry.init>[0]>["dataCollection"]>

// Sentry v11 collects cookies, request/response bodies, user info and DB query data when
// `dataCollection` is left unset; v10 collected none of them. The session cookie and
// connection-form bodies must never reach Sentry, so every Sentry.init passes this: the v10
// baseline, as the v10→v11 migration guide spells it out.
const DENY_IDENTIFYING_KEYS = { deny: ["forwarded", "-ip", "remote-", "via", "-user"] }

export const SENTRY_DATA_COLLECTION: DataCollection = {
  userInfo: false,
  cookies: false,
  httpHeaders: { request: DENY_IDENTIFYING_KEYS, response: DENY_IDENTIFYING_KEYS },
  httpBodies: [],
  urlQueryParams: DENY_IDENTIFYING_KEYS,
  genAI: { inputs: false, outputs: false },
  databaseQueryData: false,
  queues: false,
  graphQL: { document: false, variables: false },
}
