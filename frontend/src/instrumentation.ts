export async function register() {
  if (process.env.NEXT_RUNTIME === "nodejs") {
    await import("../sentry.server.config")
  }
  if (process.env.NEXT_RUNTIME === "edge") {
    await import("../sentry.edge.config")
  }
}

// Wire Sentry's onRequestError hook for server-side error capture (Next.js 15+)
export { captureRequestError as onRequestError } from "@sentry/nextjs"
