export type StartupSettingProblem = {
  /** The setting's name. Safe to show anywhere: it never carries a value. */
  setting: string
  /** One plain sentence set: what is missing, what will not work, what to do. */
  message: string
  /**
   * True when saying the setting is missing reveals something about a credential.
   * Such problems go to the server log only, never to the unauthenticated /api/health.
   */
  sensitive: boolean
}

/**
 * Settings the production frontend server needs but was not given.
 *
 * Only a production server is checked (NODE_ENV=production, which the image sets):
 * under `next dev` the localhost defaults in @/lib/config/api are intentional.
 * These are ERROR lines, never a refusal to start: the start-path census for
 * issue #24 found ORCHESTRATOR_INTERNAL_URL missing from the Helm chart and the
 * secret absent on the dev compose, and the rest of the app works without them.
 */
export function frontendStartupProblems(
  env: Record<string, string | undefined>,
): StartupSettingProblem[] {
  if (env.NODE_ENV !== "production") return []
  const unset = (name: string) => !(env[name] ?? "").trim()
  const problems: StartupSettingProblem[] = []
  if (unset("API_GATEWAY_INTERNAL_URL")) {
    problems.push({
      setting: "API_GATEWAY_INTERNAL_URL",
      sensitive: false,
      message:
        "API_GATEWAY_INTERNAL_URL is not set, empty or only spaces, so server-side requests to " +
        "the API gateway (explorer queries, SQL generation, sharing, the schema index and the " +
        "/api/health backend check) go to the built-in default http://localhost:5001. That only " +
        "works when the gateway runs on the same host as this server; in a container or on " +
        "another machine those requests fail. Set it to the gateway's in-network address, for " +
        "example http://api-gateway:8080.",
    })
  }
  if (unset("ORCHESTRATOR_INTERNAL_URL")) {
    problems.push({
      setting: "ORCHESTRATOR_INTERNAL_URL",
      sensitive: false,
      message:
        "ORCHESTRATOR_INTERNAL_URL is not set, empty or only spaces, so the dashboard's pipeline " +
        "statistics are fetched from the built-in default http://localhost:8081. That only works " +
        "when the orchestrator runs on the same host as this server; otherwise the statistics " +
        "stay empty. Set it to the orchestrator's in-network address, for example " +
        "http://orchestrator:8080.",
    })
  }
  if (unset("INTERNAL_SERVICE_SECRET")) {
    problems.push({
      setting: "INTERNAL_SERVICE_SECRET",
      sensitive: true,
      message:
        "INTERNAL_SERVICE_SECRET is not set, empty or only spaces. The dashboard's pipeline " +
        "statistics stay empty whenever the orchestrator runs with ENVIRONMENT=production, because " +
        "it refuses the request. Give the frontend the same INTERNAL_SERVICE_SECRET as api-gateway, " +
        "orchestrator and temporal-adapter (if none exists yet, generate one, for example with " +
        "openssl rand -hex 32, and give it to all four).",
    })
  }
  return problems
}

export async function register() {
  if (process.env.NEXT_RUNTIME === "nodejs") {
    for (const problem of frontendStartupProblems(process.env)) {
      console.error(`Startup check: ${problem.message}`)
    }
    await import("../sentry.server.config")
  }
  if (process.env.NEXT_RUNTIME === "edge") {
    await import("../sentry.edge.config")
  }
}

// Wire Sentry's onRequestError hook for server-side error capture (Next.js 15+)
export { captureRequestError as onRequestError } from "@sentry/nextjs"
