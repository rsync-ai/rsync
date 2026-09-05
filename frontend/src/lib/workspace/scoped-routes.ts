/**
 * Which routes must be left behind when the active workspace changes.
 *
 * A detail route pins a resource id that belongs to exactly one workspace. After
 * a switch that id is, by definition, not in the new workspace, so re-rendering
 * the same URL can only ever produce a 404 — the api-gateway returns 404 rather
 * than 403 so it never confirms a foreign resource exists. Routing into a 404 we
 * could have predicted is a dead end; the switcher should navigate to the
 * section the user was already looking at instead.
 *
 * This table is the ONE place that knows which routes are id-scoped. Adding a
 * workspace-scoped detail route = adding a row here; nothing else changes.
 *
 * Deliberately NOT covered:
 *   - list roots (/pipelines, /connections) — they re-scope by refetching
 *   - creation routes (/pipelines/new, /connections/new, /cdc/new) — "new" is
 *     not a resource id, and bouncing out of a half-filled form would discard
 *     the user's work. Guarded by NON_ID_SEGMENTS below.
 *   - /admin/* — platform-wide, not workspace-scoped
 *   - /settings — user-scoped (AUTH.ME), unaffected by the active workspace
 */

/** A workspace-scoped section: its list root and the detail routes beneath it. */
interface ScopedSection {
  /** URL prefix owning the detail routes, e.g. "/pipelines". */
  prefix: string
  /** Where to send the user when they switch away from a detail route. */
  listRoot: string
  /** Human name for the redirect toast ("showing that workspace's pipelines"). */
  label: string
}

const SCOPED_SECTIONS: readonly ScopedSection[] = [
  // Covers /pipelines/{id} and everything under it (e.g. /schema-changes).
  { prefix: "/pipelines", listRoot: "/pipelines", label: "pipelines" },
  { prefix: "/executions", listRoot: "/executions", label: "executions" },
  { prefix: "/connections", listRoot: "/connections", label: "connections" },
]

/**
 * Segments that sit where an id would but name an action, not a resource. A
 * route ending in one of these has nothing workspace-pinned to lose, so the
 * switch leaves it alone.
 */
const NON_ID_SEGMENTS = new Set(["new", "create", "generate"])

export interface WorkspaceSwitchTarget {
  /** Path to navigate to (replace, not push — the old URL is now a dead end). */
  href: string
  /** Section label for the explanatory toast. */
  label: string
}

/**
 * Decides where a workspace switch should leave the user.
 *
 * Returns the section's list root when `pathname` is a workspace-scoped detail
 * route, or null when the current route survives the switch and only needs a
 * refresh. Pure — no router, no storage — so the decision is unit-testable on
 * its own.
 */
export function resolveWorkspaceSwitchTarget(pathname: string): WorkspaceSwitchTarget | null {
  const path = normalize(pathname)

  for (const section of SCOPED_SECTIONS) {
    if (path !== section.prefix && !path.startsWith(`${section.prefix}/`)) continue

    // The segment directly after the prefix is the resource id — unless it names
    // an action, in which case there is no id and nothing to leave behind.
    const rest = path.slice(section.prefix.length + 1)
    if (!rest) return null // the list root itself
    const first = rest.split("/")[0]
    if (NON_ID_SEGMENTS.has(first)) return null

    return { href: section.listRoot, label: section.label }
  }

  return null
}

/** Strip the query/hash and any trailing slash so matching sees a bare path. */
function normalize(pathname: string): string {
  const path = (pathname || "").split(/[?#]/)[0]
  return path.length > 1 && path.endsWith("/") ? path.slice(0, -1) : path
}
