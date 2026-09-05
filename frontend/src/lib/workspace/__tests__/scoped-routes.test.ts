import { describe, expect, it } from "vitest"

import { resolveWorkspaceSwitchTarget } from "../scoped-routes"

// The route table decides what happens when the user switches workspaces while
// standing on a page. Getting it wrong is user-visible in both directions: a
// missing entry leaves them on a detail route that 404s under the new workspace
// ("Pipeline not found"), and an over-broad entry teleports them off a page that
// was perfectly valid. These tests pin both edges.

describe("resolveWorkspaceSwitchTarget", () => {
  it("redirects workspace-scoped detail routes to their list root", () => {
    // The reported bug: open a pipeline in workspace A, switch to B, get a 404.
    expect(
      resolveWorkspaceSwitchTarget("/pipelines/2cb685ed-4cf7-445b-9f77-071794d25423"),
    ).toEqual({ href: "/pipelines", label: "pipelines" })
    expect(resolveWorkspaceSwitchTarget("/executions/abc-123")).toEqual({
      href: "/executions",
      label: "executions",
    })
    expect(resolveWorkspaceSwitchTarget("/connections/abc-123")).toEqual({
      href: "/connections",
      label: "connections",
    })
  })

  it("covers nested pages under a detail route", () => {
    // /pipelines/{id}/schema-changes 404s exactly like the parent does, so a
    // prefix match — not an exact one — is what keeps new sub-pages covered.
    expect(resolveWorkspaceSwitchTarget("/pipelines/abc-123/schema-changes")).toEqual({
      href: "/pipelines",
      label: "pipelines",
    })
  })

  it("leaves list roots alone", () => {
    // These re-query under the new workspace and render fine; redirecting a page
    // to itself would flash a pointless toast.
    expect(resolveWorkspaceSwitchTarget("/pipelines")).toBeNull()
    expect(resolveWorkspaceSwitchTarget("/executions")).toBeNull()
    expect(resolveWorkspaceSwitchTarget("/connections")).toBeNull()
  })

  it("leaves creation routes alone", () => {
    // /pipelines/new is not a resource id — the segment would never 404, and
    // yanking the user out mid-form would lose their work.
    expect(resolveWorkspaceSwitchTarget("/pipelines/new")).toBeNull()
    expect(resolveWorkspaceSwitchTarget("/connections/new")).toBeNull()
    expect(resolveWorkspaceSwitchTarget("/connectors/generate")).toBeNull()
  })

  it("leaves routes outside the table alone", () => {
    // Not workspace-scoped resources: platform admin, account settings, and the
    // pages that key off the active workspace but have no id in the URL.
    expect(resolveWorkspaceSwitchTarget("/admin/users")).toBeNull()
    expect(resolveWorkspaceSwitchTarget("/settings")).toBeNull()
    expect(resolveWorkspaceSwitchTarget("/explorer")).toBeNull()
    expect(resolveWorkspaceSwitchTarget("/usage")).toBeNull()
    expect(resolveWorkspaceSwitchTarget("/")).toBeNull()
  })

  it("does not match a prefix that is only a substring of another segment", () => {
    // /pipelines must not swallow a future /pipelines-archive route.
    expect(resolveWorkspaceSwitchTarget("/pipelines-archive/abc")).toBeNull()
  })

  it("normalizes trailing slashes and query strings", () => {
    // usePathname omits the query, but callers pass raw hrefs too — a stray ?tab=
    // must not turn the id segment into "abc-123?tab=logs" and slip past the
    // creation-route guard.
    expect(resolveWorkspaceSwitchTarget("/pipelines/abc-123/")).toEqual({
      href: "/pipelines",
      label: "pipelines",
    })
    expect(resolveWorkspaceSwitchTarget("/pipelines/abc-123?tab=logs")).toEqual({
      href: "/pipelines",
      label: "pipelines",
    })
    expect(resolveWorkspaceSwitchTarget("/pipelines/new?from=chat")).toBeNull()
    expect(resolveWorkspaceSwitchTarget("/pipelines/")).toBeNull()
  })

  it("tolerates an empty pathname", () => {
    // usePathname() is typed string | null on some Next versions; the caller
    // passes "" for null and must not throw during the first render.
    expect(resolveWorkspaceSwitchTarget("")).toBeNull()
  })
})
