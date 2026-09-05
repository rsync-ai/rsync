/**
 * Derives the workspace slug the api-gateway will store from a display name, so the
 * New Workspace dialog can preview it live. Must stay in lockstep with
 * `api-gateway/internal/handlers/workspaces.go`:
 *
 *   toSlug:           lowercase -> replace [^a-z0-9]+ with "-" -> trim "-" -> cap 60
 *   CreateWorkspace:  if the slug is empty, fall back to "workspace"
 *
 * The 60-char cap is applied last (no re-trim afterwards) to mirror the backend
 * exactly — the preview should match what gets persisted.
 */
const NON_SLUG = /[^a-z0-9]+/g

export function toWorkspaceSlug(name: string): string {
  let s = name.toLowerCase().replace(NON_SLUG, "-")
  s = s.replace(/^-+/, "").replace(/-+$/, "")
  if (s.length > 60) s = s.slice(0, 60)
  return s === "" ? "workspace" : s
}
