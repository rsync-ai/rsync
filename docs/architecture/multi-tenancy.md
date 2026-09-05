# Multi-tenancy — workspaces, roles & isolation

**Audience:** engineers touching any workspace-scoped resource (connections, pipelines,
executions, members, invites) or any api-gateway handler that reads/writes tenant data.
**Scope:** the collaboration tenancy model — how rsync-ai isolates one company's data
and members from another's, and how role-based access is enforced.

> Anti-hallucination note: every mechanism below is cited to `file:line`. If the code
> and this doc disagree, trust the code and fix this doc.

---

## 1. What a workspace is

A **workspace** is a single company's collaboration boundary: a set of members, each with
a role, sharing a set of connections and pipelines. It is *not* a company-vs-company org
boundary (that is a separate future axis); it is
**single-company, multi-user**.

- Every user has exactly **one personal workspace** (`is_personal = true`), provisioned at
  signup and again by the clean-slate activation migration. It is the user's identity
  anchor: it cannot be renamed, deleted, or left, and its owner is always exactly that user.
- Users create additional **shared workspaces** and invite others into them. A shared
  workspace can have many members across the four roles.
- A user's **active workspace** is the one their requests currently operate under. It is
  selected client-side and carried per-request (see §4). Switching it changes which
  tenant's connections/pipelines every subsequent call sees.

Delivered across two PRs: backend multi-user + membership/invites/delete
(#344); operator-facing management UI +
`is_personal` DTO (#346).

---

## 2. Two orthogonal role axes — do not conflate them

rsync-ai has **two independent authorization axes**. Mixing them is a security bug.

| Axis | Values | Answers | Source of truth |
|---|---|---|---|
| **Platform role** | `user` · `power_user` · `admin` | "May this account use platform-wide admin features (user mgmt, drift, health)?" | `api-gateway/internal/security/rbac.go` (`UserRole`) |
| **Workspace role** | `viewer` · `member` · `admin` · `owner` | "What may this account do *inside this one workspace*?" | [`api-gateway/internal/security/workspace.go`](../../api-gateway/internal/security/workspace.go) (`WorkspaceRole`) |

A global `user` can be a workspace `owner`; a global `admin` need not be a member of a
given workspace at all — so a platform admin still gets a **404** on a workspace they don't
belong to. The `/admin/*` UI is titled **"Platform admin"** with a scoping banner precisely
to keep this distinction visible
([`admin/page.tsx`](../../frontend/src/app/(dashboard)/admin/page.tsx)).

The workspace role hierarchy is a strict rank, **fail-closed**: an unknown or empty role has
level 0 and satisfies no gate ([`workspace.go:19-38`](../../api-gateway/internal/security/workspace.go)).

```
owner (4) > admin (3) > member (2) > viewer (1) > unknown/empty (0)
```

| Role | Capabilities |
|---|---|
| **viewer** | read-only: view connections, pipelines, runs |
| **member** | viewer + create/run connections and pipelines |
| **admin** | member + invite/remove/change-role members + rename the workspace |
| **owner** | admin + delete the workspace / transfer ownership |

---

## 3. Data model

| Object | Table / column | Notes |
|---|---|---|
| Workspace | `workspaces` (id, name, `is_personal`, …) | Base table from migration `047_workspaces.sql`. `is_personal` marks the immutable identity-anchor workspace. |
| Membership | `workspace_members` (workspace_id, user_id, role) | The authoritative `(who, where, what-role)` triple. Every gate resolves role from **this** table on every request — never from a token claim or cache. |
| Invite | `workspace_invites` | Pending invitations (token, email, role); accept creates a `workspace_members` row. |
| Resource scope | `connections.workspace_id`, `pipelines.workspace_id` | Every tenant-scoped resource carries the owning workspace; all reads/writes filter on it. |
| Audit | `audit_logs.workspace_id` (nullable UUID, FK → `workspaces`, `ON DELETE SET NULL`) | Formalized by migration [`070_audit_logs_workspace_id.sql`](../../api-gateway/migrations/070_audit_logs_workspace_id.sql); `logAudit()` stamps the caller's active workspace. Nullable on purpose (auth-only routes run before workspace context; historical rows predate it). 069 first adds the bare column defensively (line 127). |

**The clean-slate activation migration** —
[`069_workspace_activation_clean_slate.sql`](../../api-gateway/migrations/069_workspace_activation_clean_slate.sql)
— is what turned the single-user schema into the multi-user model: it provisions a personal
workspace per user, backfills `connections.workspace_id` to each owner's personal workspace,
creates `workspace_invites`, adds the additive audit/oauth `workspace_id` columns, and then
`SET NOT NULL` on the scope columns. It is idempotent and auto-applies on api-gateway boot.
Migration `070` later formalizes the audit column (FK → `workspaces`, `ON DELETE SET NULL`,
indexes) and makes `logAudit()` stamp the caller's active workspace.

---

## 4. Request lifecycle — resolving the active workspace

Every authenticated request passes through **`WorkspaceContextMiddleware`**
([`workspace_context.go:49-114`](../../api-gateway/internal/handlers/workspace_context.go)).
It pins two values into the gin context: `workspace_id` (`ctxWorkspaceID`) and the caller's
role in it (`ctxWorkspaceRole`) — **after re-verifying membership against the DB on every
request**. The client's selection is an *untrusted hint*, honored only when membership checks
out.

**Resolution order:**

1. **`X-Workspace-ID` header** ([`:16`](../../api-gateway/internal/handlers/workspace_context.go)),
   or its **`rsync_active_workspace_id` cookie** mirror ([`:17`](../../api-gateway/internal/handlers/workspace_context.go))
   when the header is absent. (The client source of truth is `localStorage`; the cookie is
   the SSR mirror so server-rendered pages agree with the client.)
2. Fallback: the caller's **personal workspace** (`personalWorkspace()`,
   [`:145`](../../api-gateway/internal/handlers/workspace_context.go)) — every user has exactly one post-069.

Membership is resolved by `lookupWorkspaceMembership()` —
`SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2`
([`:136-141`](../../api-gateway/internal/handlers/workspace_context.go)).

**Failure semantics (deliberate — these prevent both lockout and leakage):**

| Situation | Behavior |
|---|---|
| No `user_id` (public/unauth route) | pass through untouched; DB not queried; downstream auth gates reject |
| DB unavailable | **503** |
| Explicit **header** names a workspace the caller can't access, on a non-optional route | **404** — never confirm existence, never proceed under it |
| Stale **cookie** naming an inaccessible workspace | never hard-fails → falls through to personal |
| **Optional** routes (`/admin/*`, `GET /workspaces`, `/features`) | never abort on a bad selection; resolve best-effort so the user can always recover ([`workspaceContextOptional`, :119-134](../../api-gateway/internal/handlers/workspace_context.go)) |

The header-vs-cookie asymmetry is intentional: an explicit header is a deliberate act (hard
404 on miss), while a stale cookie must never lock a user out of the routes they need to
recover.

---

## 5. The authorization gates — which to use

Handlers never trust the resolved context blindly; they call one of these gates, all
**fail-closed** (write the error response and return `false` on any failure, *before* any DB
write or request-body bind). All live in
[`workspace_context.go`](../../api-gateway/internal/handlers/workspace_context.go) unless noted.

| Gate | Gates on | Use for | On failure |
|---|---|---|---|
| **`requireWorkspaceRole`** ([:179](../../api-gateway/internal/handlers/workspace_context.go)) | role in the **active** workspace | active-workspace-scoped actions where the target *is* the active ws | 403 |
| **`requireWorkspaceParamRole`** ([:201](../../api-gateway/internal/handlers/workspace_context.go)) | role in the workspace named by the **URL `:id`** | workspace **administration** (invites, members, delete) where the target ws is in the path | 401 / 404 / 503 / 403 |
| **`requireResourceRole`** ([:244](../../api-gateway/internal/handlers/workspace_context.go)) | resource ∈ active ws **AND** caller is a member — in one JOIN | mutating a **specific resource** (`connections`, `pipelines`) | 401 / 500 / 404 / 503 / 403 |
| **`requirePipelineWorkspaceRole`** ([`pipeline_ownership.go:21-22`](../../api-gateway/internal/handlers/pipeline_ownership.go)) | wraps `requireResourceRole` for `pipelines` | **every** `/pipelines/:id/*` handler — reads at `security.WSViewer` as well as mutations | delegates |
| **`requireNonPersonalWorkspace`** | active ws is not personal | actions forbidden on the personal identity anchor | 409 |

Dozens of call sites across the handlers wire these gates. Two representative examples:
- Connection delete/update — `requireResourceRole(c, "connections", id, security.WSMember)`
  ([`connections.go:1140`](../../api-gateway/internal/handlers/connections.go), [`:1415`](../../api-gateway/internal/handlers/connections.go)).
- Pipeline mutations — `requirePipelineWorkspaceRole(c, id, security.WSMember)`
  ([`pipelines.go:2052`](../../api-gateway/internal/handlers/pipelines.go) and 7 more sites, e.g. `:1300` at viewer level).

### 5.1 Why `requireResourceRole` is the IDOR-safe gate

The classic IDOR attack: workspace-A admin sends `X-Workspace-ID: A` but a resource ID that
belongs to workspace B. A naïve "is the caller an admin of their active ws?" check would pass.
`requireResourceRole` closes this by proving three things **in a single query** —
([`:267-275`](../../api-gateway/internal/handlers/workspace_context.go)):

```sql
SELECT wm.role
FROM <table> r
JOIN workspace_members wm ON wm.workspace_id = r.workspace_id
WHERE r.id = $1 AND wm.user_id = $2 AND r.workspace_id = $3   -- $3 = ACTIVE ws
```

(a) the resource exists, (b) it belongs to the caller's **active** workspace, and (c) the
caller is a member of that workspace with a sufficient role. There is **no `created_by`
fallback** — clean-slate ownership is by membership only.

Two hardening details:
- **Table-name allowlist.** `table` is interpolated into SQL (identifiers can't be bound as
  params), so it must come from the `resourceTables` allowlist
  (`{pipelines, connections}`, [`:25-28`](../../api-gateway/internal/handlers/workspace_context.go)) and
  **never** from request input. A non-allowlisted table is a 500 (programming error), not a query.
- **404, not 403, on a foreign or missing resource.** The gate never reveals whether a
  resource it can't authorize actually exists — a cross-tenant probe and a genuine 404 are
  indistinguishable to the caller.

### 5.2 Reads need this gate too — membership alone is not authorization

A read gate that asks only *"is the caller a member of the workspace that owns this
resource?"* passes for a user who belongs to two workspaces while the **other** one is
active. That is not a cross-user breach, but it breaks the isolation model the rest of the
API enforces, and it is worse than it looks in the UI: a stale detail route that should
render "not found" instead renders the other tenant's real data.

This was live: `canAccessPipeline` gated three pipeline **reads** on membership only, so with
`demo` active, `GET /pipelines/{other-ws-id}` correctly 404'd while `/table-stats`,
`/monitoring/overview` and `/schedules` returned 200 with real schema and row counts. It is
**deleted**; all three now use `requirePipelineWorkspaceRole(…, security.WSViewer)`, and
`monitoring.go` carries a tombstone comment so it is not reintroduced.

**Rule: a `/pipelines/:id/*` or `/connections/:id` handler gets a gate regardless of HTTP
verb.** GET differs from POST only in the minimum role (`WSViewer` vs `WSMember`+), never in
whether the active workspace is checked. Anything that filters by `resource_id` alone —
`ListPipelineSchedules`' query is the example — has no other predicate keeping another
workspace's rows out of the response.

### 5.3 `created_by` is not an authorization predicate

`WHERE … AND p.created_by = <caller>` looks like a tenancy check and is not one. It asks
whether the caller *once created* the row, which is **workspace-blind in both directions**:

- the creator keeps reading the row from a workspace they have since switched **away** from —
  the exact scenario §5.2 describes, except membership isn't even required;
- a **teammate** in the resource's own workspace is denied, or silently served an empty list,
  because they didn't create it. Collaboration is the point of a shared workspace.

Seven pipeline observability endpoints were live with this predicate — `/checkpoints`,
`/events`, `POST /events/raw`, `/compare`, `/trends`, the `/events/stream` WebSocket, and the
chat `diagnose` resolver. With `demo` active, `/events` answered 100 KB of execution ids,
trace ids and data-plane metrics for a `personal` pipeline. All now use
`requirePipelineWorkspaceRole(…, security.WSViewer)`.

Three ordering rules fall out of that fix, and each one is a way the gate can be present but
useless:

1. **Gate before parameter validation.** `/compare` answered 400 "missing execution_a" for a
   pipeline the caller couldn't see — which distinguishes "exists, bad params" from "does not
   exist" just as surely as a 403 would.
2. **Gate before RBAC.** `power_user`/`admin` is a role *within* a workspace, not a passport
   across them. `/events/raw` must 404 for an admin acting in the wrong workspace — and 404
   before the audit row is written, since the access never happened.
3. **Gate before the WebSocket upgrade.** A socket authorized once keeps pushing for as long
   as it stays open, so a stream opened before a switch outlives it. The gate must run on the
   HTTP request, ahead of `upgrader.Upgrade`.

**Then delete the creator predicate from the query itself.** Once the gate proves tenancy, a
surviving `AND p.created_by = $n` no longer protects anything — it only hides a teammate's
rows. Both halves are one change; keeping the predicate "just in case" reintroduces the second
failure mode above.

---

## 6. Cross-tenant isolation invariant

**Every per-connection / per-pipeline / per-execution DB read MUST be scoped to the caller's
workspace** (`WHERE workspace_id = $active`) *and* pass a membership gate. This is a P0 rule
with no exceptions — no "internal" endpoint may skip it. Examples of the scope predicate in
list/read paths: `ListConnections` (`WHERE workspace_id = $1`,
[`connections.go:504`](../../api-gateway/internal/handlers/connections.go)),
`ListExecutions` / `GetExecution` / `CancelExecution`
(`WHERE p.workspace_id = $1`, [`pipelines.go:3230`](../../api-gateway/internal/handlers/pipelines.go) and siblings).

This workspace-scoping is the successor to the older per-user `AND user_id = $X` invariant
noted in [ARCHITECTURE.md §6](../../ARCHITECTURE.md); as sites migrate to the workspace model
they scope by `workspace_id` + membership instead. When adding a new read path, scope it the
same way — see §9.

---

## 7. Owner-safe mutations & personal-workspace immutability

Two invariants protect the workspace from being orphaned or corrupted; both are enforced
server-side (the UI only mirrors them):

- **Last-owner guard.** A workspace must always retain at least one owner. Removing the last
  owner or demoting the last owner is rejected atomically (`applyOwnerSafeMutation` in
  [`workspaces.go`](../../api-gateway/internal/handlers/workspaces.go)) with a **409**. This
  covers `ChangeMemberRole`, `RemoveMember`, and self-`Leave`.
- **Personal-workspace immutability.** The personal workspace cannot be renamed, deleted, or
  left. `DeleteWorkspace` is **owner-only**, returns **409** on a personal workspace, and
  **409** on a non-empty workspace. Administration endpoints gate the *path* workspace via
  `requireWorkspaceParamRole` (IDOR-safe), and the role gate fires **before** the request body
  is bound (e.g. `ChangeMemberRole` — gate at [`workspaces.go:391`](../../api-gateway/internal/handlers/workspaces.go),
  bind after).

---

## 8. Frontend mirror — advisory only

The UI has its own role logic so gated controls (disabled buttons, hidden sections, the
read-only permissions matrix) stay consistent, but it is **advisory only** — every mutation
is independently re-enforced by the api-gateway. Never rely on the client for isolation.

| Piece | File | Role |
|---|---|---|
| Roles module | [`lib/workspace/roles.ts`](../../frontend/src/lib/workspace/roles.ts) | `roleRank` / `meetsRole` / `can(action)` + capability matrix + labels/badge variants. Mirrors the backend hierarchy and its fail-closed `Meets()`. One source for every client gate — replaces the per-component `roleRank` duplication bug class. |
| Context | [`contexts/WorkspaceContext.tsx`](../../frontend/src/contexts/WorkspaceContext.tsx) | `WorkspaceProvider` + `useWorkspace()` / `useWorkspaceRole()`: one `/workspaces` fetch, active-ws tracking, exposes `can()`/`meets()`/`refresh()`/`is_personal`; fails closed outside a provider. |
| Slug preview | [`lib/workspace/slug.ts`](../../frontend/src/lib/workspace/slug.ts) | `toWorkspaceSlug()` — live create-dialog preview; mirrors the api-gateway `toSlug` (`"" → "workspace"`). |
| Settings hub | [`workspace/settings/page.tsx`](../../frontend/src/app/(dashboard)/workspace/settings/page.tsx) | Tabbed General / Members & invitations / Roles (`?tab=` deep-link) + persistent role banner. |

The client action → backend guard mapping is documented inline in
[`roles.ts:19-35`](../../frontend/src/lib/workspace/roles.ts): `view`→viewer+, `create_pipeline`→member+,
`manage_members`/`rename_workspace`→admin+, `delete_workspace`→owner, `leave_workspace`→any member
(the backend rejects the last owner with 409; the call site hides it on personal workspaces).

### 8.1 The three client-side leaks the backend cannot close

Role gating is advisory, but these three are not — no server gate can see them, because the
data never leaves the browser or the request never happens. Each has one owner:

| Leak | Owner | Rule |
|---|---|---|
| **The switch itself** — a detail URL pins a resource id owned by the workspace being left, so the route is a dead end the moment the selection changes | the `onActiveWorkspaceChange` subscription in [`Header.tsx`](../../frontend/src/components/layout/Header.tsx), via [`scoped-routes.ts`](../../frontend/src/lib/workspace/scoped-routes.ts) | Handle the **change**, never the click. The selection also changes from another tab (`storage` event) and from the delete flow re-pointing it; a click handler misses both and leaves the previous tenant's resource on screen under the new header. |
| **In-flight responses** — `authFetch` stamps `X-Workspace-ID` at call time, so an A-scoped response can land after the switch to B | `captureWorkspace()` in [`active-workspace.ts`](../../frontend/src/lib/workspace/active-workspace.ts) | Capture before the await, drop the response if `isStale()`. Every workspace-scoped fetch needs it; a *user*-scoped one (`/workspaces`) does not — say so in a comment where you skip it. |
| **localStorage** — per-origin, not per-tenant, so a bare key survives the switch and the next workspace reads the previous one's cache | [`scoped-storage.ts`](../../frontend/src/lib/workspace/scoped-storage.ts) | Any value derived from workspace-scoped API data is keyed `base:<workspaceId>`. Keys already carrying a workspace-unique id and pure UI prefs are exempt. Logout clears **every** scope, not just the active one. |

---

## 9. Extending — adding a new workspace-scoped resource or endpoint

1. **Schema:** add a `workspace_id UUID NOT NULL` column referencing `workspaces(id)`; backfill
   existing rows in the migration before `SET NOT NULL`.
2. **Reads:** scope every query `WHERE workspace_id = $active` (resolve active ws with
   `activeWorkspaceID(c)` / `resolveActiveWorkspace(c)`).
3. **Reads AND mutations on a specific id:** gate with
   `requireResourceRole(c, "<table>", id, <minRole>)` — and add `"<table>"` to the
   `resourceTables` allowlist
   ([`workspace_context.go:25`](../../api-gateway/internal/handlers/workspace_context.go)), or the
   gate 500s by design. Put the gate **before** binding the request body. GET differs only in
   the minimum role (`WSViewer`), never in whether the gate runs — see §5.2.
4. **Foreign/missing → 404**, insufficient role → 403, personal-ws-forbidden → 409. Never
   reveal existence of a resource the caller can't authorize.
5. **Frontend:** gate controls through `useWorkspaceRole().can(action)`; if you add a new
   action, add it to `WorkspaceAction` and `can()` in `roles.ts` — do not re-introduce a
   per-component `roleRank`. A new **detail route** also needs a row in `SCOPED_SECTIONS` and
   its own `not-found.tsx`; a new **cached value** needs `workspaceScopedKey`; a new
   workspace-scoped **fetch** needs `captureWorkspace()` — see §8.1.

---

## 10. Verification

The model was verified end-to-end on Azure `staging` (2026-07-01) with a 4-user × 2-tenant
matrix: IDOR foreign GET/DELETE → 404, symmetric list scoping, RBAC viewer-create 403 /
member-create 201 / member-invite 403, personal-ws 409s, execution scoping (foreign
exec/cancel → 404), membership guards (last-owner 409, non-admin-remove 403, self-leave 200,
non-empty-delete 409, non-owner-delete 403), and audit `workspace_id` stamping — **34/34**,
cross-checked by five independent cold reviewers (unanimous pass, no cross-tenant leak). Unit
coverage: `workspace_context_test.go`, `workspace_delete_test.go`, `workspace_invites_test.go`,
`workspace_member_mgmt_test.go`, `connection_mutation_scoping_test.go` (Go); the frontend
roles/context/settings suites (vitest).

---

## 11. See also

- [ARCHITECTURE.md §6 — cross-cutting invariants](../../ARCHITECTURE.md)
- [`docs/services/API_GATEWAY_SERVICE.md`](../services/API_GATEWAY_SERVICE.md)
- [`docs/services/FRONTEND_SERVICE.md`](../services/FRONTEND_SERVICE.md)
</content>
