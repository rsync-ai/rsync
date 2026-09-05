import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { render, screen, waitFor, fireEvent } from "@testing-library/react"
import type { Mock } from "vitest"

import InviteLanding from "@/components/workspace/InviteLanding"
import { authFetch } from "@/lib/api/auth-fetch"
import { setActiveWorkspaceId } from "@/lib/workspace/active-workspace"
import { toast } from "sonner"

// P5b-UI #3: the /invite/[token] landing page. An invitee opens an invite link and
// must be able to (a) PREVIEW the workspace publicly without a session, (b) ACCEPT
// when logged in with the matching email, (c) be routed to login/signup with a
// return path when logged out, and (d) be blocked client-side when logged in as a
// DIFFERENT email (the backend 403 email-bind, mirrored so we don't burn a round
// trip). The preview/accept endpoints already exist on api.ts (WORKSPACES.INVITE_*).

const pushMock = vi.fn()
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: pushMock, replace: pushMock }),
}))
vi.mock("next/link", () => ({
  default: ({ href, children, ...rest }: { href: string; children: React.ReactNode }) => (
    <a href={typeof href === "string" ? href : "#"} {...rest}>
      {children}
    </a>
  ),
}))
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("@/lib/workspace/active-workspace", () => ({ setActiveWorkspaceId: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  if (!Element.prototype.hasPointerCapture) Element.prototype.hasPointerCapture = () => false
  if (!Element.prototype.scrollIntoView) Element.prototype.scrollIntoView = () => {}
})

const PREVIEW = {
  workspace_id: "ws-1",
  workspace_name: "Acme",
  email: "bob@acme.com",
  role: "member",
  status: "pending",
  expires_at: "2099-01-01T00:00:00Z",
}

interface MeStub {
  ok: boolean
  user?: { user_id: string; email: string }
}
interface ResStub {
  ok: boolean
  status?: number
  body?: unknown
}
interface FetchPlan {
  me?: MeStub
  preview?: ResStub
  accept?: ResStub
}

function installFetch(plan: FetchPlan) {
  ;(authFetch as Mock).mockImplementation(async (url: string, opts?: RequestInit) => {
    const u = String(url)
    const method = (opts?.method ?? "GET").toUpperCase()

    if (u.includes("/auth/me")) {
      const me = plan.me ?? { ok: true, user: { user_id: "u-bob", email: "bob@acme.com" } }
      return me.ok
        ? { ok: true, status: 200, json: async () => me.user }
        : { ok: false, status: 401, json: async () => ({ error: "unauthorized" }) }
    }
    // accept must be checked before the generic preview regex (it also matches)
    if (/\/workspace-invites\/[^/]+\/accept$/.test(u) && method === "POST") {
      const a = plan.accept ?? {
        ok: true,
        status: 200,
        body: { workspace_id: "ws-1", role: "member", status: "accepted" },
      }
      return { ok: a.ok, status: a.status ?? (a.ok ? 200 : 400), json: async () => a.body ?? {} }
    }
    if (/\/workspace-invites\/[^/]+$/.test(u) && method === "GET") {
      const p = plan.preview ?? { ok: true, status: 200, body: PREVIEW }
      return { ok: p.ok, status: p.status ?? (p.ok ? 200 : 400), json: async () => p.body ?? {} }
    }
    return { ok: true, status: 200, json: async () => ({}) }
  })
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe("InviteLanding", () => {
  it("shows the workspace preview and an Accept button to a logged-in matching invitee", async () => {
    installFetch({
      me: { ok: true, user: { user_id: "u-bob", email: "bob@acme.com" } },
      preview: { ok: true, body: PREVIEW },
    })

    render(<InviteLanding token="tok-1" />)

    expect(await screen.findByText("Acme")).toBeInTheDocument()
    // the role offered and the address the invite was issued to are both surfaced
    expect(screen.getByText(/member/i)).toBeInTheDocument()
    expect(screen.getByText(/bob@acme\.com/)).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /accept invitation/i })).toBeInTheDocument()
  })

  it("accepts the invite, marks the joined workspace active, and redirects home", async () => {
    installFetch({
      me: { ok: true, user: { user_id: "u-bob", email: "bob@acme.com" } },
      preview: { ok: true, body: PREVIEW },
      accept: { ok: true, status: 200, body: { workspace_id: "ws-1", role: "member", status: "accepted" } },
    })

    render(<InviteLanding token="tok-1" />)
    fireEvent.click(await screen.findByRole("button", { name: /accept invitation/i }))

    await waitFor(() => {
      const acceptCall = (authFetch as Mock).mock.calls.find(
        ([u, o]) => /\/workspace-invites\/tok-1\/accept$/.test(String(u)) && (o?.method ?? "").toUpperCase() === "POST",
      )
      expect(acceptCall).toBeTruthy()
    })
    await waitFor(() => expect(setActiveWorkspaceId).toHaveBeenCalledWith("ws-1"))
    expect(toast.success).toHaveBeenCalled()
    expect(pushMock).toHaveBeenCalledWith("/")
  })

  it("blocks accept and offers an account switch when signed in as a different email", async () => {
    installFetch({
      me: { ok: true, user: { user_id: "u-carol", email: "carol@other.com" } },
      preview: { ok: true, body: PREVIEW },
    })

    render(<InviteLanding token="tok-1" />)
    await screen.findByText("Acme")

    // mirrors the backend email-bind 403: don't even offer Accept, explain why,
    // and point at re-auth with a return path back to this invite.
    expect(screen.getByText(/signed in as/i)).toBeInTheDocument()
    expect(screen.getByText(/carol@other\.com/)).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: /accept invitation/i })).toBeNull()
    const switchLink = screen.getByRole("link", { name: /different account/i })
    expect(switchLink.getAttribute("href")).toBe(`/logout?next=${encodeURIComponent("/invite/tok-1")}`)
    // a11y: the CTA is a single anchor, not a <button> nested inside an <a>
    expect(screen.queryByRole("button", { name: /different account/i })).toBeNull()
  })

  it("routes a logged-out invitee to login/signup carrying a return path, with no Accept", async () => {
    installFetch({ me: { ok: false }, preview: { ok: true, body: PREVIEW } })

    render(<InviteLanding token="tok-1" />)
    await screen.findByText("Acme")

    const loginLink = screen.getByRole("link", { name: /log in/i })
    const signupLink = screen.getByRole("link", { name: /create an account/i })
    expect(loginLink.getAttribute("href")).toBe(`/login?next=${encodeURIComponent("/invite/tok-1")}`)
    expect(signupLink.getAttribute("href")).toBe(`/signup?next=${encodeURIComponent("/invite/tok-1")}`)
    expect(screen.queryByRole("button", { name: /accept invitation/i })).toBeNull()
    // a11y: CTAs are single anchors, not <button> nested inside <a>
    expect(screen.queryByRole("button", { name: /log in to accept/i })).toBeNull()
    expect(screen.queryByRole("button", { name: /create an account/i })).toBeNull()
  })

  it("shows an expired message for a 410 expired preview", async () => {
    installFetch({
      me: { ok: true, user: { user_id: "u-bob", email: "bob@acme.com" } },
      preview: { ok: false, status: 410, body: { error: "invite has expired", status: "expired" } },
    })

    render(<InviteLanding token="tok-1" />)
    expect(await screen.findByText(/expired/i)).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: /accept invitation/i })).toBeNull()
  })

  it("shows an invalid message for a 404 preview", async () => {
    installFetch({
      me: { ok: true, user: { user_id: "u-bob", email: "bob@acme.com" } },
      preview: { ok: false, status: 404, body: { error: "invite not found" } },
    })

    render(<InviteLanding token="tok-1" />)
    expect(await screen.findByText(/invalid|no longer exists/i)).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: /accept invitation/i })).toBeNull()
  })

  it("shows a no-longer-valid message for a 410 already-accepted preview", async () => {
    installFetch({
      me: { ok: true, user: { user_id: "u-bob", email: "bob@acme.com" } },
      preview: { ok: false, status: 410, body: { error: "invite is no longer valid", status: "accepted" } },
    })

    render(<InviteLanding token="tok-1" />)
    expect(await screen.findByText(/no longer valid/i)).toBeInTheDocument()
  })

  it("surfaces a 409 already-used error on accept without redirecting", async () => {
    installFetch({
      me: { ok: true, user: { user_id: "u-bob", email: "bob@acme.com" } },
      preview: { ok: true, body: PREVIEW },
      accept: { ok: false, status: 409, body: { error: "invite has already been used or revoked" } },
    })

    render(<InviteLanding token="tok-1" />)
    fireEvent.click(await screen.findByRole("button", { name: /accept invitation/i }))

    await waitFor(() => expect(toast.error).toHaveBeenCalled())
    expect(pushMock).not.toHaveBeenCalled()
    expect(setActiveWorkspaceId).not.toHaveBeenCalled()
  })

  it("surfaces a 403 email-mismatch error on accept without redirecting", async () => {
    // emails match client-side (button shows), but the backend is authoritative and
    // returns 403 (e.g. a race or a tampered request). The handler must surface it.
    installFetch({
      me: { ok: true, user: { user_id: "u-bob", email: "bob@acme.com" } },
      preview: { ok: true, body: PREVIEW },
      accept: { ok: false, status: 403, body: { error: "this invite was issued to a different email address" } },
    })

    render(<InviteLanding token="tok-1" />)
    fireEvent.click(await screen.findByRole("button", { name: /accept invitation/i }))

    await waitFor(() => expect(toast.error).toHaveBeenCalled())
    expect(pushMock).not.toHaveBeenCalled()
    expect(setActiveWorkspaceId).not.toHaveBeenCalled()
  })
})
