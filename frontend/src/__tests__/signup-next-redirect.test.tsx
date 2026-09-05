import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { render, screen, waitFor, fireEvent } from "@testing-library/react"
import type { Mock } from "vitest"

import SignupClient from "@/app/(auth)/signup/signup-client"
import { POST_VERIFY_NEXT_KEY } from "@/lib/auth/safe-next"

// P5b-UI #3 (supporting + review remediation): the invite landing page sends a
// logged-out invitee to /signup?next=/invite/:token so they can return and accept.
// Signup must (a) honor a SAFE next on success, (b) reject protocol-relative /
// absolute values (open redirect), (c) actually hit /auth/register with the form
// body, and (d) when email verification is required, stash the safe next so the
// invitee returns after verifying instead of being stranded at the dashboard.

const pushMock = vi.fn()
// mutable so each test can vary the query string the page reads
let searchParamsStr = ""
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: pushMock }),
  useSearchParams: () => new URLSearchParams(searchParamsStr),
}))
vi.mock("next/link", () => ({
  default: ({ href, children, ...rest }: { href: string; children: React.ReactNode }) => (
    <a href={typeof href === "string" ? href : "#"} {...rest}>
      {children}
    </a>
  ),
}))
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

// jsdom's web storage is not fully functional under this harness — install a minimal
// in-memory sessionStorage so the post-verify-next bridge can be asserted.
function installMemorySessionStorage() {
  const store: Record<string, string> = {}
  const ss: Storage = {
    getItem: (k) => (k in store ? store[k] : null),
    setItem: (k, v) => {
      store[k] = String(v)
    },
    removeItem: (k) => {
      delete store[k]
    },
    clear: () => {
      for (const k of Object.keys(store)) delete store[k]
    },
    key: (i) => Object.keys(store)[i] ?? null,
    get length() {
      return Object.keys(store).length
    },
  }
  Object.defineProperty(window, "sessionStorage", { value: ss, configurable: true, writable: true })
}

function fillAndSubmit() {
  fireEvent.change(screen.getByLabelText(/work email/i), { target: { value: "bob@acme.com" } })
  fireEvent.change(screen.getByLabelText("Password"), { target: { value: "password123" } })
  fireEvent.change(screen.getByLabelText(/confirm password/i), { target: { value: "password123" } })
  fireEvent.click(screen.getByRole("button", { name: /create account/i }))
}

function mockRegister(emailVerified: boolean) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => ({
      ok: true,
      status: 201,
      json: async () => ({ email: "bob@acme.com", email_verified: emailVerified }),
    })),
  )
}

beforeEach(() => {
  vi.clearAllMocks()
  searchParamsStr = ""
  installMemorySessionStorage()
  mockRegister(true)
})

describe("SignupClient next handling", () => {
  it("redirects to a safe next after signup, hitting /auth/register with the form body", async () => {
    searchParamsStr = `next=${encodeURIComponent("/invite/tok-1")}`

    render(<SignupClient />)
    fillAndSubmit()

    await waitFor(() => expect(pushMock).toHaveBeenCalledWith("/invite/tok-1"))

    // not tautological: prove the actual register call + body, so a wrong
    // endpoint/method/body would fail this test.
    const calls = (global.fetch as Mock).mock.calls
    const reg = calls.find((c) => String(c[0]).includes("/api/v1/auth/register"))
    expect(reg).toBeTruthy()
    expect((reg![1]?.method ?? "").toUpperCase()).toBe("POST")
    const body = JSON.parse(reg![1].body as string)
    expect(body.email).toBe("bob@acme.com")
    expect(body.password).toBe("password123")
  })

  it("ignores an absolute/external next and falls back to home", async () => {
    searchParamsStr = `next=${encodeURIComponent("https://evil.example.com")}`

    render(<SignupClient />)
    fillAndSubmit()

    await waitFor(() => expect(pushMock).toHaveBeenCalled())
    expect(pushMock).toHaveBeenCalledWith("/")
    expect(pushMock).not.toHaveBeenCalledWith("https://evil.example.com")
  })

  it("ignores a protocol-relative next (open redirect) and falls back to home", async () => {
    searchParamsStr = `next=${encodeURIComponent("//evil.example.com")}`

    render(<SignupClient />)
    fillAndSubmit()

    await waitFor(() => expect(pushMock).toHaveBeenCalled())
    expect(pushMock).toHaveBeenCalledWith("/")
    expect(pushMock).not.toHaveBeenCalledWith("//evil.example.com")
  })

  it("stashes a safe next for after verification when email_verified is false", async () => {
    searchParamsStr = `next=${encodeURIComponent("/invite/tok-1")}`
    mockRegister(false)

    render(<SignupClient />)
    fillAndSubmit()

    // pending-verification screen, NOT a redirect — the invite context must survive
    // the email round-trip.
    expect(await screen.findByText(/check your email/i)).toBeInTheDocument()
    expect(window.sessionStorage.getItem(POST_VERIFY_NEXT_KEY)).toBe("/invite/tok-1")
    expect(pushMock).not.toHaveBeenCalled()
  })
})
