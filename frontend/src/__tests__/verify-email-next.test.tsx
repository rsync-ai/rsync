import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { render, screen, fireEvent } from "@testing-library/react"

import VerifyEmailPage from "@/app/(auth)/verify-email/page"
import { POST_VERIFY_NEXT_KEY } from "@/lib/auth/safe-next"

// P5b-UI #3 (review remediation): the email-verification round-trip must not strand an
// invitee. signup stashes a safe next in sessionStorage when verification is required;
// after the user verifies, this page must send them THERE, not hard-coded to "/".

const pushMock = vi.fn()
let searchParamsStr = "token=tok"
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
  if (!Element.prototype.scrollIntoView) Element.prototype.scrollIntoView = () => {}
})

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

beforeEach(() => {
  vi.clearAllMocks()
  searchParamsStr = "token=tok"
  installMemorySessionStorage()
  vi.stubGlobal("fetch", vi.fn(async () => ({ ok: true, status: 200, json: async () => ({}) })))
})

describe("VerifyEmailPage post-verify next", () => {
  it("sends the user to a stashed safe next after verifying, then clears it", async () => {
    window.sessionStorage.setItem(POST_VERIFY_NEXT_KEY, "/invite/tok-1")

    render(<VerifyEmailPage />)
    await screen.findByText(/email verified/i)

    fireEvent.click(screen.getByRole("button", { name: /continue/i }))
    expect(pushMock).toHaveBeenCalledWith("/invite/tok-1")
    expect(window.sessionStorage.getItem(POST_VERIFY_NEXT_KEY)).toBeNull()
  })

  it("defaults to the dashboard when nothing is stashed", async () => {
    render(<VerifyEmailPage />)
    await screen.findByText(/email verified/i)

    fireEvent.click(screen.getByRole("button", { name: /dashboard/i }))
    expect(pushMock).toHaveBeenCalledWith("/")
  })

  it("ignores an unsafe stashed next and falls back to the dashboard", async () => {
    window.sessionStorage.setItem(POST_VERIFY_NEXT_KEY, "//evil.example.com")

    render(<VerifyEmailPage />)
    await screen.findByText(/email verified/i)

    fireEvent.click(screen.getByRole("button", { name: /dashboard/i }))
    expect(pushMock).toHaveBeenCalledWith("/")
    expect(pushMock).not.toHaveBeenCalledWith("//evil.example.com")
  })
})
