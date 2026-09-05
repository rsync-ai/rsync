import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { render, screen, waitFor, fireEvent } from "@testing-library/react"
import { toast } from "sonner"
import type { Mock } from "vitest"

import SignupClient from "@/app/(auth)/signup/signup-client"

// The register API answers failures with a machine code AND a sentence:
//   {"error": "invite_required", "message": "Registration is invite-only. Please use an invite link."}
// signup-client.tsx:157 reads `data.error || data.message`, so the code wins and
// the person trying to sign up is shown "invite_required" — a token meant for
// the client's own branching, not for a human.
//
// This matters more now that a failed registration-policy read returns 503
// registration_unavailable (auth.go Register): without this, the entire
// user-facing explanation of why signup stopped is one snake_case word.

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn() }),
  useSearchParams: () => new URLSearchParams(""),
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

beforeEach(() => {
  vi.clearAllMocks()
})

function mockRegisterFailure(status: number, body: Record<string, unknown>) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => ({ ok: false, status, json: async () => body })),
  )
}

function fillAndSubmit() {
  fireEvent.change(screen.getByLabelText(/work email/i), { target: { value: "bob@acme.com" } })
  fireEvent.change(screen.getByLabelText("Password"), { target: { value: "password123" } })
  fireEvent.change(screen.getByLabelText(/confirm password/i), { target: { value: "password123" } })
  fireEvent.click(screen.getByRole("button", { name: /create account/i }))
}

const errorToast = toast.error as unknown as Mock

describe("Signup shows the sentence, not the error code", () => {
  it("surfaces the human message when the instance cannot read its registration policy", async () => {
    mockRegisterFailure(503, {
      error: "registration_unavailable",
      message: "Registration is temporarily unavailable. Please try again shortly.",
    })

    render(<SignupClient />)
    fillAndSubmit()

    await waitFor(() => expect(errorToast).toHaveBeenCalled())
    expect(errorToast).toHaveBeenCalledWith(
      "Registration is temporarily unavailable. Please try again shortly.",
    )
    expect(errorToast).not.toHaveBeenCalledWith("registration_unavailable")
  })

  it("surfaces the human message on an invite-only instance", async () => {
    mockRegisterFailure(403, {
      error: "invite_required",
      message: "Registration is invite-only. Please use an invite link.",
    })

    render(<SignupClient />)
    fillAndSubmit()

    await waitFor(() => expect(errorToast).toHaveBeenCalled())
    expect(errorToast).toHaveBeenCalledWith("Registration is invite-only. Please use an invite link.")
  })

  // The bound: when the API sends only a code, showing it still beats a generic
  // "Registration failed" that says nothing at all.
  it("falls back to the code when there is no message", async () => {
    mockRegisterFailure(409, { error: "Email already registered" })

    render(<SignupClient />)
    fillAndSubmit()

    await waitFor(() => expect(errorToast).toHaveBeenCalled())
    expect(errorToast).toHaveBeenCalledWith("Email already registered")
  })
})
