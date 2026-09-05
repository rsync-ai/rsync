/**
 * KI-HITL-GENERIC-SUBMIT-WIRED-TO-CONSOLE-WARN — a parked run must not offer a
 * button that throws the click away.
 *
 * The live-state panel's generic HITL arm handed `GenericHitlForm` an `onSubmit`
 * whose entire body was `console.warn("[HITL] Generic schema submit not wired
 * for this request type", data)`. The pipeline was stopped waiting for an
 * answer, the operator typed one, clicked an enabled Submit, and the answer went
 * to the browser console. Nothing posted, nothing surfaced, the run stayed
 * parked. That is the worst available outcome: a dead end that looks like a
 * working control.
 *
 * The same `? :` chain ended in a bare `: null`, which is the same defect with
 * no button at all — a `policy_approval` park, or a generic park carrying no
 * `input_schema`, rendered the big "Pipeline Paused - Action Required" panel
 * with an empty body and no action anywhere in it.
 *
 * The fix stops at *failing visibly*, deliberately. `hitlResumeEndpointFor`
 * (api-gateway/internal/handlers/pipeline_state.go) returns "" for an unknown
 * blocking type, so there is nowhere correct to post. Wiring the form to
 * `hitl/node-input` — which does accept an arbitrary `config_patch` — would
 * return 200 while the workflow ignored the fields, converting a visible dead
 * end into an invisible one.
 *
 * Case (e) is the control. Without it, a blanket `disabled` — which would break
 * PipelineAccordionView's live generic arm, the one caller that still has
 * somewhere to send the data — looks exactly like a correct fix.
 */

import { describe, it, expect, vi, beforeAll, beforeEach, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import "@testing-library/jest-dom"

// Radix's Checkbox measures itself through ResizeObserver, which jsdom does not
// implement; without this the boolean field below throws during layout effects
// and the whole tree unmounts. Same guarded stub the other suites in this
// directory install.
beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
})

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
}))

// The panel's liveness probe polls on its own timer and has nothing to do with
// HITL rendering; an unmocked one would only add noise.
vi.mock("@/lib/hooks/usePipelineRuntime", () => ({
  usePipelineRuntime: () => ({ runtime: null, loading: false, error: null }),
}))

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...args: unknown[]) => authFetch(...args),
  authFetchOrThrow: (...args: unknown[]) => authFetch(...args),
}))

import { PipelineLiveStatePanel } from "@/components/pipeline/PipelineLiveStatePanel"
import {
  GenericHitlForm,
  UNSUPPORTED_HITL_TITLE,
  UNSUPPORTED_HITL_MESSAGE,
  type JsonSchema,
} from "@/components/chat/GenericHitlForm"

/**
 * `hitlKind` classifies on substrings of `blocking_reason.type`: anything
 * containing "table" / "connection" / "connector" is routed to a purpose-built
 * arm, and "policy" / "approval" to `policy_approval`. A type that lands on
 * "generic" — the arm under test — must therefore avoid all five. Hence
 * `needs_export_sign_off` rather than anything with "approval" in it.
 */
const GENERIC_TYPE = "needs_export_sign_off"

const APPROVE_SCHEMA = {
  type: "object",
  required: ["approve"],
  properties: {
    approve: { type: "boolean", description: "Approve" },
  },
}

/**
 * Deliberately has no `required` array. `handleSubmit` validates required
 * fields before it calls the handler, so a schema with an unfilled required
 * field never reaches the handler at all — a submit test built on one would
 * pass against the broken code for the wrong reason. With nothing required,
 * validation is a no-op and the only thing standing between the click and the
 * old `console.warn` is the fix itself.
 */
const NOTE_SCHEMA = {
  type: "object",
  properties: {
    note: { type: "string", description: "Anything you want to say" },
  },
}

function jsonOk(body: unknown) {
  return {
    ok: true,
    status: 200,
    json: async () => body,
    text: async () => JSON.stringify(body),
  }
}

function parkedOn(type: string, details?: Record<string, unknown>) {
  return {
    schema_version: 1,
    pipeline_id: "p1",
    execution_id: "e1",
    status: "waiting_for_user",
    message: "Waiting",
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
    current_stage: "transform",
    blocking_reason: {
      type,
      description: "Approve the export",
      ...(details ? { details } : {}),
    },
  }
}

function mountWith(state: Record<string, unknown>) {
  authFetch.mockImplementation(async (url: string) => {
    const u = String(url)
    if (u.includes("/state")) return jsonOk(state)
    if (u.includes("/events")) return jsonOk({ events: [] })
    return jsonOk({})
  })
  return render(<PipelineLiveStatePanel pipelineId="p1" />)
}

beforeEach(() => authFetch.mockReset())
afterEach(() => cleanup())

describe("a generic park says so instead of offering a button that discards the click", () => {
  it("renders the schema read-only, with Submit disabled and the reason visible", async () => {
    mountWith(parkedOn(GENERIC_TYPE, { input_schema: APPROVE_SCHEMA }))

    // Before the fix: no notice anywhere, and Submit rendered enabled.
    expect(await screen.findByText(UNSUPPORTED_HITL_TITLE)).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /submit/i })).toBeDisabled()

    // The raw blocking type is the one fact this build genuinely has about the
    // request, so it is printed for the operator to quote in a bug report.
    expect(screen.getByText(GENERIC_TYPE)).toBeInTheDocument()
  })

  it("swallows nothing: submitting the form neither posts nor warns to the console", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {})
    try {
      const { container } = mountWith(parkedOn(GENERIC_TYPE, { input_schema: NOTE_SCHEMA }))
      await screen.findByText(UNSUPPORTED_HITL_TITLE)

      const form = container.querySelector("form")
      expect(form).not.toBeNull()
      fireEvent.submit(form!)

      // Nothing was sent...
      expect(authFetch.mock.calls.every(([u]) => !String(u).includes("/hitl/"))).toBe(true)
      // ...and, the actual defect, nothing was quietly dropped into the console
      // either. On HEAD this call was the entire handler.
      expect(warn).not.toHaveBeenCalled()
    } finally {
      warn.mockRestore()
    }
  })

  it("still explains itself when the park carries no input_schema at all", async () => {
    const { container } = mountWith(parkedOn(GENERIC_TYPE))

    // Before the fix the chain fell to `: null` and the HITL panel body was empty.
    expect(await screen.findByText(UNSUPPORTED_HITL_TITLE)).toBeInTheDocument()
    expect(screen.getByText(GENERIC_TYPE)).toBeInTheDocument()
    // No schema means no form to show, not an empty form.
    expect(container.querySelector("form")).toBeNull()
  })

  it("explains a policy_approval park too, which had no branch of its own", async () => {
    mountWith(parkedOn("policy_approval"))

    expect(await screen.findByText(UNSUPPORTED_HITL_TITLE)).toBeInTheDocument()
    expect(screen.getByText("policy_approval")).toBeInTheDocument()
  })
})

describe("GenericHitlForm decides read-only from the presence of a handler", () => {
  it("CONTROL: a caller that does pass onSubmit keeps a working, enabled form", () => {
    // PipelineAccordionView still passes one. If a fix disabled Submit
    // unconditionally, this is the case that catches it — the panel-side
    // assertions above cannot tell the two apart.
    const onSubmit = vi.fn()
    const schema: JsonSchema = {
      type: "object",
      required: ["reason"],
      properties: { reason: { type: "string", description: "Why" } },
    }
    const { container } = render(<GenericHitlForm schema={schema} onSubmit={onSubmit} />)

    const submit = screen.getByRole("button", { name: /submit/i })
    expect(submit).toBeEnabled()
    expect(screen.queryByText(UNSUPPORTED_HITL_MESSAGE)).not.toBeInTheDocument()

    const input = container.querySelector("#reason") as HTMLInputElement
    expect(input).not.toBeNull()
    expect(input.disabled).toBe(false)
    fireEvent.change(input, { target: { value: "approved by ops" } })
    fireEvent.submit(container.querySelector("form")!)

    expect(onSubmit).toHaveBeenCalledTimes(1)
    expect(onSubmit).toHaveBeenCalledWith({ reason: "approved by ops" })
  })

  it("omitting onSubmit renders the notice, disables Submit and disables the fields", () => {
    const schema: JsonSchema = {
      type: "object",
      required: ["reason"],
      properties: { reason: { type: "string", description: "Why" } },
    }
    const { container } = render(<GenericHitlForm schema={schema} />)

    expect(screen.getByText(UNSUPPORTED_HITL_MESSAGE)).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /submit/i })).toBeDisabled()
    // Read-only for real, not just a dead button.
    expect((container.querySelector("#reason") as HTMLInputElement).disabled).toBe(true)
  })
})
