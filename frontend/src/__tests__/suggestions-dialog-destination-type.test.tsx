/**
 * The setup suggestions must know where the data lands, and must not read as
 * something Apply & Continue will do when it will not.
 *
 * Prod 2026-09-26 (pipeline 93e1e11d, Postgres -> GCS): the dialog offered 14
 * "Add index on ..." optimizations for an object store, because it told the
 * service the destination was the placeholder "storage". The button read
 * "Apply & Continue (1)" (the PII rule), under copy that said everything listed
 * "will be applied". Nothing applies an optimization, on any destination.
 */
import { describe, it, expect, vi, beforeAll, beforeEach, afterEach } from "vitest"
import { render, screen, cleanup } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver
})

vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(async () => ({ ok: true, status: 200, json: async () => ({}) })),
}))

const generateSuggestions = vi.fn()
vi.mock("@/lib/api/suggestions", () => ({
  generateSuggestions: (...args: unknown[]) => generateSuggestions(...args),
}))

import { SuggestionsReviewDialog } from "@/components/chat/SuggestionsReviewDialog"

const schema = [{ name: "users", columns: [{ name: "id", type: "int" }, { name: "email", type: "varchar" }] }]

function open(destinationType?: string) {
  return render(
    <SuggestionsReviewDialog
      isOpen
      onClose={vi.fn()}
      pipelineId="p1"
      sourceConnectionId="c1"
      selectedTables={["users"]}
      onApplyContinue={vi.fn(async () => {})}
      prefetchedSchema={schema}
      destinationType={destinationType}
    />
  )
}

function sentDestination(): unknown {
  const req = generateSuggestions.mock.calls[0]?.[0] as { intent?: { destination_type?: unknown } } | undefined
  return req?.intent?.destination_type
}

beforeEach(() => {
  generateSuggestions.mockReset()
  generateSuggestions.mockResolvedValue({
    pii_columns: [{ column: "users.email", pii_types: ["email"], confidence: "high", suggested_action: "mask" }],
    transforms: [],
    optimizations: [{ type: "compression", suggestion: "Enable Parquet compression (snappy) for storage efficiency" }],
  })
})
afterEach(() => cleanup())

describe("SuggestionsReviewDialog — the destination it asks about", () => {
  it("sends the pipeline's destination type", async () => {
    open("gcs")
    await screen.findByText(/Apply & Continue/)
    expect(sentDestination()).toBe("gcs")
  })

  it("sends 'unknown', never the old 'storage' placeholder, when the type is not loaded", async () => {
    open(undefined)
    await screen.findByText(/Apply & Continue/)
    expect(sentDestination()).toBe("unknown")
  })
})

describe("SuggestionsReviewDialog — optimizations are advice", () => {
  it("says so in the description and on the tab, and the Apply count leaves them out", async () => {
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    open("gcs")
    expect(await screen.findByText("Apply & Continue (1)")).toBeInTheDocument()
    expect(screen.getByText(/optimizations are advice only/)).toBeInTheDocument()
    expect(screen.queryByText(/These will be applied to\s+your data/)).toBeNull()

    await user.click(screen.getByRole("tab", { name: /Optimizations \(1\)/ }))
    expect(await screen.findByText(/^Advice only: Apply & Continue does not change any of these\.$/)).toBeInTheDocument()
    expect(screen.getByText(/Enable Parquet compression/)).toBeInTheDocument()
  })
})
