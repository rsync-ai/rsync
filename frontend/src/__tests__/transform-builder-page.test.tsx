/**
 * The Transform Builder page (/transforms) — the orphaned page whose
 * "Save Plan" button shipped with no `onClick` at all.
 *
 * This file lives in src/__tests__ because vitest.config.ts's `include` globs
 * do not cover src/app/**.
 *
 * Three things here are load-bearing and none of them are visible in review:
 *
 *  1. Saving is a REPLACE (transforms.go:432-462 deletes every row for the
 *     pipeline inside the transaction, then re-inserts the body). So the
 *     builder must first LOAD what is stored, and the confirm dialog must say
 *     in numbers what is about to go.
 *  2. `mask_pii` is not one of this page's 17 operations — the natural-language
 *     pipeline setup materializes it. It therefore has no card, and a naive
 *     save would leave it out of the request and delete it, which makes the
 *     next run copy those columns to the destination in the clear. It is
 *     carried through untouched. That is the assertion this file exists for.
 *  3. Generate and Preview used a bare `fetch` against /api/v1, which is behind
 *     AuthRequiredMiddleware + CSRFMiddleware (main.go:866-875) — so both were
 *     a 401/403 every time, and `if (res.ok)` turned that into silence.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { cleanup, render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

let mockRole = "member"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...args: unknown[]) => authFetch(...args),
}))

const toastError = vi.fn()
const toastSuccess = vi.fn()
vi.mock("sonner", () => ({
  toast: {
    error: (...a: unknown[]) => toastError(...a),
    success: (...a: unknown[]) => toastSuccess(...a),
  },
}))

vi.mock("@/contexts/WorkspaceContext", () => ({
  useWorkspaceRole: () => ({
    role: mockRole,
    isLoading: false,
    error: false,
    activeWorkspace: null,
    can: () => false,
    // Delegate to the real ladder so this test can't drift from roles.ts.
    meets: (min: string) => realMeetsRole(mockRole, min as never),
  }),
}))

import { meetsRole as realMeetsRole } from "@/lib/workspace/roles"
import TransformBuilderPage from "@/app/(dashboard)/transforms/page"

// Real UUIDs: toDefinitions blanks an id that is not one, because the column is
// `uuid` and a client-minted non-UUID would roll the whole save back.
const FILTER_ID = "11111111-1111-4111-8111-111111111111"
const MASK_ID = "22222222-2222-4222-8222-222222222222"

const PIPELINES = {
  pipelines: [
    { id: "p1", name: "Orders to Warehouse" },
    // A second one, so "switching pipelines" is something a test can do.
    { id: "p2", name: "Events to Lake" },
  ],
}

/** A stored plan: one rule the builder can draw, one it cannot. */
const STORED = {
  pipeline_id: "p1",
  producer_transforms: [
    {
      id: FILTER_ID,
      transform_type: "producer",
      transform_order: 0,
      transform_config: { operation: "filter", condition: "amount > 100" },
      enabled: true,
    },
  ],
  consumer_transforms: [
    {
      id: MASK_ID,
      transform_type: "consumer",
      transform_order: 1,
      transform_config: { operation: "mask_pii", column: "email", mask_type: "hash" },
      enabled: true,
    },
  ],
}

/** What `Generate` returns: one rule the builder appends to whatever is there. */
const GENERATED = {
  transforms: [
    {
      transform_type: "producer",
      transform_order: 0,
      transform_config: { operation: "rename", column: "cust", new_name: "customer" },
      enabled: true,
    },
  ],
}

function json(status: number, body: unknown) {
  return { ok: status < 400, status, json: async () => body }
}

type Routes = {
  plan?: unknown
  planStatus?: number
  save?: { status: number; body: unknown }
  parse?: { status: number; body: unknown }
  preview?: { status: number; body: unknown }
}

function serve(routes: Routes = {}) {
  authFetch.mockImplementation(async (url: string, init?: RequestInit) => {
    const u = String(url)
    const post = init?.method === "POST"
    if (u.includes("/transforms/pipeline/")) {
      if (post) return json(routes.save?.status ?? 200, routes.save?.body ?? { count: 2 })
      return json(routes.planStatus ?? 200, routes.plan ?? { producer_transforms: [], consumer_transforms: [] })
    }
    if (u.includes("/transforms/parse")) {
      return json(routes.parse?.status ?? 200, routes.parse?.body ?? { transforms: [] })
    }
    if (u.includes("/transforms/preview")) {
      return json(routes.preview?.status ?? 200, routes.preview?.body ?? { preview: [] })
    }
    return json(200, PIPELINES)
  })
}

function callsTo(fragment: string, method?: string) {
  return authFetch.mock.calls
    .filter(([url, init]) => {
      const m = (init as RequestInit | undefined)?.method
      return String(url).includes(fragment) && (method ? m === method : true)
    })
    .map(([url, init]) => ({
      url: String(url),
      body: (init as RequestInit | undefined)?.body
        ? JSON.parse(String((init as RequestInit).body))
        : undefined,
    }))
}

/** Pick a pipeline by name in the selector. */
async function pickPipeline(user: ReturnType<typeof userEvent.setup>, name: string) {
  await waitFor(() => expect(callsTo("/api/v1/pipelines")).toHaveLength(1))
  await user.click(screen.getByRole("combobox"))
  await user.click(await screen.findByRole("option", { name }))
}

/** Pick "Orders to Warehouse" in the pipeline selector. */
async function choosePipeline(user: ReturnType<typeof userEvent.setup>) {
  await pickPipeline(user, "Orders to Warehouse")
}

const saveButton = () => screen.getByRole("button", { name: /save plan/i })

/**
 * The natural-language box, found by placeholder: its <Label> is not associated
 * with it, so it has no accessible name. Worth fixing on the page one day —
 * but not in a change about wiring up dead endpoints.
 */
const nlBox = () => screen.getByPlaceholderText(/mask all email addresses/i)

beforeEach(() => {
  mockRole = "member"
  authFetch.mockReset()
  toastError.mockReset()
  toastSuccess.mockReset()
  serve()
})
afterEach(() => cleanup())

describe("Transform Builder — saving a plan to a pipeline", () => {
  it("cannot save before a pipeline is chosen, and says which thing is missing", async () => {
    render(<TransformBuilderPage />)

    await waitFor(() => expect(saveButton()).toBeDisabled())
    expect(saveButton()).toHaveAttribute(
      "title",
      "Pick a pipeline first — transforms are saved to one pipeline."
    )
  })

  it("replaces the builder with the pipeline's stored plan when one is chosen", async () => {
    serve({ plan: STORED })
    render(<TransformBuilderPage />)

    await choosePipeline(userEvent.setup())

    // The stored `filter` row is drawn as a card...
    await waitFor(() => expect(screen.getByText("Filter Rows")).toBeInTheDocument())
    expect(screen.getByText("2 rules are stored on this pipeline.")).toBeInTheDocument()
    expect(saveButton()).toBeEnabled()
  })

  it("names the rules it cannot draw instead of hiding them", async () => {
    serve({ plan: STORED })
    render(<TransformBuilderPage />)

    await choosePipeline(userEvent.setup())

    await waitFor(() =>
      expect(screen.getByText(/1 rule uses an operation this builder cannot display/)).toBeInTheDocument()
    )
    expect(screen.getByText(/\(mask_pii\)/)).toBeInTheDocument()
    expect(screen.getByText(/kept unchanged when you save/)).toBeInTheDocument()
  })

  it("asks before replacing, in counts, and sends nothing if the answer is no", async () => {
    serve({ plan: STORED })
    render(<TransformBuilderPage />)
    const user = userEvent.setup()
    await choosePipeline(user)
    await waitFor(() => expect(saveButton()).toBeEnabled())

    await user.click(saveButton())

    const dialog = await screen.findByRole("alertdialog")
    expect(within(dialog).getByText(/Saving REPLACES every transform on this pipeline/)).toBeInTheDocument()
    expect(within(dialog).getByText(/the 2 existing rules are deleted/)).toBeInTheDocument()
    expect(callsTo("/transforms/pipeline/", "POST")).toHaveLength(0)

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }))
    expect(callsTo("/transforms/pipeline/", "POST")).toHaveLength(0)
  })

  // THE ONE THAT MATTERS. The save deletes everything and re-inserts the body,
  // so a mask left out of the request is a mask deleted — and the columns it
  // covered reach the destination in the clear on the next run.
  it("keeps the un-drawable mask_pii rule on the wire", async () => {
    serve({ plan: STORED })
    render(<TransformBuilderPage />)
    const user = userEvent.setup()
    await choosePipeline(user)
    await waitFor(() => expect(saveButton()).toBeEnabled())

    await user.click(saveButton())
    await user.click(await screen.findByRole("button", { name: "Replace and save" }))

    await waitFor(() => expect(callsTo("/transforms/pipeline/", "POST")).toHaveLength(1))
    const sent = callsTo("/transforms/pipeline/", "POST")[0]
    expect(sent.url).toContain("/api/v1/transforms/pipeline/p1")

    const ops = sent.body.transforms.map(
      (t: { transform_config: { operation: string } }) => t.transform_config.operation
    )
    expect(ops).toEqual(["filter", "mask_pii"])
    // Carried through byte-for-byte, id included — this is an UPDATE of the
    // same row, not a re-creation that would renumber it.
    expect(sent.body.transforms[1]).toMatchObject({
      id: MASK_ID,
      transform_type: "consumer",
      enabled: true,
      transform_config: { operation: "mask_pii", column: "email", mask_type: "hash" },
    })
  })

  it("re-reads the stored plan after a save, because the server mints ids", async () => {
    serve({ plan: STORED, save: { status: 200, body: { count: 2 } } })
    render(<TransformBuilderPage />)
    const user = userEvent.setup()
    await choosePipeline(user)
    await waitFor(() => expect(saveButton()).toBeEnabled())

    await user.click(saveButton())
    await user.click(await screen.findByRole("button", { name: "Replace and save" }))

    await waitFor(() => expect(toastSuccess).toHaveBeenCalled())
    expect(String(toastSuccess.mock.calls[0][0])).toContain("Saved 2 transforms")
    // One GET on selection, a second after the save.
    expect(callsTo("/transforms/pipeline/").filter((c) => !c.body)).toHaveLength(2)
  })

  it("surfaces a rejected save instead of pretending it landed", async () => {
    serve({ plan: STORED, save: { status: 403, body: { error: "insufficient workspace role" } } })
    render(<TransformBuilderPage />)
    const user = userEvent.setup()
    await choosePipeline(user)
    await waitFor(() => expect(saveButton()).toBeEnabled())

    await user.click(saveButton())
    await user.click(await screen.findByRole("button", { name: "Replace and save" }))

    await waitFor(() => expect(toastError).toHaveBeenCalledWith("insufficient workspace role"))
    expect(toastSuccess).not.toHaveBeenCalled()
  })

  it("renders a failed plan read as an error, not as an empty pipeline", async () => {
    serve({ planStatus: 500, plan: { error: "database is down" } })
    render(<TransformBuilderPage />)

    await choosePipeline(userEvent.setup())

    await waitFor(() => expect(screen.getByText("database is down")).toBeInTheDocument())
    expect(screen.queryByText("This pipeline has no transforms yet.")).not.toBeInTheDocument()
  })

  it("lets a viewer build and preview a plan but not save one", async () => {
    mockRole = "viewer"
    serve({ plan: STORED })
    render(<TransformBuilderPage />)
    await choosePipeline(userEvent.setup())

    await waitFor(() => expect(screen.getByText("Filter Rows")).toBeInTheDocument())
    expect(saveButton()).toBeDisabled()
    expect(saveButton()).toHaveAttribute(
      "title",
      "Only workspace members and admins can change a pipeline's transforms."
    )
    expect(
      screen.getByText(/only workspace members and admins can save one to a pipeline/i)
    ).toBeInTheDocument()
  })

  it("fails closed while the workspace role is still loading", async () => {
    // `role` is "" until WorkspaceContext resolves; roleRank ranks that 0.
    mockRole = ""
    serve({ plan: STORED })
    render(<TransformBuilderPage />)
    await choosePipeline(userEvent.setup())

    await waitFor(() => expect(screen.getByText("Filter Rows")).toBeInTheDocument())
    expect(saveButton()).toBeDisabled()
  })
})

describe("Transform Builder — Generate and Preview", () => {
  it("sends Generate through authFetch, so it carries auth and CSRF", async () => {
    render(<TransformBuilderPage />)
    const user = userEvent.setup()

    await user.type(nlBox(), "mask every email")
    await user.click(screen.getByRole("button", { name: /generate/i }))

    await waitFor(() => expect(callsTo("/transforms/parse")).toHaveLength(1))
    // Relative: authFetch prefixes the gateway and attaches X-CSRF-Token. An
    // absolute URL would skip neither, but a bare fetch() would skip both.
    expect(callsTo("/transforms/parse")[0].url).toBe("/api/v1/transforms/parse")
    expect(callsTo("/transforms/parse")[0].body).toEqual({ natural_language: "mask every email" })
  })

  it("says so when Generate is refused, rather than doing nothing", async () => {
    serve({ parse: { status: 403, body: { error: "CSRF token missing" } } })
    render(<TransformBuilderPage />)
    const user = userEvent.setup()

    await user.type(nlBox(), "mask every email")
    await user.click(screen.getByRole("button", { name: /generate/i }))

    await waitFor(() => expect(toastError).toHaveBeenCalledWith("CSRF token missing"))
  })

  it("sends Preview through authFetch and shows what came back wrong", async () => {
    serve({ preview: { status: 400, body: { error: "sample_data must be an array of objects" } } })
    render(<TransformBuilderPage />)

    // Two buttons run the preview (the toolbar and the sample-data card); either
    // will do, the point is where the request goes.
    await userEvent.setup().click(screen.getAllByRole("button", { name: /^Preview$/ })[0])

    await waitFor(() => expect(callsTo("/transforms/preview")).toHaveLength(1))
    expect(callsTo("/transforms/preview")[0].url).toBe("/api/v1/transforms/preview")
    expect(await screen.findByText("sample_data must be an array of objects")).toBeInTheDocument()
  })
})

/**
 * Picking a pipeline REPLACES the builder's contents — which is right, because
 * editing has to start from what is really stored. What was wrong is that it
 * did so over unsaved work without asking, and that nothing anywhere on the
 * page said work was unsaved. A plan built with Generate, or half an hour of
 * hand-editing, went in one click of the selector with no prompt, no marker and
 * no way back.
 */
describe("Transform Builder — unsaved work", () => {
  /** Append a generated rule, which is the cheapest way to make the plan dirty. */
  async function makeDirty(user: ReturnType<typeof userEvent.setup>) {
    await user.type(nlBox(), "rename cust to customer")
    await user.click(screen.getByRole("button", { name: /generate/i }))
    await waitFor(() => expect(screen.getByText("Rename Columns")).toBeInTheDocument())
  }

  const unsavedMarker = () => screen.queryByText("Unsaved changes")
  const getPlans = () => callsTo("/transforms/pipeline/").filter((c) => !c.body)

  it("says nothing about unsaved work on a plan that is exactly what is stored", async () => {
    // Non-zero control. A marker that is always on is a marker nobody reads,
    // and a load is not an edit.
    serve({ plan: STORED })
    render(<TransformBuilderPage />)

    await choosePipeline(userEvent.setup())

    await waitFor(() => expect(screen.getByText("Filter Rows")).toBeInTheDocument())
    expect(unsavedMarker()).not.toBeInTheDocument()
  })

  it("marks the plan unsaved as soon as it differs from what is stored", async () => {
    serve({ plan: STORED, parse: { status: 200, body: GENERATED } })
    render(<TransformBuilderPage />)
    const user = userEvent.setup()
    await choosePipeline(user)
    await waitFor(() => expect(screen.getByText("Filter Rows")).toBeInTheDocument())

    await makeDirty(user)

    expect(unsavedMarker()).toBeInTheDocument()
    expect(unsavedMarker()).toHaveAttribute("role", "status")
  })

  it("asks before a pipeline switch throws that work away", async () => {
    serve({ plan: STORED, parse: { status: 200, body: GENERATED } })
    render(<TransformBuilderPage />)
    const user = userEvent.setup()
    await choosePipeline(user)
    await waitFor(() => expect(screen.getByText("Filter Rows")).toBeInTheDocument())
    await makeDirty(user)

    await pickPipeline(user, "Events to Lake")

    const dialog = await screen.findByRole("alertdialog")
    expect(within(dialog).getByText("Discard the unsaved plan?")).toBeInTheDocument()
    expect(within(dialog).getByText(/cannot be recovered/)).toBeInTheDocument()
    // Nothing was loaded yet: the builder still holds the operator's work.
    expect(getPlans().some((c) => c.url.includes("/p2"))).toBe(false)
    expect(screen.getByText("Rename Columns")).toBeInTheDocument()
  })

  it("keeps the edits when the answer is no", async () => {
    serve({ plan: STORED, parse: { status: 200, body: GENERATED } })
    render(<TransformBuilderPage />)
    const user = userEvent.setup()
    await choosePipeline(user)
    await waitFor(() => expect(screen.getByText("Filter Rows")).toBeInTheDocument())
    await makeDirty(user)
    await pickPipeline(user, "Events to Lake")

    const dialog = await screen.findByRole("alertdialog")
    await user.click(within(dialog).getByRole("button", { name: "Keep editing" }))

    await waitFor(() => expect(screen.queryByRole("alertdialog")).not.toBeInTheDocument())
    expect(screen.getByText("Rename Columns")).toBeInTheDocument()
    expect(unsavedMarker()).toBeInTheDocument()
    expect(getPlans().some((c) => c.url.includes("/p2"))).toBe(false)
  })

  it("loads the other pipeline when the answer is yes", async () => {
    serve({ plan: STORED, parse: { status: 200, body: GENERATED } })
    render(<TransformBuilderPage />)
    const user = userEvent.setup()
    await choosePipeline(user)
    await waitFor(() => expect(screen.getByText("Filter Rows")).toBeInTheDocument())
    await makeDirty(user)
    await pickPipeline(user, "Events to Lake")

    const dialog = await screen.findByRole("alertdialog")
    await user.click(within(dialog).getByRole("button", { name: "Discard and load" }))

    await waitFor(() => expect(getPlans().some((c) => c.url.includes("/p2"))).toBe(true))
    // And the freshly loaded plan is a baseline again, not still "unsaved".
    await waitFor(() => expect(unsavedMarker()).not.toBeInTheDocument())
  })

  it("switches with no prompt at all when there is nothing to lose", async () => {
    // Non-zero control: the guard must not turn every switch into a dialog.
    serve({ plan: STORED })
    render(<TransformBuilderPage />)
    const user = userEvent.setup()
    await choosePipeline(user)
    await waitFor(() => expect(screen.getByText("Filter Rows")).toBeInTheDocument())

    await pickPipeline(user, "Events to Lake")

    await waitFor(() => expect(getPlans().some((c) => c.url.includes("/p2"))).toBe(true))
    expect(screen.queryByRole("alertdialog")).not.toBeInTheDocument()
  })

  it("lets the browser warn before a reload discards the work", async () => {
    // The page's own dialogs cannot stop a reload or a closed tab; only
    // beforeunload can, and the builder registered no handler at all.
    serve({ plan: STORED, parse: { status: 200, body: GENERATED } })
    render(<TransformBuilderPage />)
    const user = userEvent.setup()
    await choosePipeline(user)
    await waitFor(() => expect(screen.getByText("Filter Rows")).toBeInTheDocument())

    // Non-zero control: a plan as loaded must not nag on every navigation.
    const fire = () => window.dispatchEvent(new Event("beforeunload", { cancelable: true }))
    expect(fire()).toBe(true)

    await makeDirty(user)
    // dispatchEvent returns false once a listener has called preventDefault,
    // which is what makes Chrome show its "Leave site?" prompt.
    await waitFor(() => expect(fire()).toBe(false))
  })

  it("clears the marker once the work is actually saved", async () => {
    serve({ plan: STORED, parse: { status: 200, body: GENERATED }, save: { status: 200, body: { count: 3 } } })
    render(<TransformBuilderPage />)
    const user = userEvent.setup()
    await choosePipeline(user)
    await waitFor(() => expect(screen.getByText("Filter Rows")).toBeInTheDocument())
    await makeDirty(user)
    expect(unsavedMarker()).toBeInTheDocument()

    await user.click(saveButton())
    await user.click(await screen.findByRole("button", { name: "Replace and save" }))

    // The save re-reads the stored plan, and that re-read is the new baseline.
    await waitFor(() => expect(toastSuccess).toHaveBeenCalled())
    await waitFor(() => expect(unsavedMarker()).not.toBeInTheDocument())
  })
})
