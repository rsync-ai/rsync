/**
 * "Alert if older than…" on the model page (ModelFreshnessDeadline.tsx). The gateway
 * stored a freshness deadline (PUT /explorer/saved/:id/freshness) and the sweep flagged
 * models past it, but nothing in the UI could read or set the deadline.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { cleanup, render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

import { authFetch } from "@/lib/api/auth-fetch"
import {
  ModelFreshnessDeadline,
  deadlineFrom,
  splitDeadline,
} from "@/components/explorer/ModelFreshnessDeadline"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

const mockFetch = authFetch as unknown as Mock
const ID = "6f1c0a52-3c52-4a0e-9d7c-2d2f0f6a1b11"
const SAVED = `/api/v1/explorer/saved/${ID}`
const PUT_URL = `${SAVED}/freshness`

function res(body: unknown, status = 200) {
  return { ok: status < 400, status, json: async () => body } as unknown as Response
}

/** GET answers with the stored deadline; PUT stores what it is sent, as the gateway does. */
function gateway(initial: number | null, putAnswer?: { status: number; body: unknown }) {
  let stored = initial
  mockFetch.mockImplementation(async (url: string, init?: RequestInit) => {
    if (url === PUT_URL && init?.method === "PUT") {
      if (putAnswer) return res(putAnswer.body, putAnswer.status)
      stored = JSON.parse(String(init.body)).deadline_seconds
      return res({ saved_query_id: ID, deadline_seconds: stored })
    }
    if (url === SAVED) return res(stored == null ? { id: ID } : { id: ID, freshness_deadline_seconds: stored })
    return res({}, 404)
  })
}

function putBodies() {
  return mockFetch.mock.calls
    .filter(([url, init]) => url === PUT_URL && (init as RequestInit)?.method === "PUT")
    .map(([, init]) => JSON.parse(String((init as RequestInit).body)))
}

beforeEach(() => {
  mockFetch.mockReset()
})
afterEach(() => cleanup())

function renderRow(props: Partial<Parameters<typeof ModelFreshnessDeadline>[0]> = {}) {
  const onSaved = vi.fn()
  const ui = (id: string) => (
    <ModelFreshnessDeadline savedQueryId={id} materialization="table" canEdit onSaved={onSaved} {...props} />
  )
  const { rerender } = render(ui(ID))
  rerenderWith = (id: string) => rerender(ui(id))
  return { onSaved, row: screen.getByTestId("model-freshness-deadline") }
}
let rerenderWith: (id: string) => void = () => {}

describe("ModelFreshnessDeadline row", () => {
  it("shows the stored deadline", async () => {
    gateway(21600)
    const { row } = renderRow()
    expect(await within(row).findByText("Alert if older than 6h")).toBeInTheDocument()
    expect(within(row).getByRole("button", { name: "Edit freshness deadline" })).toBeEnabled()
  })

  it("says there is none, and offers to set one", async () => {
    gateway(null)
    const { row } = renderRow()
    expect(await within(row).findByText("No deadline")).toBeInTheDocument()
    expect(within(row).getByRole("button", { name: "Set freshness deadline" })).toBeInTheDocument()
  })

  it("marks a deadline the sweep will not check, on a model that rebuilds no table", async () => {
    gateway(3600)
    const { row } = renderRow({ materialization: "statement" })
    await within(row).findByText("Alert if older than 1h")
    expect(row).toHaveTextContent("not checked")
  })

  it("does not show the previous model's deadline while the next one loads", async () => {
    // The model page stays mounted when a graph link moves it to another model.
    gateway(21600)
    const { row } = renderRow()
    await within(row).findByText("Alert if older than 6h")

    const OTHER = "0b0c3d1e-4f5a-4b6c-8d7e-9f0a1b2c3d4e"
    mockFetch.mockImplementation(() => new Promise(() => {}))
    rerenderWith(OTHER)
    expect(within(row).queryByText("Alert if older than 6h")).toBeNull()
    expect(row).toHaveTextContent("Loading…")
  })

  it("locks the control for a role below the model-run bar, and says why", async () => {
    gateway(3600)
    const reason = "Setting a freshness deadline needs the admin role or higher. Your role is member."
    const { row } = renderRow({ canEdit: false, disabledReason: reason })
    const edit = await within(row).findByRole("button", { name: "Edit freshness deadline" })
    expect(edit).toBeDisabled()
    expect(edit).toHaveAttribute("title", reason)
  })
})

describe("ModelFreshnessDeadline dialog", () => {
  it("saves a preset as seconds and shows the new value", async () => {
    gateway(null)
    const user = userEvent.setup()
    const { row, onSaved } = renderRow()
    await user.click(await within(row).findByRole("button", { name: "Set freshness deadline" }))

    const dialog = await screen.findByRole("dialog")
    await user.click(within(dialog).getByRole("button", { name: "1 day" }))
    await user.click(within(dialog).getByRole("button", { name: "Save" }))

    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull())
    expect(putBodies()).toEqual([{ deadline_seconds: 86400 }])
    expect(within(row).getByText("Alert if older than 1d")).toBeInTheDocument()
    expect(onSaved).toHaveBeenCalledTimes(1)
  })

  it("saves a typed amount in the chosen unit", async () => {
    gateway(21600)
    const user = userEvent.setup()
    const { row } = renderRow()
    await user.click(await within(row).findByRole("button", { name: "Edit freshness deadline" }))

    const dialog = await screen.findByRole("dialog")
    const amount = within(dialog).getByLabelText("Alert if older than")
    expect(amount).toHaveValue(6)
    await user.clear(amount)
    await user.type(amount, "90")
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "Unit" }), "minutes")
    await user.click(within(dialog).getByRole("button", { name: "Save" }))

    await waitFor(() => expect(putBodies()).toEqual([{ deadline_seconds: 5400 }]))
    expect(await within(row).findByText("Alert if older than 1h 30m")).toBeInTheDocument()
  })

  it("removes the deadline with null", async () => {
    gateway(3600)
    const user = userEvent.setup()
    const { row } = renderRow()
    await user.click(await within(row).findByRole("button", { name: "Edit freshness deadline" }))
    await user.click(within(await screen.findByRole("dialog")).getByRole("button", { name: "Remove deadline" }))

    await waitFor(() => expect(putBodies()).toEqual([{ deadline_seconds: null }]))
    expect(await within(row).findByText("No deadline")).toBeInTheDocument()
  })

  it("refuses an out-of-range deadline before asking the server", async () => {
    gateway(null)
    const user = userEvent.setup()
    const { row } = renderRow()
    await user.click(await within(row).findByRole("button", { name: "Set freshness deadline" }))

    const dialog = await screen.findByRole("dialog")
    const amount = within(dialog).getByLabelText("Alert if older than")
    await user.clear(amount)
    await user.type(amount, "400")
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "Unit" }), "days")

    expect(dialog).toHaveTextContent("A deadline must be between 1 minute and 365 days.")
    expect(within(dialog).getByRole("button", { name: "Save" })).toBeDisabled()
    expect(putBodies()).toEqual([])
  })

  it("keeps the dialog open and shows the server's reason when the save is refused", async () => {
    const reason = "freshness deadline must be between 60 seconds and one year, in SECONDS"
    gateway(null, { status: 400, body: { error: reason } })
    const user = userEvent.setup()
    const { row, onSaved } = renderRow()
    await user.click(await within(row).findByRole("button", { name: "Set freshness deadline" }))
    const dialog = await screen.findByRole("dialog")
    await user.click(within(dialog).getByRole("button", { name: "Save" }))

    expect(await within(dialog).findByRole("alert")).toHaveTextContent(reason)
    expect(within(row).getByText("No deadline")).toBeInTheDocument()
    expect(onSaved).not.toHaveBeenCalled()
  })

  it("reopens on what is stored, not on an edit that was cancelled", async () => {
    gateway(21600)
    const user = userEvent.setup()
    const { row } = renderRow()
    const edit = await within(row).findByRole("button", { name: "Edit freshness deadline" })
    await user.click(edit)
    let dialog = await screen.findByRole("dialog")
    await user.click(within(dialog).getByRole("button", { name: "7 days" }))
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }))
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull())

    await user.click(edit)
    dialog = await screen.findByRole("dialog")
    expect(within(dialog).getByLabelText("Alert if older than")).toHaveValue(6)
    expect(within(dialog).getByRole("combobox", { name: "Unit" })).toHaveValue("hours")
    expect(putBodies()).toEqual([])
  })

  it("warns that widening closes an open alert as widened, not fixed", async () => {
    gateway(3600)
    const user = userEvent.setup()
    const { row } = renderRow()
    await user.click(await within(row).findByRole("button", { name: "Edit freshness deadline" }))
    const dialog = await screen.findByRole("dialog")
    await user.click(within(dialog).getByRole("button", { name: "12 hours" }))
    expect(dialog).toHaveTextContent("closes an open alert as “deadline widened”, not as fixed")
  })
})

describe("deadline arithmetic", () => {
  it("states a stored deadline in the largest exact unit", () => {
    expect(splitDeadline(21600)).toEqual({ amount: "6", unit: "hours" })
    expect(splitDeadline(172800)).toEqual({ amount: "2", unit: "days" })
    expect(splitDeadline(5400)).toEqual({ amount: "90", unit: "minutes" })
  })

  it("holds the gateway's range: 60 seconds to one year", () => {
    expect(deadlineFrom("1", "minutes")).toEqual({ seconds: 60 })
    expect(deadlineFrom("365", "days")).toEqual({ seconds: 31536000 })
    expect(deadlineFrom("0.5", "minutes")).toHaveProperty("error")
    expect(deadlineFrom("366", "days")).toHaveProperty("error")
    expect(deadlineFrom("", "hours")).toHaveProperty("error")
  })
})
