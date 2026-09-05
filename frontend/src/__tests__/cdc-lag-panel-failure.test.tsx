/**
 * Regression tests for F-280 — the CDC lag panel answers a question it could
 * not ask.
 *
 * `CDCLagAlertsPanel` has three read outcomes and only two renders. On a 500,
 * or on a thrown fetch, it sets `available = true` and `issues = []`
 * (`CDCLagAlertsPanel.tsx:52-58`), which lands on the same branch as a genuine
 * empty list — a GREEN card reading "No replication lag alerts — source
 * database is keeping up".
 *
 * That string is a positive claim about the source database, derived from a
 * read that failed. It is the worst possible failure mode for a monitoring
 * panel: it does not merely omit the alarm, it actively tells the operator to
 * stop looking. A blank panel would have been safer.
 *
 * The `404 || 403` branch above it is DELIBERATE and must keep working — the
 * sentinel API returns those when the feature is disabled or the caller lacks
 * the role, and hiding the panel is the right answer there. This file guards
 * that branch too, because the obvious "fix" (route everything through a
 * throwing fetch helper) would delete it.
 */

import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

import { CDCLagAlertsPanel } from "@/components/pipeline/CDCLagAlertsPanel"

const mockFetch = vi.fn()
vi.stubGlobal("fetch", mockFetch)

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: String(status),
    text: async () => JSON.stringify(body),
    json: async () => body,
  } as unknown as Response
}

const ONE_ALERT = {
  issues: [
    {
      id: "i1",
      type: "replication_lag",
      severity: "warning",
      component_id: "p1",
      component_type: "cdc_pipeline",
      description: "Replication slot is 512 MB behind",
      detected_at: "2026-08-05T12:00:00Z",
      occurrence_count: 3,
      last_occurrence: "2026-08-05T12:30:00Z",
      metadata: { lag_mb: 512 },
    },
  ],
}

const ALL_CLEAR = /no replication lag alerts/i
const UNAVAILABLE = /could not check/i

describe("CDCLagAlertsPanel must not answer green when it could not read (F-280)", () => {
  beforeEach(() => vi.clearAllMocks())

  it("THE REGRESSION: a 500 does not render the all-clear", async () => {
    mockFetch.mockResolvedValue(res(500, { error: "sentinel unavailable" }))

    render(<CDCLagAlertsPanel pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText(UNAVAILABLE)).toBeInTheDocument())
    expect(screen.queryByText(ALL_CLEAR)).not.toBeInTheDocument()
  })

  it("THE REGRESSION: a thrown fetch does not render the all-clear either", async () => {
    // The `catch` arm had the identical defect as the `else` arm — a dropped
    // connection is exactly when an operator most needs to not be reassured.
    mockFetch.mockRejectedValue(new TypeError("Failed to fetch"))

    render(<CDCLagAlertsPanel pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText(UNAVAILABLE)).toBeInTheDocument())
    expect(screen.queryByText(ALL_CLEAR)).not.toBeInTheDocument()
  })

  it("positive control: a real empty list still renders the green all-clear", async () => {
    // Without this the fix could pass by never showing green at all, which
    // would just be a different lie.
    mockFetch.mockResolvedValue(res(200, { issues: [] }))

    render(<CDCLagAlertsPanel pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText(ALL_CLEAR)).toBeInTheDocument())
    expect(screen.queryByText(UNAVAILABLE)).not.toBeInTheDocument()
  })

  it("positive control: real alerts still render", async () => {
    mockFetch.mockResolvedValue(res(200, ONE_ALERT))

    render(<CDCLagAlertsPanel pipelineId="p1" />)

    await waitFor(() =>
      expect(screen.getByText("Replication slot is 512 MB behind")).toBeInTheDocument(),
    )
    expect(screen.queryByText(UNAVAILABLE)).not.toBeInTheDocument()
  })

  it("a failed REFRESH keeps the alerts already on screen instead of dropping to green", async () => {
    // Dropping a known-bad state to "all clear" on one flaky poll is the same
    // bug wearing a timer.
    mockFetch.mockResolvedValueOnce(res(200, ONE_ALERT))
    render(<CDCLagAlertsPanel pipelineId="p1" />)
    await waitFor(() =>
      expect(screen.getByText("Replication slot is 512 MB behind")).toBeInTheDocument(),
    )

    mockFetch.mockResolvedValue(res(500, { error: "sentinel unavailable" }))
    await userEvent.click(screen.getByRole("button", { name: /refresh/i }))

    await waitFor(() => expect(screen.getByText(/may be out of date/i)).toBeInTheDocument())
    expect(screen.getByText("Replication slot is 512 MB behind")).toBeInTheDocument()
    expect(screen.queryByText(ALL_CLEAR)).not.toBeInTheDocument()
  })
})

describe("the deliberate hide-the-panel branch still works", () => {
  beforeEach(() => vi.clearAllMocks())

  it("404 (feature disabled) renders nothing at all — not an error card", async () => {
    mockFetch.mockResolvedValue(res(404, { error: "not found" }))

    const { container } = render(<CDCLagAlertsPanel pipelineId="p1" />)

    await waitFor(() => expect(mockFetch).toHaveBeenCalled())
    await waitFor(() => expect(container).toBeEmptyDOMElement())
  })

  it("403 (no permission) renders nothing at all — not an error card", async () => {
    mockFetch.mockResolvedValue(res(403, { error: "insufficient workspace role" }))

    const { container } = render(<CDCLagAlertsPanel pipelineId="p1" />)

    await waitFor(() => expect(mockFetch).toHaveBeenCalled())
    await waitFor(() => expect(container).toBeEmptyDOMElement())
  })
})
