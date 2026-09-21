/**
 * The Data flow tab's layout, top to bottom: CDC lag alert, the Monitoring card
 * (Overview / Activity), then Throughput, Diagnose, Dependencies. The Live
 * events card that used to close the list is gone: it read the same event
 * stream as Activity.
 */

import { afterEach, describe, expect, it, vi } from "vitest"
import { cleanup, render, screen } from "@testing-library/react"
import "@testing-library/jest-dom"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
  useSearchParams: () => new URLSearchParams("tab=monitor"),
  usePathname: () => "/pipelines/p1",
}))

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
  authFetchOrThrow: (...a: unknown[]) => authFetch(...a),
}))

vi.mock("@/components/pipeline/PipelineHealthHeader", () => ({ PipelineHealthHeader: () => null }))
vi.mock("@/components/pipeline/DataLoadingStrategyCard", () => ({ DataLoadingStrategyCard: () => null }))
vi.mock("@/components/pipeline/PipelineSchedulePanel", () => ({ PipelineSchedulePanel: () => null }))
vi.mock("@/components/pipeline/PipelineLiveStatePanel", () => ({ PipelineLiveStatePanel: () => null }))
vi.mock("@/components/pipeline/PipelineRecentRuns", () => ({ PipelineRecentRuns: () => null }))
vi.mock("@/components/pipeline/SelfHealingPanel", () => ({ SelfHealingPanel: () => null }))
vi.mock("@/components/pipeline/ExecutionHistoryTab", () => ({ ExecutionHistoryTab: () => null }))
vi.mock("@/components/pipeline/StepsDAGTab", () => ({ StepsDAGTab: () => null }))
vi.mock("@/components/pipeline/PipelineTransformsTab", () => ({ PipelineTransformsTab: () => null }))
vi.mock("@/components/pipeline/AssessmentTab", () => ({ AssessmentTab: () => null, AssessmentTabBadge: () => null }))
vi.mock("@/components/pipeline/CDCLagAlertsPanel", () => ({ CDCLagAlertsPanel: () => <div data-testid="lag-alert" /> }))
vi.mock("@/components/pipeline/PipelineMonitoringPanelNoSSR", () => ({
  PipelineMonitoringPanelNoSSR: ({ variant }: { variant: string }) => <div data-testid={`monitoring-${variant}`} />,
}))

import { PipelineDetailTabsClient } from "@/components/pipeline/PipelineDetailTabsClient"
import { MonitorTab } from "@/components/pipeline/MonitorTab"

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: String(status),
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response
}

/** `a` is rendered before `b` in document order. */
function before(a: HTMLElement, b: HTMLElement) {
  return Boolean(a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING)
}

afterEach(() => {
  cleanup()
  authFetch.mockReset()
})

describe("Data flow tab order", () => {
  it("CDC: lag alert, then Monitoring, then the numbers", () => {
    authFetch.mockResolvedValue(res(200, {}))
    render(<PipelineDetailTabsClient pipelineId="p1" pipelineType="cdc" initialTab="monitor" />)

    const lag = screen.getByTestId("lag-alert")
    const monitoring = screen.getByTestId("monitoring-monitoring")
    const throughput = screen.getByText("Throughput")
    expect(before(lag, monitoring)).toBe(true)
    expect(before(monitoring, throughput)).toBe(true)
  })

  it("batch: no lag alert, Monitoring still first", () => {
    authFetch.mockResolvedValue(res(200, {}))
    render(<PipelineDetailTabsClient pipelineId="p1" pipelineType="etl" initialTab="monitor" />)

    expect(screen.queryByTestId("lag-alert")).not.toBeInTheDocument()
    expect(before(screen.getByTestId("monitoring-monitoring"), screen.getByText("Throughput"))).toBe(true)
  })

  it("the numbers read Throughput, Diagnose, Dependencies, and there is no Live events card", () => {
    authFetch.mockResolvedValue(res(200, {}))
    render(<MonitorTab pipelineId="p1" />)

    // Card titles, in document order ("Diagnose" is also the card's button).
    const titles = screen.getAllByRole("heading", { level: 3 }).map((h) => h.textContent?.trim() ?? "")
    const at = (name: string) => titles.findIndex((t) => t.startsWith(name))
    expect(at("Throughput")).toBe(0)
    expect(at("Diagnose")).toBe(1)
    expect(at("Dependencies")).toBe(2)
    expect(titles).toHaveLength(3)
    expect(screen.queryByText(/live events/i)).not.toBeInTheDocument()
  })
})
