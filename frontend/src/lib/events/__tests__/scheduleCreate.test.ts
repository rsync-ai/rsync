import { describe, it, expect, vi, afterEach } from "vitest"
import {
  emitPipelineSchedulesChanged,
  onPipelineSchedulesChanged,
  emitPipelineScheduleCreate,
  onPipelineScheduleCreate,
} from "../scheduleCreate"

const cleanups: Array<() => void> = []
afterEach(() => {
  while (cleanups.length) cleanups.pop()!()
})

function subscribe(handler: (pipelineId: string) => void) {
  const off = onPipelineSchedulesChanged(handler)
  cleanups.push(off)
  return off
}

const PID = "f65adf5f-1c84-40ec-8cad-b0df9f749211"

describe("pipeline schedules-changed bus", () => {
  // The defect: schedules are mutated from two places — the header's "Create schedule"
  // action and PipelineSchedulePanel's own controls — and the panel refetched only after
  // its own mutations. Creating from the header left the panel reading "No schedules
  // configured" until a hard reload, while deleting from the panel refreshed instantly.
  it("delivers a mutation notification to a listener in another component", () => {
    const handler = vi.fn()
    subscribe(handler)

    emitPipelineSchedulesChanged(PID)

    expect(handler).toHaveBeenCalledTimes(1)
    expect(handler).toHaveBeenCalledWith(PID)
  })

  it("fans out to every listener", () => {
    const panel = vi.fn()
    const strategyCard = vi.fn()
    subscribe(panel)
    subscribe(strategyCard)

    emitPipelineSchedulesChanged(PID)

    expect(panel).toHaveBeenCalledWith(PID)
    expect(strategyCard).toHaveBeenCalledWith(PID)
  })

  it("carries the pipeline id so listeners can filter foreign pipelines", () => {
    const seen: string[] = []
    subscribe((pid) => seen.push(pid))

    emitPipelineSchedulesChanged(PID)
    emitPipelineSchedulesChanged("11111111-2222-3333-4444-555555555555")

    expect(seen).toEqual([PID, "11111111-2222-3333-4444-555555555555"])
  })

  it("drops events with a blank pipeline id rather than refetching everything", () => {
    const handler = vi.fn()
    subscribe(handler)

    emitPipelineSchedulesChanged("")
    emitPipelineSchedulesChanged("   ")

    expect(handler).not.toHaveBeenCalled()
  })

  it("trims the pipeline id", () => {
    const handler = vi.fn()
    subscribe(handler)

    emitPipelineSchedulesChanged(`  ${PID}  `)

    expect(handler).toHaveBeenCalledWith(PID)
  })

  it("stops delivering after unsubscribe", () => {
    const handler = vi.fn()
    const off = onPipelineSchedulesChanged(handler)

    emitPipelineSchedulesChanged(PID)
    off()
    emitPipelineSchedulesChanged(PID)

    expect(handler).toHaveBeenCalledTimes(1)
  })

  // The two buses mean different things: CREATE is "open the create dialog", CHANGED is
  // "something was created/paused/resumed/deleted". Crossing them would pop a dialog on
  // every delete.
  it("is independent of the open-create-dialog bus", () => {
    const changed = vi.fn()
    const openDialog = vi.fn()
    subscribe(changed)
    cleanups.push(onPipelineScheduleCreate(openDialog))

    emitPipelineSchedulesChanged(PID)
    expect(openDialog).not.toHaveBeenCalled()

    emitPipelineScheduleCreate(PID)
    expect(changed).toHaveBeenCalledTimes(1)
  })
})
