import { afterEach, describe, expect, it, vi } from "vitest"
import ElkOnMainThread from "elkjs/lib/elk.bundled.js"

// The engine keeps its worker in module state, so each test loads a fresh copy.
async function freshLayout() {
  vi.resetModules()
  return (await import("@/lib/elk-layout")).layoutGraph
}

const box = (id: string, width = 100, height = 40) => ({ id, width, height })

afterEach(() => {
  vi.unstubAllGlobals()
})

describe("layoutGraph", () => {
  it("points every link right, from the origin", async () => {
    const layoutGraph = await freshLayout()
    const { positions, bounds } = await layoutGraph(
      [box("a"), box("b"), box("c")],
      [
        { from: "a", to: "b" },
        { from: "b", to: "c" },
      ],
      { layerGap: 50 },
    )
    const [a, b, c] = ["a", "b", "c"].map((id) => positions.get(id)!)
    expect(a.x).toBe(0)
    expect(b.x - a.x).toBe(150)
    expect(c.x - b.x).toBe(150)
    expect(Math.min(a.y, b.y, c.y)).toBe(0)
    expect(bounds).toEqual({ x: 0, y: 0, width: 400, height: 40 })
  })

  it("takes any id a caller has, the engine's own root id included", async () => {
    const layoutGraph = await freshLayout()
    const { positions } = await layoutGraph([box("graph"), box("root"), box("n0")], [
      { from: "graph", to: "root" },
      { from: "root", to: "n0" },
    ])
    expect([...positions.keys()]).toEqual(["graph", "root", "n0"])
    expect(positions.get("root")!.x).toBeGreaterThan(positions.get("graph")!.x)
    expect(positions.get("n0")!.x).toBeGreaterThan(positions.get("root")!.x)
  })

  it("lays nothing out for nothing", async () => {
    const layoutGraph = await freshLayout()
    expect(await layoutGraph([], [{ from: "a", to: "b" }])).toEqual({
      positions: new Map(),
      bounds: { x: 0, y: 0, width: 0, height: 0 },
    })
  })

  it("ignores self, unknown and repeated links, and a repeated box keeps its first size", async () => {
    const layoutGraph = await freshLayout()
    const { positions, bounds } = await layoutGraph(
      [box("a"), box("b"), box("a", 999, 999)],
      [
        { from: "a", to: "a" },
        { from: "a", to: "ghost" },
        { from: "ghost", to: "b" },
        { from: "a", to: "b" },
        { from: "a", to: "b" },
      ],
      { layerGap: 20 },
    )
    expect(positions.size).toBe(2)
    expect(positions.get("b")!.x).toBe(120)
    expect(bounds.width).toBe(220)
    expect(bounds.height).toBe(40)
  })
})

/** A stand-in Web Worker that answers the way elk-worker does, or fails the way a broken one does. */
function fakeWorker(behaviour: "answers" | "dies" | "layout error" | "refused") {
  const made: FakeWorker[] = []
  const engine = new ElkOnMainThread()
  class FakeWorker {
    onmessage: ((e: { data: unknown }) => void) | null = null
    posted: { cmd: string }[] = []
    terminate = vi.fn()
    private onError: ((e: { message: string; preventDefault: () => void }) => void) | null = null
    constructor() {
      if (behaviour === "refused") throw new Error("Workers are not allowed here")
      made.push(this)
    }
    addEventListener(type: string, listener: (e: { message: string; preventDefault: () => void }) => void) {
      if (type === "error") this.onError = listener
    }
    postMessage(msg: { id: number; cmd: string; graph?: Parameters<typeof engine.layout>[0] }) {
      this.posted.push(msg)
      const reply = (data: unknown) => queueMicrotask(() => this.onmessage?.({ data }))
      if (behaviour === "dies") {
        queueMicrotask(() => this.onError?.({ message: "script failed to load", preventDefault: () => {} }))
      } else if (msg.cmd !== "layout") {
        reply({ id: msg.id })
      } else if (behaviour === "layout error") {
        reply({ id: msg.id, error: { message: "bad graph" } })
      } else {
        engine.layout(msg.graph!).then((data) => reply({ id: msg.id, data }))
      }
    }
  }
  vi.stubGlobal("Worker", FakeWorker)
  return made
}

describe("layoutGraph's engine", () => {
  const links = [{ from: "a", to: "b" }]

  it("lays out in the worker when there is one", async () => {
    const made = fakeWorker("answers")
    const layoutGraph = await freshLayout()
    const { positions } = await layoutGraph([box("a"), box("b")], links, { layerGap: 10 })
    expect(positions.get("b")).toEqual({ x: 110, y: 0 })
    await layoutGraph([box("a"), box("b")], links)
    expect(made).toHaveLength(1)
    expect(made[0].posted.filter((m) => m.cmd === "layout")).toHaveLength(2)
  })

  it("lays out on the page when the worker dies, and does not start another", async () => {
    const made = fakeWorker("dies")
    const layoutGraph = await freshLayout()
    const { positions } = await layoutGraph([box("a"), box("b")], links, { layerGap: 10 })
    expect(positions.get("b")).toEqual({ x: 110, y: 0 })
    expect(made).toHaveLength(1)
    expect(made[0].terminate).toHaveBeenCalled()
    await layoutGraph([box("a"), box("b")], links)
    expect(made).toHaveLength(1)
  })

  it("lays out on the page when a worker is refused", async () => {
    fakeWorker("refused")
    const layoutGraph = await freshLayout()
    const { positions } = await layoutGraph([box("a"), box("b")], links, { layerGap: 10 })
    expect(positions.get("b")).toEqual({ x: 110, y: 0 })
  })

  it("reports a layout the engine rejects, rather than running it twice", async () => {
    const made = fakeWorker("layout error")
    const layoutGraph = await freshLayout()
    await expect(layoutGraph([box("a"), box("b")], links)).rejects.toThrow("bad graph")
    expect(made[0].terminate).not.toHaveBeenCalled()
  })
})
