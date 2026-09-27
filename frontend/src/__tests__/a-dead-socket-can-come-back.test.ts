import { describe, it, expect, beforeEach, afterEach, vi } from "vitest"
import { AgentWebSocket } from "@/lib/websocket"

/**
 * A live view whose socket has given up is worse than one that never had a
 * socket: it keeps rendering the last state it received as though it were
 * current. `attemptReconnect` used to answer an exhausted budget with a bare
 * `return`, and the provider's `hasAttemptedConnect` ref was set once and never
 * cleared -- so after eight failures (a couple of minutes of capped backoff,
 * i.e. a laptop that slept) nothing could ever ask for the socket back.
 */

class FakeSocket {
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSING = 2
  static readonly CLOSED = 3
  static instances: FakeSocket[] = []

  readyState = FakeSocket.CONNECTING
  onopen: (() => void) | null = null
  onclose: (() => void) | null = null
  onerror: ((e: unknown) => void) | null = null
  onmessage: ((e: { data: string }) => void) | null = null

  constructor(public url: string) {
    FakeSocket.instances.push(this)
  }

  open() {
    this.readyState = FakeSocket.OPEN
    this.onopen?.()
  }

  /** The server was never there / dropped us. */
  die() {
    this.readyState = FakeSocket.CLOSED
    this.onclose?.()
  }

  close() {
    this.readyState = FakeSocket.CLOSED
  }

  send() {}
}

const MAX_ATTEMPTS = 8

/** Burn the whole ladder: every socket dies the moment it is created. */
function exhaust(ws: AgentWebSocket) {
  ws.connect()
  for (let i = 0; i < MAX_ATTEMPTS + 1; i++) {
    FakeSocket.instances[FakeSocket.instances.length - 1].die()
    vi.runOnlyPendingTimers()
  }
}

describe("a socket that gave up", () => {
  beforeEach(() => {
    vi.useFakeTimers()
    FakeSocket.instances = []
    vi.stubGlobal("WebSocket", FakeSocket)
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.unstubAllGlobals()
  })

  it("says so instead of falling silent", () => {
    const ws = new AgentWebSocket("ws://test")
    const gaveUp = vi.fn()
    ws.onGiveUp(gaveUp)

    expect(ws.hasGivenUp()).toBe(false)
    exhaust(ws)

    expect(ws.hasGivenUp()).toBe(true)
    expect(gaveUp).toHaveBeenCalled()
  })

  it("stops on its own before that — the probe is armed", () => {
    // Non-zero control: if the ladder never stopped, the test above would pass
    // for the wrong reason. It must be bounded, and bounded where it says.
    const ws = new AgentWebSocket("ws://test")
    exhaust(ws)
    const after = FakeSocket.instances.length
    vi.advanceTimersByTime(10 * 60_000)
    expect(FakeSocket.instances.length).toBe(after)
    expect(after).toBeLessThanOrEqual(MAX_ATTEMPTS + 1)
  })

  it("reconnects when told the world changed", () => {
    // What a tab regaining visibility, or the network returning, now does.
    const ws = new AgentWebSocket("ws://test")
    exhaust(ws)
    const before = FakeSocket.instances.length

    ws.reconnectNow()

    expect(FakeSocket.instances.length).toBe(before + 1)
    expect(ws.hasGivenUp()).toBe(false)
  })

  it("restores the whole budget, not one more attempt", () => {
    const ws = new AgentWebSocket("ws://test")
    exhaust(ws)
    const before = FakeSocket.instances.length

    ws.reconnectNow()
    // The revived socket dies too: a full ladder must follow, not a single retry.
    for (let i = 0; i < MAX_ATTEMPTS + 1; i++) {
      FakeSocket.instances[FakeSocket.instances.length - 1].die()
      vi.runOnlyPendingTimers()
    }
    expect(FakeSocket.instances.length - before).toBeGreaterThan(MAX_ATTEMPTS)
  })

  it("a successful open re-arms the ladder for the next outage", () => {
    const ws = new AgentWebSocket("ws://test")
    ws.connect()
    for (let i = 0; i < MAX_ATTEMPTS - 1; i++) {
      FakeSocket.instances[FakeSocket.instances.length - 1].die()
      vi.runOnlyPendingTimers()
    }
    // One good connection, then it drops again.
    FakeSocket.instances[FakeSocket.instances.length - 1].open()
    FakeSocket.instances[FakeSocket.instances.length - 1].die()
    vi.runOnlyPendingTimers()
    expect(ws.hasGivenUp()).toBe(false)
  })

  it("does not open a second socket during a handshake", () => {
    // connect() guarded on OPEN only, so a caller arriving mid-handshake opened
    // a rival socket whose own close started a second reconnect ladder.
    const ws = new AgentWebSocket("ws://test")
    ws.connect()
    expect(FakeSocket.instances.length).toBe(1)
    ws.connect()
    expect(FakeSocket.instances.length).toBe(1)
  })

  it("supersedes a scheduled retry rather than racing it", () => {
    const ws = new AgentWebSocket("ws://test")
    ws.connect()
    FakeSocket.instances[0].die()
    expect(ws.isReconnecting()).toBe(true)

    ws.reconnectNow()
    const after = FakeSocket.instances.length
    vi.runOnlyPendingTimers()
    expect(FakeSocket.instances.length).toBe(after)
  })
})
