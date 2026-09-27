import { describe, it, expect, beforeEach, afterEach, vi } from "vitest"
import { useEffect } from "react"
import { render, act } from "@testing-library/react"
import { WebSocketProvider, useWebSocket } from "@/contexts/WebSocketContext"

/**
 * The provider half of the same bug. `hasAttemptedConnect` was a ref set once
 * and never cleared, so the FIRST disconnect was permanent as far as the
 * provider was concerned: no later subscribe() or sendMessage() could ask for
 * the socket back, and every view fed by it went on showing the last state it
 * had received.
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
  die() {
    this.readyState = FakeSocket.CLOSED
    this.onclose?.()
  }
  close() {
    this.readyState = FakeSocket.CLOSED
  }
  send() {}
}

const last = () => FakeSocket.instances[FakeSocket.instances.length - 1]

/** A consumer that wants realtime, like every pipeline view. */
function Subscriber() {
  const { subscribe } = useWebSocket()
  useEffect(() => subscribe(() => {}), [subscribe])
  return <div data-testid="sub" />
}

function setVisibility(state: "visible" | "hidden") {
  Object.defineProperty(document, "visibilityState", {
    configurable: true,
    get: () => state,
  })
  document.dispatchEvent(new Event("visibilitychange"))
}

describe("WebSocketProvider", () => {
  beforeEach(() => {
    vi.useFakeTimers()
    FakeSocket.instances = []
    vi.stubGlobal("WebSocket", FakeSocket)
    // jsdom's document is shared across the file, and a test below hides it.
    setVisibility("visible")
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.unstubAllGlobals()
  })

  it("connects once for a page that wants realtime", () => {
    render(
      <WebSocketProvider>
        <Subscriber />
      </WebSocketProvider>,
    )
    expect(FakeSocket.instances.length).toBe(1)
  })

  it("does not connect for a page that never subscribes", () => {
    // Non-zero control: the lazy connect is deliberate, and reviving must not
    // start opening sockets on /connections.
    render(
      <WebSocketProvider>
        <div />
      </WebSocketProvider>,
    )
    act(() => setVisibility("visible"))
    expect(FakeSocket.instances.length).toBe(0)
  })

  it("comes back when the tab does, after the socket died", () => {
    render(
      <WebSocketProvider>
        <Subscriber />
      </WebSocketProvider>,
    )
    act(() => {
      last().open()
    })
    act(() => {
      last().die()
      // Burn the client's whole retry ladder, as a long sleep would.
      for (let i = 0; i < 10; i++) {
        last().die()
        vi.runOnlyPendingTimers()
      }
    })
    const dead = FakeSocket.instances.length

    act(() => setVisibility("visible"))

    expect(FakeSocket.instances.length).toBe(dead + 1)
  })

  it("stays put while the tab is hidden", () => {
    // Non-zero control: a hidden tab is not a reason to reconnect.
    render(
      <WebSocketProvider>
        <Subscriber />
      </WebSocketProvider>,
    )
    act(() => {
      for (let i = 0; i < 10; i++) {
        last().die()
        vi.runOnlyPendingTimers()
      }
    })
    const dead = FakeSocket.instances.length
    act(() => setVisibility("hidden"))
    expect(FakeSocket.instances.length).toBe(dead)
  })

  it("comes back when the network does", () => {
    render(
      <WebSocketProvider>
        <Subscriber />
      </WebSocketProvider>,
    )
    act(() => {
      for (let i = 0; i < 10; i++) {
        last().die()
        vi.runOnlyPendingTimers()
      }
    })
    const dead = FakeSocket.instances.length
    act(() => {
      window.dispatchEvent(new Event("online"))
    })
    expect(FakeSocket.instances.length).toBe(dead + 1)
  })
})
