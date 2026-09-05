import { describe, it, expect, vi } from "vitest"
import {
  handleExplorerRunShortcut,
  type ExplorerKeyboardEvent,
} from "../runShortcut"

function makeEvent(over: Partial<ExplorerKeyboardEvent> = {}) {
  const e = {
    metaKey: false,
    ctrlKey: false,
    key: "Enter",
    defaultPrevented: false,
    preventDefault: vi.fn(),
    ...over,
  }
  return e as ExplorerKeyboardEvent & { preventDefault: ReturnType<typeof vi.fn> }
}

function makeDeps(over: Partial<Parameters<typeof handleExplorerRunShortcut>[1]> = {}) {
  return {
    hasNlInput: false,
    hasSql: true,
    generateSQL: vi.fn(),
    attemptRunQuery: vi.fn(),
    ...over,
  }
}

describe("handleExplorerRunShortcut", () => {
  // The regression this whole module exists for.
  //
  // Prod repro: type `DROP TABLE public.zz724_inc` in the SQL editor and press Cmd+Enter.
  // The editor's Mod-Enter keymap fires (preventDefault, opens the confirm dialog, issues
  // no request), the same native event bubbles to the page root, and the root handler used
  // to call executeQuery() directly. The table was dropped without the user ever typing
  // "DROP" into the confirmation box — schema went 74 -> 73 tables and the follow-up
  // SELECT returned `pq: relation ... does not exist (42P01)`.
  describe("already-handled events", () => {
    it("does nothing when an inner control already claimed the keystroke", () => {
      const deps = makeDeps()
      const e = makeEvent({ metaKey: true, defaultPrevented: true })

      expect(handleExplorerRunShortcut(e, deps)).toBe("already-handled")
      expect(deps.attemptRunQuery).not.toHaveBeenCalled()
      expect(deps.generateSQL).not.toHaveBeenCalled()
      expect(e.preventDefault).not.toHaveBeenCalled()
    })

    it("bails before the NL branch too", () => {
      const deps = makeDeps({ hasNlInput: true })
      const e = makeEvent({ ctrlKey: true, defaultPrevented: true })

      expect(handleExplorerRunShortcut(e, deps)).toBe("already-handled")
      expect(deps.generateSQL).not.toHaveBeenCalled()
    })
  })

  describe("fallback dispatch", () => {
    it("runs the query through the destructive gate on Cmd+Enter", () => {
      const deps = makeDeps()
      const e = makeEvent({ metaKey: true })

      expect(handleExplorerRunShortcut(e, deps)).toBe("run")
      expect(deps.attemptRunQuery).toHaveBeenCalledTimes(1)
      expect(e.preventDefault).toHaveBeenCalledTimes(1)
    })

    it("accepts Ctrl+Enter for non-Mac keyboards", () => {
      const deps = makeDeps()
      expect(handleExplorerRunShortcut(makeEvent({ ctrlKey: true }), deps)).toBe("run")
      expect(deps.attemptRunQuery).toHaveBeenCalledTimes(1)
    })

    it("prefers NL generation when the prompt box has content", () => {
      const deps = makeDeps({ hasNlInput: true, hasSql: true })

      expect(handleExplorerRunShortcut(makeEvent({ metaKey: true }), deps)).toBe("generate")
      expect(deps.generateSQL).toHaveBeenCalledTimes(1)
      expect(deps.attemptRunQuery).not.toHaveBeenCalled()
    })

    it("does nothing when both inputs are empty", () => {
      const deps = makeDeps({ hasNlInput: false, hasSql: false })

      expect(handleExplorerRunShortcut(makeEvent({ metaKey: true }), deps)).toBe("ignored")
      expect(deps.attemptRunQuery).not.toHaveBeenCalled()
      expect(deps.generateSQL).not.toHaveBeenCalled()
    })
  })

  describe("unrelated keys", () => {
    it.each([
      ["plain Enter", { metaKey: false, ctrlKey: false, key: "Enter" }],
      ["Cmd+K", { metaKey: true, key: "k" }],
      ["Escape", { key: "Escape" }],
    ])("ignores %s without preventing default", (_label, over) => {
      const deps = makeDeps()
      const e = makeEvent(over)

      expect(handleExplorerRunShortcut(e, deps)).toBe("ignored")
      expect(e.preventDefault).not.toHaveBeenCalled()
      expect(deps.attemptRunQuery).not.toHaveBeenCalled()
    })
  })

  // Structural guarantee, not a behavioural one: the deps type has no executeQuery, so
  // no future edit can make the fallback path reach the database ungated without also
  // widening the interface — which is a visible, reviewable change.
  it("exposes no ungated execution path", () => {
    const deps = makeDeps()
    expect(Object.keys(deps)).not.toContain("executeQuery")
  })
})
