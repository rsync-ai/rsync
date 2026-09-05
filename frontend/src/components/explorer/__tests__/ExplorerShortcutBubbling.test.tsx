import { describe, it, expect, vi } from "vitest"
import { render } from "@testing-library/react"
import { handleExplorerRunShortcut } from "@/lib/explorer/runShortcut"

/**
 * The integration half of KI-EXPLORER-SHORTCUT-BYPASSES-DESTRUCTIVE-GATE.
 *
 * runShortcut.test.ts proves the *decision* is right given `defaultPrevented`. This file
 * proves the premise that decision rests on, and which is the part that could actually be
 * wrong: when a deeper handler calls `preventDefault()` on the native keydown — exactly
 * what CodeMirror's `keymap.of([{ key: "Mod-Enter", preventDefault: true, … }])` does in
 * SqlEditor.tsx:216-239 — React's synthetic event at an ancestor's `onKeyDown` still
 * reports `defaultPrevented === true`, and the event *does* still reach that ancestor
 * (preventDefault is not stopPropagation).
 *
 * Both halves matter. If the flag were dropped, the guard would silently stop guarding
 * and the pure unit tests would still pass. If propagation stopped on its own, the guard
 * would be unnecessary — and the bug would never have existed.
 *
 * The real SqlEditor is deliberately NOT mounted here: CodeMirror needs layout APIs
 * (`Range.getClientRects`) that jsdom does not implement, so its keymap never dispatches
 * and a test built on it would pass or fail for reasons unrelated to this fix. The
 * CodeMirror end of the wire is verified in a real browser instead.
 */
describe("preventDefault from a nested control, seen at the page root", () => {
  it("reaches the root handler still flagged as handled, so the fallback stands down", () => {
    const rootSawEvent = vi.fn()
    const rootRanQuery = vi.fn()

    const { container } = render(
      <div
        onKeyDown={(e) => {
          rootSawEvent(e.defaultPrevented)
          handleExplorerRunShortcut(e, {
            hasNlInput: false,
            hasSql: true,
            generateSQL: vi.fn(),
            attemptRunQuery: rootRanQuery,
          })
        }}
      >
        <div data-testid="inner" tabIndex={-1} />
      </div>
    )

    const inner = container.querySelector('[data-testid="inner"]') as HTMLElement

    // Stand in for the CodeMirror keymap: a listener on the inner element that claims the
    // keystroke with preventDefault and does NOT stop propagation.
    inner.addEventListener("keydown", (e) => e.preventDefault())

    inner.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", metaKey: true, bubbles: true, cancelable: true })
    )

    // The event still bubbles — that is why the guard is needed at all.
    expect(rootSawEvent).toHaveBeenCalledTimes(1)
    // …and it arrives flagged. This is the assumption the fix depends on.
    expect(rootSawEvent).toHaveBeenCalledWith(true)
    // …so the ungated second dispatch never happens. This is the bug.
    expect(rootRanQuery).not.toHaveBeenCalled()
  })

  it("still runs the fallback for an unclaimed keystroke", () => {
    const rootRanQuery = vi.fn()

    const { container } = render(
      <div
        onKeyDown={(e) =>
          handleExplorerRunShortcut(e, {
            hasNlInput: false,
            hasSql: true,
            generateSQL: vi.fn(),
            attemptRunQuery: rootRanQuery,
          })
        }
      >
        <div data-testid="inner" tabIndex={-1} />
      </div>
    )

    // No inner listener this time — nothing claims it, so the page-level fallback is the
    // only thing that can run the query. It must still work.
    const inner = container.querySelector('[data-testid="inner"]') as HTMLElement
    inner.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", metaKey: true, bubbles: true, cancelable: true })
    )

    expect(rootRanQuery).toHaveBeenCalledTimes(1)
  })
})
