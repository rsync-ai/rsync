/**
 * THE MODAL HAD NO VISIBLE WAY OUT.
 *
 * `components/ui/dialog.tsx` is a hand-rolled primitive, not Radix. It imported
 * `X` from lucide-react on line 4 and never rendered it, and
 * `ExecutionDetailDialog` reserved `pr-8` on its title for a button that did not
 * exist — two independent traces of a close affordance that was intended and
 * lost. Escape and backdrop-click did work, so every dialog was *closable*; none
 * was *discoverably* closable, which is the defect a user reports as "there is
 * no X".
 *
 * The affordance is pinned on the shared primitive rather than on one dialog,
 * because all 23 call sites inherit the gap from this single component. For the
 * same reason the header's right gutter lives on `DialogHeader`: the button
 * reaches 24px into the content box, and only 3 of 20 dialog files reserved any
 * gutter at all — each on the title, which is the header line least likely to
 * collide. (Those three `pr-8`s were removed; the one on `ExecutionDetailDialog`
 * named above is history, not current code.)
 */

import { describe, it, expect, vi } from "vitest"
import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog"

function open(onOpenChange = vi.fn()) {
  render(
    <Dialog open onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Failed run of Chat Pipeline</DialogTitle>
        </DialogHeader>
        <button type="button">Copy ID</button>
      </DialogContent>
    </Dialog>,
  )
  return onOpenChange
}

describe("every dialog offers a visible way out", () => {
  it("renders a close control with an accessible name", () => {
    open()
    expect(screen.getByRole("button", { name: /close/i })).toBeInTheDocument()
  })

  it("closes the dialog when the close control is activated", async () => {
    const onOpenChange = open()

    await userEvent.click(screen.getByRole("button", { name: /close/i }))

    expect(onOpenChange).toHaveBeenCalledTimes(1)
    expect(onOpenChange).toHaveBeenCalledWith(false)
  })

  it("renders exactly one close control", () => {
    // A second X — from the primitive and a hand-rolled one in the same header —
    // is the regression this fix can most plausibly introduce.
    open()
    expect(screen.getAllByRole("button", { name: /close/i })).toHaveLength(1)
  })

  it("puts the close control ahead of the dialog body, not buried under it", () => {
    // The execution dialog scrolls for 85vh. A close control that a keyboard or
    // screen-reader user only reaches after the whole run report is not an exit.
    open()
    const closeButton = screen.getByRole("button", { name: /close/i })
    const buttons = screen.getAllByRole("button")

    expect(buttons[0]).toBe(closeButton)
    expect(closeButton.compareDocumentPosition(screen.getByText("Failed run of Chat Pipeline")))
      .toBe(Node.DOCUMENT_POSITION_FOLLOWING)
  })

  it("reserves a right gutter on the header so title and description clear the button", () => {
    // Measured in a browser at 1280x800 on a default `max-w-lg` panel: the close
    // button spans x=848..880 while the content box runs to x=872, so header text
    // reaches 24px *into* the button's column. With `admin/users`' real string
    // — "Change role for {email}" with a long address — the description wraps and
    // its first line's glyphs land 4px past the button's left edge, under it.
    //
    // jsdom applies no Tailwind, so this asserts the declaration rather than the
    // geometry; the 32px-clears-24px arithmetic is what the browser measurement
    // above proves. This test is the regression guard, not the proof.
    open()
    expect(screen.getByText("Failed run of Chat Pipeline").parentElement).toHaveClass("pr-8")
  })

  it("keeps the gutter in one place — the header, never the title", () => {
    // Both would compound to a 64px inset and re-introduce the ragged headers the
    // gutter exists to prevent. Three dialogs used to reserve `pr-8` on the title.
    open()
    expect(screen.getByText("Failed run of Chat Pipeline")).not.toHaveClass("pr-8")
  })

  it("renders nothing at all while closed", () => {
    render(
      <Dialog open={false} onOpenChange={vi.fn()}>
        <DialogContent>
          <DialogTitle>Failed run of Chat Pipeline</DialogTitle>
        </DialogContent>
      </Dialog>,
    )
    expect(screen.queryByRole("button", { name: /close/i })).not.toBeInTheDocument()
  })
})
