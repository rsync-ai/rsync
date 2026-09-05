/**
 * THE MODAL WAS NOT A MODAL — KI-DIALOG-PRIMITIVE-HAS-NO-ROLE-OR-ARIA-MODAL.
 *
 * `components/ui/dialog.tsx` is hand-rolled, not Radix, and its panel rendered
 * as a bare `<div>`: no `role="dialog"`, no `aria-modal`, no `aria-labelledby`.
 * To a screen reader the whole thing was an anonymous group in the middle of a
 * page that was still fully readable behind it — 25 call sites, every one of
 * them inheriting the gap from this one component.
 *
 * The fix ships the role AND focus containment together, deliberately.
 * `aria-modal="true"` is an instruction to assistive tech to ignore everything
 * outside the panel; with focus free to Tab out, the user lands on content the
 * screen reader has just been told to hide, and hears nothing. Adding the
 * attribute alone would have traded a missing role for a false claim, which is
 * the same defect in a better disguise — so the wrap-around cases below are not
 * scope creep, they are what makes the attribute true.
 *
 * `alert-dialog.tsx` was the other file matched when sweeping for sibling
 * primitives (`grep -l 'fixed inset-0' components/ui/*.tsx`). It is built on
 * `@radix-ui/react-alert-dialog`, so the sweep hit is a non-defect — but only
 * running it showed *why*, and it is not the reason a reader would assume.
 * Radix does not set `aria-modal` at all; it reaches the same end by marking
 * every sibling of its portal `aria-hidden`, which is stronger (it survives
 * assistive tech that ignores `aria-modal`) and unavailable to us, because this
 * primitive renders in place rather than through a portal. The last case pins
 * the properties Radix actually ships, so "the sibling is fine" stays a
 * measurement rather than a comment.
 */

import * as React from "react"
import { describe, it, expect, vi } from "vitest"
import { render, screen, cleanup } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogContent,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"

describe("the dialog panel identifies itself", () => {
  it("exposes role=dialog and aria-modal", () => {
    render(
      <Dialog open onOpenChange={vi.fn()}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete pipeline</DialogTitle>
          </DialogHeader>
        </DialogContent>
      </Dialog>,
    )

    const panel = screen.getByRole("dialog")
    expect(panel).toHaveAttribute("aria-modal", "true")
  })

  it("takes its accessible name from the DialogTitle", () => {
    render(
      <Dialog open onOpenChange={vi.fn()}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete pipeline</DialogTitle>
            <DialogDescription>This cannot be undone.</DialogDescription>
          </DialogHeader>
        </DialogContent>
      </Dialog>,
    )

    // Resolved through aria-labelledby, so this fails if the id wiring breaks.
    const panel = screen.getByRole("dialog", { name: "Delete pipeline" })
    expect(panel).toHaveAccessibleDescription("This cannot be undone.")
  })

  it("omits aria-labelledby entirely when there is no title, rather than dangling", () => {
    // The failure mode this guards is subtle: an `aria-labelledby` pointing at
    // an id that is not in the document leaves the dialog with NO name at all,
    // which is worse than the attribute being absent.
    render(
      <Dialog open onOpenChange={vi.fn()}>
        <DialogContent>
          <p>Just some body copy.</p>
        </DialogContent>
      </Dialog>,
    )

    const panel = screen.getByRole("dialog")
    expect(panel).not.toHaveAttribute("aria-labelledby")
    expect(panel).not.toHaveAttribute("aria-describedby")
  })

  it("follows the title across a branch swap", async () => {
    // `layout/Header.tsx:614` swaps its whole header on state, so the title that
    // exists is a question only the mounted tree can answer. A registration that
    // leaked on unmount would leave two ids here and name the dialog twice.
    function Swapping({ created }: { created: boolean }) {
      return (
        <Dialog open onOpenChange={vi.fn()}>
          <DialogContent>
            {created ? (
              <DialogTitle>Acme is ready</DialogTitle>
            ) : (
              <DialogTitle>New Workspace</DialogTitle>
            )}
          </DialogContent>
        </Dialog>
      )
    }

    const { rerender } = render(<Swapping created={false} />)
    expect(screen.getByRole("dialog", { name: "New Workspace" })).toBeInTheDocument()

    rerender(<Swapping created />)
    const panel = await screen.findByRole("dialog", { name: "Acme is ready" })
    expect(panel.getAttribute("aria-labelledby")?.split(" ")).toHaveLength(1)
  })

  it("registers a caller-supplied id instead of its own", () => {
    render(
      <Dialog open onOpenChange={vi.fn()}>
        <DialogContent>
          <DialogTitle id="my-own-heading">Run details</DialogTitle>
        </DialogContent>
      </Dialog>,
    )

    const panel = screen.getByRole("dialog", { name: "Run details" })
    expect(panel).toHaveAttribute("aria-labelledby", "my-own-heading")
  })
})

describe("aria-modal is backed by real focus containment", () => {
  function openWithOutsideControl() {
    return render(
      <>
        <button type="button">Behind the overlay</button>
        <Dialog open onOpenChange={vi.fn()}>
          <DialogContent>
            <DialogTitle>Retry execution</DialogTitle>
            <button type="button">Confirm</button>
          </DialogContent>
        </Dialog>
      </>,
    )
  }

  it("moves focus to the panel on open, so its name is announced", () => {
    openWithOutsideControl()
    expect(screen.getByRole("dialog", { name: "Retry execution" })).toHaveFocus()
  })

  it("wraps Tab at the last stop instead of escaping to the page behind", async () => {
    const user = userEvent.setup()
    openWithOutsideControl()

    const close = screen.getByRole("button", { name: /close/i })
    const confirm = screen.getByRole("button", { name: "Confirm" })
    const outside = screen.getByRole("button", { name: "Behind the overlay" })

    await user.tab()
    expect(close).toHaveFocus()
    await user.tab()
    expect(confirm).toHaveFocus()
    await user.tab()

    // The control: without containment this is where focus reaches the button
    // that `aria-modal` has just told the screen reader does not exist.
    expect(outside).not.toHaveFocus()
    expect(close).toHaveFocus()
  })

  it("wraps Shift+Tab from the first stop instead of stepping out behind it", async () => {
    const user = userEvent.setup()
    openWithOutsideControl()

    const close = screen.getByRole("button", { name: /close/i })
    const confirm = screen.getByRole("button", { name: "Confirm" })
    const outside = screen.getByRole("button", { name: "Behind the overlay" })

    // Focused explicitly, and from the FIRST stop, because that is the only
    // position that discriminates. An earlier version of this case shift-tabbed
    // from `<body>` and passed with the trap removed — the browser's own
    // wrap-to-last happened to land inside the dialog, so the test was reporting
    // document order as containment. Mutation testing is what surfaced that.
    close.focus()
    await user.tab({ shift: true })

    expect(confirm).toHaveFocus()
    expect(outside).not.toHaveFocus()
  })

  it("returns focus to the control that opened it", async () => {
    const user = userEvent.setup()

    function Host() {
      const [open, setOpen] = React.useState(false)
      return (
        <>
          <button type="button" onClick={() => setOpen(true)}>
            Open settings
          </button>
          <Dialog open={open} onOpenChange={setOpen}>
            <DialogContent>
              <DialogTitle>Settings</DialogTitle>
            </DialogContent>
          </Dialog>
        </>
      )
    }

    render(<Host />)
    const trigger = screen.getByRole("button", { name: "Open settings" })
    await user.click(trigger)
    expect(screen.getByRole("dialog", { name: "Settings" })).toHaveFocus()

    await user.click(screen.getByRole("button", { name: /close/i }))
    expect(trigger).toHaveFocus()
  })
})

describe("the sibling the sweep matched", () => {
  it("alert-dialog already carries its role and name, because Radix supplies them", () => {
    // Not an assumption: `alert-dialog.tsx` wraps AlertDialogPrimitive.Content,
    // and this is the check that the delegation actually reaches the DOM.
    render(
      <>
        <button type="button">Behind the overlay</button>
        <AlertDialog open>
          <AlertDialogContent>
            <AlertDialogTitle>Discard changes?</AlertDialogTitle>
            <AlertDialogAction>Discard</AlertDialogAction>
          </AlertDialogContent>
        </AlertDialog>
      </>,
    )

    const panel = screen.getByRole("alertdialog", { name: "Discard changes?" })
    expect(panel).toHaveAttribute("tabindex", "-1")

    // Deliberately asserting the ABSENCE: Radix takes the other route, and a
    // future "consistency" patch that adds `aria-modal` here would be layering a
    // weaker mechanism on top of a stronger one that already holds.
    expect(panel).not.toHaveAttribute("aria-modal")

    // The stronger mechanism, measured rather than assumed. The button behind
    // the overlay is still in the DOM and still findable by text — but it has
    // dropped out of the accessibility tree entirely, which is exactly what
    // `aria-modal` only *asks* for.
    const behind = screen.getByText("Behind the overlay")
    expect(behind.closest("[aria-hidden='true']")).not.toBeNull()
    expect(screen.queryByRole("button", { name: "Behind the overlay" })).toBeNull()

    cleanup()
  })
})
