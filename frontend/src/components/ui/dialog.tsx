"use client"

import * as React from "react"
import { X } from "lucide-react"
import { cn } from "@/lib/utils"

interface DialogProps {
  open?: boolean
  onOpenChange?: (open: boolean) => void
  children: React.ReactNode
}

/**
 * Lets `DialogContent` render its own close control without every call site
 * threading a handler down. Null outside a `Dialog`, in which case no close
 * button is drawn — there would be nothing for it to close.
 */
const DialogCloseContext = React.createContext<(() => void) | null>(null)

/**
 * Wires the panel to the `DialogTitle`/`DialogDescription` its caller renders as
 * children. The ids are minted by the panel and applied by the headings — not
 * the other way round — so no call site has to invent one, and the headings
 * register themselves on mount so the panel knows whether they exist at all.
 *
 * Registration rather than inspection, because several dialogs here swap their
 * entire header behind a ternary (`layout/Header.tsx:614`), so "does a title
 * exist" is a question only the mounted tree can answer. And presence has to be
 * known: `aria-labelledby` pointing at an id that is not in the document leaves
 * the dialog with *no* accessible name, which is worse than omitting the
 * attribute and letting content speak.
 */
interface DialogA11y {
  titleId: string
  descriptionId: string
  registerTitle: (id: string) => () => void
  registerDescription: (id: string) => () => void
}

const DialogA11yContext = React.createContext<DialogA11y | null>(null)

const FOCUSABLE_SELECTOR = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  '[tabindex]:not([tabindex="-1"])',
].join(",")

/**
 * Tab stops inside the panel, in document order. `[hidden]` is excluded because
 * jsdom honours it; genuinely invisible-by-CSS elements are not, since jsdom has
 * no layout engine and any `offsetParent`/`getClientRects` filter would drop
 * *every* element under test. Real browsers skip those on their own for
 * `display:none`; the gap is `visibility:hidden` inside an open dialog, which no
 * call site here produces.
 */
function focusableWithin(panel: HTMLElement): HTMLElement[] {
  return Array.from(
    panel.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR),
  ).filter((el) => !el.closest("[hidden]") && el.getAttribute("aria-hidden") !== "true")
}

const Dialog = ({ open, onOpenChange, children }: DialogProps) => {
  // Hooks run unconditionally (gated on `open`) to satisfy the rules of hooks.
  const close = React.useCallback(() => onOpenChange?.(false), [onOpenChange])

  // Close on Escape and lock background scroll while the dialog is open.
  React.useEffect(() => {
    if (!open) return
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") close()
    }
    document.addEventListener("keydown", onKeyDown)
    const prevOverflow = document.body.style.overflow
    document.body.style.overflow = "hidden"
    return () => {
      document.removeEventListener("keydown", onKeyDown)
      document.body.style.overflow = prevOverflow
    }
  }, [open, close])

  if (!open) return null

  return (
    <DialogCloseContext.Provider value={close}>
      <div className="fixed inset-0 z-50">
        <div
          className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50"
          onClick={close}
        />
        <div className="fixed inset-0 flex items-center justify-center p-4 z-50">
          {children}
        </div>
      </div>
    </DialogCloseContext.Provider>
  )
}

const DialogContent = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement>
>(({ className, children, ...props }, ref) => {
  const close = React.useContext(DialogCloseContext)

  const scope = React.useId()
  const titleId = `${scope}title`
  const descriptionId = `${scope}description`
  const [titleIds, setTitleIds] = React.useState<string[]>([])
  const [descriptionIds, setDescriptionIds] = React.useState<string[]>([])

  const a11y = React.useMemo<DialogA11y>(
    () => ({
      titleId,
      descriptionId,
      registerTitle: (id) => {
        setTitleIds((ids) => [...ids, id])
        return () => setTitleIds((ids) => ids.filter((existing) => existing !== id))
      },
      registerDescription: (id) => {
        setDescriptionIds((ids) => [...ids, id])
        return () =>
          setDescriptionIds((ids) => ids.filter((existing) => existing !== id))
      },
    }),
    [titleId, descriptionId],
  )

  const panelRef = React.useRef<HTMLDivElement | null>(null)
  const setPanelRef = React.useCallback(
    (node: HTMLDivElement | null) => {
      panelRef.current = node
      if (typeof ref === "function") ref(node)
      else if (ref) ref.current = node
    },
    [ref],
  )

  // `aria-modal="true"` tells assistive tech to ignore everything outside this
  // panel. That is only true if focus cannot leave it: without containment, Tab
  // walks into content the screen reader is now hiding, and the user ends up
  // focused on something that is announced as nothing at all. So the attribute
  // and the trap ship together — the attribute alone would trade a missing role
  // for a lie. `Dialog` above already owns Escape and the scroll lock; this
  // effect owns focus, and runs on mount because `Dialog` unmounts its children
  // when closed.
  React.useEffect(() => {
    const panel = panelRef.current
    if (!panel) return
    const doc = panel.ownerDocument
    const restoreTo = doc.activeElement as HTMLElement | null

    // Focus the panel, not its first control: the panel is what carries
    // `aria-labelledby`/`aria-describedby`, so focusing it is what makes a
    // screen reader announce the dialog's name and description on open. One Tab
    // then reaches the close button, which is the first child.
    panel.focus({ preventScroll: true })

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Tab" || event.defaultPrevented) return
      const stops = focusableWithin(panel)
      if (stops.length === 0) {
        event.preventDefault()
        panel.focus({ preventScroll: true })
        return
      }
      const first = stops[0]
      const last = stops[stops.length - 1]
      const active = doc.activeElement
      const inside = active instanceof Node && panel.contains(active)
      if (event.shiftKey) {
        if (!inside || active === first || active === panel) {
          event.preventDefault()
          last.focus()
        }
      } else if (!inside || active === last) {
        event.preventDefault()
        first.focus()
      }
    }

    doc.addEventListener("keydown", onKeyDown, true)
    return () => {
      doc.removeEventListener("keydown", onKeyDown, true)
      // Restore only to something still in the document: the control that opened
      // the dialog is often unmounted by the same state change that closed it,
      // and focusing a detached node silently sends focus to `<body>`.
      if (restoreTo?.isConnected) restoreTo.focus({ preventScroll: true })
    }
  }, [])

  return (
    <div
      ref={setPanelRef}
      // Ahead of `{...props}` so a call site can still override — an
      // `alertdialog`, say — rather than having the primitive win silently.
      role="dialog"
      aria-modal="true"
      aria-labelledby={titleIds.length > 0 ? titleIds.join(" ") : undefined}
      aria-describedby={
        descriptionIds.length > 0 ? descriptionIds.join(" ") : undefined
      }
      tabIndex={-1}
      className={cn(
        "relative z-[60] w-full max-w-lg rounded-lg bg-white p-6 shadow-xl outline-none dark:bg-zinc-900",
        className
      )}
      {...props}
    >
      {close && (
        // `sticky` rather than `absolute`: several dialogs scroll to 85–90vh,
        // and an absolutely-positioned corner button scrolls away with the
        // content it is anchored to. `h-0 min-h-0` keeps it out of layout so no
        // existing dialog shifts down — including the four whose content is a
        // `flex flex-col`, where `min-height: auto` would otherwise reserve the
        // button's height. It is the first child so it is also the first thing
        // keyboard and screen-reader users reach.
        <div className="pointer-events-none sticky top-0 z-10 flex h-0 min-h-0 shrink-0 justify-end">
          <button
            type="button"
            onClick={close}
            aria-label="Close"
            className="pointer-events-auto -mr-2 flex h-8 w-8 items-center justify-center rounded-full bg-white/90 text-zinc-500 backdrop-blur transition-colors hover:bg-zinc-100 hover:text-zinc-900 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-zinc-400 dark:bg-zinc-900/90 dark:hover:bg-zinc-800 dark:hover:text-zinc-100"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
      )}
      <DialogA11yContext.Provider value={a11y}>{children}</DialogA11yContext.Provider>
    </div>
  )
})
DialogContent.displayName = "DialogContent"

const DialogHeader = ({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) => (
  // `pr-8` keeps the header clear of the close button above. The button reaches
  // 24px into the content box (it is 32px wide, pulled 8px into the panel's 24px
  // padding), so a 32px gutter leaves 8px of daylight. The gutter lives here and
  // not on `DialogTitle` because the description collides more often than the
  // title does: it is longer prose in the same width, so it wraps, and a wrapped
  // line ends near the right edge by construction.
  <div
    className={cn("flex flex-col space-y-1.5 pr-8 text-center sm:text-left", className)}
    {...props}
  />
)

const DialogTitle = React.forwardRef<
  HTMLHeadingElement,
  React.HTMLAttributes<HTMLHeadingElement>
>(({ className, id, ...props }, ref) => {
  const a11y = React.useContext(DialogA11yContext)
  // A caller-supplied `id` wins and is what gets registered, so the panel always
  // names the element that actually carries the id.
  const resolvedId = id ?? a11y?.titleId
  React.useEffect(() => {
    if (!a11y || !resolvedId) return
    return a11y.registerTitle(resolvedId)
  }, [a11y, resolvedId])

  return (
    <h2
      ref={ref}
      id={resolvedId}
      className={cn("text-lg font-semibold leading-none tracking-tight", className)}
      {...props}
    />
  )
})
DialogTitle.displayName = "DialogTitle"

const DialogDescription = React.forwardRef<
  HTMLParagraphElement,
  React.HTMLAttributes<HTMLParagraphElement>
>(({ className, id, ...props }, ref) => {
  const a11y = React.useContext(DialogA11yContext)
  const resolvedId = id ?? a11y?.descriptionId
  React.useEffect(() => {
    if (!a11y || !resolvedId) return
    return a11y.registerDescription(resolvedId)
  }, [a11y, resolvedId])

  return (
    <p
      ref={ref}
      id={resolvedId}
      className={cn("text-sm text-zinc-500 dark:text-zinc-400", className)}
      {...props}
    />
  )
})
DialogDescription.displayName = "DialogDescription"

const DialogFooter = ({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) => (
  <div
    className={cn("flex flex-col-reverse sm:flex-row sm:justify-end sm:space-x-2", className)}
    {...props}
  />
)

export {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
}
