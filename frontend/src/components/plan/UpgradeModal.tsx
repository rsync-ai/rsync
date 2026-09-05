"use client"

import { useState } from "react"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Sparkles, Lock, Check, Mail } from "lucide-react"
import { authFetch } from "@/lib/api/auth-fetch"
import type { PlanLimitPayload } from "@/lib/api/pipelines"

// There is no self-serve checkout yet — Pro is set up by the team. Clicking the
// CTA POSTs to the backend, which emails the team directly (via Resend) so the
// user never has to compose an email themselves. If the backend can't send
// (self-hosted with no mail provider), we fall back to copying the address to the
// clipboard, which is also shown as selectable text. We deliberately avoid a bare
// `mailto:` as the primary action: on a machine whose mail handler is a browser
// (or unset) it opens an external app / blank tab and reads as a dead click.
// The compiled-in default matches the backend's own default (UPGRADE_SALES_EMAIL in
// plan_upgrade.go). It is only a starting value: the backend returns `contact_email`
// on every non-"sent" response, and we adopt it. Without that, an operator who sets
// UPGRADE_SALES_EMAIL gets their address emailed by the server while this dialog
// still tells their user to write to ours.
const DEFAULT_CONTACT_EMAIL = "sales@rsync.ai"
const mailtoFor = (addr: string) => `mailto:${addr}?subject=Upgrade to Pro`
const UPGRADE_REQUEST_ENDPOINT = "/api/v1/plan/upgrade-request"

type ContactStatus = "idle" | "sending" | "sent" | "copied"

interface UpgradeModalProps {
  /**
   * 402-driven trigger: pass the plan-limit payload and the modal opens whenever it
   * is non-null (trial expired / pipeline limit reached).
   */
  payload?: PlanLimitPayload | null
  /**
   * Proactive trigger (e.g. the "Upgrade to Pro" plan banner): control the open
   * state explicitly. Defaults to `!!payload`, so existing 402 callers that pass
   * only `payload` keep their current behavior.
   */
  open?: boolean
  onClose: () => void
}

export function UpgradeModal({ payload, open, onClose }: UpgradeModalProps) {
  const isOpen = open ?? !!payload
  const isTrialExpired = payload?.error === "trial_expired"
  const isLimitReached = payload?.error === "pipeline_limit_reached"
  const isGbLimit = payload?.error === "gb_limit_reached"
  // Proactive = opened without a 402 payload (e.g. the plan banner's "Upgrade to
  // Pro"). 402 flows keep their existing "Cancel" wording.
  const isProactive = !payload

  const [status, setStatus] = useState<ContactStatus>("idle")
  const [contactEmail, setContactEmail] = useState(DEFAULT_CONTACT_EMAIL)

  // Reset the CTA state on close so it never shows a stale "Request sent"
  // confirmation the next time the dialog opens.
  const handleClose = () => {
    setStatus("idle")
    onClose()
  }

  // Fallback when the backend can't auto-send (self-hosted / no mail provider):
  // put the address on the clipboard so the user can email us in one paste.
  // `addr` is passed in rather than read from state: setContactEmail() below does not
  // take effect until the next render, so reading the state here would copy the
  // previous address on the very attempt that discovered the right one.
  const copyFallback = async (addr: string) => {
    try {
      await navigator.clipboard.writeText(addr)
      setStatus("copied")
    } catch {
      // Clipboard blocked (insecure context / permission denied): last resort is
      // the OS mail handler so the click still does something useful.
      window.location.href = mailtoFor(addr)
    }
  }

  const handleContact = async () => {
    setStatus("sending")
    try {
      const res = await authFetch(UPGRADE_REQUEST_ENDPOINT, { method: "POST" })
      const data = (await res.json().catch(() => null)) as {
        status?: string
        contact_email?: string
      } | null
      if (res.ok && data?.status === "sent") {
        setStatus("sent")
        return
      }
      // "unconfigured" (self-hosted) or any soft failure → let the user reach whoever
      // actually runs this install, which the backend tells us.
      const addr = data?.contact_email?.trim() || DEFAULT_CONTACT_EMAIL
      setContactEmail(addr)
      await copyFallback(addr)
    } catch {
      await copyFallback(contactEmail)
    }
  }

  const title = isTrialExpired
    ? "Trial expired"
    : isGbLimit
      ? "Data transfer limit reached"
      : isLimitReached
        ? "Pipeline limit reached"
        : "Upgrade to Pro"

  const description = isTrialExpired
    ? "Your 14-day trial has ended. Upgrade to Pro to keep running pipelines."
    : isGbLimit
      ? `This workspace has used ${payload?.used ?? 0} GB of the ${payload?.limit ?? "your"} GB included on the ${payload?.plan ?? "current"} plan. Upgrade for more data transfer.`
      : isLimitReached
        ? `You've used ${payload?.used ?? 0} of ${payload?.limit ?? "your"} allowed pipelines on the ${payload?.plan ?? "current"} plan. Upgrade to Pro for unlimited pipelines.`
        : "Pro unlocks unlimited pipelines, real-time CDC, custom connectors and priority support."

  return (
    <Dialog open={isOpen} onOpenChange={(next) => { if (!next) handleClose() }}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <div className="flex items-center gap-2">
            {isTrialExpired ? (
              <Lock className="h-5 w-5 text-amber-500" />
            ) : (
              <Sparkles className="h-5 w-5 text-primary" />
            )}
            <DialogTitle>{title}</DialogTitle>
          </div>
          <DialogDescription className="pt-1">{description}</DialogDescription>
        </DialogHeader>

        <div className="rounded-lg border bg-muted/50 p-4 text-sm">
          <p className="font-medium">Pro plan includes:</p>
          <ul className="mt-2 space-y-1 text-muted-foreground">
            <li>• Unlimited pipelines</li>
            <li>• CDC (real-time sync)</li>
            <li>• Priority support</li>
            <li>• Custom connectors</li>
          </ul>
        </div>

        <p className="text-sm text-muted-foreground">
          {status === "sent" ? (
            <>Thanks — your request is on its way to our team. We&apos;ll reach out shortly to get you upgraded.</>
          ) : (
            <>
              Pro plans are set up by our team. Send a request and we&apos;ll reach out to get
              you upgraded — or email us at{" "}
              <span className="font-medium text-foreground">{contactEmail}</span>.
            </>
          )}
        </p>

        <DialogFooter className="gap-2 sm:gap-0">
          <Button variant="ghost" onClick={handleClose}>
            {status === "sent" ? "Close" : isProactive ? "Maybe later" : "Cancel"}
          </Button>
          <Button
            type="button"
            onClick={handleContact}
            loading={status === "sending"}
            disabled={status === "sent" || status === "sending"}
            aria-live="polite"
            // Brand CTA gradient using the DEFAULT Tailwind palette (violet/indigo),
            // matching the app's other upgrade CTAs (login/signup/connectors). The
            // shadcn semantic tokens (`bg-primary`/`from-primary`) are NOT mapped in
            // this Tailwind v4 setup, so they generate no CSS — that dead gradient +
            // white text is exactly what made the old button invisible until hover.
            // `dark:text-white` pins the label white in dark mode (the default Button
            // variant otherwise flips text to `dark:text-zinc-900`).
            className="bg-gradient-to-r from-violet-600 to-indigo-600 text-white dark:text-white shadow hover:from-violet-700 hover:to-indigo-700 focus-visible:ring-violet-500 disabled:opacity-100"
          >
            {status === "sent" ? (
              <>
                <Check className="h-4 w-4" />
                Request sent
              </>
            ) : status === "copied" ? (
              <>
                <Check className="h-4 w-4" />
                Copied {contactEmail}
              </>
            ) : status === "sending" ? (
              "Sending…"
            ) : (
              <>
                <Mail className="h-4 w-4" />
                Contact the rsync team
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
