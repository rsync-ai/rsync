import { AlertCircle, RefreshCw, WifiOff, ServerCrash, ShieldAlert } from "lucide-react"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"
import type { AppError } from "@/lib/utils/error-handling"

interface ErrorBannerProps {
  error: AppError
  onRetry?: () => void
  retryLabel?: string
  className?: string
  /** "card" (default) = full-width bordered card; "inline" = compact no-border variant */
  variant?: "card" | "inline"
}

function ErrorIcon({ code }: { code?: string }) {
  const cls = "h-5 w-5 mt-0.5 shrink-0 text-red-500"
  if (!code) return <AlertCircle className={cls} />
  if (code.includes("network") || code.includes("NETWORK")) return <WifiOff className={cls} />
  if (code.includes("auth") || code.includes("403") || code.includes("401")) return <ShieldAlert className={cls} />
  if (code.includes("service") || code.includes("503")) return <ServerCrash className={cls} />
  return <AlertCircle className={cls} />
}

export function ErrorBanner({ error, onRetry, retryLabel = "Retry", className, variant = "card" }: ErrorBannerProps) {
  const inner = (
    <div className="flex items-start gap-3">
      <ErrorIcon />
      <div className="flex-1 min-w-0 space-y-1">
        <p className="font-semibold text-sm text-red-700 dark:text-red-400">{error.title}</p>
        <p className="text-sm text-red-600 dark:text-red-300 font-mono break-words">{error.message}</p>
        {error.hint && (
          <div className="flex items-start gap-1.5 mt-2 text-xs text-zinc-600 dark:text-zinc-400 bg-white/60 dark:bg-zinc-900/60 rounded px-2 py-1.5">
            <span className="shrink-0 mt-px">💡</span>
            <span>{error.hint}</span>
          </div>
        )}
        {onRetry && (
          <Button variant="outline" size="sm" className="mt-2 h-7 text-xs" onClick={onRetry}>
            <RefreshCw className="h-3 w-3 mr-1.5" />
            {retryLabel}
          </Button>
        )}
      </div>
    </div>
  )

  if (variant === "inline") {
    return <div className={cn("py-2", className)}>{inner}</div>
  }

  return (
    <div className={cn("rounded-lg border border-red-200 bg-red-50 dark:bg-red-950/20 p-4", className)}>
      {inner}
    </div>
  )
}

/** Full-page centered error state (for load failures). */
export function ErrorPage({ error, onRetry }: { error: AppError; onRetry?: () => void }) {
  return (
    <div className="flex flex-col items-center justify-center py-20 gap-4 text-center max-w-sm mx-auto">
      <AlertCircle className="h-10 w-10 text-red-400" />
      <div className="space-y-1">
        <p className="font-semibold text-zinc-800 dark:text-zinc-200">{error.title}</p>
        <p className="text-sm text-zinc-500 font-mono break-words">{error.message}</p>
      </div>
      {error.hint && (
        <p className="text-xs text-zinc-400 italic">{error.hint}</p>
      )}
      {onRetry && (
        <Button variant="outline" size="sm" onClick={onRetry}>
          <RefreshCw className="h-4 w-4 mr-2" />
          Try again
        </Button>
      )}
    </div>
  )
}
