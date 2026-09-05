"use client"

import { PageErrorBoundary } from "@/components/error-boundary/PageErrorBoundary"

export default function ConnectionDetailError({
  error,
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  return (
    <PageErrorBoundary
      title="Connection Error"
      description="Failed to load connection details."
      backHref="/connections"
      backLabel="Back to Connections"
      error={error}
      reset={reset}
    />
  )
}

