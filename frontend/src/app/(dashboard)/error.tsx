"use client"

import { PageErrorBoundary } from "@/components/error-boundary/PageErrorBoundary"

export default function DashboardError({
  error,
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  return (
    <PageErrorBoundary
      title="Dashboard Error"
      description="Something went wrong while loading this page."
      backHref="/dashboard"
      backLabel="Back to Dashboard"
      error={error}
      reset={reset}
    />
  )
}

