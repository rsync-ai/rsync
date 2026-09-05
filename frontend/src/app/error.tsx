"use client"

import { PageErrorBoundary } from "@/components/error-boundary/PageErrorBoundary"

export default function RootError({
  error,
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  return (
    <PageErrorBoundary
      title="Something went wrong"
      description="An unexpected error occurred."
      backHref="/"
      backLabel="Back home"
      error={error}
      reset={reset}
    />
  )
}

