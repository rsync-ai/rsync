"use client"

import { PageErrorBoundary } from "@/components/error-boundary/PageErrorBoundary"

export default function ConnectorGenerateError({
  error,
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  return (
    <PageErrorBoundary
      title="Connector Generation Error"
      description="Something went wrong while generating the connector."
      backHref="/connectors"
      backLabel="Back to Connectors"
      error={error}
      reset={reset}
    />
  )
}

