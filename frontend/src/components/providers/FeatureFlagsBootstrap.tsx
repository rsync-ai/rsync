"use client"

import { useEffect } from "react"
import { API_ENDPOINTS } from "@/lib/config/api"
import { fetchFeatureFlags } from "@/config/features"

/**
 * Bootstraps feature flags from the API at runtime.
 * This avoids needing to rebuild the frontend image just to toggle flags.
 */
export function FeatureFlagsBootstrap() {
  useEffect(() => {
    fetchFeatureFlags(API_ENDPOINTS.API_GATEWAY_URL)
  }, [])

  return null
}


