/**
 * Feature Flags Configuration
 * Controls availability of monitoring UI tabs and features
 * 
 * These are read from environment variables at build time (NEXT_PUBLIC_*)
 * and can be overridden at runtime via API response
 */

import React from "react"

export interface FeatureFlags {
  // Monitoring feature flags
  monitoringOverview: boolean
  monitoringInfra: boolean
  monitoringTraces: boolean
  sigNozDeepLinks: boolean
  
  // SigNoz availability
  sigNozAvailable: boolean
}

/**
 * Default feature flags from environment variables
 * These are the build-time defaults
 */
export const DEFAULT_FEATURES: FeatureFlags = {
  // Monitoring Overview - defaults to OFF for safety
  monitoringOverview: parseBool(process.env.NEXT_PUBLIC_FEATURE_MONITORING_OVERVIEW, false),
  
  // Monitoring Infrastructure tab - defaults to OFF for safety
  monitoringInfra: parseBool(process.env.NEXT_PUBLIC_FEATURE_MONITORING_INFRA, false),
  
  // Monitoring Traces tab - defaults to OFF in production, ON in dev
  monitoringTraces: parseBool(
    process.env.NEXT_PUBLIC_FEATURE_MONITORING_TRACES,
    process.env.NODE_ENV === 'development'
  ),
  
  // SigNoz deep links - defaults to ON
  sigNozDeepLinks: parseBool(process.env.NEXT_PUBLIC_FEATURE_SIGNOZ_DEEPLINKS, true),
  
  // SigNoz availability
  sigNozAvailable: Boolean(process.env.NEXT_PUBLIC_SIGNOZ_UI_URL),
}

/**
 * Parse a boolean environment variable
 * @param value - Environment variable value
 * @param defaultValue - Default value if not set or invalid
 */
function parseBool(value: string | undefined, defaultValue: boolean): boolean {
  if (value === undefined || value === '') {
    return defaultValue
  }
  
  const normalized = value.toLowerCase()
  if (normalized === 'true' || normalized === '1' || normalized === 'yes') {
    return true
  }
  if (normalized === 'false' || normalized === '0' || normalized === 'no') {
    return false
  }
  
  console.warn(`Invalid boolean value for feature flag: ${value}, using default: ${defaultValue}`)
  return defaultValue
}

/**
 * Feature flags state management
 * Allows runtime override from API
 */
class FeatureFlagsManager {
  private flags: FeatureFlags = { ...DEFAULT_FEATURES }
  private listeners: Array<(flags: FeatureFlags) => void> = []

  /**
   * Get current feature flags
   */
  getFlags(): FeatureFlags {
    return { ...this.flags }
  }

  /**
   * Update feature flags (typically from API response)
   * @param updates - Partial feature flags to update
   */
  updateFlags(updates: Partial<FeatureFlags>): void {
    this.flags = { ...this.flags, ...updates }
    this.notifyListeners()
  }

  /**
   * Reset to default feature flags
   */
  resetFlags(): void {
    this.flags = { ...DEFAULT_FEATURES }
    this.notifyListeners()
  }

  /**
   * Subscribe to feature flag changes
   * @param listener - Callback function
   * @returns Unsubscribe function
   */
  subscribe(listener: (flags: FeatureFlags) => void): () => void {
    this.listeners.push(listener)
    return () => {
      this.listeners = this.listeners.filter(l => l !== listener)
    }
  }

  private notifyListeners(): void {
    this.listeners.forEach(listener => listener(this.flags))
  }

  /**
   * Check if any monitoring feature is enabled
   */
  isMonitoringEnabled(): boolean {
    return this.flags.monitoringOverview || this.flags.monitoringInfra || this.flags.monitoringTraces
  }

  /**
   * Check if a specific monitoring tab should be shown
   */
  shouldShowTab(tab: 'overview' | 'infrastructure' | 'traces'): boolean {
    switch (tab) {
      case 'overview':
        return this.flags.monitoringOverview
      case 'infrastructure':
        return this.flags.monitoringInfra
      case 'traces':
        return this.flags.monitoringTraces
      default:
        return false
    }
  }
}

// Singleton instance
export const featureFlagsManager = new FeatureFlagsManager()

/**
 * React hook for feature flags
 * Usage: const flags = useFeatureFlags()
 */
export function useFeatureFlags(): FeatureFlags {
  const [flags, setFlags] = React.useState<FeatureFlags>(featureFlagsManager.getFlags())

  React.useEffect(() => {
    // Ensure we start from the latest value (in case something updated before subscription)
    setFlags(featureFlagsManager.getFlags())
    return featureFlagsManager.subscribe((next) => setFlags({ ...next }))
  }, [])

  return flags
}

/**
 * Fetch feature flags from API and update manager
 * This should be called during app initialization
 */
export async function fetchFeatureFlags(apiUrl: string): Promise<void> {
  try {
    const response = await fetch(`${apiUrl}/api/v1/features`)
    if (response.ok) {
      const data = await response.json()
      featureFlagsManager.updateFlags({
        monitoringOverview: data.monitoring_overview ?? DEFAULT_FEATURES.monitoringOverview,
        monitoringInfra: data.monitoring_infra ?? DEFAULT_FEATURES.monitoringInfra,
        monitoringTraces: data.monitoring_traces ?? DEFAULT_FEATURES.monitoringTraces,
        sigNozDeepLinks: data.signoz_deeplinks ?? DEFAULT_FEATURES.sigNozDeepLinks,
        sigNozAvailable: data.signoz_available ?? DEFAULT_FEATURES.sigNozAvailable,
      })
    }
  } catch (error) {
    console.warn('Failed to fetch feature flags from API, using defaults:', error)
  }
}

/**
 * Helper to check if monitoring is available
 */
export function isMonitoringAvailable(): boolean {
  return featureFlagsManager.isMonitoringEnabled()
}

