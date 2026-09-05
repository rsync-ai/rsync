"use client"

/**
 * ConnectionLogo - Single source of truth for displaying connector logos
 * 
 * Usage:
 * 1. With MCPConnector object: <ConnectionLogo connector={connector} />
 * 2. With connector name:       <ConnectionLogo connectorType="mysql" />
 * 
 * Logo loading:
 * - Fetches SVG from API Gateway (preferred)
 * - Falls back to PNG if SVG unavailable
 * - Shows emoji in gradient box on error
 */

import { useState } from "react"
import { MCPConnector, getConnectorEmoji, getCategoryColor } from "@/lib/types/mcp-connector"
import { getConnectorLogoUrl } from "@/lib/api/mcp-connectors"
import { API_GATEWAY_URL } from "@/lib/config/api"

interface ConnectionLogoProps {
  /** Full connector object (preferred) */
  connector?: MCPConnector
  /** Connector name (e.g., "mysql", "postgresql") */
  connectorType?: string
  /** Size variant */
  size?: "sm" | "md" | "lg"
  /** Category for fallback color (auto-detected from connector) */
  category?: string
  /** Enable debug logging */
  debug?: boolean
}

const sizeClasses = {
  sm: "h-8 w-8 text-lg",
  md: "h-12 w-12 text-2xl",
  lg: "h-16 w-16 text-3xl",
}

export function ConnectionLogo({
  connector,
  connectorType,
  size = "md",
  category,
  debug = false,
}: ConnectionLogoProps) {
  const [imageError, setImageError] = useState(false)
  const [imageLoading, setImageLoading] = useState(true)
  
  // Derive values from props
  const name = connector?.name || connectorType
  const displayName = connector?.display_name || connectorType || "Unknown"
  const effectiveCategory = category || connector?.category || "database"

  // Handle missing connector
  if (!name) {
    return (
      <div
        className={`flex ${sizeClasses[size]} shrink-0 items-center justify-center 
                   rounded-xl bg-gradient-to-br from-zinc-500 to-zinc-600 
                   text-white shadow-sm`}
      >
        🔗
      </div>
    )
  }
  
  // Get logo URL - prefer API-provided logo_url, fallback to constructed URL
  // If logo_url is relative, prepend API Gateway URL
  let logoUrl: string
  if (connector?.logo_url) {
    // Check if URL is relative (starts with /)
    if (connector.logo_url.startsWith('/')) {
      // Prepend the self-correcting API Gateway URL for relative paths (see
      // @/lib/config/api) so a mis-baked localhost value never leaks into the img src.
      logoUrl = `${API_GATEWAY_URL}${connector.logo_url}`
    } else {
      // Already absolute URL
      logoUrl = connector.logo_url
    }
  } else {
    // Fallback to constructed URL (already absolute)
    logoUrl = getConnectorLogoUrl(name)
  }

  if (debug) {
    console.log(`[ConnectionLogo] Rendering ${name}:`, {
      logoUrl,
      imageError,
      imageLoading,
    })
  }

  const handleImageError = (e: React.SyntheticEvent<HTMLImageElement>) => {
    if (debug) {
      console.error(`[ConnectionLogo] Failed to load ${name}:`, {
        logoUrl,
        error: e.type,
        currentSrc: (e.target as HTMLImageElement).currentSrc,
      })
    }
    setImageError(true)
    setImageLoading(false)
  }

  const handleImageLoad = () => {
    if (debug) {
      console.log(`[ConnectionLogo] Successfully loaded: ${name}`)
    }
    setImageLoading(false)
  }

  // Try to load logo
  if (!imageError) {
    return (
      <div className={`relative shrink-0 ${sizeClasses[size].split(" ").slice(0, 2).join(" ")}`}>
        {/* Loading placeholder */}
        {imageLoading && (
          <div
            className={`absolute inset-0 flex items-center justify-center rounded-xl 
                       bg-gradient-to-br ${getCategoryColor(effectiveCategory)} 
                       text-white animate-pulse`}
          >
            {getConnectorEmoji(name)}
          </div>
        )}
        
        {/* Logo image */}
        <img
          src={logoUrl}
          alt={`${displayName} logo`}
          className={`h-full w-full object-contain rounded-lg transition-opacity duration-200
                     ${imageLoading ? "opacity-0" : "opacity-100"}`}
          onLoad={handleImageLoad}
          onError={handleImageError}
          loading="eager"
          decoding="async"
        />
      </div>
    )
  }

  // Fallback: emoji in gradient box
  return (
    <div
      className={`flex ${sizeClasses[size]} shrink-0 items-center justify-center 
                 rounded-xl bg-gradient-to-br ${getCategoryColor(effectiveCategory)} 
                 text-white shadow-sm`}
    >
      {getConnectorEmoji(name)}
    </div>
  )
}
