"use client"

import { useState } from "react"
import { Database, Cloud, Zap, FileJson, Server, Box } from "lucide-react"
import { cn } from "@/lib/utils"

interface ConnectorLogoProps {
  connectorType: string
  logoUrl?: string
  size?: "sm" | "md" | "lg"
  className?: string
}

const sizeClasses = {
  sm: "h-6 w-6",
  md: "h-10 w-10",
  lg: "h-14 w-14",
}

const iconSizeClasses = {
  sm: "h-4 w-4",
  md: "h-6 w-6",
  lg: "h-8 w-8",
}

// Fallback icons based on connector type
const getFallbackIcon = (connectorType: string) => {
  const typeLC = connectorType.toLowerCase()
  
  if (typeLC.includes("mysql") || typeLC.includes("postgres") || typeLC.includes("sql") || typeLC.includes("db")) {
    return Database
  }
  if (typeLC.includes("s3") || typeLC.includes("cloud") || typeLC.includes("storage") || typeLC.includes("minio")) {
    return Cloud
  }
  if (typeLC.includes("kafka") || typeLC.includes("stream")) {
    return Zap
  }
  if (typeLC.includes("json") || typeLC.includes("file")) {
    return FileJson
  }
  if (typeLC.includes("api") || typeLC.includes("rest")) {
    return Server
  }
  
  return Box
}

// Color based on connector type
const getIconColor = (connectorType: string): string => {
  const typeLC = connectorType.toLowerCase()
  
  if (typeLC.includes("mysql")) return "text-blue-500"
  if (typeLC.includes("postgres")) return "text-blue-700"
  if (typeLC.includes("s3") || typeLC.includes("aws")) return "text-orange-500"
  if (typeLC.includes("kafka")) return "text-black dark:text-white"
  if (typeLC.includes("minio")) return "text-red-500"
  if (typeLC.includes("mongo")) return "text-green-500"
  if (typeLC.includes("redis")) return "text-red-600"
  if (typeLC.includes("stripe")) return "text-purple-500"
  if (typeLC.includes("notion")) return "text-gray-800 dark:text-gray-200"
  
  return "text-muted-foreground"
}

export function ConnectorLogo({
  connectorType,
  logoUrl,
  size = "md",
  className,
}: ConnectorLogoProps) {
  const [imgError, setImgError] = useState(false)
  
  const FallbackIcon = getFallbackIcon(connectorType)
  const iconColor = getIconColor(connectorType)
  
  const containerClasses = cn(
    "flex items-center justify-center rounded-lg bg-muted overflow-hidden",
    sizeClasses[size],
    className
  )

  // If we have a logo URL and no error loading it
  if (logoUrl && !imgError) {
    return (
      <div className={containerClasses}>
        {/* Plain <img>, not next/image — mirrors ConnectionLogo.tsx, which renders
            the same URLs without failing. The optimizer at /_next/image 400s on
            these logos twice over: the remote hosts are absent from
            next.config.ts images.remotePatterns, and most connector logos are
            image/svg+xml, which next/image refuses unless dangerouslyAllowSVG is
            set — and enabling that would regress the #679 SVG-XXE hardening.
            These are small static icons, so there is nothing for the optimizer
            to win here. */}
        <img
          src={logoUrl}
          alt={`${connectorType} logo`}
          width={size === "lg" ? 56 : size === "md" ? 40 : 24}
          height={size === "lg" ? 56 : size === "md" ? 40 : 24}
          className="object-contain p-1"
          onError={() => setImgError(true)}
          loading="lazy"
          decoding="async"
        />
      </div>
    )
  }

  // Fallback to icon
  return (
    <div className={containerClasses}>
      <FallbackIcon className={cn(iconSizeClasses[size], iconColor)} />
    </div>
  )
}
