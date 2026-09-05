"use client"

import { cn } from "@/lib/utils"

// Logo SVG viewBox: 901 × 1163
// Icon mark visible region: x 100–740 (width ~640), y 245–715 (height ~470)
// These constants match the website's LogoIcon cropping exactly.
const CROP = { x: 100, y: 245, w: 640, h: 470, svgW: 901, svgH: 1163 }

interface LogoIconProps {
  size?: number
  className?: string
}

function LogoIcon({ size = 40, className }: LogoIconProps) {
  const displayH = size * (CROP.h / CROP.w)
  return (
    <div
      className={cn("shrink-0 overflow-hidden relative inline-block", className)}
      style={{ width: size, height: displayH }}
    >
      <img
        src="/logo.svg"
        alt="rsync.ai"
        draggable={false}
        style={{
          position: "absolute",
          top: -size * (CROP.y / CROP.w),
          left: -size * (CROP.x / CROP.w),
          width: size * (CROP.svgW / CROP.w),
          height: size * (CROP.svgH / CROP.w),
          maxWidth: "none",
          display: "block",
        }}
      />
    </div>
  )
}

interface RsyncLogoProps {
  className?: string
  size?: "sm" | "md" | "lg"
  showText?: boolean
}

const iconSizes = { sm: 28, md: 36, lg: 48 }

export function RsyncLogo({ className, size = "md", showText }: RsyncLogoProps) {
  const px = iconSizes[size]
  return (
    <div className={cn("flex items-center gap-2", className)}>
      <LogoIcon size={px} />
      {showText && (
        <span className="font-bold tracking-tight text-slate-800 text-xl">
          rsync.ai
        </span>
      )}
    </div>
  )
}

export function RsyncIcon({ className }: { className?: string }) {
  return <LogoIcon size={28} className={className} />
}
