"use client"

import { Toaster } from "sonner"
import { useTheme } from "next-themes"
import { useEffect, useState } from "react"

type ToasterTheme = "light" | "dark" | "system"

/**
 * Theme-aware wrapper around sonner's <Toaster>.
 *
 * The root layout is a server component and next-themes' ThemeProvider is only
 * mounted in the (dashboard) layout, so useTheme() can return undefined outside
 * the dashboard. We therefore fall back to reading the `dark` class that
 * next-themes writes onto <html> (attribute="class"), observed live via a
 * MutationObserver so the toaster follows theme changes.
 */
export function ThemedToaster() {
  const { resolvedTheme } = useTheme()
  const [domTheme, setDomTheme] = useState<ToasterTheme>("system")

  useEffect(() => {
    const root = document.documentElement

    const read = (): ToasterTheme =>
      root.classList.contains("dark") ? "dark" : "light"

    setDomTheme(read())

    const observer = new MutationObserver(() => {
      setDomTheme(read())
    })
    observer.observe(root, { attributes: true, attributeFilter: ["class"] })

    return () => observer.disconnect()
  }, [])

  const theme: ToasterTheme =
    resolvedTheme === "dark" || resolvedTheme === "light"
      ? resolvedTheme
      : domTheme

  // closeButton: sonner *pauses* (does not extend) its 4s auto-close timer while
  // the toast list is hovered/expanded or the document is hidden. A toast that
  // catches a stuck pointer — or is raised in a backgrounded tab — therefore
  // stays up indefinitely with no way to dismiss it. The close button is the
  // manual escape hatch; without it the only exit is a page reload.
  return <Toaster richColors closeButton position="bottom-right" offset={96} theme={theme} />
}
