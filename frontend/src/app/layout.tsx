import type { Metadata } from "next"
import { Inter } from "next/font/google"
import { WebSocketProvider } from "@/contexts/WebSocketContext"
import { FeatureFlagsBootstrap } from "@/components/providers/FeatureFlagsBootstrap"
import { ThemedToaster } from "@/components/providers/ThemedToaster"
import { runtimeConfigScript } from "@/lib/config/runtime-env"
import "./globals.css"

const inter = Inter({
  subsets: ["latin"],
  variable: "--font-inter",
})

/**
 * Every route renders per request so the inline runtime config in <head> below
 * reflects this container, not the CI machine that built the image. Six routes
 * were static before this -- /login, /signup, /logout, /oauth/callback,
 * /verify-email, /_not-found -- and they are exactly the pages that must reach
 * the api-gateway before a session exists, so they are the ones that most need
 * a correct address.
 */
export const dynamic = "force-dynamic"

export const metadata: Metadata = {
  title: {
    default: "Rsync",
    template: "%s | Rsync",
  },
  description: "AI-powered data synchronization and replication platform",
  keywords: ["data", "sync", "replication", "CDC", "pipeline", "AI", "automation"],
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        {/*
          Where the api-gateway actually is, answered by the running container
          rather than by whoever built the image. See @/lib/config/runtime-env
          for why this cannot be a NEXT_PUBLIC_* read.

          INLINE, and that is the whole design. src/lib/config/api.ts resolves
          its base URL at MODULE SCOPE (api.ts:107), so this global has to exist
          before the first app chunk runs -- and an inline script executes inside
          the parser's own task, which an async external script cannot preempt.
          Next injects 14 async chunk tags ahead of anything this layout renders,
          so a <script src> here is a race, and next/script's `beforeInteractive`
          is worse still: in the App Router it renders as `self.__next_s.push()`,
          a queue the runtime drains later. Both were tried; both let the login
          chunk resolve first and send every request to the UI's own origin.

          Inlining a per-request value is also why this layout is force-dynamic:
          prerendered, the string below would freeze at build time and we would
          be back to shipping the builder's idea of the address.
        */}
        <script dangerouslySetInnerHTML={{ __html: runtimeConfigScript() }} />
      </head>
      <body className={`${inter.variable} font-sans antialiased`}>
        <WebSocketProvider>
          <FeatureFlagsBootstrap />
          {children}
        </WebSocketProvider>
        {/* Keep toasts away from bottom action bars (e.g. Save buttons) */}
        <ThemedToaster />
      </body>
    </html>
  )
}
