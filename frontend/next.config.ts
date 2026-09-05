import type { NextConfig } from "next";
import { withSentryConfig } from "@sentry/nextjs";

const nextConfig: NextConfig = {
  output: 'standalone',
  reactStrictMode: true,
  async redirects() {
    return [
      // `/home` has no page; the sidebar "Home" link uses `/`. Redirect direct
      // navigation / bookmarks to the dashboard root instead of a 404.
      { source: '/home', destination: '/', permanent: false },
    ]
  },
};

export default withSentryConfig(nextConfig, {
  // Suppress Sentry CLI output except on errors
  silent: !process.env.CI,

  // Only upload source maps in CI / production builds
  widenClientFileUpload: true,
  sourcemaps: {
    deleteSourcemapsAfterUpload: true,
  },
  disableLogger: true,

  // Avoid build failure when SENTRY_DSN is missing in dev
  telemetry: false,
});
