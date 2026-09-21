// Human label for a connector config key ("sslmode" → "SSL Mode", "api_key" → "API Key").
// Keys come straight from connector metadata, so a plain title-case of the key printed
// "Sslmode", "Uri" and "Authsource". Shared by the connection form and the connection
// detail page so the same field reads the same in both.

// Keys that are several words run together.
const JOINED_KEYS: Record<string, string> = {
  sslmode: "SSL Mode",
  sslrootcert: "SSL Root Cert",
  sslcert: "SSL Cert",
  sslkey: "SSL Key",
  authsource: "Auth Source",
  authmechanism: "Auth Mechanism",
  replicaset: "Replica Set",
  dbname: "Database Name",
  readpreference: "Read Preference",
  directconnection: "Direct Connection",
  mongodb: "MongoDB",
  postgresql: "PostgreSQL",
}

const ACRONYMS = new Set([
  "api", "aws", "ca", "cdc", "dsn", "gcp", "gcs", "http", "https", "id", "iam",
  "ip", "jdbc", "json", "kms", "oauth", "s3", "sasl", "sql", "ssh", "ssl", "tls",
  "uri", "url", "utc",
])

export function formatConfigLabel(key: string): string {
  const joined = JOINED_KEYS[key.toLowerCase()]
  if (joined) return joined
  return key
    .split(/[-_\s]+/)
    .filter(Boolean)
    .map((word) => {
      const lower = word.toLowerCase()
      if (ACRONYMS.has(lower)) return lower.toUpperCase()
      return JOINED_KEYS[lower] ?? word.charAt(0).toUpperCase() + word.slice(1)
    })
    .join(" ")
}
