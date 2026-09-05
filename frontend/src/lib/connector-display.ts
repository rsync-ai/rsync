// Canonical display names for connectors.
//
// We render connector ids in three places (chat assistant message, chat
// confirmation card, execution detail page) and the Reviewer flagged
// they disagreed: "PostgreSQL" vs "Postgresql", "Amazon S3" vs "AWS S3",
// "Shopify-admin-graphql" rendered raw. Single source of truth lives here.
//
// Match by canonical id (the connector_type stored in DB). If a known id
// isn't in the map we titlecase the parts as a graceful fallback.

const DISPLAY_NAMES: Record<string, string> = {
  // Databases
  postgresql: "PostgreSQL",
  mysql: "MySQL",
  mariadb: "MariaDB",
  oracle: "Oracle",
  mssql: "SQL Server",
  sqlite: "SQLite",
  mongodb: "MongoDB",
  cassandra: "Cassandra",
  dynamodb: "DynamoDB",
  redis: "Redis",

  // Warehouses
  snowflake: "Snowflake",
  bigquery: "BigQuery",
  redshift: "Redshift",
  databricks: "Databricks",
  clickhouse: "ClickHouse",

  // Object stores
  s3: "Amazon S3",
  gcs: "Google Cloud Storage",
  azure_blob: "Azure Blob Storage",

  // SaaS / APIs
  "shopify-admin-graphql": "Shopify Admin (GraphQL)",
  stripe: "Stripe",
  hubspot: "HubSpot",
  salesforce: "Salesforce",
  intercom: "Intercom",
  zendesk: "Zendesk",
  github: "GitHub",
  gitlab: "GitLab",
  notion: "Notion",
  airtable: "Airtable",
  slack: "Slack",
  jira: "Jira",
  asana: "Asana",
  linear: "Linear",
  monday: "Monday.com",
  pipedrive: "Pipedrive",
  mailchimp: "Mailchimp",
  sendgrid: "SendGrid",
  twilio: "Twilio",

  // Analytics
  amplitude: "Amplitude",
  mixpanel: "Mixpanel",
  segment: "Segment",
  google_analytics: "Google Analytics",

  // Streaming
  kafka: "Apache Kafka",
  kinesis: "Kinesis",
  pubsub: "Pub/Sub",
}

function titlecase(part: string): string {
  if (!part) return part
  return part[0].toUpperCase() + part.slice(1).toLowerCase()
}

export function displayConnectorName(connectorType: string | null | undefined): string {
  if (!connectorType) return ""
  const key = String(connectorType).trim().toLowerCase()
  if (DISPLAY_NAMES[key]) return DISPLAY_NAMES[key]

  // Aliases people commonly type
  if (key === "postgres" || key === "pg" || key === "postgesql") return DISPLAY_NAMES.postgresql
  if (key === "shopify" || key === "shopify_admin_graphql") return DISPLAY_NAMES["shopify-admin-graphql"]
  if (key === "bq") return DISPLAY_NAMES.bigquery
  if (key === "amazon_s3" || key === "aws_s3" || key === "aws-s3") return DISPLAY_NAMES.s3

  // Fallback: title-case each token, preserving the original separator.
  return key
    .split(/[-_\s]+/)
    .map(titlecase)
    .join(" ")
}
