package handlers

import "strings"

// getFriendlyName returns a user-friendly display name for a connector type.
// Keep in sync with frontend/src/lib/connector-display.ts — both are
// rendered in user-visible strings.
//
// (Relocated from the now-removed chat_orchestrator.go — the V1 Kafka chat
// path — where it was the only symbol still used by the live chat handler
// chat_nl_pipeline.go.)
func getFriendlyName(connectorType string) string {
	nameMap := map[string]string{
		// Databases
		"mysql":      "MySQL",
		"postgresql": "PostgreSQL",
		"postgres":   "PostgreSQL",
		"pg":         "PostgreSQL",
		"mariadb":    "MariaDB",
		"oracle":     "Oracle",
		"mssql":      "SQL Server",
		"sqlite":     "SQLite",
		"mongodb":    "MongoDB",
		"cassandra":  "Cassandra",
		"dynamodb":   "DynamoDB",
		"redis":      "Redis",
		// Object stores
		"aws_s3":               "Amazon S3",
		"aws-s3":               "Amazon S3",
		"s3":                   "Amazon S3",
		"google_cloud_storage": "Google Cloud Storage",
		"gcs":                  "Google Cloud Storage",
		"azure_blob":           "Azure Blob Storage",
		// Warehouses
		"snowflake":  "Snowflake",
		"bigquery":   "BigQuery",
		"bq":         "BigQuery",
		"redshift":   "Redshift",
		"databricks": "Databricks",
		"clickhouse": "ClickHouse",
		// Streaming
		"kafka":         "Apache Kafka",
		"kinesis":       "Kinesis",
		"pubsub":        "Pub/Sub",
		"elasticsearch": "Elasticsearch",
		// SaaS / APIs
		"shopify-admin-graphql": "Shopify Admin (GraphQL)",
		"shopify_admin_graphql": "Shopify Admin (GraphQL)",
		"shopify":               "Shopify Admin (GraphQL)",
		"stripe":                "Stripe",
		"hubspot":               "HubSpot",
		"salesforce":            "Salesforce",
		"intercom":              "Intercom",
		"zendesk":               "Zendesk",
		"github":                "GitHub",
		"gitlab":                "GitLab",
		"notion":                "Notion",
		"airtable":              "Airtable",
		"slack":                 "Slack",
		"jira":                  "Jira",
		"asana":                 "Asana",
		"linear":                "Linear",
		"monday":                "Monday.com",
		"pipedrive":             "Pipedrive",
		"mailchimp":             "Mailchimp",
		"sendgrid":              "SendGrid",
		"twilio":                "Twilio",
		// Analytics
		"amplitude":        "Amplitude",
		"mixpanel":         "Mixpanel",
		"segment":          "Segment",
		"google_analytics": "Google Analytics",
		// Generic fallbacks
		"database":      "Database",
		"cloud_storage": "Cloud Storage",
	}

	key := strings.ToLower(strings.TrimSpace(connectorType))
	if friendly, ok := nameMap[key]; ok {
		return friendly
	}

	// Fallback: title-case each token, preserving readability.
	if len(key) > 0 {
		parts := strings.FieldsFunc(key, func(r rune) bool {
			return r == '-' || r == '_' || r == ' '
		})
		for i, p := range parts {
			if p == "" {
				continue
			}
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
		return strings.Join(parts, " ")
	}

	return connectorType
}
