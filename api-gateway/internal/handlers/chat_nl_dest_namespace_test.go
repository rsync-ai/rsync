package handlers

import (
	"testing"

	"api-gateway/internal/chat"
)

// #45: "sync … into my MongoDB Dest, database datingapp_pg3" created the pipeline
// with the destination's default namespace, and the Confirm card never showed
// which database the rows would land in.
func TestParseDestinationNamespaceIntent(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		want string
	}{
		{"report request", "Stream CDC from PostgreSQL to MongoDB. Sync all tables from my Postgresql Source datingapp database into my MongoDB Dest, database datingapp_pg3, with CDC: initial snapshot plus streaming changes", "datingapp_pg3"},
		{"schema after to", "copy postgres to snowflake schema analytics", "analytics"},
		{"named", "replicate mysql into bigquery dataset named sales_copy", "sales_copy"},
		{"quoted", "sync orders to mysql database `reporting`", "reporting"},
		{"card token wins", "Yes sync_mode=cdc cdc_mode=initial destination_namespace=sales_v2", "sales_v2"},
		// The source half never matches: the keyword comes before the name, after into/to.
		{"source database only", "sync my datingapp database to mongodb", ""},
		{"from clause", "sync from database orders_db to mongodb", ""},
		{"keyword with no name", "sync postgres to mysql database", ""},
		// Filler words and names that would need quoting are dropped.
		{"filler word", "sync postgres into database the", ""},
		{"bad characters token", "Yes destination_namespace=a-b", ""},
		{"no destination", "sync postgres to mongodb", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := parseDestinationNamespaceIntent(c.msg); got != c.want {
				t.Fatalf("parseDestinationNamespaceIntent(%q) = %q, want %q", c.msg, got, c.want)
			}
		})
	}
}

func TestConfirmationNamespaceFields(t *testing.T) {
	data := confirmationNamespaceFields(map[string]interface{}{"source_type": "postgresql"}, "postgresql", "mongodb",
		"sync postgres into my MongoDB Dest, database datingapp_pg3")
	if data["requested_destination_namespace"] != "datingapp_pg3" {
		t.Fatalf("requested = %v", data["requested_destination_namespace"])
	}
	if data["source_type"] != "postgresql" {
		t.Fatal("existing keys must survive")
	}
	if _, ok := data["destination_namespace_kind"].(string); !ok {
		t.Fatal("namespace kind missing")
	}
	if gcs := confirmationNamespaceFields(map[string]interface{}{}, "mongodb", "gcs", "sync mongo to gcs"); gcs["destination_namespace_kind"] != "path" || gcs["default_destination_namespace"] != "" {
		t.Fatalf("gcs fields = %v", gcs)
	}
}

// The card appends the typed name to the command it already sends. The extra token
// must not stop the turn from reading as a confirmation, and must be the name used.
func TestCardConfirmCommandWithNamespace(t *testing.T) {
	for _, cmd := range []string{
		"Yes sync_mode=batch destination_namespace=datingapp_pg3",
		"Yes sync_mode=cdc cdc_mode=initial destination_namespace=datingapp_pg3",
		"Yes sync_mode=cdc cdc_mode=streaming_only destination_namespace=datingapp_pg3",
	} {
		if got := chat.ParseConfirmation(cmd); got != chat.ConfirmationYes {
			t.Fatalf("ParseConfirmation(%q) = %v, want yes", cmd, got)
		}
		if got := parseDestinationNamespaceIntent(cmd); got != "datingapp_pg3" {
			t.Fatalf("namespace from %q = %q", cmd, got)
		}
	}
	if syncMode, cdcMode, _ := parseSyncModeOverrides("Yes sync_mode=cdc cdc_mode=streaming_only destination_namespace=batch_db"); syncMode != "cdc" || cdcMode != "streaming_only" {
		t.Fatalf("sync overrides = %q/%q", syncMode, cdcMode)
	}
}
