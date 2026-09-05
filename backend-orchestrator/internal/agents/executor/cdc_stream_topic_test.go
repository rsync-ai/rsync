package executor

import (
	"github.com/rsync-ai/shared/kafkaclient"
	"strings"
	"testing"
)

// TestResolveCDCStreamTopic locks in the CDC -> object-storage delivery fix.
//
// The CDC *streaming* sink must subscribe to the Debezium per-table topics
// ("cdc-<id>.<db>.<table>"), never the plan-level pre-provisioned
// "pipeline.<id>.data" batch topic. When a "pipeline." value reaches
// startKafkaMCPSink it trips the batch-backfill guard (isBatchBackfillTopic),
// which disables the per-table fan-out — so the sink drains a single empty topic
// and writes 0 rows to the destination while every component still reports
// healthy. That was the prod failure (PostgreSQL -> aws-s3, "CDC snapshot+changes").
func TestResolveCDCStreamTopic(t *testing.T) {
	const conn = "cdc-3a7e63e5"

	cases := []struct {
		name   string
		result map[string]interface{}
		want   string
	}{
		{
			name:   "prefers provider first-table topic (top-level)",
			result: map[string]interface{}{"kafka_topic": "cdc-3a7e63e5.dev_partner_config.service_product"},
			want:   "cdc-3a7e63e5.dev_partner_config.service_product",
		},
		{
			name:   "reads topic nested under result",
			result: map[string]interface{}{"result": map[string]interface{}{"kafka_topic": "cdc-3a7e63e5.public.orders"}},
			want:   "cdc-3a7e63e5.public.orders",
		},
		{
			name:   "reads topic nested under structuredContent",
			result: map[string]interface{}{"structuredContent": map[string]interface{}{"kafka_topic": "cdc-3a7e63e5.sales.invoices"}},
			want:   "cdc-3a7e63e5.sales.invoices",
		},
		{
			name:   "falls back to connector name when provider topic absent",
			result: map[string]interface{}{"connector_name": "cdc-3a7e63e5"},
			want:   kafkaclient.Topic("cdc-3a7e63e5"),
		},
		{
			name:   "nil result falls back to connector name",
			result: nil,
			want:   kafkaclient.Topic("cdc-3a7e63e5"),
		},
		{
			// THE REGRESSION GUARD: a leaked pre-provisioned batch topic must be
			// rejected in favour of the Debezium prefix so the fan-out still fires.
			name:   "rejects leaked pipeline batch topic",
			result: map[string]interface{}{"kafka_topic": "pipeline.3a7e63e5.data"},
			want:   kafkaclient.Topic("cdc-3a7e63e5"),
		},
		{
			name:   "blank provider topic falls back",
			result: map[string]interface{}{"kafka_topic": "   "},
			want:   kafkaclient.Topic("cdc-3a7e63e5"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveCDCStreamTopic(conn, tc.result)
			if got != tc.want {
				t.Fatalf("resolveCDCStreamTopic() = %q, want %q", got, tc.want)
			}
			// Invariant the whole fix rests on: a CDC stream topic is never empty
			// and never the batch "pipeline." topic (either would drain nothing).
			if got == "" || kafkaclient.InNamespace(got, "pipeline.") {
				t.Fatalf("resolveCDCStreamTopic() returned a batch/empty topic %q — CDC sink would drain nothing", got)
			}
		})
	}
}

// TestKafkaTopicFromResult covers the envelope-shape tolerance directly: the prod
// override silently missed because the provider's kafka_topic did not arrive at
// the top level the caller checked, so cdcTopic fell back to the batch topic.
func TestKafkaTopicFromResult(t *testing.T) {
	cases := []struct {
		name   string
		result map[string]interface{}
		want   string
	}{
		{"nil", nil, ""},
		{"top-level", map[string]interface{}{"kafka_topic": "cdc-x.db.t"}, "cdc-x.db.t"},
		{"trimmed", map[string]interface{}{"kafka_topic": "  cdc-x.db.t  "}, "cdc-x.db.t"},
		{"nested result", map[string]interface{}{"result": map[string]interface{}{"kafka_topic": "cdc-x.db.t"}}, "cdc-x.db.t"},
		{"nested structuredContent", map[string]interface{}{"structuredContent": map[string]interface{}{"kafka_topic": "cdc-x.db.t"}}, "cdc-x.db.t"},
		{"absent", map[string]interface{}{"connector_name": "cdc-x"}, ""},
		{"blank", map[string]interface{}{"kafka_topic": "   "}, ""},
		{"wrong type", map[string]interface{}{"kafka_topic": 123}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := kafkaTopicFromResult(tc.result); got != tc.want {
				t.Fatalf("kafkaTopicFromResult() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestBuildCDCSinkTopics locks in the multi-schema sink topic-resolution fix.
//
// Regression: the SQL-Server re-qualification (#383) prepended the FIRST table's
// schema to every foreign-schema table on a multi-schema PostgreSQL/MySQL source,
// producing phantom topics ("cdc-x.blended_cost.fpa.line_items") that Debezium never
// writes to — stranding every non-first-schema table's rows in Kafka while the sink
// reported healthy. The database prepend must run for SQL Server only.
func TestBuildCDCSinkTopics(t *testing.T) {
	cases := []struct {
		name         string
		prefix       string
		dbQualifier  string
		sourceType   string
		tables       []string
		unifiedTopic string
		want         []string
	}{
		{
			name:        "multi-schema postgres keeps each table's own schema",
			prefix:      "cdc-58338eb7",
			dbQualifier: "blended_cost", // first table's schema; must NOT leak onto others
			sourceType:  "postgresql",
			tables:      []string{"blended_cost.costs", "fpa.document_line_items", "partner_model.leg_config"},
			want: []string{
				"cdc-58338eb7.blended_cost.costs",
				"cdc-58338eb7.fpa.document_line_items",
				"cdc-58338eb7.partner_model.leg_config",
			},
		},
		{
			name:        "multi-schema mysql keeps each table's own schema",
			prefix:      "cdc-abc",
			dbQualifier: "sales",
			sourceType:  "mysql",
			tables:      []string{"sales.orders", "inventory.items"},
			want:        []string{"cdc-abc.sales.orders", "cdc-abc.inventory.items"},
		},
		{
			name:        "sqlserver prepends the database segment (4-segment topics)",
			prefix:      "cdc-xyz",
			dbQualifier: "mydb",
			sourceType:  "sqlserver",
			tables:      []string{"dbo.cdc_test", "sales.orders"},
			want:        []string{"cdc-xyz.mydb.dbo.cdc_test", "cdc-xyz.mydb.sales.orders"},
		},
		{
			name:        "sqlserver leaves already-database-qualified table unchanged",
			prefix:      "cdc-xyz",
			dbQualifier: "mydb",
			sourceType:  "sqlserver",
			tables:      []string{"mydb.dbo.cdc_test"},
			want:        []string{"cdc-xyz.mydb.dbo.cdc_test"},
		},
		{
			name:        "bare table name gets the qualifier prepended",
			prefix:      "cdc-e2e",
			dbQualifier: "e2e_db",
			sourceType:  "mysql",
			tables:      []string{"big_table"},
			want:        []string{"cdc-e2e.e2e_db.big_table"},
		},
		{
			name:        "already prefix-qualified table left unchanged",
			prefix:      "cdc-e2e",
			dbQualifier: "e2e_db",
			sourceType:  "postgresql",
			tables:      []string{"cdc-e2e.public.foo"},
			want:        []string{"cdc-e2e.public.foo"},
		},
		{
			name:         "dimension table routed to unified topic",
			prefix:       "cdc-e2e",
			dbQualifier:  "public",
			sourceType:   "postgresql",
			tables:       []string{"public.fact_sales", "public.dim_customer"},
			unifiedTopic: "cdc-e2e.unified",
			want:         []string{"cdc-e2e.public.fact_sales", "cdc-e2e.unified"},
		},
		{
			name:        "duplicate tables are de-duplicated",
			prefix:      "cdc-e2e",
			dbQualifier: "public",
			sourceType:  "postgresql",
			tables:      []string{"public.foo", "public.foo"},
			want:        []string{"cdc-e2e.public.foo"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildCDCSinkTopics(tc.prefix, tc.dbQualifier, tc.sourceType, tc.tables, tc.unifiedTopic)
			if strings.Join(got, "|") != strings.Join(tc.want, "|") {
				t.Fatalf("buildCDCSinkTopics()\n got  = %v\n want = %v", got, tc.want)
			}
		})
	}
}
