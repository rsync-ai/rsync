package cdcstats

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// handleSchemaChangeRecord reads one Debezium DDL record two ways: drop tracking (a
// health fact, always on) and a report onto healer.HealerTopic (the schema-drift path).
// With RSYNC_SCHEMA_DRIFT_ENABLED off, kafka.EnsurePlatformTopics does not provision
// rsync.healer.schema-changes and nothing consumes it, so a produce would only
// auto-create the topic at the broker's defaults.
//
// The agent here has NO Kafka manager (a.kafka == nil): any produce dereferences it
// and panics, so "did not panic" is "did not produce". The flag-on control proves the
// record really reaches the produce, so the flag-off pass is not vacuous.
func runSchemaChangeRecordWithoutKafka(t *testing.T, flag string) (panicked interface{}, mock sqlmock.Sqlmock) {
	t.Helper()
	t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", flag)
	a, w, mock := dropTrackAgent(t, []string{"cdc_drift"})
	mock.ExpectExec(`INSERT INTO cdc_source_table_drops`).
		WithArgs(dropTrackPipelineID, "cdc_drift").
		WillReturnResult(sqlmock.NewResult(0, 1))

	func() {
		defer func() { panicked = recover() }()
		a.handleSchemaChangeRecord(context.Background(), w,
			[]byte(dropRecord("DROP", "false", `"pipeline_test"."cdc_drift"`)))
	}()
	return panicked, mock
}

func TestSchemaChangeRecordDoesNotReportDriftWhenTheFlagIsOff(t *testing.T) {
	for _, flag := range []string{"", "false"} {
		t.Run("flag="+flag, func(t *testing.T) {
			p, mock := runSchemaChangeRecordWithoutKafka(t, flag)
			if p != nil {
				t.Fatalf("handleSchemaChangeRecord produced to the healer topic with "+
					"RSYNC_SCHEMA_DRIFT_ENABLED=%q (nil Kafka manager dereferenced: %v); "+
					"with the flag off that produce auto-creates rsync.healer.schema-changes", flag, p)
			}
			// Drop tracking is not part of the drift path and must still run.
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("the selected-table DROP was not recorded with the flag off: %v", err)
			}
		})
	}
}

func TestSchemaChangeRecordReachesTheProduceWhenTheFlagIsOn(t *testing.T) {
	p, _ := runSchemaChangeRecordWithoutKafka(t, "true")
	if p == nil {
		t.Fatal("with RSYNC_SCHEMA_DRIFT_ENABLED=true this DROP record never reached the " +
			"healer-topic produce, so the flag-off test above proves nothing")
	}
}
