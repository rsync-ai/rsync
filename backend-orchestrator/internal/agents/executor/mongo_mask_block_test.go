package executor

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// KI-MONGO-CDC-MASK-SILENT-NOOP (issue #23 stop-gap): a MongoDB CDC/streaming run
// that carries an enabled consumer mask transform must fail BEFORE any connector
// starts, because the sink lands the document packed under one field and the
// column mask would silently match nothing.

const mongoMaskPipelineID = "44444444-4444-4444-4444-444444444444"

const consumerMaskQuery = "SELECT transform_config\\s+FROM transform_definitions\\s+WHERE pipeline_id = \\$1 AND transform_type = 'consumer' AND enabled = TRUE"

// runTransferUntilBlocked calls executeDataTransfer and converts a panic into a
// test failure: with the block removed the run proceeds into the CDC start path,
// which this hermetic Agent cannot serve.
func runTransferUntilBlocked(t *testing.T, a *Agent, task ExecutorTask) (resp ExecutorResponse) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("run proceeded past the MongoDB CDC mask block into the CDC start path (panic: %v)", r)
		}
	}()
	return a.executeDataTransfer(context.Background(), task)
}

func mongoTask(params map[string]interface{}) ExecutorTask {
	return ExecutorTask{
		TaskID:      "task-1",
		PipelineID:  mongoMaskPipelineID,
		Source:      &ConnectorConfig{Type: "mongodb", Config: map[string]string{}},
		Destination: &ConnectorConfig{Type: "minio", Config: map[string]string{}},
		Params:      params,
	}
}

func TestMongoCDCMaskBlock_FailsRunBeforeStartSink(t *testing.T) {
	cases := []struct {
		name     string
		params   map[string]interface{}
		planLess bool // sync_mode absent from params → backfilled from pipelines row
	}{
		{name: "cdc", params: map[string]interface{}{"sync_mode": "cdc"}},
		{name: "streaming", params: map[string]interface{}{"sync_mode": "streaming"}},
		{name: "plan-less rerun", params: map[string]interface{}{}, planLess: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()

			mock.ExpectQuery("SELECT config->'nl_transforms' FROM pipelines").
				WithArgs(mongoMaskPipelineID).
				WillReturnRows(sqlmock.NewRows([]string{"nl_transforms"}).AddRow([]byte("null")))
			if tc.planLess {
				mock.ExpectQuery("SELECT sync_mode, cdc_mode FROM pipelines").
					WithArgs(mongoMaskPipelineID).
					WillReturnRows(sqlmock.NewRows([]string{"sync_mode", "cdc_mode"}).AddRow("cdc", "debezium"))
			}
			mock.ExpectQuery(consumerMaskQuery).
				WithArgs(mongoMaskPipelineID).
				WillReturnRows(sqlmock.NewRows([]string{"transform_config"}).
					AddRow([]byte(`{"operation":"mask","column":"email","mask_type":"hash"}`)).
					AddRow([]byte(`{"operation":"rename","from":"a","to":"b"}`)).
					AddRow([]byte(`{"type":"mask_pii","columns":["ssn","email"]}`)))

			a := &Agent{db: db}
			resp := runTransferUntilBlocked(t, a, mongoTask(tc.params))

			if resp.Status != "failed" {
				t.Fatalf("status = %q, want failed (resp=%+v)", resp.Status, resp)
			}
			if !strings.Contains(resp.Error, mongoCDCMaskBlockKI) {
				t.Fatalf("error does not carry the block marker: %q", resp.Error)
			}
			if !strings.Contains(resp.Error, "2 enabled mask transform(s) on [email ssn]") {
				t.Fatalf("error should name the masked columns (names only): %q", resp.Error)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("DB expectations: %v", err)
			}
		})
	}

	t.Run("DB error fails closed", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT config->'nl_transforms' FROM pipelines").
			WithArgs(mongoMaskPipelineID).
			WillReturnRows(sqlmock.NewRows([]string{"nl_transforms"}).AddRow([]byte("null")))
		dbErr := errors.New("pq: connection to 10.0.0.9 refused for user secret_admin")
		mock.ExpectQuery(consumerMaskQuery).WithArgs(mongoMaskPipelineID).WillReturnError(dbErr)

		a := &Agent{db: db}
		resp := runTransferUntilBlocked(t, a, mongoTask(map[string]interface{}{"sync_mode": "cdc"}))
		if resp.Status != "failed" || !strings.Contains(resp.Error, mongoCDCMaskBlockKI) {
			t.Fatalf("DB error must fail closed with the block marker, got %+v", resp)
		}
		if strings.Contains(resp.Error, "10.0.0.9") || strings.Contains(resp.Error, "secret_admin") {
			t.Fatalf("raw DB error text leaked into the run error: %q", resp.Error)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("DB expectations: %v", err)
		}
	})

	t.Run("unreadable transform config fails closed", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(consumerMaskQuery).WithArgs(mongoMaskPipelineID).
			WillReturnRows(sqlmock.NewRows([]string{"transform_config"}).AddRow([]byte(`{not json`)))
		a := &Agent{db: db}
		if err := a.mongoCDCMaskBlockError(context.Background(), mongoTask(nil), "cdc"); err == nil ||
			!strings.Contains(err.Error(), mongoCDCMaskBlockKI) {
			t.Fatalf("want fail-closed block error, got %v", err)
		}
	})
}

// TestMongoCDCMaskBlock_DoesNotBlockOtherRuns: the block is scoped to MongoDB
// CDC/streaming with an enabled mask. Relational CDC, Mongo batch, and a Mongo
// CDC pipeline with no mask all pass.
func TestMongoCDCMaskBlock_DoesNotBlockOtherRuns(t *testing.T) {
	t.Run("relational CDC control: no DB read, no block", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		a := &Agent{db: db}
		task := mongoTask(nil)
		task.Source.Type = "postgresql"
		if err := a.mongoCDCMaskBlockError(context.Background(), task, "cdc"); err != nil {
			t.Fatalf("relational CDC must not be blocked: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unexpected DB interaction: %v", err)
		}
	})

	t.Run("mongo batch: no block", func(t *testing.T) {
		db, _, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		a := &Agent{db: db}
		for _, mode := range []string{"batch", ""} {
			if err := a.mongoCDCMaskBlockError(context.Background(), mongoTask(nil), mode); err != nil {
				t.Fatalf("mongo sync_mode=%q must not be blocked: %v", mode, err)
			}
		}
	})

	t.Run("mongo CDC with only non-mask transforms: no block", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(consumerMaskQuery).WithArgs(mongoMaskPipelineID).
			WillReturnRows(sqlmock.NewRows([]string{"transform_config"}).
				AddRow([]byte(`{"operation":"type_convert","column":"price","to":"float"}`)))
		a := &Agent{db: db}
		if err := a.mongoCDCMaskBlockError(context.Background(), mongoTask(nil), "cdc"); err != nil {
			t.Fatalf("no mask → no block, got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("DB expectations: %v", err)
		}
	})
}

func TestIsMongoSourceFamily(t *testing.T) {
	for _, ct := range []string{"mongodb", "MongoDB", " mongo ", "mongodb-atlas", "mongodb_atlas", "mongodbatlas", "atlas"} {
		if !isMongoSourceFamily(ct) {
			t.Errorf("%q should be MongoDB family", ct)
		}
	}
	for _, ct := range []string{"postgresql", "mysql", "sqlserver", "oracle", "minio", ""} {
		if isMongoSourceFamily(ct) {
			t.Errorf("%q must not be MongoDB family", ct)
		}
	}
}

func TestIsMaskTransformConfig(t *testing.T) {
	for i, c := range []struct {
		cfg  map[string]any
		want bool
	}{
		{map[string]any{"operation": "mask"}, true},
		{map[string]any{"operation": "MASK_PII"}, true},
		{map[string]any{"type": "mask_pii"}, true},
		{map[string]any{"operation": "type_convert"}, false},
		{map[string]any{}, false},
	} {
		if got := isMaskTransformConfig(c.cfg); got != c.want {
			t.Errorf("case %d: got %v want %v", i, got, c.want)
		}
	}
}
