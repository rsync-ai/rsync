package handlers

// Remembered acknowledgements for the pre-migration assessment gate (U-19).
//
// RunPipeline's gate stops a run on any WARNING until the caller sends
// ack_warnings. That ack used to be forgotten as soon as the run started, so
// every later Start or Reload of the same pipeline stopped at the gate again
// for the same, unchanged warnings — the most common "the pipeline hangs" in
// the demo matrix. Each acknowledged warning is now stored per pipeline under a
// content identity (code + table + object + message); a warning whose identity
// is stored no longer gates. A warning that changes, or a new one, still does.
// Errors are never waivable, stored or not (evaluateAssessmentGate).

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// assessmentWarningKey is the identity of one finding: stable across runs while
// the finding says the same thing about the same object, different as soon as
// it does not.
func assessmentWarningKey(t AssessmentTable, f AssessmentFinding) string {
	object := ""
	if f.Details != nil {
		if o, ok := f.Details["object"]; ok && o != nil {
			object = fmt.Sprint(o)
		}
	}
	h := sha256.New()
	for _, part := range []string{
		strings.TrimSpace(f.Code),
		strings.ToLower(strings.TrimSpace(t.Schema)),
		strings.ToLower(strings.TrimSpace(t.Name)),
		strings.ToLower(strings.TrimSpace(object)),
		strings.TrimSpace(f.Message),
	} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

type assessmentWarningRef struct {
	key, code, table string
}

// assessmentWarningKeys lists the distinct WARNING findings of a report.
func assessmentWarningKeys(report *AssessmentReport) []assessmentWarningRef {
	if report == nil {
		return nil
	}
	seen := map[string]bool{}
	var out []assessmentWarningRef
	for _, t := range report.Tables {
		for _, f := range t.Findings {
			if f.Severity != AssessmentWarning {
				continue
			}
			k := assessmentWarningKey(t, f)
			if seen[k] {
				continue
			}
			seen[k] = true
			table := t.Name
			if s := strings.TrimSpace(t.Schema); s != "" {
				table = s + "." + t.Name
			}
			out = append(out, assessmentWarningRef{key: k, code: f.Code, table: table})
		}
	}
	return out
}

// loadAssessmentAcks returns the warning identities acknowledged for a pipeline.
func loadAssessmentAcks(ctx context.Context, database *sql.DB, pipelineID string) (map[string]bool, error) {
	rows, err := database.QueryContext(ctx,
		`SELECT warning_key FROM pipeline_assessment_acks WHERE pipeline_id = $1::uuid`, pipelineID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out[k] = true
	}
	return out, rows.Err()
}

// saveAssessmentAcks remembers every warning of the report as acknowledged.
func saveAssessmentAcks(ctx context.Context, database *sql.DB, pipelineID, userID string, report *AssessmentReport) error {
	var by interface{}
	if strings.TrimSpace(userID) != "" {
		by = userID
	}
	for _, w := range assessmentWarningKeys(report) {
		if _, err := database.ExecContext(ctx, `
			INSERT INTO pipeline_assessment_acks (pipeline_id, warning_key, code, table_name, acknowledged_by)
			VALUES ($1::uuid, $2, $3, $4, $5::uuid)
			ON CONFLICT (pipeline_id, warning_key) DO NOTHING
		`, pipelineID, w.key, w.code, w.table, by); err != nil {
			return err
		}
	}
	return nil
}

// runAssessmentGate is RunPipeline's gate decision: remembered acks count as
// acknowledged, and a run the caller acks (and the gate then allows) is
// remembered for the next one. Storage failures are logged and fail toward
// asking again — never toward waiving.
func runAssessmentGate(ctx context.Context, database *sql.DB, pipelineID, userID string, report *AssessmentReport, ackWarnings bool) assessmentGateOutcome {
	acked, err := loadAssessmentAcks(ctx, database, pipelineID)
	if err != nil {
		log.Warnf("assessment gate pipeline=%s: load remembered acks: %v", pipelineID, err)
		acked = nil
	}
	markAcknowledged(report, acked)
	outcome := evaluateAssessmentGate(report, ackWarnings, acked)
	if outcome == assessmentGateAllow && ackWarnings {
		saveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := saveAssessmentAcks(saveCtx, database, pipelineID, userID, report); err != nil {
			log.Warnf("assessment gate pipeline=%s: remember acks: %v", pipelineID, err)
		}
	}
	return outcome
}

// markAcknowledged flags the warnings already acknowledged, so the modal can
// tell them apart from new ones.
func markAcknowledged(report *AssessmentReport, acked map[string]bool) {
	if report == nil || len(acked) == 0 {
		return
	}
	for i := range report.Tables {
		for j := range report.Tables[i].Findings {
			f := &report.Tables[i].Findings[j]
			if f.Severity == AssessmentWarning && acked[assessmentWarningKey(report.Tables[i], *f)] {
				f.Acknowledged = true
			}
		}
	}
}
