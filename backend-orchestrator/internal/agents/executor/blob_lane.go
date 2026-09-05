package executor

// Blob (raw-bytes passthrough) data plane — universal-blob-passthrough plan §3.
//
// This is the executor lane for the "blob" move modality: moving opaque objects
// (PDFs, parquet, images, arbitrary bytes) byte-identical from an object-storage
// source to an object-storage destination, gated by a capability match. It runs
// ONLY after enforceCapabilityGate (executor.go) has proven the destination
// accepts the "blob" modality, and ONLY in batch mode (CDC of blobs is a
// documented non-goal — plan §7).
//
// Unlike the structured batch lane it does NOT discover schema, infer column
// types, or run the transform engine — a blob is opaque bytes, not rows. The
// source connector stages each object byte-identical into the claim-check store
// (rsync's internal MinIO, the S3-compatible lingua franca every dest connector
// can read) and returns a pointer envelope; this lane forwards one Kafka message
// per envelope to the kafka-mcp-sink, which has the destination connector fetch
// the bytes and write them raw. The bytes never transit the executor.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// maxBlobPages caps the per-table blob export pagination so a connector that
// never advances its cursor can't loop forever (mirrors the structured lane's
// maxExportBatches safety cap).
const maxBlobPages = 100000

// blobExportPageLimit is the number of objects requested per source export page.
const blobExportPageLimit = 200

// bundledMinIOEndpoint is the compose stack's MinIO service
// (docker-compose.yml:675, :717, :917 all default MINIO_ENDPOINT_URL to it). It
// is the discriminator for whether the well-known minioadmin credentials are a
// sensible default or an active hazard -- see buildStagingConfig.
//
// Deliberately NOT the chart's MinIO: that Service is "<release>-minio", so its
// name depends on the release and this constant could never match it. It does
// not need to. In objectStorage.mode=minio the chart always emits both
// credentials from its Secret (_helpers.tpl "rsync-ai.objectStorageEnv"), so
// the defaulting branch is never reached there in the first place.
const bundledMinIOEndpoint = "http://rsync-ai-minio:9000"

// buildStagingConfig assembles the claim-check MinIO credentials the blob lane
// hands to BOTH the source connector (to stage bytes) and — via the sink message
// — the destination connector (to fetch them). The orchestrator process holds no
// MinIO creds of its own (structured staging is delegated to the minio MCP), so
// this reads the same MINIO_* env the rest of the stack uses and falls back to
// the well-known compose defaults. The bytes are staged under the "staging/"
// prefix so the existing MinIO bucket-lifecycle rule reclaims them (the GC
// backstop — see the sink's blob branch).
func (a *Agent) buildStagingConfig() map[string]interface{} {
	env := func(key, def string) string {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
		return def
	}
	prefix := env("MINIO_PREFIX", "staging")
	endpoint := env("MINIO_ENDPOINT_URL", bundledMinIOEndpoint)

	// The static credentials are defaulted ONLY against the bundled MinIO.
	//
	// Against any other endpoint, absent credentials mean workload identity:
	// IRSA on EKS, Workload Identity on GKE, the AKS equivalent. The Helm chart
	// emits no credential env at all on that path on purpose -- _helpers.tpl:659
	// gates the pair on objectStorage.external.accessKeyId, and values.yaml:375-378
	// documents leaving both empty as the way to use it -- because the SDK is
	// supposed to read a projected token instead. Substituting "minioadmin" here
	// does not fall back, it OVERRIDES the token, and the run fails with an
	// authentication error naming a key nobody configured.
	//
	// The consumer is base_connector.py _get_staging_client, which reads exactly
	// the "access_key"/"secret_key" names emitted below and omits the boto3
	// credential kwargs entirely when BOTH are empty, so the default chain
	// engages. Naming it matters: this comment used to cite the minio connector's
	// `cfg.get("access_key_id") or None`, which reads a key this map does not
	// emit, and it claimed the empty case was already handled when in fact
	// _get_staging_client rejected it outright -- so leaving these empty produced
	// exactly the failure the branch exists to prevent, one layer down, as
	// "blob passthrough requires a claim-check staging client"
	// (object_storage_source.py:509-514). Both halves are load-bearing; the
	// Python one is pinned by
	// shared/mcp-connectors/tests/test_staging_client_workload_identity.py.
	//
	// Against the bundled MinIO the old defaults are kept: they are that
	// deployment's real credentials, and compose sets them explicitly anyway
	// (docker-compose.yml:917-922), so this branch only covers a stack that set
	// neither. (KI-CHART-ORCHESTRATOR-LOSES-OBJECTSTORAGE-ENV.)
	accessKey := strings.TrimSpace(os.Getenv("MINIO_ACCESS_KEY_ID"))
	secretKey := strings.TrimSpace(os.Getenv("MINIO_SECRET_ACCESS_KEY"))
	if endpoint == bundledMinIOEndpoint {
		if accessKey == "" {
			accessKey = "minioadmin"
		}
		if secretKey == "" {
			secretKey = "minioadmin"
		}
	}

	return map[string]interface{}{
		"endpoint":   endpoint,
		"access_key": accessKey,
		"secret_key": secretKey,
		"region":     env("MINIO_REGION", "us-east-1"),
		"bucket":     env("MINIO_BUCKET", "pipeline-data"),
		"prefix":     strings.Trim(prefix, "/") + "/blobs",
	}
}

// blobTablesToExport resolves the "what to move" unit for a blob pipeline. Object
// stores select by bucket/prefix, not relational tables, but the planner still
// passes a selection list; we honor it when present and otherwise export the
// whole configured bucket (one pass with an empty table).
func blobTablesToExport(params map[string]interface{}) []string {
	out := []string{}
	for _, key := range []string{"tables", "selected_tables"} {
		switch v := params[key].(type) {
		case []interface{}:
			for _, it := range v {
				if s := strings.TrimSpace(fmt.Sprint(it)); s != "" {
					out = append(out, s)
				}
			}
		case []string:
			for _, s := range v {
				if s = strings.TrimSpace(s); s != "" {
					out = append(out, s)
				}
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return []string{""} // whole-bucket export
}

// executeBlobDataTransfer copies every selected object from the source object
// store to the destination object store via the claim-check / kafka-mcp-sink rail.
func (a *Agent) executeBlobDataTransfer(ctx context.Context, task ExecutorTask, kafkaTopic string, traceID string) ExecutorResponse {
	log.Infof("🗂️  Starting blob (raw-files passthrough) transfer: %s", task.PipelineID)

	executionID := ""
	if task.Params != nil {
		if v, ok := task.Params["execution_id"].(string); ok {
			executionID = strings.TrimSpace(v)
		}
	}

	sourceVersion := ""
	if task.Source != nil {
		sourceVersion = strings.TrimSpace(task.Source.Version)
		if sourceVersion == "" && task.Source.Config != nil {
			sourceVersion = strings.TrimSpace(task.Source.Config["connector_version"])
			if sourceVersion == "" {
				sourceVersion = strings.TrimSpace(task.Source.Config["version"])
			}
		}
	}
	if sourceVersion == "" {
		sourceVersion = "latest"
	}

	stagingConfig := a.buildStagingConfig()

	// Start the kafka-mcp-sink consumer BEFORE producing anything. Without it we'd
	// produce blob messages to a topic with NO consumer — the destination would stay
	// empty while the run reported success (the exact silent-drop this feature
	// exists to prevent). Fail loud if it can't start. Mirrors the structured batch
	// lane (executor.go executeBatchDataTransfer).
	sinkResult := a.startKafkaMCPSink(ctx, task, kafkaTopic, executionID, traceID)
	if sinkResult == nil || !sinkResult.Success {
		msg := "unknown error"
		if sinkResult != nil && sinkResult.Error != "" {
			msg = sinkResult.Error
		}
		return blobFailure(task, fmt.Sprintf("failed to start sink for blob transfer: %s", msg))
	}
	log.Infof("✅ Kafka-MCP-Sink started; copying blobs to topic=%s", kafkaTopic)

	// Forward the blob opt-in signals + object-selection knobs verbatim so the
	// source connector's _is_passthrough() (which mirrors the Go gate's
	// deriveMoveModality signal-for-signal) takes the blob-export path and honors
	// the per-file size cap.
	passthroughKeys := []string{
		"copy_raw_files", "passthrough", "raw_files", "move_modality",
		"delivery_method", "max_blob_bytes", "max_file_bytes", "prefix",
		"since", "modified_since", "modified_after",
	}

	totalBlobs := int64(0)
	totalBytes := int64(0)

	for _, tbl := range blobTablesToExport(task.Params) {
		destTableName := strings.TrimSpace(tbl)
		if destTableName == "" {
			destTableName = "blobs"
		}

		var cursor interface{}
		prevCursorJSON := ""
		idx := 0

		for page := 0; page < maxBlobPages; page++ {
			params := map[string]interface{}{
				"format": "json",
				"limit":  blobExportPageLimit,
				// Be explicit: even if the planner forgot the opt-in, the gate already
				// selected the blob modality, so force the source onto the blob lane.
				"copy_raw_files": true,
				"move_modality":  "blob",
				"staging_config": stagingConfig,
			}
			if tbl != "" {
				params["table"] = tbl
			}
			for _, k := range passthroughKeys {
				if v, ok := task.Params[k]; ok {
					params[k] = v
				}
			}
			if cursor != nil {
				params["cursor"] = cursor
			}

			exportReq := mcp.ExecuteRequest{
				Connector: task.Source.Type,
				Version:   sourceVersion,
				Operation: "export",
				Config:    task.Source.Config,
				Params:    params,
			}

			exportResp, err := a.executeWithRetry(ctx, exportReq)
			if err != nil {
				return blobFailure(task, fmt.Sprintf("blob export of %q failed: %v", destTableName, err))
			}
			if !exportResp.Success {
				return blobFailure(task, fmt.Sprintf("blob export of %q failed: %s", destTableName, exportResp.Error))
			}

			res := exportResp.Result
			if res == nil {
				res = map[string]interface{}{}
			}
			if nested, ok := res["result"].(map[string]interface{}); ok && nested != nil {
				res = nested
			}

			blobs, _ := res["blobs"].([]interface{})
			if len(blobs) == 0 {
				if page == 0 {
					log.Infof("  blob source %q: no objects to copy", destTableName)
				}
				break
			}

			for _, raw := range blobs {
				env, ok := raw.(map[string]interface{})
				if !ok {
					continue
				}
				objectKey := blobStr(env["object_key"])
				dataRef := blobStr(env["data_ref"])
				if objectKey == "" || dataRef == "" {
					// Fail loud: a blob envelope with no key/data_ref means the source
					// staged nothing — never silently skip it.
					return blobFailure(task, fmt.Sprintf("blob envelope missing object_key/data_ref: %v", env))
				}
				size := blobInt64(env["size"])
				message := map[string]interface{}{
					"pipeline_id":     task.PipelineID,
					"execution_id":    executionID,
					"table":           destTableName,
					"storage_type":    "blob",
					"is_blob":         true,
					"object_key":      objectKey,
					"content_type":    blobStr(env["content_type"]),
					"claim_check_url": dataRef,
					"sha256":          blobStr(env["sha256"]),
					"size":            size,
					"staging_config":  stagingConfig,
					"trace_id":        traceID,
					"batch_idx":       idx,
					"timestamp":       time.Now().UTC().Format(time.RFC3339),
				}
				msgBytes, _ := json.Marshal(message)
				headers := map[string]string{
					"pipeline_id":  task.PipelineID,
					"execution_id": executionID,
					"trace_id":     traceID,
					"table":        destTableName,
					"storage_type": "blob",
				}

				if err := a.produceBatchWithOutbox(ctx, kafkaTopic, []byte(objectKey), msgBytes, headers,
					task.PipelineID, executionID, destTableName, int64(idx), 1, size, "blob", dataRef); err != nil {
					return blobFailure(task, fmt.Sprintf("failed to enqueue blob %q: %v", objectKey, err))
				}
				idx++
				totalBlobs++
				totalBytes += size
				log.Infof("  📎 queued blob %s (%d bytes) → %s", objectKey, size, dataRef)
			}

			// Advance the per-file cursor; stop when exhausted or non-advancing.
			nextCursor, ok := res["next_cursor"]
			if !ok || nextCursor == nil {
				break
			}
			if b, mErr := json.Marshal(nextCursor); mErr == nil {
				nextJSON := string(b)
				if nextJSON == prevCursorJSON {
					log.Warnf("  blob source %q: cursor did not advance; stopping", destTableName)
					break
				}
				prevCursorJSON = nextJSON
			}
			cursor = nextCursor
		}
	}

	log.Infof("📨 Blob transfer dispatched: %d objects (%d bytes) for %s — verifying destination landing", totalBlobs, totalBytes, task.PipelineID)

	// Destination-truth verification (mirror the structured lane's silent-drop
	// guard): producing N messages to Kafka is DISPATCH truth, not LANDING truth —
	// the sink writes asynchronously. Reconcile against the pipeline_batch_acks
	// ledger (the sink writes one ack per blob AFTER a confirmed dest write) so a
	// crash-looping or misconfigured sink can never let the run report success with
	// nothing landed. This is the gate-4 "never silently drop" guarantee for blobs.
	status := "success"
	result := map[string]interface{}{
		"modality":     "blob",
		"blobs_copied": totalBlobs,
		"bytes":        totalBytes,
	}
	if totalBlobs > 0 {
		if executionID == "" {
			log.Warnf("⚠️ blob lane dispatched %d objects but execution_id is empty — ack-ledger reconciliation bypassed; destination landing unverifiable", totalBlobs)
		} else {
			// received (SUM(rows_read)) is ignored here: every blob is a distinct
			// object written once, so there is no benign upsert/dedup undercount to
			// disambiguate — a blob shortfall is already surfaced as unverified below.
			landed, _, ackRows, sinkErr := a.reconcileLandedRows(ctx, task.PipelineID, executionID, totalBlobs, batchAckReconcileDeadline())
			switch {
			case landed == 0 && ackRows > 0:
				// POSITIVE evidence: the sink wrote acks summing to 0 landed. Acks are
				// written only AFTER a successful dest write (fail-closed ledger), so
				// this means the destination genuinely dropped everything.
				reason := fmt.Sprintf("destination silently dropped all blobs: dispatched %d, ack ledger confirmed 0 landed across %d ack batches (execution %s)", totalBlobs, ackRows, executionID)
				if strings.TrimSpace(sinkErr) != "" {
					reason += fmt.Sprintf("; sink error: %s", sinkErr)
				}
				log.Warnf("🚨 blob silent drop detected (ack-ledger evidence): %s", reason)
				return ExecutorResponse{
					TaskID: task.TaskID, PipelineID: task.PipelineID,
					Status: "silent_drop_detected", Error: reason, Result: result,
				}
			case landed == 0 && ackRows == 0:
				// ABSENCE of evidence (ledger lag / unavailable) — ambiguous, never a
				// hard fail (the sink retries the ack forever). Surface as unverified.
				log.Warnf("⚠️ blob lane: dispatched %d objects but found 0 ack batches within the deadline (execution %s) — ledger lag/unavailable; marking unverified_completion", totalBlobs, executionID)
				status = "unverified_completion"
			case landed < totalBlobs:
				// Partial landing. Unlike rows (upsert/dedup can legitimately
				// undercount), every blob is a distinct object written once, so a
				// shortfall is meaningful — surface as unverified rather than success.
				log.Warnf("⚠️ blob lane: dispatched %d objects but ack ledger shows %d landed (execution %s)", totalBlobs, landed, executionID)
				result["blobs_landed"] = landed
				status = "unverified_completion"
			default:
				log.Infof("✅ Blob transfer verified: %d/%d objects landed for %s", landed, totalBlobs, task.PipelineID)
			}
		}
	}

	return ExecutorResponse{
		TaskID:        task.TaskID,
		PipelineID:    task.PipelineID,
		Status:        status,
		RowsProcessed: int(totalBlobs),
		PipelineType:  "batch",
		KafkaTopic:    kafkaTopic,
		Result:        result,
	}
}

func blobFailure(task ExecutorTask, msg string) ExecutorResponse {
	log.Errorf("❌ blob transfer failed: %s", msg)
	return ExecutorResponse{
		TaskID:     task.TaskID,
		PipelineID: task.PipelineID,
		Status:     "failed",
		Error:      msg,
	}
}

func blobStr(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

func blobInt64(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i
		}
	case string:
		if i, err := strconv.ParseInt(strings.TrimSpace(n), 10, 64); err == nil {
			return i
		}
	}
	return 0
}
