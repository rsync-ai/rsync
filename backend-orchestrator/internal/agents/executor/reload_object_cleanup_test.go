package executor

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
	"github.com/rsync-ai/backend-orchestrator/internal/storage"
)

// TestReloadObjectTablePrefixMatchesSinkGolden pins the orchestrator's reload
// delete_prefix scope to the v1 table_prefix cases the sink's
// objectLayoutV1TablePrefix is tested against (object_layout_test.go), so the two
// cleanups empty the same folder the sink writes.
func TestReloadObjectTablePrefixMatchesSinkGolden(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "shared", "object_layout_golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v (run from a full repo checkout)", err)
	}
	var golden struct {
		V1 struct {
			TablePrefix []struct {
				Name string `json:"name"`
				In   struct {
					DestConfig map[string]string `json:"dest_config"`
					PipelineID string            `json:"pipeline_id"`
					Dataset    string            `json:"dataset"`
					DBOrSchema string            `json:"db_or_schema"`
					Table      string            `json:"table"`
				} `json:"in"`
				Want string `json:"want"`
			} `json:"table_prefix"`
		} `json:"v1"`
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(golden.V1.TablePrefix) < 4 {
		t.Fatalf("golden v1.table_prefix has %d cases, want >= 4", len(golden.V1.TablePrefix))
	}
	for _, c := range golden.V1.TablePrefix {
		t.Run(c.Name, func(t *testing.T) {
			// The executor hands the sink these two values (resolveBatchDataset and
			// the object-storage dbOrSchema default); the sink slugifies/sanitizes
			// them the same way.
			dataset := storage.Slugify(c.In.Dataset)
			if dataset == "" {
				dataset = resolveBatchDataset(true, c.In.PipelineID, "")
			}
			db := storage.SanitizePath(c.In.DBOrSchema)
			if db == "" {
				db = "default"
			}
			if got := reloadObjectTablePrefix(c.In.DestConfig, dataset, db, c.In.Table); got != c.Want {
				t.Fatalf("reloadObjectTablePrefix = %q, want %q", got, c.Want)
			}
		})
	}
}

func TestReloadObjectDeleteParamsNamesTheContainerForAzure(t *testing.T) {
	cfg := map[string]string{"bucket": "b1"}
	if p := reloadObjectDeleteParams("azure-blob", cfg, "x/"); p["container"] != "b1" || p["bucket"] != nil {
		t.Fatalf("azure params = %v, want container=b1 and no bucket", p)
	}
	if p := reloadObjectDeleteParams("gcs", cfg, "x/"); p["bucket"] != "b1" || p["prefix"] != "x/" {
		t.Fatalf("gcs params = %v, want bucket=b1 prefix=x/", p)
	}
}

func TestClassifyReloadDeleteResultFailsClosed(t *testing.T) {
	cases := []struct {
		name  string
		resp  *mcp.ExecuteResponse
		err   error
		fatal bool
	}{
		{"complete delete", &mcp.ExecuteResponse{Success: true, Result: map[string]interface{}{"success": true, "complete": true, "deleted": 12.0, "failed": 0.0}}, nil, false},
		{"empty folder", &mcp.ExecuteResponse{Success: true, Result: map[string]interface{}{"success": true, "complete": true, "deleted": 0.0, "failed": 0.0}}, nil, false},
		{"transport error", nil, errors.New("connection refused"), true},
		{"nil response", nil, nil, true},
		{"envelope failure", &mcp.ExecuteResponse{Success: false, Error: "bucket not found"}, nil, true},
		{"inner success false", &mcp.ExecuteResponse{Success: true, Result: map[string]interface{}{"success": false, "error": "denied"}}, nil, true},
		{"capped listing", &mcp.ExecuteResponse{Success: true, Result: map[string]interface{}{"success": true, "complete": false, "deleted": 1000.0, "failed": 0.0}}, nil, true},
		{"some objects failed", &mcp.ExecuteResponse{Success: true, Result: map[string]interface{}{"success": true, "complete": true, "deleted": 9.0, "failed": 3.0}}, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fatal, detail := classifyReloadDeleteResult(c.resp, c.err)
			if fatal != c.fatal {
				t.Fatalf("fatal = %v (detail %q), want %v", fatal, detail, c.fatal)
			}
			if fatal && detail == "" {
				t.Fatal("a fatal result must say why")
			}
		})
	}
}
