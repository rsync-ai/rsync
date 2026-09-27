package handlers

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
)

// The pipeline page's problem chips (frontend PipelineInsightsBar.tsx) send the
// prompts in this shared golden; PipelineInsightsBar.chipPrompts.test.tsx pins
// the frontend to the same strings. Each one must reach the per-pipeline
// diagnosis with the pipeline's id. Before the id was added, "these stages are
// taking unusually long" matched no diagnose pattern, went to intent
// classification, and on prod's LLM timeout came back as the generic examples.
type chatInsightChipGolden struct {
	PipelineID string   `json:"pipeline_id"`
	Prompts    []string `json:"prompts"`
}

func loadChatInsightChipGolden(t *testing.T) chatInsightChipGolden {
	t.Helper()
	path := filepath.Join("..", "..", "..", "shared", "chat_insight_chip_prompts_golden.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	var g chatInsightChipGolden
	if err := json.Unmarshal(data, &g); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if g.PipelineID == "" || len(g.Prompts) == 0 {
		t.Fatalf("golden fixture is empty: %+v", g)
	}
	return g
}

func TestChatInsightChipPromptsReachThePipelinesDiagnosis(t *testing.T) {
	g := loadChatInsightChipGolden(t)
	const workspace = "ws-1"

	for _, prompt := range g.Prompts {
		t.Run(prompt, func(t *testing.T) {
			sqlDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			prev := db.DB
			db.DB = sqlDB
			t.Cleanup(func() {
				db.DB = prev
				sqlDB.Close()
			})
			// The id lookup must run with the chip's pipeline id, scoped to
			// the caller's workspace. Both miss here, so the handler answers
			// that it could not find it; the diagnosis itself is not under test.
			mock.ExpectQuery(`FROM pipelines p\s+WHERE p\.id = \$1 AND p\.workspace_id = \$2`).
				WithArgs(g.PipelineID, workspace).
				WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))
			mock.ExpectQuery(`FROM executions e`).
				WithArgs(g.PipelineID, workspace).
				WillReturnRows(sqlmock.NewRows([]string{"pipeline_id", "id", "name"}))

			resp, handled := (&ChatHandler{}).maybeHandleDiagnoseCommand(context.Background(), prompt, workspace, "trace")
			if !handled {
				t.Fatalf("the diagnose fast path did not take the chip's prompt; it would go to intent classification")
			}
			if !strings.Contains(resp.Message, "`"+g.PipelineID+"`") {
				t.Errorf("reply is not about the chip's pipeline: %q", resp.Message)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("the pipeline id was not looked up: %v", err)
			}
		})
	}
}

// The control: the prompt the user sent on prod, which is the first golden
// prompt without its id, matches no diagnose pattern. That is why it reached
// the model, and why the id is what routes it now.
func TestChatInsightChipPromptWithoutTheIdMissesEveryDiagnosePattern(t *testing.T) {
	g := loadChatInsightChipGolden(t)
	suffix := " (pipeline " + g.PipelineID + ")"
	if !strings.HasSuffix(g.Prompts[0], suffix) {
		t.Fatalf("first golden prompt does not end with %q: %q", suffix, g.Prompts[0])
	}
	bare := strings.TrimSuffix(g.Prompts[0], suffix)
	for name, re := range map[string]interface{ MatchString(string) bool }{
		"reDiagWithUUID": reDiagWithUUID,
		"reDiagLast":     reDiagLast,
		"reDiagGeneric":  reDiagGeneric,
		"reDiagBroad":    reDiagBroad,
	} {
		if re.MatchString(bare) {
			t.Errorf("%s matches the prompt without an id, so the id is not what routes it: %q", name, bare)
		}
	}
}
