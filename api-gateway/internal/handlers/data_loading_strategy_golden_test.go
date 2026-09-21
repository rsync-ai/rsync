package handlers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestDataLoadingStrategyStepsMatchSharedGolden pins dataLoadingStrategySteps to
// shared/data_loading_strategy_golden.json, which the frontend's
// computeStrategySteps is pinned to as well. The frontend rebuilds these lines
// when a strategy arrives without explanation_steps, so a change made to one
// builder and not the other shows the same pipeline two ways on two pages.
func TestDataLoadingStrategyStepsMatchSharedGolden(t *testing.T) {
	path := filepath.Join("..", "..", "..", "shared", "data_loading_strategy_golden.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	var cases []struct {
		Name  string `json:"name"`
		Input struct {
			Mode             string `json:"mode"`
			EffectiveCDCMode string `json:"effective_cdc_mode"`
			ScheduleStatus   string `json:"schedule_status"`
			RerunDefault     string `json:"rerun_default"`
			Dataset          string `json:"dataset"`
		} `json:"input"`
		Steps []string `json:"steps"`
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(cases) == 0 {
		t.Fatal("golden fixture is empty")
	}
	for _, c := range cases {
		in := c.Input
		got := dataLoadingStrategySteps(in.Mode, in.EffectiveCDCMode, in.ScheduleStatus, in.RerunDefault, in.Dataset)
		if !reflect.DeepEqual(got, c.Steps) {
			t.Errorf("%s:\n got: %q\nwant: %q", c.Name, got, c.Steps)
		}
	}
}
