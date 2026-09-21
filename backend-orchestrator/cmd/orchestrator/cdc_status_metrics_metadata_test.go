package main

import (
	"encoding/json"
	"testing"
)

// roundTrip returns the metadata the way readers see it: marshalled into the event and
// decoded again.
func roundTrip(t *testing.T, meta map[string]interface{}) map[string]interface{} {
	t.Helper()
	b, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

// The status poll used to send lag times ten as rows_processed, which the pipeline page
// showed as "written 12800" beside 72,670 rows applied. Lag must still be reported; no
// number may be reported as rows.
func TestCDCStatusMetricsMetadata_ReportsLagButNoRowCount(t *testing.T) {
	result := map[string]interface{}{"health_status": "healthy", "connector_state": "RUNNING"}
	got := roundTrip(t, cdcStatusMetricsMetadata("conn-a", map[string]int64{"t1": 1000, "t2": 280}, result))

	for _, key := range []string{"rows_processed", "bytes_processed"} {
		if v, ok := got[key].(float64); ok {
			t.Errorf("%s = %v, want no number (lag is not a row or byte count)", key, v)
		}
	}
	if lag, ok := got["cdc_lag_ms"].(float64); !ok || lag != 12800 {
		t.Errorf("cdc_lag_ms = %v, want 12800", got["cdc_lag_ms"])
	}
	if n, ok := got["sink_lag_messages"].(float64); !ok || n != 1280 {
		t.Errorf("sink_lag_messages = %v, want 1280 (the summed lag across topics)", got["sink_lag_messages"])
	}
	if got["source"] != "cdc_status_poll" || got["connector_name"] != "conn-a" || got["connector_state"] != "RUNNING" {
		t.Errorf("identity fields changed: %v", got)
	}
}

func TestCDCStatusMetricsMetadata_NoLagReadingSendsNoLag(t *testing.T) {
	got := roundTrip(t, cdcStatusMetricsMetadata("conn-a", nil, map[string]interface{}{}))
	if v, ok := got["cdc_lag_ms"].(float64); ok {
		t.Errorf("cdc_lag_ms = %v without a lag reading, want null", v)
	}
	if v, ok := got["sink_lag_messages"].(float64); ok {
		t.Errorf("sink_lag_messages = %v without a lag reading, want null", v)
	}
}
