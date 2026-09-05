package executor

import "testing"

// TestExtractSnapshotVersion pins the resolution order for plan-time
// connector snapshots. The api-gateway records concrete versions in
// pipelines.{source,destination}_connector_snapshot at RunPipeline time and
// forwards them via Temporal's workflowInput; the executor must prefer
// those snapshots over the connection record's "connector_version" (which
// can be "latest" and would silently drift if a new connector ships
// mid-pipeline).
func TestExtractSnapshotVersion(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]interface{}
		key    string
		want   string
	}{
		{
			name:   "nil params returns empty",
			params: nil,
			key:    "source_connector_snapshot",
			want:   "",
		},
		{
			name:   "empty key returns empty",
			params: map[string]interface{}{"source_connector_snapshot": map[string]interface{}{"version": "v1.0.3"}},
			key:    "",
			want:   "",
		},
		{
			name:   "missing snapshot returns empty",
			params: map[string]interface{}{"other": "thing"},
			key:    "source_connector_snapshot",
			want:   "",
		},
		{
			name:   "nil snapshot value returns empty",
			params: map[string]interface{}{"source_connector_snapshot": nil},
			key:    "source_connector_snapshot",
			want:   "",
		},
		{
			name:   "snapshot not a map returns empty",
			params: map[string]interface{}{"source_connector_snapshot": "v1.0.3"},
			key:    "source_connector_snapshot",
			want:   "",
		},
		{
			name: "happy path returns version",
			params: map[string]interface{}{
				"source_connector_snapshot": map[string]interface{}{
					"version": "v1.0.3",
					"type":    "postgresql",
				},
			},
			key:  "source_connector_snapshot",
			want: "v1.0.3",
		},
		{
			name: "version with whitespace is trimmed",
			params: map[string]interface{}{
				"destination_connector_snapshot": map[string]interface{}{"version": "  v2.5.0  "},
			},
			key:  "destination_connector_snapshot",
			want: "v2.5.0",
		},
		{
			name: "non-string version returns empty",
			params: map[string]interface{}{
				"source_connector_snapshot": map[string]interface{}{"version": 1.03},
			},
			key:  "source_connector_snapshot",
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractSnapshotVersion(tc.params, tc.key)
			if got != tc.want {
				t.Fatalf("extractSnapshotVersion(%v, %q) = %q, want %q", tc.params, tc.key, got, tc.want)
			}
		})
	}
}
