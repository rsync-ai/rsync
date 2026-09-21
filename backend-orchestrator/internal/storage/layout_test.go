package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// layoutGoldenPath locates shared/object_layout_golden.json from this package directory.
// It lives outside this module because three separately compiled copies share it.
func layoutGoldenPath() string {
	return filepath.Join("..", "..", "..", "shared", "object_layout_golden.json")
}

type layoutGoldenCase struct {
	Name  string          `json:"name"`
	In    json.RawMessage `json:"in"`
	Want  *string         `json:"want"`
	Error *string         `json:"error"`
}

type layoutV2GoldenIn struct {
	LayoutV2Table
	Dt          string `json:"dt"`
	LoadSeq     int64  `json:"load_seq"`
	TsMs        int64  `json:"ts_ms"`
	Partition   int    `json:"partition"`
	FirstOffset int64  `json:"first_offset"`
}

func layoutGoldenRunners() map[string]map[string]func(json.RawMessage) (string, error) {
	v1 := func(f func(LayoutV1Input) string) func(json.RawMessage) (string, error) {
		return func(raw json.RawMessage) (string, error) {
			var in LayoutV1Input
			if err := layoutStrictUnmarshal(raw, &in); err != nil {
				return "", err
			}
			return f(in), nil
		}
	}
	v2 := func(f func(layoutV2GoldenIn) (string, error)) func(json.RawMessage) (string, error) {
		return func(raw json.RawMessage) (string, error) {
			var in layoutV2GoldenIn
			if err := layoutStrictUnmarshal(raw, &in); err != nil {
				return "", err
			}
			return f(in)
		}
	}
	return map[string]map[string]func(json.RawMessage) (string, error){
		"v1": {
			"batch_part_key":           v1(LayoutV1BatchPartKey),
			"batch_manifest_key":       v1(LayoutV1BatchManifestKey),
			"batch_success_key":        v1(LayoutV1BatchSuccessKey),
			"table_prefix":             v1(LayoutV1TablePrefix),
			"cdc_object_key":           v1(LayoutV1CDCObjectKey),
			"cdc_pipeline_root_prefix": v1(LayoutV1CDCPipelineRootPrefix),
		},
		"v2": {
			"pipeline_root_prefix": v2(func(in layoutV2GoldenIn) (string, error) {
				return LayoutV2PipelineRootPrefix(in.ConnPrefix, in.PipelinePrefix)
			}),
			"table_prefix": v2(func(in layoutV2GoldenIn) (string, error) {
				return LayoutV2TablePrefix(in.LayoutV2Table)
			}),
			"sidecar_table_prefix": v2(func(in layoutV2GoldenIn) (string, error) {
				return LayoutV2SidecarTablePrefix(in.LayoutV2Table)
			}),
			"load_key": v2(func(in layoutV2GoldenIn) (string, error) {
				return LayoutV2LoadKey(in.LayoutV2Table, in.Dt, in.LoadSeq)
			}),
			"cdc_key": v2(func(in layoutV2GoldenIn) (string, error) {
				return LayoutV2CDCKey(in.LayoutV2Table, in.TsMs, in.Partition, in.FirstOffset)
			}),
			"manifest_key": v2(func(in layoutV2GoldenIn) (string, error) {
				return LayoutV2ManifestKey(in.LayoutV2Table, in.Dt)
			}),
			"success_key": v2(func(in layoutV2GoldenIn) (string, error) {
				return LayoutV2SuccessKey(in.LayoutV2Table, in.Dt)
			}),
		},
	}
}

// layoutStrictUnmarshal rejects unknown input fields so a typo in the golden file
// fails loudly instead of silently testing a zero value.
func layoutStrictUnmarshal(raw json.RawMessage, v interface{}) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// TestLayoutMatchesCrossLanguageGolden pins the LayoutV1* port to what the
// kafka-sink-worker writes today and LayoutV2* to the agreed layout, using the fixture
// the worker (object_layout_test.go) and rsync_protocol copies also read.
func TestLayoutMatchesCrossLanguageGolden(t *testing.T) {
	data, err := os.ReadFile(layoutGoldenPath())
	if err != nil {
		t.Fatalf("read golden: %v (run from a full repo checkout)", err)
	}
	var golden map[string]json.RawMessage
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	runners := layoutGoldenRunners()
	total := 0
	for _, version := range []string{"v1", "v2"} {
		raw, ok := golden[version]
		if !ok {
			t.Fatalf("golden has no %q block", version)
		}
		var sections map[string][]layoutGoldenCase
		if err := json.Unmarshal(raw, &sections); err != nil {
			t.Fatalf("parse %s: %v", version, err)
		}
		var names []string
		for name := range sections {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if _, ok := runners[version][name]; !ok {
				t.Errorf("%s.%s: section has no runner in this port", version, name)
			}
		}
		for name, run := range runners[version] {
			cases := sections[name]
			if len(cases) == 0 {
				t.Errorf("%s.%s: no cases in the golden file", version, name)
			}
			for _, c := range cases {
				total++
				label := version + "." + name + ": " + c.Name
				if (c.Want == nil) == (c.Error == nil) {
					t.Errorf("%s: a case needs exactly one of want / error", label)
					continue
				}
				got, err := run(c.In)
				if c.Error != nil {
					var le *LayoutV2Error
					if !errors.As(err, &le) || le.Code != *c.Error {
						t.Errorf("%s: got (%q, %v), want error %q", label, got, err, *c.Error)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s: unexpected error %v", label, err)
					continue
				}
				if got != *c.Want {
					t.Errorf("%s:\n got  %q\n want %q", label, got, *c.Want)
				}
			}
		}
	}
	if total < 50 {
		t.Fatalf("only %d golden cases ran; the fixture looks truncated", total)
	}
	t.Logf("%d golden cases matched", total)
}
