package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// objectLayoutGoldenPath locates shared/object_layout_golden.json from this package
// directory. Like scrubber_golden.json it lives outside the worker module because three
// separately compiled copies share it.
func objectLayoutGoldenPath() string {
	return filepath.Join("..", "..", "..", "..", "..", "..", "..", "shared", "object_layout_golden.json")
}

type objectLayoutGoldenCase struct {
	Name  string          `json:"name"`
	In    json.RawMessage `json:"in"`
	Want  *string         `json:"want"`
	Error *string         `json:"error"`
}

type objectLayoutV2GoldenIn struct {
	objectLayoutV2Table
	Dt          string `json:"dt"`
	LoadSeq     int64  `json:"load_seq"`
	TsMs        int64  `json:"ts_ms"`
	Partition   int    `json:"partition"`
	FirstOffset int64  `json:"first_offset"`
}

func objectLayoutGoldenRunners() map[string]map[string]func(json.RawMessage) (string, error) {
	v1 := func(f func(objectLayoutV1Input) string) func(json.RawMessage) (string, error) {
		return func(raw json.RawMessage) (string, error) {
			var in objectLayoutV1Input
			if err := objectLayoutStrictUnmarshal(raw, &in); err != nil {
				return "", err
			}
			return f(in), nil
		}
	}
	v2 := func(f func(objectLayoutV2GoldenIn) (string, error)) func(json.RawMessage) (string, error) {
		return func(raw json.RawMessage) (string, error) {
			var in objectLayoutV2GoldenIn
			if err := objectLayoutStrictUnmarshal(raw, &in); err != nil {
				return "", err
			}
			return f(in)
		}
	}
	return map[string]map[string]func(json.RawMessage) (string, error){
		"v1": {
			"batch_part_key":           v1(objectLayoutV1BatchPartKey),
			"batch_manifest_key":       v1(objectLayoutV1BatchManifestKey),
			"batch_success_key":        v1(objectLayoutV1BatchSuccessKey),
			"table_prefix":             v1(objectLayoutV1TablePrefix),
			"cdc_object_key":           v1(objectLayoutV1CDCObjectKey),
			"cdc_pipeline_root_prefix": v1(objectLayoutV1CDCPipelineRootPrefix),
		},
		"v2": {
			"pipeline_root_prefix": v2(func(in objectLayoutV2GoldenIn) (string, error) {
				return objectLayoutV2PipelineRootPrefix(in.ConnPrefix, in.PipelinePrefix)
			}),
			"table_prefix": v2(func(in objectLayoutV2GoldenIn) (string, error) {
				return objectLayoutV2TablePrefix(in.objectLayoutV2Table)
			}),
			"sidecar_table_prefix": v2(func(in objectLayoutV2GoldenIn) (string, error) {
				return objectLayoutV2SidecarTablePrefix(in.objectLayoutV2Table)
			}),
			"load_key": v2(func(in objectLayoutV2GoldenIn) (string, error) {
				return objectLayoutV2LoadKey(in.objectLayoutV2Table, in.Dt, in.LoadSeq)
			}),
			"cdc_key": v2(func(in objectLayoutV2GoldenIn) (string, error) {
				return objectLayoutV2CDCKey(in.objectLayoutV2Table, in.TsMs, in.Partition, in.FirstOffset)
			}),
			"manifest_key": v2(func(in objectLayoutV2GoldenIn) (string, error) {
				return objectLayoutV2ManifestKey(in.objectLayoutV2Table, in.Dt)
			}),
			"success_key": v2(func(in objectLayoutV2GoldenIn) (string, error) {
				return objectLayoutV2SuccessKey(in.objectLayoutV2Table, in.Dt)
			}),
		},
	}
}

// objectLayoutStrictUnmarshal rejects unknown input fields so a typo in the golden file
// fails loudly instead of silently testing a zero value.
func objectLayoutStrictUnmarshal(raw json.RawMessage, v interface{}) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// TestObjectLayoutV2DestinationMatchesGolden pins which destination types write layout
// v2 to the v2_destinations block the orchestrator and the frontend also read, so the
// three lists cannot drift apart.
func TestObjectLayoutV2DestinationMatchesGolden(t *testing.T) {
	data, err := os.ReadFile(objectLayoutGoldenPath())
	if err != nil {
		t.Fatalf("read golden: %v (run from a full repo checkout)", err)
	}
	var golden struct {
		V2Destinations *struct {
			Eligible    []string `json:"eligible"`
			NotEligible []string `json:"not_eligible"`
		} `json:"v2_destinations"`
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	d := golden.V2Destinations
	if d == nil || len(d.Eligible) == 0 || len(d.NotEligible) == 0 {
		t.Fatal("golden has no v2_destinations eligible / not_eligible lists")
	}
	for _, c := range d.Eligible {
		if !objectLayoutV2Destination(c) {
			t.Errorf("objectLayoutV2Destination(%q) = false, golden says eligible", c)
		}
	}
	for _, c := range d.NotEligible {
		if objectLayoutV2Destination(c) {
			t.Errorf("objectLayoutV2Destination(%q) = true, golden says not eligible", c)
		}
	}
}

// TestObjectLayoutMatchesCrossLanguageGolden pins the v1 key builders to what this worker
// writes today and the v2 builders to the agreed layout, using the fixture the
// orchestrator and rsync_protocol copies also read.
func TestObjectLayoutMatchesCrossLanguageGolden(t *testing.T) {
	data, err := os.ReadFile(objectLayoutGoldenPath())
	if err != nil {
		t.Fatalf("read golden: %v (run from a full repo checkout)", err)
	}
	var golden map[string]json.RawMessage
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	runners := objectLayoutGoldenRunners()
	total := 0
	for _, version := range []string{"v1", "v2"} {
		raw, ok := golden[version]
		if !ok {
			t.Fatalf("golden has no %q block", version)
		}
		var sections map[string][]objectLayoutGoldenCase
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
					var le *objectLayoutV2Error
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
