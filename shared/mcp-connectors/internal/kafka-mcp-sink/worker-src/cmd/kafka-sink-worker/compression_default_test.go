package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/segmentio/kafka-go"
)

// A GCS connection saved without a `compression` value used to write uncompressed
// objects while the connector's own schema, and every connection created in the form,
// said gzip. Two runs into the same bucket could then disagree on codec depending only
// on how their connection was saved. These tests pin the one rule that replaced the
// three separate "absent means none" fallbacks: absent or blank takes the connector's
// schema default, an explicit value always wins.

func TestObjectStorageCompressionDefaults(t *testing.T) {
	cases := []struct {
		name     string
		destType string
		cfg      map[string]interface{}
		want     string
	}{
		{"gcs absent takes the schema default", "gcs", map[string]interface{}{"bucket": "b"}, "gzip"},
		{"gcs blank is absent", "gcs", map[string]interface{}{"compression": "   "}, "gzip"},
		{"gcs nil config", "gcs", nil, "gzip"},
		{"explicit none is honoured", "gcs", map[string]interface{}{"compression": "none"}, "none"},
		{"explicit codec is honoured", "gcs", map[string]interface{}{"compression": "zstd"}, "zstd"},
		{"aws-s3 absent", "aws-s3", map[string]interface{}{}, "gzip"},
		{"aws-s3 explicit none", "aws-s3", map[string]interface{}{"compression": "none"}, "none"},
		{"azure-blob absent", "azure-blob", map[string]interface{}{}, "gzip"},
		{"underscore spelling canonicalizes", "azure_blob", map[string]interface{}{}, "gzip"},
		{"case canonicalizes", "GCS", map[string]interface{}{}, "gzip"},
		// minio's connector has no compression setting and writes the bytes it is given;
		// a ".gz" name there would describe a codec nothing applied.
		{"minio absent stays none", "minio", map[string]interface{}{}, "none"},
		// "s3" and "*s3*" are object storage to the sink but have no schema to copy from.
		{"bare s3 absent stays none", "s3", map[string]interface{}{}, "none"},
		{"custom *s3* absent stays none", "custom-s3", map[string]interface{}{}, "none"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := objectStorageCompression(tc.destType, tc.cfg); got != tc.want {
				t.Fatalf("objectStorageCompression(%q, %v) = %q, want %q", tc.destType, tc.cfg, got, tc.want)
			}
		})
	}
}

// The codec also names the object. A default that reached params["compression"] but
// not the key (or the reverse) would write gzip bytes under a ".jsonl" name.
func TestDefaultedCompressionNamesTheObject(t *testing.T) {
	cases := []struct {
		destType, format string
		cfg              map[string]interface{}
		want             string
	}{
		{"gcs", "jsonl", map[string]interface{}{}, "jsonl.gz"},
		{"gcs", "csv", map[string]interface{}{}, "csv.gz"},
		// Parquet compresses its pages internally; the default must not add ".gz".
		{"gcs", "parquet", map[string]interface{}{}, "parquet"},
		{"gcs", "jsonl", map[string]interface{}{"compression": "none"}, "jsonl"},
		{"minio", "jsonl", map[string]interface{}{}, "jsonl"},
	}
	for _, tc := range cases {
		got := fileExt(tc.format, objectStorageCompression(tc.destType, tc.cfg))
		if got != tc.want {
			t.Errorf("%s/%s cfg=%v: extension %q, want %q", tc.destType, tc.format, tc.cfg, got, tc.want)
		}
	}
}

// storageConnectorsDir is the public storage connectors tree, from this package's dir.
func storageConnectorsDir() string {
	return filepath.Join("..", "..", "..", "..", "..", "..", "..", "shared", "mcp-connectors", "public", "storage")
}

type storageSchemaDefault struct {
	id       string
	defaults map[string]string // schema block -> compression default ("" when the property is absent)
}

// readStorageSchemaDefaults resolves each storage connector through latest.json's
// current_version — the directory that actually ships — and reads the compression
// default from both schema blocks.
func readStorageSchemaDefaults(t *testing.T) []storageSchemaDefault {
	t.Helper()
	root := storageConnectorsDir()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("cannot read storage connectors at %s: %v", root, err)
	}
	var out []storageSchemaDefault
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, e.Name(), "latest.json"))
		if err != nil {
			t.Fatalf("%s: cannot read latest.json: %v", e.Name(), err)
		}
		var latest struct {
			CurrentVersion string `json:"current_version"`
		}
		if err := json.Unmarshal(raw, &latest); err != nil || latest.CurrentVersion == "" {
			t.Fatalf("%s: latest.json has no current_version (err=%v)", e.Name(), err)
		}
		raw, err = os.ReadFile(filepath.Join(root, e.Name(), "versions", latest.CurrentVersion, "metadata.json"))
		if err != nil {
			t.Fatalf("%s: cannot read metadata.json for %s: %v", e.Name(), latest.CurrentVersion, err)
		}
		var md map[string]json.RawMessage
		if err := json.Unmarshal(raw, &md); err != nil {
			t.Fatalf("%s: metadata.json is not JSON: %v", e.Name(), err)
		}
		var id string
		_ = json.Unmarshal(md["id"], &id)
		if id == "" {
			t.Fatalf("%s: metadata.json has no id", e.Name())
		}
		sd := storageSchemaDefault{id: id, defaults: map[string]string{}}
		for _, block := range []string{"configuration_schema", "config_schema"} {
			var schema struct {
				Properties map[string]struct {
					Default interface{} `json:"default"`
				} `json:"properties"`
			}
			if blob, ok := md[block]; ok {
				if err := json.Unmarshal(blob, &schema); err != nil {
					t.Fatalf("%s: %s is not a schema: %v", id, block, err)
				}
			}
			if p, ok := schema.Properties["compression"]; ok {
				s, _ := p.Default.(string)
				sd.defaults[block] = s
			} else {
				sd.defaults[block] = ""
			}
		}
		out = append(out, sd)
	}
	return out
}

// Lockstep in both directions: every entry in the map copies a schema default that
// still exists, and every storage connector that declares a compression default and
// that the sink routes to object storage has an entry. A schema default changed
// without the sink, or a new storage connector added without it, fails here.
func TestCompressionDefaultsMatchConnectorSchemas(t *testing.T) {
	connectors := readStorageSchemaDefaults(t)
	byID := map[string]storageSchemaDefault{}
	declared := 0
	for _, c := range connectors {
		byID[canonicalConnectorType(c.id)] = c
		cfg, sch := c.defaults["configuration_schema"], c.defaults["config_schema"]
		if cfg != sch {
			t.Errorf("%s: configuration_schema says compression=%q but config_schema says %q; "+
				"the form and the sink would disagree", c.id, cfg, sch)
		}
		if cfg == "" || !isObjectStorageConnector(c.id) {
			continue
		}
		declared++
		got, ok := objectStorageCompressionDefaults[canonicalConnectorType(c.id)]
		if !ok {
			t.Errorf("%s declares compression default %q but objectStorageCompressionDefaults has no entry; "+
				"a connection saved without the key would write uncompressed", c.id, cfg)
		} else if got != cfg {
			t.Errorf("%s: sink defaults compression to %q, connector schema says %q", c.id, got, cfg)
		}
	}
	// Vacuity floor: a wrong path or an emptied tree would make the loop above pass.
	if declared == 0 {
		t.Fatalf("found no storage connector declaring a compression default under %s; "+
			"the census checked nothing", storageConnectorsDir())
	}
	for id, want := range objectStorageCompressionDefaults {
		c, ok := byID[id]
		if !ok {
			t.Errorf("objectStorageCompressionDefaults[%q]=%q names no connector under %s", id, want, storageConnectorsDir())
			continue
		}
		if c.defaults["configuration_schema"] != want {
			t.Errorf("objectStorageCompressionDefaults[%q]=%q, but its schema default is %q",
				id, want, c.defaults["configuration_schema"])
		}
	}
}

// importDataArgs returns the arguments of the captured <type>_import_data call.
func importDataArgs(t *testing.T, ct *captureTransport) map[string]interface{} {
	t.Helper()
	for i := len(ct.bodies) - 1; i >= 0; i-- {
		var env struct {
			Params struct {
				Name      string                 `json:"name"`
				Arguments map[string]interface{} `json:"arguments"`
			} `json:"params"`
		}
		if json.Unmarshal(ct.bodies[i], &env) == nil && strings.HasSuffix(env.Params.Name, "_import_data") {
			return env.Params.Arguments
		}
	}
	t.Fatalf("no _import_data call among %d captured request(s)", len(ct.bodies))
	return nil
}

func assertCodecAndKey(t *testing.T, args map[string]interface{}, wantCodec, wantSuffix string) {
	t.Helper()
	if args["compression"] != wantCodec {
		t.Errorf("params compression=%v, want %q", args["compression"], wantCodec)
	}
	key, _ := args["key"].(string)
	if !strings.HasSuffix(key, wantSuffix) {
		t.Errorf("object key %q, want suffix %q", key, wantSuffix)
	}
}

// The CDC single-event write, driven end to end through the JSON-RPC call.
func TestCDCWriteDefaultsCompressionFromConnector(t *testing.T) {
	cases := []struct {
		name       string
		destType   string
		cfg        map[string]interface{}
		wantCodec  string
		wantSuffix string
	}{
		{"gcs without compression", "gcs", map[string]interface{}{"bucket": "b", "file_format": "jsonl"}, "gzip", ".jsonl.gz"},
		{"gcs with explicit none", "gcs", map[string]interface{}{"bucket": "b", "file_format": "jsonl", "compression": "none"}, "none", ".jsonl"},
		{"minio without compression", "minio", map[string]interface{}{"bucket": "b", "file_format": "jsonl"}, "none", ".jsonl"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, ct := nsTestClient()
			cfg := &WorkerConfig{PipelineID: "p1", DestinationConnector: tc.destType, DestinationConfig: tc.cfg}
			sm := &SinkMessage{
				PipelineID: "p1",
				Table:      "public.orders",
				DBOrSchema: "public",
				RowCount:   1,
				Data:       []map[string]interface{}{{"id": 1}},
				KeyFields:  []string{"id"},
				CDCOp:      "u",
			}
			msg := kafka.Message{Topic: "t", Partition: 0, Offset: 7}
			if _, _, err := writeCDCToDestination(context.Background(), client, cfg, nil, msg, sm, "upsert"); err != nil {
				t.Fatalf("writeCDCToDestination errored: %v", err)
			}
			assertCodecAndKey(t, importDataArgs(t, ct), tc.wantCodec, tc.wantSuffix)
		})
	}
}

// The batch (full/incremental) write, the other lane.
func TestBatchWriteDefaultsCompressionFromConnector(t *testing.T) {
	cases := []struct {
		name       string
		destType   string
		cfg        map[string]interface{}
		wantCodec  string
		wantSuffix string
	}{
		{"gcs csv without compression", "gcs", map[string]interface{}{"bucket": "b", "file_format": "csv"}, "gzip", ".csv.gz"},
		{"gcs parquet without compression", "gcs", map[string]interface{}{"bucket": "b", "file_format": "parquet"}, "gzip", ".parquet"},
		{"aws-s3 explicit none", "aws-s3", map[string]interface{}{"bucket": "b", "file_format": "csv", "compression": "none"}, "none", ".csv"},
		{"minio without compression", "minio", map[string]interface{}{"bucket": "b", "file_format": "csv"}, "none", ".csv"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, ct := nsTestClient()
			cfg, sm, rows := batchMessage(tc.destType, "public", tc.cfg)
			if _, _, err := writeToDestination(context.Background(), client, cfg, nil, sm, rows, "", "", nil); err != nil {
				t.Fatalf("writeToDestination errored: %v", err)
			}
			assertCodecAndKey(t, importDataArgs(t, ct), tc.wantCodec, tc.wantSuffix)
		})
	}
}

// The batched CDC writer (cdcObjectBatcher.add) needs a live Kafka reader to drive, so
// its half is guarded structurally: the connection's compression value is read in
// exactly one place, objectStorageCompression. A fourth site reading it directly would
// bring back a lane-specific fallback, which is how the lanes came to disagree.
func TestCompressionIsReadOnlyThroughTheHelper(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("cannot read main.go: %v", err)
	}
	// Vacuity floor: a truncated or misread file would pass trivially.
	if len(src) < 100_000 {
		t.Fatalf("main.go read back as %d bytes; that is not the worker", len(src))
	}

	read := regexp.MustCompile(`firstStr\([^)]*"compression"`)
	// Positive control: the pattern must match the form the old sites used.
	if !read.MatchString(`compression := firstStr(b.destCfg, "compression")`) {
		t.Fatal("the scan pattern does not match a direct compression read; a clean result would prove nothing")
	}
	if hits := read.FindAllString(string(src), -1); len(hits) != 1 {
		t.Fatalf("main.go reads the compression setting directly %d time(s) %q, want exactly 1 "+
			"(inside objectStorageCompression)", len(hits), hits)
	}

	calls := regexp.MustCompile(`compression := objectStorageCompression\(`)
	if n := len(calls.FindAllString(string(src), -1)); n < 3 {
		t.Fatalf("objectStorageCompression is called from %d write site(s), want the batcher, the CDC "+
			"single write and the batch write (3)", n)
	}
}
