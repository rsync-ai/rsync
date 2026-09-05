package storage

import (
	"testing"
)

func TestSlugify(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"My Pipeline Name", "my-pipeline-name"},
		{"pipeline_with_underscores", "pipeline-with-underscores"},
		{"Pipeline 123", "pipeline-123"},
		{"  spaces  around  ", "spaces-around"},
		{"UPPERCASE", "uppercase"},
		{"special!@#$%chars", "specialchars"},
		{"multiple---hyphens", "multiple-hyphens"},
		{"", ""},
		{"a", "a"},
		{"ab", "ab"},
		{"test-pipeline", "test-pipeline"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := Slugify(tt.input)
			if result != tt.expected {
				t.Errorf("Slugify(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestSanitizePath(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"normal_table", "normal_table"},
		{"schema/table", "schema_table"},
		{"path\\with\\backslash", "path_with_backslash"},
		{"../parent", "__parent"},
		{"table:name", "table_name"},
		{"  spaces  ", "spaces"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := SanitizePath(tt.input)
			if result != tt.expected {
				t.Errorf("SanitizePath(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestKeyBuilder_PartKey(t *testing.T) {
	kb := NewKeyBuilder().
		WithPathPrefix("data").
		WithDataset("My Test Pipeline").
		WithDBOrSchema("public").
		WithTable("users").
		WithFormat("parquet").
		WithCompression("gzip").
		WithRunDate("2024-01-15")

	expected := "data/my-test-pipeline/public/users/dt=2024-01-15/part-000000.parquet.gz"
	result := kb.PartKey(0)
	if result != expected {
		t.Errorf("PartKey(0) = %q, want %q", result, expected)
	}

	expected = "data/my-test-pipeline/public/users/dt=2024-01-15/part-000042.parquet.gz"
	result = kb.PartKey(42)
	if result != expected {
		t.Errorf("PartKey(42) = %q, want %q", result, expected)
	}
}

func TestKeyBuilder_PartitionPrefix(t *testing.T) {
	kb := NewKeyBuilder().
		WithPathPrefix("staging").
		WithDataset("analytics").
		WithDBOrSchema("events").
		WithTable("clicks").
		WithRunDate("2024-06-20")

	expected := "staging/analytics/events/clicks/dt=2024-06-20/"
	result := kb.PartitionPrefix()
	if result != expected {
		t.Errorf("PartitionPrefix() = %q, want %q", result, expected)
	}
}

func TestKeyBuilder_TablePrefix(t *testing.T) {
	kb := NewKeyBuilder().
		WithPathPrefix("data").
		WithDataset("sales").
		WithDBOrSchema("warehouse").
		WithTable("orders").
		WithRunDate("2024-06-20")

	expected := "data/sales/warehouse/orders/"
	result := kb.TablePrefix()
	if result != expected {
		t.Errorf("TablePrefix() = %q, want %q", result, expected)
	}
}

func TestKeyBuilder_DatasetPrefix(t *testing.T) {
	kb := NewKeyBuilder().
		WithPathPrefix("lake").
		WithDataset("customer-data").
		WithDBOrSchema("crm").
		WithTable("contacts")

	expected := "lake/customer-data/"
	result := kb.DatasetPrefix()
	if result != expected {
		t.Errorf("DatasetPrefix() = %q, want %q", result, expected)
	}
}

func TestKeyBuilder_CDCKey(t *testing.T) {
	kb := NewKeyBuilder().
		WithPathPrefix("cdc").
		WithDataset("realtime").
		WithDBOrSchema("postgres").
		WithTable("events").
		WithFormat("jsonl").
		WithCompression("zstd").
		WithRunDate("2024-03-10")

	expected := "cdc/realtime/postgres/events/dt=2024-03-10/cdc-000000000123.jsonl.zst"
	result := kb.CDCKey(123)
	if result != expected {
		t.Errorf("CDCKey(123) = %q, want %q", result, expected)
	}
}

func TestKeyBuilder_CDCRangeKey(t *testing.T) {
	kb := NewKeyBuilder().
		WithPathPrefix("cdc").
		WithDataset("streaming").
		WithDBOrSchema("mysql").
		WithTable("transactions").
		WithFormat("json").
		WithCompression("none").
		WithRunDate("2024-03-10")

	expected := "cdc/streaming/mysql/transactions/dt=2024-03-10/cdc-1000-2000.json"
	result := kb.CDCRangeKey(1000, 2000)
	if result != expected {
		t.Errorf("CDCRangeKey(1000, 2000) = %q, want %q", result, expected)
	}
}

func TestKeyBuilder_ManifestKey(t *testing.T) {
	kb := NewKeyBuilder().
		WithPathPrefix("data").
		WithDataset("analytics").
		WithDBOrSchema("dw").
		WithTable("facts").
		WithRunDate("2024-01-01")

	expected := "data/analytics/dw/facts/dt=2024-01-01/_MANIFEST.json"
	result := kb.ManifestKey()
	if result != expected {
		t.Errorf("ManifestKey() = %q, want %q", result, expected)
	}
}

func TestKeyBuilder_SuccessMarkerKey(t *testing.T) {
	kb := NewKeyBuilder().
		WithPathPrefix("output").
		WithDataset("pipeline").
		WithDBOrSchema("schema").
		WithTable("table").
		WithRunDate("2024-12-25")

	expected := "output/pipeline/schema/table/dt=2024-12-25/_SUCCESS"
	result := kb.SuccessMarkerKey()
	if result != expected {
		t.Errorf("SuccessMarkerKey() = %q, want %q", result, expected)
	}
}

func TestKeyBuilder_FileExtensions(t *testing.T) {
	tests := []struct {
		format      string
		compression string
		expected    string
	}{
		{"json", "none", "json"},
		{"json", "", "json"},
		{"jsonl", "gzip", "jsonl.gz"},
		{"ndjson", "none", "jsonl"}, // normalized
		{"parquet", "snappy", "parquet.snappy"},
		{"csv", "zstd", "csv.zst"},
		{"avro", "lz4", "avro.lz4"},
		{"json", "brotli", "json.brotli"}, // custom compression
	}

	for _, tt := range tests {
		t.Run(tt.format+"/"+tt.compression, func(t *testing.T) {
			kb := NewKeyBuilder().
				WithPathPrefix("test").
				WithDataset("ds").
				WithDBOrSchema("db").
				WithTable("tbl").
				WithFormat(tt.format).
				WithCompression(tt.compression).
				WithRunDate("2024-01-01")

			result := kb.PartKey(0)
			expectedSuffix := "part-000000." + tt.expected
			if !contains(result, expectedSuffix) {
				t.Errorf("PartKey(0) with format=%s, compression=%s = %q, want suffix %q",
					tt.format, tt.compression, result, expectedSuffix)
			}
		})
	}
}

func TestKeyBuilder_BuildManifest(t *testing.T) {
	kb := NewKeyBuilder().
		WithPathPrefix("data").
		WithDataset("test").
		WithDBOrSchema("db").
		WithTable("table").
		WithRunDate("2024-01-15")

	keys := []string{"data/test/db/table/dt=2024-01-15/part-000000.json", "data/test/db/table/dt=2024-01-15/part-000001.json"}
	rowCounts := []int64{1000, 500}

	manifest := kb.BuildManifest("pipeline-123", "exec-456", keys, rowCounts, map[string]string{"offset": "1500"})

	if manifest.PipelineID != "pipeline-123" {
		t.Errorf("PipelineID = %q, want %q", manifest.PipelineID, "pipeline-123")
	}
	if manifest.ExecutionID != "exec-456" {
		t.Errorf("ExecutionID = %q, want %q", manifest.ExecutionID, "exec-456")
	}
	if manifest.Dt != "2024-01-15" {
		t.Errorf("Dt = %q, want %q", manifest.Dt, "2024-01-15")
	}
	if manifest.TotalRows != 1500 {
		t.Errorf("TotalRows = %d, want %d", manifest.TotalRows, 1500)
	}
	if len(manifest.UploadedKeys) != 2 {
		t.Errorf("UploadedKeys length = %d, want %d", len(manifest.UploadedKeys), 2)
	}
}

func TestRunMode(t *testing.T) {
	tests := []struct {
		input    string
		expected RunMode
	}{
		{"resume", RunModeResume},
		{"Resume", RunModeResume},
		{"RESUME", RunModeResume},
		{"reload", RunModeReload},
		{"Reload", RunModeReload},
		{"RELOAD", RunModeReload},
		{"", RunModeResume},
		{"invalid", RunModeResume},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ParseRunMode(tt.input)
			if result != tt.expected {
				t.Errorf("ParseRunMode(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestLockKey(t *testing.T) {
	batchLock := BatchLockKey("conn-1", "dataset", "db", "table", "2024-01-15")
	expected := "batch:conn-1:dataset:db:table:2024-01-15"
	if batchLock.String() != expected {
		t.Errorf("BatchLockKey.String() = %q, want %q", batchLock.String(), expected)
	}

	cdcLock := CDCLockKey("conn-2", "dataset", "db", "table")
	expected = "cdc:conn-2:dataset:db:table"
	if cdcLock.String() != expected {
		t.Errorf("CDCLockKey.String() = %q, want %q", cdcLock.String(), expected)
	}
}

func TestExtractSchemaAndTable(t *testing.T) {
	tests := []struct {
		input      string
		wantSchema string
		wantTable  string
	}{
		{"users", "", "users"},
		{"public.users", "public", "users"},
		{"mydb.public.users", "public", "users"},
		{"a.b.c.d", "c", "d"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			schema, table := ExtractSchemaAndTable(tt.input)
			if schema != tt.wantSchema || table != tt.wantTable {
				t.Errorf("ExtractSchemaAndTable(%q) = (%q, %q), want (%q, %q)",
					tt.input, schema, table, tt.wantSchema, tt.wantTable)
			}
		})
	}
}

func TestValidateDataset(t *testing.T) {
	validTests := []string{
		"a",
		"ab",
		"abc",
		"test",
		"test-pipeline",
		"my-data-pipeline-123",
	}

	for _, input := range validTests {
		t.Run("valid:"+input, func(t *testing.T) {
			if err := ValidateDataset(input); err != nil {
				t.Errorf("ValidateDataset(%q) returned error: %v", input, err)
			}
		})
	}

	invalidTests := []struct {
		input   string
		wantErr string
	}{
		{"", "cannot be empty"},
		{"-test", "must start/end with alphanumeric"},
		{"test-", "must start/end with alphanumeric"},
		{"TEST", "must contain only lowercase"},
		{"test_pipeline", "must contain only lowercase"},
		{"test pipeline", "must contain only lowercase"},
	}

	for _, tt := range invalidTests {
		t.Run("invalid:"+tt.input, func(t *testing.T) {
			err := ValidateDataset(tt.input)
			if err == nil {
				t.Errorf("ValidateDataset(%q) expected error containing %q, got nil", tt.input, tt.wantErr)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s[len(s)-len(substr):] == substr || indexOf(s, substr) >= 0)
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
