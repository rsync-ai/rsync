package handlers

import (
	"os"
	"path/filepath"
	"testing"
)

// TestResolveConnectorDirNameTraversal is the regression guard for the G304
// path-traversal fix (SEC-G304-CONNECTOR-NAME-FASTPATH). resolveConnectorDirName
// must resolve legitimate display/snake/kebab names to the in-root connector dir
// via the canonicalized metadata index ONLY, and must never join a raw,
// unsanitized name onto the filesystem (which previously let "../secret" escape
// the connectors root).
func TestResolveConnectorDirNameTraversal(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "connectors")

	// A legitimate connector under the root.
	awsDir := filepath.Join(root, "aws-s3")
	if err := os.MkdirAll(awsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(awsDir, "metadata.json"),
		[]byte(`{"id":"aws-s3","connector_type":"aws_s3","display_name":"AWS S3","name":"aws-s3"}`),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	// A "secret" connector dir OUTSIDE the connectors root. The old raw fast path
	// would os.Stat(filepath.Join(root, "../secret", "metadata.json")) and return
	// "../secret", escaping the root. The metadata index (in-root only) must not.
	secretDir := filepath.Join(base, "secret")
	if err := os.MkdirAll(secretDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(secretDir, "metadata.json"),
		[]byte(`{"id":"secret","connector_type":"secret"}`),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	// Legitimate names (display, snake, kebab) all resolve to the in-root dir.
	for _, name := range []string{"aws-s3", "aws_s3", "AWS S3"} {
		got, err := resolveConnectorDirName(root, name)
		if err != nil {
			t.Fatalf("resolveConnectorDirName(root, %q) unexpected error: %v", name, err)
		}
		if got != "aws-s3" {
			t.Fatalf("resolveConnectorDirName(root, %q) = %q, want %q", name, got, "aws-s3")
		}
	}

	// Traversal inputs must NOT resolve and must never reach the sibling dir.
	for _, evil := range []string{
		"../secret",
		"../../etc",
		`..\..\secret`,
		"/etc",
		"aws-s3/../../secret",
	} {
		got, err := resolveConnectorDirName(root, evil)
		if err == nil {
			t.Fatalf("resolveConnectorDirName(root, %q) = %q, want not-found error (traversal must be blocked)", evil, got)
		}
	}
}

// TestIsCleanRelDir guards the defense-in-depth containment check applied to
// resolver results before they are joined onto a connectors root and read.
func TestIsCleanRelDir(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"aws-s3", true},
		{"storage/aws-s3", true},
		{"", false},
		{"..", false},
		{"../secret", false},
		{"a/../../secret", false},
	}
	for _, c := range cases {
		if got := isCleanRelDir(c.name); got != c.want {
			t.Errorf("isCleanRelDir(%q) = %v, want %v", c.name, got, c.want)
		}
	}
	// Absolute paths are always rejected (use an OS-appropriate absolute path).
	if abs := filepath.Join(string(filepath.Separator), "etc", "passwd"); isCleanRelDir(abs) {
		t.Errorf("isCleanRelDir(%q) = true, want false (absolute path)", abs)
	}
}
