package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// Every object key this worker writes — batch part-files, manifests, _SUCCESS
// markers, CDC bronze objects — is built by appending to tablePrefix. These tests
// pin the one case that was wrong: a destination configured with no path_prefix,
// i.e. "write at the bucket root".
//
// Two independent bugs made that case land objects somewhere nobody looks:
//
//  1. tablePrefix appended the prefix unconditionally, so an empty one contributed
//     an empty leading segment and every key began with "/". S3 and GCS accept such
//     a key, but it renders as an unnamed top-level folder and matches no
//     list_blobs(prefix="<dataset>/…") call the readers issue.
//
//  2. Six call sites substituted the literal "test-aws-s3" for an empty prefix — a
//     test fixture's bucket path that leaked into the production default in
//     6e63105b9 ("chore: add e2e pipeline tests and production guardrails").
//
// Together they are a silent-data-loss shape rather than a cosmetic one: CDC
// deltas went to test-aws-s3/… while a batch backfill, which runs through the
// destination connector's own export and honours path_prefix, went to the
// configured location. Neither half is complete, and nothing errors.

const emptyPrefix = "" // the configuration under test, named so the intent is legible

func TestTablePrefixEmptyPrefixWritesAtBucketRoot(t *testing.T) {
	cases := []struct {
		name                            string
		prefix, dataset, dbOrSchema, tb string
		want                            string
	}{
		{"empty prefix contributes no segment", emptyPrefix, "", "shop", "orders", "shop/orders/"},
		{"empty prefix with a dataset", emptyPrefix, "bronze", "shop", "orders", "bronze/shop/orders/"},
		{"empty prefix, table only", emptyPrefix, "", "", "orders", "orders/"},
		{"whitespace-free slashes only is still empty", "/", "", "shop", "orders", "shop/orders/"},
		// Regression floor: the non-empty path must be untouched by the fix.
		{"a configured prefix still leads", "lake", "", "shop", "orders", "lake/shop/orders/"},
		{"a configured prefix is slash-trimmed", "/lake/", "bronze", "shop", "orders", "lake/bronze/shop/orders/"},
		{"a multi-segment prefix survives intact", "lake/raw", "", "shop", "orders", "lake/raw/shop/orders/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tablePrefix(tc.prefix, tc.dataset, tc.dbOrSchema, tc.tb)
			if got != tc.want {
				t.Fatalf("tablePrefix(%q, %q, %q, %q) = %q, want %q",
					tc.prefix, tc.dataset, tc.dbOrSchema, tc.tb, got, tc.want)
			}
			if strings.HasPrefix(got, "/") {
				t.Fatalf("key prefix %q begins with a slash; object storage accepts it but it "+
					"renders as an unnamed top-level folder and matches no list_blobs(prefix=…)", got)
			}
		})
	}
}

// Every builder that feeds a real write, driven with no path_prefix. tablePrefix
// is the only place the fix lives, so this is the assertion that it actually
// reaches the keys rather than merely being correct in isolation.
func TestObjectKeysWithNoPathPrefixLandUnderTheDataset(t *testing.T) {
	const (
		dataset = "bronze"
		schema  = "shop"
		table   = "orders"
		dt      = "2026-09-15"
	)

	keys := map[string]string{
		"partitionPrefix": partitionPrefix(emptyPrefix, dataset, schema, table, dt),
		"manifestKey":     manifestKey(emptyPrefix, dataset, schema, table, dt),
		"successKey":      successKey(emptyPrefix, dataset, schema, table, dt),
		"partKey":         partKey(emptyPrefix, dataset, schema, table, "", dt, 42, "", "jsonl"),
		"partKey/rolled":  partKey(emptyPrefix, dataset, schema, table, "region=eu/", dt, 42, "0001", "jsonl"),
		"cdcObjectKey": cdcObjectKey(emptyPrefix, schema, table, "dt="+dt, "",
			1789000000000, 0, 42, 42, "jsonl", ""),
	}

	// Vacuity floor: a builder that returned "" would satisfy every "does not
	// contain" assertion below.
	if len(keys) != 6 {
		t.Fatalf("expected 6 builders under test, got %d", len(keys))
	}

	for name, key := range keys {
		t.Run(name, func(t *testing.T) {
			if key == "" {
				t.Fatalf("%s returned an empty key; the assertions below would be vacuous", name)
			}
			if strings.HasPrefix(key, "/") {
				t.Fatalf("%s = %q — a leading slash puts the object in an unnamed root folder "+
					"that no reader's list_blobs prefix matches", name, key)
			}
			if strings.Contains(key, "test-aws-s3") {
				t.Fatalf("%s = %q — a destination with no path_prefix must write where the user "+
					"configured it, not under a leaked e2e fixture path", name, key)
			}
			if strings.Contains(key, "//") {
				t.Fatalf("%s = %q contains an empty path segment", name, key)
			}
		})
	}

	// The cdc key is the one that matters most for "no data loss": it must sit
	// under the same <schema>/<table>/ the batch backfill writes to.
	if want := schema + "/" + table + "/"; !strings.HasPrefix(keys["cdcObjectKey"], want) {
		t.Fatalf("cdcObjectKey = %q, want it to start with %q so CDC deltas and the batch "+
			"backfill share one location", keys["cdcObjectKey"], want)
	}
	for _, name := range []string{"partitionPrefix", "manifestKey", "successKey", "partKey"} {
		if want := dataset + "/" + schema + "/" + table + "/"; !strings.HasPrefix(keys[name], want) {
			t.Fatalf("%s = %q, want it to start with %q", name, keys[name], want)
		}
	}
}

// The six substitutions lived inside functions too large to drive from a unit
// test, so this guards the source directly: nothing may assign that literal to a
// prefix again. It is a structural guard, not a behavioural one — the point is
// that it fires on the next person who re-adds a "sensible default" here.
func TestNoHardcodedFixturePrefixRemainsInTheSource(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("cannot read main.go: %v", err)
	}
	// Vacuity floor: a truncated or misread file would pass trivially.
	if len(src) < 100_000 {
		t.Fatalf("main.go read back as %d bytes; that is not the worker, and the scan below "+
			"would be meaningless", len(src))
	}

	// Matches `prefix = "test-aws-s3"` and `batch.prefix = "test-aws-s3"`, i.e. an
	// assignment, not the explanatory comment that records why it is gone.
	assign := regexp.MustCompile(`(?m)^\s*[\w.]*[Pp]refix\s*=\s*"test-aws-s3"`)

	// Positive control: prove the pattern can match before trusting that it did not.
	if !assign.MatchString("\t\tbatch.prefix = \"test-aws-s3\"\n") {
		t.Fatal("the scan pattern does not match the very assignment it exists to find; " +
			"a zero-hit result below would prove nothing")
	}

	if hits := assign.FindAllString(string(src), -1); len(hits) > 0 {
		t.Fatalf("main.go defaults a destination prefix to the e2e fixture path %d time(s): %q — "+
			"an unset path_prefix means the bucket root, and substituting a literal splits CDC "+
			"output away from the batch backfill", len(hits), hits)
	}
}
