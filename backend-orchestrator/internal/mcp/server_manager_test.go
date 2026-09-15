package mcp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/connectorpaths"
)

// shippedMetadata loads a public database connector's REAL metadata.json.
//
// It resolves through latest.json with the same resolver the orchestrator itself
// uses, rather than naming versions/v1.0.0: per CLAUDE.md the canonical source is
// versions/<latest.json current_version>, so a hardcoded version dir stops
// pointing at the code that runs the moment anyone bumps it.
//
// Every failure here is t.Fatalf, never t.Skipf. These files ship in this repo,
// so "not present" means the test lost its subject — and a skipped test is
// indistinguishable from a passing one in a CI summary, which is how a guard
// silently stops guarding.
func shippedMetadata(t *testing.T, connector string) ConnectorMetadata {
	t.Helper()
	root := filepath.Join("..", "..", "..", "shared", "mcp-connectors",
		"public", "database", connector)
	path, ok := connectorpaths.ResolveVersionedMetadataPath(root)
	if !ok {
		t.Fatalf("cannot resolve %s metadata via %s/latest.json; this file ships in "+
			"this repo, so it has moved rather than become inapplicable", connector, root)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("cannot read %s: %v", path, err)
	}
	var md ConnectorMetadata
	if err := json.Unmarshal(raw, &md); err != nil {
		t.Fatalf("%s does not unmarshal into ConnectorMetadata: %v", path, err)
	}
	return md
}

// TestMissingRequiredConfig covers the connector config field-alias gate.
//
// Regression: a connector may normalize one config key to another inside its own
// process (azure-blob maps `container` -> `bucket` in _get_config), but the
// orchestrator's required-config gate runs BEFORE the connector starts and only saw
// the canonical key. So a container-only azure-blob connection passed test_connection
// and discover_schema (which start the connector and normalize) yet failed export
// with "missing required config: bucket". Declaring the alias in metadata
// (config_aliases: {"bucket": ["container"]}) lets the gate accept exactly what the
// connector accepts, keeping test/discovery/export consistent. Connectors that declare
// no aliases (aws-s3, gcs use `bucket` natively) keep the strict canonical-key check.
func TestMissingRequiredConfig(t *testing.T) {
	required := []string{"bucket"}
	azureAliases := map[string][]string{"bucket": {"container"}}

	cases := []struct {
		name    string
		aliases map[string][]string
		config  map[string]string
		want    []string
	}{
		{"alias satisfies canonical (azure container-only)", azureAliases, map[string]string{"container": "mydata", "connection_string": "x"}, nil},
		{"canonical key satisfies", azureAliases, map[string]string{"bucket": "mydata"}, nil},
		{"both keys present is fine", azureAliases, map[string]string{"bucket": "b", "container": "c"}, nil},
		{"neither key present is reported missing", azureAliases, map[string]string{"connection_string": "x"}, []string{"bucket"}},
		{"no aliases declared, canonical present (aws-s3/gcs)", nil, map[string]string{"bucket": "b"}, nil},
		{"no aliases declared, alias key does NOT satisfy", nil, map[string]string{"container": "c"}, []string{"bucket"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := missingRequiredConfig(required, tc.aliases, tc.config)
			// Treat nil and empty slice as equivalent for the "nothing missing" case.
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("missingRequiredConfig(%v, %v, %v) = %v, want %v",
					required, tc.aliases, tc.config, got, tc.want)
			}
		})
	}
}

// TestOracleDSNSatisfiesRequiredConfig drives the gate with the REAL oracle
// metadata.json off disk, not a hand-written fixture.
//
// Regression: oracle's required_config is [host, port, user, password], but its
// own metadata documents `dsn` as "Takes precedence over host/port/service_name"
// and the connector honours it verbatim, reading host/port only in the else
// branch. So a user connecting by tnsnames alias or Autonomous-DB wallet DSN was
// rejected with "missing required config: host, port" before the container ever
// started. The gate checks key PRESENCE, not value, so the only way through was
// typing placeholder values into two fields the connector then ignores.
//
// Reading the shipped metadata rather than a fixture is deliberate: a fixture
// keeps passing after someone drops config_aliases from the file that actually
// ships, which is precisely the regression worth catching.
func TestOracleDSNSatisfiesRequiredConfig(t *testing.T) {
	md := shippedMetadata(t, "oracle")

	// Vacuity floor: an empty required list makes every assertion below trivially
	// true, and a typo in the path would produce exactly that.
	if len(md.RequiredConfig) == 0 {
		t.Fatalf("oracle declares no required_config; the gate assertions below would be vacuous")
	}

	for _, key := range []string{"dsn", "tns", "connect_string"} {
		t.Run(key+"-only connection is accepted", func(t *testing.T) {
			cfg := map[string]string{key: "adb_high", "user": "scott", "password": "tiger"}
			if missing := missingRequiredConfig(md.RequiredConfig, md.ConfigAliases, cfg); len(missing) != 0 {
				t.Fatalf("a %s-only oracle connection was rejected with missing=%v; "+
					"config_aliases must map host and port onto the dsn chain", key, missing)
			}
		})
	}

	t.Run("a connection with neither host nor dsn is still rejected", func(t *testing.T) {
		cfg := map[string]string{"user": "scott", "password": "tiger"}
		missing := missingRequiredConfig(md.RequiredConfig, md.ConfigAliases, cfg)
		if len(missing) == 0 {
			t.Fatal("the gate accepted an oracle connection naming no host and no dsn; " +
				"the alias map has been widened until it no longer gates anything")
		}
	})
}

// TestMongoAtlasConnectionStringSatisfiesRequiredConfig is the oracle case again
// on the connector a MongoDB→GCS pipeline actually starts from, driven by the
// REAL mongodb metadata off disk.
//
// Regression: mongodb's required_config is [host, database], but MongoDB Atlas
// is reachable only as `mongodb+srv://…` — SRV resolves the seed list, so there
// is no single host:port to name — and the connector's own _build_uri returns an
// explicit connection_string and never reads `host` at all
// (connector.py, versions/v1.0.0). The gate therefore rejected every Atlas
// connection before the container started, over a field the connector ignores.
//
// The gate checks key PRESENCE, not value, so the only way through was a
// placeholder host. That is not a workaround worth documenting: it makes a
// connection that works look identical to one misconfigured with a real but
// wrong host.
func TestMongoAtlasConnectionStringSatisfiesRequiredConfig(t *testing.T) {
	md := shippedMetadata(t, "mongodb")

	// Vacuity floor — see the oracle test above for why this must be asserted
	// rather than assumed.
	if len(md.RequiredConfig) == 0 {
		t.Fatalf("mongodb declares no required_config; the gate assertions below would be vacuous")
	}

	// Every key _build_uri accepts as an explicit URI. The alias map and that
	// function must stay in lockstep: an alias the connector does not read lets a
	// connection start that cannot possibly connect.
	for _, key := range []string{"connection_string", "mongodb_connection_string", "mongodb_uri", "uri"} {
		t.Run(key+"-only connection is accepted", func(t *testing.T) {
			cfg := map[string]string{
				key:        "mongodb+srv://u:p@cluster0.abcd.mongodb.net/?retryWrites=true",
				"database": "shop",
			}
			if missing := missingRequiredConfig(md.RequiredConfig, md.ConfigAliases, cfg); len(missing) != 0 {
				t.Fatalf("an Atlas connection supplied via %s was rejected with missing=%v; "+
					"config_aliases must map host onto the URI chain _build_uri reads", key, missing)
			}
		})
	}

	t.Run("a self-hosted host:port connection is still accepted", func(t *testing.T) {
		cfg := map[string]string{"host": "mongo.internal", "port": "27017", "database": "shop"}
		if missing := missingRequiredConfig(md.RequiredConfig, md.ConfigAliases, cfg); len(missing) != 0 {
			t.Fatalf("adding aliases broke the pre-existing discrete-field path: missing=%v", missing)
		}
	})

	t.Run("database has no alias and is still required", func(t *testing.T) {
		cfg := map[string]string{"connection_string": "mongodb+srv://u:p@cluster0.abcd.mongodb.net/"}
		missing := missingRequiredConfig(md.RequiredConfig, md.ConfigAliases, cfg)
		if len(missing) != 1 || missing[0] != "database" {
			t.Fatalf("missing = %v, want exactly [database]; a URI does not name the database "+
				"rsync captures collections from", missing)
		}
	})

	t.Run("a connection naming neither host nor any URI key is still rejected", func(t *testing.T) {
		cfg := map[string]string{"database": "shop", "user": "u", "password": "p"}
		if missing := missingRequiredConfig(md.RequiredConfig, md.ConfigAliases, cfg); len(missing) == 0 {
			t.Fatal("the gate accepted a mongodb connection naming no host and no URI; " +
				"the alias map has been widened until it no longer gates anything")
		}
	})
}
