package handlers

import "testing"

// First-run destination-namespace collision probing was CLIENT-GATED.
//
// ResumeTables only called resolveFirstRunNamespace inside `if req.DestinationConfig
// != nil`, so a caller that sent no destination_config got no probe at all: the
// pipeline ran into whatever namespace was seeded at creation (usually the
// destination's default — "public") without ever asking whether the selected tables
// already existed there. Merging into a stranger's table is the loudest possible
// version of this bug and it happened silently.
//
// That was not hypothetical. PipelineMonitoringPanel resumes the FIRST-RUN table
// selection with only {execution_id, selected_tables} — no mapping — so every
// pipeline resumed from that surface skipped the probe entirely.
//
// The probe is a data-safety property, so it belongs on the server, where it holds
// for every caller. These tests pin the decision: probe when the caller stayed
// silent, stand down when probing would itself cause harm.

func destCfg(ns string) *DestinationConfig {
	return &DestinationConfig{Namespace: ns, NamespaceKind: "schema", CreateIfNotExists: true}
}

func TestServerSideFirstRunNamespace(t *testing.T) {
	cases := []struct {
		name string
		// clientSentConfig: the request carried a destination_config, even if the
		// preserve/mirror rule later nil'ed it.
		clientSentConfig bool
		persisted        *DestinationConfig
		schemaMode       string
		wantProbe        bool
		wantNamespace    string
	}{
		{
			// The regression. Nothing from the client, a real namespace on the
			// pipeline — probe it.
			name:          "silent caller with a seeded namespace is probed",
			persisted:     destCfg("public"),
			wantProbe:     true,
			wantNamespace: "public",
		},
		{
			// A caller that DID send a mapping already took the client path; the
			// only way it reaches here is the preserve/mirror blank-namespace rule,
			// which deliberately drops the config so each source schema is mirrored.
			// Re-adding a single namespace here would flatten a multi-schema pipeline
			// into one schema and collide same-named tables — the PR #549 data loss.
			name:             "config dropped by the preserve rule is not resurrected",
			clientSentConfig: true,
			persisted:        destCfg("public"),
			wantProbe:        false,
		},
		{
			// Same hazard from the other direction: the directive is sticky on the
			// pipeline from a prior run, so mirroring is in effect even though this
			// request said nothing.
			name:       "sticky preserve directive stands down",
			persisted:  destCfg("public"),
			schemaMode: "preserve",
			wantProbe:  false,
		},
		{
			name:       "sticky mirror directive stands down",
			persisted:  destCfg("public"),
			schemaMode: "mirror",
			wantProbe:  false,
		},
		{
			// flatten names one real target, so probing that target is exactly right.
			name:          "flatten directive is still probed",
			persisted:     destCfg("analytics"),
			schemaMode:    "FLATTEN",
			wantProbe:     true,
			wantNamespace: "analytics",
		},
		{
			name:      "no persisted mapping leaves nothing to probe",
			persisted: nil,
			wantProbe: false,
		},
		{
			name:      "blank namespace leaves nothing to probe",
			persisted: destCfg("   "),
			wantProbe: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, probe := serverSideFirstRunNamespace(tc.clientSentConfig, tc.persisted, tc.schemaMode)
			if probe != tc.wantProbe {
				t.Fatalf("probe = %v, want %v", probe, tc.wantProbe)
			}
			if !probe {
				return
			}
			if cfg.Namespace != tc.wantNamespace {
				t.Errorf("namespace = %q, want %q", cfg.Namespace, tc.wantNamespace)
			}
			// The synthesized config is persisted verbatim once resolved, so it has
			// to carry the pipeline's real mapping fields, not zero values.
			if cfg.NamespaceKind != tc.persisted.NamespaceKind {
				t.Errorf("namespace_kind = %q, want %q", cfg.NamespaceKind, tc.persisted.NamespaceKind)
			}
			if cfg.CreateIfNotExists != tc.persisted.CreateIfNotExists {
				t.Errorf("create_if_not_exists = %v, want %v", cfg.CreateIfNotExists, tc.persisted.CreateIfNotExists)
			}
			// SchemaMode is a request-time directive, never part of the stored object.
			if cfg.SchemaMode != "" {
				t.Errorf("synthesized config must not carry a schema_mode directive, got %q", cfg.SchemaMode)
			}
		})
	}
}
