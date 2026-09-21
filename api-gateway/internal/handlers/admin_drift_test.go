package handlers

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// driftServedBy is, per drift target, the source that proves its service
// answers the probe: where the route is registered and where the default
// listen port is set. Paths are relative to this package directory.
//
// It exists because the adapter target named http://temporal-adapter:8080/version
// while the adapter only ever listened on :8082 and had no /version route, so
// the drift check reported it "unreachable" on every call and could never say
// all_agree. A new drift target must add an entry here.
var driftServedBy = map[string]struct {
	routeFile   string
	routeNeedle string // one %s: the URL path
	portFile    string
	portRe      *regexp.Regexp // group 1: the default port
	portEnv     string         // env var that overrides the port ("" = none)
	alsoFile    string         // optional: a wiring line that must also be present
	alsoRe      *regexp.Regexp
}{
	"api-gateway": {
		routeFile:   "../../cmd/server/main.go",
		routeNeedle: `r.GET("%s", handlers.GetVersion(`,
		portFile:    "../../cmd/server/main.go",
		portRe:      regexp.MustCompile(`port := os\.Getenv\("PORT"\)\s+if port == "" \{\s+port = "(\d+)"`),
		portEnv:     "PORT",
	},
	"backend-orchestrator": {
		routeFile:   "../../../backend-orchestrator/cmd/orchestrator/main.go",
		routeNeedle: `router.GET("%s", `,
		portFile:    "../../../backend-orchestrator/internal/config/config.go",
		portRe:      regexp.MustCompile(`v\.SetDefault\("PORT", "(\d+)"\)`),
		portEnv:     "PORT",
	},
	"backend-temporal-adapter": {
		routeFile:   "../../../backend-temporal-adapter/cmd/adapter/ops_server.go",
		routeNeedle: `mux.HandleFunc("GET %s", `,
		portFile:    "../../../backend-temporal-adapter/cmd/adapter/ops_server.go",
		portRe:      regexp.MustCompile(`const defaultOpsAddr = ":(\d+)"`),
		portEnv:     "METRICS_ADDR",
		// The constant and the mux only matter if main() serves them.
		alsoFile: "../../../backend-temporal-adapter/cmd/adapter/main.go",
		alsoRe:   regexp.MustCompile(`getEnv\("METRICS_ADDR", defaultOpsAddr\)(?s:.*?)Handler:\s+newOpsMux\(\)`),
	},
	"llm-service": {
		routeFile:   "../../../llm-service/src/gateway/main.py",
		routeNeedle: `@app.get("%s")`,
		portFile:    "../../../llm-service/src/gateway/main.py",
		portRe:      regexp.MustCompile(`uvicorn\.run\(app, host="0\.0\.0\.0", port=(\d+)\)`),
		// uvicorn.run hardcodes the port; no env var moves it.
	},
}

// composeFilesForDrift are the stacks the drift check runs in: the dev/prod
// source build (base + overlays) and the one-command install.
var composeFilesForDrift = []string{
	"../../../docker-compose.yml",
	"../../../docker-compose.prod.yml",
	"../../../docker-compose.staging.yml",
	"../../../docker-compose.quickstart.yml",
}

func readRepoFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(path))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// composeService returns the `services.<name>` mapping node of a compose file,
// or nil. Walks yaml.Node so compose-only tags (!override, !reset) decode.
func composeService(t *testing.T, path, name string) *yaml.Node {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(readRepoFile(t, path)), &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if len(doc.Content) == 0 {
		return nil
	}
	return mappingValue(mappingValue(doc.Content[0], "services"), name)
}

func mappingValue(n *yaml.Node, key string) *yaml.Node {
	if n == nil || n.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return n.Content[i+1]
		}
	}
	return nil
}

// composeEnv returns the value a compose service sets for key (map or list
// form of `environment:`), and whether it sets it at all.
func composeEnv(svc *yaml.Node, key string) (string, bool) {
	env := mappingValue(svc, "environment")
	if env == nil {
		return "", false
	}
	if env.Kind == yaml.MappingNode {
		if v := mappingValue(env, key); v != nil {
			return v.Value, true
		}
		return "", false
	}
	for _, item := range env.Content {
		if k, v, ok := strings.Cut(item.Value, "="); ok && k == key {
			return v, true
		}
	}
	return "", false
}

var lastDigits = regexp.MustCompile(`(\d+)\D*$`)

// TestDriftTargetsNameAPortAndPathTheServiceServes fails when a drift target's
// URL names a host, port or path its service does not serve — the bug that made
// the adapter permanently "unreachable" (:8080 named, :8082 served, no route).
func TestDriftTargetsNameAPortAndPathTheServiceServes(t *testing.T) {
	for _, target := range driftTargets {
		t.Run(target.Name, func(t *testing.T) {
			u, err := url.Parse(target.URL)
			if err != nil {
				t.Fatalf("drift URL %q: %v", target.URL, err)
			}
			host, port, path := u.Hostname(), u.Port(), u.Path

			ev, ok := driftServedBy[target.Name]
			if !ok {
				t.Fatalf("drift target %q has no driftServedBy entry — add where its %s route and port are defined", target.Name, path)
			}

			routeSrc := readRepoFile(t, ev.routeFile)
			if needle := fmt.Sprintf(ev.routeNeedle, path); !strings.Contains(routeSrc, needle) {
				t.Errorf("drift URL %s names path %s, but %s does not register it (looked for %q)", target.URL, path, ev.routeFile, needle)
			}

			m := ev.portRe.FindStringSubmatch(readRepoFile(t, ev.portFile))
			if m == nil {
				t.Fatalf("could not find %s's default port in %s (pattern %s) — update driftServedBy", target.Name, ev.portFile, ev.portRe)
			}
			if m[1] != port {
				t.Errorf("drift URL %s names port %s, but %s listens on %s by default (%s)", target.URL, port, target.Name, m[1], ev.portFile)
			}

			if ev.alsoRe != nil && !ev.alsoRe.MatchString(readRepoFile(t, ev.alsoFile)) {
				t.Errorf("%s does not serve the listener the drift URL names: %s must match %s", target.Name, ev.alsoFile, ev.alsoRe)
			}

			// The host must be a compose service, and no compose file may move
			// the port with an env override.
			foundHost := false
			for _, f := range composeFilesForDrift {
				svc := composeService(t, f, host)
				if svc == nil {
					continue
				}
				foundHost = true
				if ev.portEnv == "" {
					continue
				}
				if v, set := composeEnv(svc, ev.portEnv); set {
					if d := lastDigits.FindStringSubmatch(v); d == nil || d[1] != port {
						t.Errorf("%s sets %s=%q for service %s, but the drift URL %s names port %s", f, ev.portEnv, v, host, target.URL, port)
					}
				}
			}
			if !foundHost {
				t.Errorf("drift URL %s names host %s, which is not a service in any of %v", target.URL, host, composeFilesForDrift)
			}
		})
	}
}

// TestDriftTargetsMirrorServicesYAML keeps the hardcoded list and
// config/services.yaml (which says it is the single source of truth) from
// drifting apart — the YAML named temporal-adapter:8080 too.
func TestDriftTargetsMirrorServicesYAML(t *testing.T) {
	var cfg struct {
		Services map[string]struct {
			InternalURL string `yaml:"internal_url"`
			VersionPath string `yaml:"version_path"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(readRepoFile(t, "../../../config/services.yaml")), &cfg); err != nil {
		t.Fatalf("parse config/services.yaml: %v", err)
	}
	want := map[string]string{}
	for name, s := range cfg.Services {
		if s.VersionPath != "" {
			want[name] = strings.TrimSuffix(s.InternalURL, "/") + s.VersionPath
		}
	}
	got := map[string]string{}
	for _, target := range driftTargets {
		got[target.Name] = target.URL
	}
	for name, u := range want {
		if got[name] != u {
			t.Errorf("config/services.yaml says %s serves %s, driftTargets has %q", name, u, got[name])
		}
	}
	for name, u := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("driftTargets probes %s at %s, but config/services.yaml declares no version_path for it", name, u)
		}
	}
}
