module github.com/rsync-ai/connector-deployer

go 1.25.7

// Pin the patched toolchain so CI/local builds + govulncheck analyze against the
// fixed stdlib. go1.25.13 clears the five reachable stdlib CVEs go1.25.12
// carries here -- GO-2026-6218 (net/url), GO-2026-6090 (crypto/tls),
// GO-2026-6089 + GO-2026-5026 (net/http), GO-2026-5972 (encoding/asn1) -- on
// top of the crypto/tls, net/url, os CVEs go1.25.7 carried. Matches the other
// Go modules in this repo; the golang:1.25 Docker base already floats to this
// patch. Docker-daemon CVEs stay in .govulncheck-allow.txt (fix is N/A there).
toolchain go1.25.13

require (
	github.com/docker/docker v28.5.2+incompatible
	github.com/docker/go-connections v0.6.0
)

require (
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/containerd/errdefs v1.0.0 // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/sys/atomicwriter v0.1.0 // indirect
	github.com/moby/term v0.5.2 // indirect
	github.com/morikuni/aec v1.1.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.69.0 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.44.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	gotest.tools/v3 v3.5.2 // indirect
)
