# Third-Party Notices

rsync.ai (Elastic License 2.0) redistributes the third-party open-source components listed
below inside its container images and source distribution. Their licenses and copyright
notices are reproduced here to satisfy their redistribution terms. Nothing here is legal advice.

> **How this file is produced.** The component inventory below is generated from a full
> dependency scan of the shipped surface -- Go: `go list -deps` across every shipped module;
> Python: each connector/image `requirements*.txt`; Frontend: `frontend/package.json` direct
> production dependencies. Licenses are SPDX identifiers resolved from each dependency's own
> `LICENSE` file / package metadata. The **full transitive npm tree** and the verbatim
> **license texts** are produced at release time by
> [`scripts/gen-third-party-notices.sh`](scripts/gen-third-party-notices.sh) -- run it on the
> release tree and commit the result. A blocking [`Licenses`](.github/workflows/licenses.yml)
> CI check independently fails any PR that introduces a new GPL / AGPL / SSPL / BUSL dependency.

_Inventory: 137 Go module@version rows, 27 Python packages, 49 frontend (npm) direct
production dependencies._

> **Provenance, honestly.** The Go table was rebuilt on 2026-08-25 directly from
> `go list -deps` across the five shipped Go modules, with each SPDX identifier resolved from
> that version's own `LICENSE` file in the module cache. It had drifted badly: 58 shipped
> modules were missing (the whole of `connector-deployer`'s closure, `jackc/pgx/v5`, the
> `aws-sdk-go-v2` family), 29 versions were stale, and `github.com/lib/pq` was still listed
> although it is in no `go.mod` and no `go.sum` (replaced by a pgx shim). The Python, npm and
> OS-package sections below are from the last generator run and are NOT re-verified here.
> The line this replaced claimed the whole file was "scanned from the current source tree; not
> manually edited", which is what made 90 wrong rows read as freshly generated.

## Copyleft / attention-required dependencies

A scan of the **language-level** dependencies (Go modules, Python packages, npm production deps)
found **no strong-copyleft (GPL / AGPL / SSPL / BUSL)** among them; the only copyleft language
deps are **weak-copyleft** (MPL-2.0 / LGPL-3.0), which ELv2 redistribution permits because each is
consumed as a separable, dynamically-linked pre-compiled component (no static linking of
rsync-ai's own code into the copyleft work). **Separately, OS/system packages baked into the
connector container images (see § "Container image / OS packages") add two attention items —
notably the SQL Server connector's `msodbcsql18` (Microsoft-proprietary, redistributed under the
Microsoft EULA, not ELv2) and `unixODBC`.** Each copyleft/attention component is listed below and
must remain separable and replaceable.

| Dependency | Component | License | Notes |
|---|---|---|---|
| [`github.com/go-sql-driver/mysql`](https://github.com/go-sql-driver/mysql) | Go services (MySQL driver) | **MPL-2.0** | File-level weak copyleft; dynamically linked, unmodified. Redistributable under ELv2. |
| [`github.com/hashicorp/go-uuid`](https://github.com/hashicorp/go-uuid) | Go services (indirect) | **MPL-2.0** | File-level weak copyleft; indirect, unmodified. |
| [`psycopg2-binary`](https://github.com/psycopg/psycopg2) | `postgresql` + `redshift` connectors | **LGPL-3.0-or-later** (w/ OpenSSL exception) | Weak copyleft; shipped as a pre-compiled binary wheel, dynamically imported as a separable component. The only copyleft dependency on the shipped Python **language** surface. Its LGPL text is distributed with the wheel. |
| [`certifi`](https://github.com/certifi/python-certifi) | transitive (requests/httpx/boto3), Python connector images | **MPL-2.0** | CA-bundle data; weak copyleft, unmodified, redistributed as-is. Transitive — enumerated fully at release-time regen. |
| [`sharp`](https://github.com/lovell/sharp) (`@img/sharp-*`, libvips) | frontend image optimization (Next.js) | **LGPL-3.0-or-later** (libvips; npm reports `Apache-2.0 AND LGPL-3.0-or-later [AND MIT]`) | Weak copyleft; the libvips native lib is a dynamically-linked, separable pre-built binary — ELv2-redistributable. Surfaced by the Trivy license scan (the direct-dep frontend scan missed the transitive libvips). Its licenses are covered by `licenses.yml`'s package.json grep + the release-time `license-checker`; the frontend npm lockfile is scoped out of the blocking Trivy license gate (a Trivy 3-component-compound bug — see `.github/workflows/security.yml`). |
| [`msodbcsql18`](https://learn.microsoft.com/sql/connect/odbc/) | `sqlserver` connector image (apt) | **Proprietary — Microsoft ODBC Driver EULA** | Installed with `ACCEPT_EULA=Y`. Redistribution inside a published image is governed by the Microsoft EULA, **not** ELv2 — **flag for legal review before public distribution.** No other connector bundles it. |
| [`unixODBC`](https://www.unixodbc.org/) (`unixodbc`/`unixodbc-dev`) | `sqlserver` connector image (apt) | **LGPL-2.1** (libs); **GPL-2.0** (some CLI tools, e.g. `isql`) | pyodbc's driver manager. The linked libraries are LGPL; the GPL-2.0 CLI tools ship in the image but are not invoked by the connector. |
| `mysql-connector-python` | -- (removed) | GPL-2.0 | **No longer shipped.** Swapped to MIT `PyMySQL` in #499; remains only in dev/test manifests (`e2e/`, `tests/`) which are not part of any distributed image. |
| `pymssql` | -- (not installed) | LGPL-2.1 | Present only as an inert commented line in some connector manifests. The SQL Server connector uses `pyodbc` (MIT-0). Do not uncomment without review. |

## License distribution (shipped surface)

- **Go** (137 module@version rows): Apache-2.0 x 59; MIT x 43; BSD-3-Clause x 24; BSD-2-Clause x 7; ISC x 2; MPL-2.0 x 2
- **Python** (27 packages): Apache-2.0 x 9; MIT x 7; BSD-3-Clause x 6; Apache-2.0 OR BSD-3-Clause x 2; LGPL-3.0-or-later x 1; MIT-0 x 1; UPL-1.0 OR Apache-2.0 x 1
- **Frontend** (49 direct prod deps): MIT x 46; Apache-2.0 x 2; ISC x 1

## Go modules

Third-party modules compiled into the shipped Go binaries (api-gateway, orchestrator,
temporal-adapter, connector-deployer, kafka-mcp-sink worker; `shared/go/*` are pulled in via
`replace`). First-party `github.com/rsync-ai/*` modules and the Go standard library are excluded.

A module appears twice when two services resolve different versions of it (each service has its
own build list, and both binaries ship) -- e.g. `go.opentelemetry.io/otel` at v1.44.0 and v1.45.0.

| Component | Version | License |
|---|---|---|
| [filippo.io/edwards25519](https://filippo.io/edwards25519) | v1.2.0 | BSD-3-Clause |
| [github.com/IBM/sarama](https://github.com/IBM/sarama) | v1.60.2 | MIT |
| [github.com/aws/aws-msk-iam-sasl-signer-go](https://github.com/aws/aws-msk-iam-sasl-signer-go) | v1.0.4 | Apache-2.0 |
| [github.com/aws/aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2) | v1.32.4 | Apache-2.0 |
| [github.com/aws/aws-sdk-go-v2/config](https://github.com/aws/aws-sdk-go-v2) | v1.28.2 | Apache-2.0 |
| [github.com/aws/aws-sdk-go-v2/credentials](https://github.com/aws/aws-sdk-go-v2) | v1.17.43 | Apache-2.0 |
| [github.com/aws/aws-sdk-go-v2/feature/ec2/imds](https://github.com/aws/aws-sdk-go-v2) | v1.16.19 | Apache-2.0 |
| [github.com/aws/aws-sdk-go-v2/internal/configsources](https://github.com/aws/aws-sdk-go-v2) | v1.3.23 | Apache-2.0 |
| [github.com/aws/aws-sdk-go-v2/internal/endpoints/v2](https://github.com/aws/aws-sdk-go-v2) | v2.6.23 | Apache-2.0 |
| [github.com/aws/aws-sdk-go-v2/internal/ini](https://github.com/aws/aws-sdk-go-v2) | v1.8.1 | Apache-2.0 |
| [github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding](https://github.com/aws/aws-sdk-go-v2) | v1.12.0 | Apache-2.0 |
| [github.com/aws/aws-sdk-go-v2/service/internal/presigned-url](https://github.com/aws/aws-sdk-go-v2) | v1.12.4 | Apache-2.0 |
| [github.com/aws/aws-sdk-go-v2/service/sso](https://github.com/aws/aws-sdk-go-v2) | v1.24.4 | Apache-2.0 |
| [github.com/aws/aws-sdk-go-v2/service/ssooidc](https://github.com/aws/aws-sdk-go-v2) | v1.28.4 | Apache-2.0 |
| [github.com/aws/aws-sdk-go-v2/service/sts](https://github.com/aws/aws-sdk-go-v2) | v1.32.4 | Apache-2.0 |
| [github.com/aws/smithy-go](https://github.com/aws/smithy-go) | v1.22.0 | Apache-2.0 |
| [github.com/beorn7/perks](https://github.com/beorn7/perks) | v1.0.1 | MIT |
| [github.com/cenkalti/backoff/v5](https://github.com/cenkalti/backoff) | v5.0.3 | MIT |
| [github.com/cespare/xxhash/v2](https://github.com/cespare/xxhash) | v2.3.0 | MIT |
| [github.com/containerd/errdefs](https://github.com/containerd/errdefs) | v1.0.0 | Apache-2.0 |
| [github.com/containerd/errdefs/pkg](https://github.com/containerd/errdefs) | v0.3.0 | Apache-2.0 |
| [github.com/davecgh/go-spew](https://github.com/davecgh/go-spew) | v1.1.1 | ISC |
| [github.com/davecgh/go-spew](https://github.com/davecgh/go-spew) | v1.1.2-0.20180830191138-d8f796af33cc | ISC |
| [github.com/dgryski/go-rendezvous](https://github.com/dgryski/go-rendezvous) | v0.0.0-20200823014737-9f7001d12a5f | MIT |
| [github.com/distribution/reference](https://github.com/distribution/reference) | v0.6.0 | Apache-2.0 |
| [github.com/docker/docker](https://github.com/docker/docker) | v28.5.2+incompatible | Apache-2.0 |
| [github.com/docker/go-connections](https://github.com/docker/go-connections) | v0.6.0 | Apache-2.0 |
| [github.com/docker/go-units](https://github.com/docker/go-units) | v0.5.0 | Apache-2.0 |
| [github.com/eapache/go-resiliency](https://github.com/eapache/go-resiliency) | v1.7.0 | MIT |
| [github.com/facebookgo/clock](https://github.com/facebookgo/clock) | v0.0.0-20150410010913-600d898af40a | MIT |
| [github.com/felixge/httpsnoop](https://github.com/felixge/httpsnoop) | v1.0.4 | MIT |
| [github.com/fsnotify/fsnotify](https://github.com/fsnotify/fsnotify) | v1.9.0 | BSD-3-Clause |
| [github.com/gabriel-vasile/mimetype](https://github.com/gabriel-vasile/mimetype) | v1.4.13 | MIT |
| [github.com/gin-contrib/sse](https://github.com/gin-contrib/sse) | v1.1.0 | MIT |
| [github.com/gin-gonic/gin](https://github.com/gin-gonic/gin) | v1.12.0 | MIT |
| [github.com/go-logr/logr](https://github.com/go-logr/logr) | v1.4.3 | Apache-2.0 |
| [github.com/go-logr/logr](https://github.com/go-logr/logr) | v1.4.4 | Apache-2.0 |
| [github.com/go-logr/stdr](https://github.com/go-logr/stdr) | v1.2.2 | Apache-2.0 |
| [github.com/go-playground/locales](https://github.com/go-playground/locales) | v0.14.1 | MIT |
| [github.com/go-playground/universal-translator](https://github.com/go-playground/universal-translator) | v0.18.1 | MIT |
| [github.com/go-playground/validator/v10](https://github.com/go-playground/validator) | v10.30.3 | MIT |
| [github.com/go-redis/redis/v8](https://github.com/go-redis/redis) | v8.11.5 | BSD-2-Clause |
| [github.com/go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) | v1.10.1 | MPL-2.0 |
| [github.com/go-viper/mapstructure/v2](https://github.com/go-viper/mapstructure) | v2.5.0 | MIT |
| [github.com/goccy/go-yaml](https://github.com/goccy/go-yaml) | v1.19.2 | MIT |
| [github.com/gogo/protobuf](https://github.com/gogo/protobuf) | v1.3.2 | BSD-3-Clause |
| [github.com/golang-sql/civil](https://github.com/golang-sql/civil) | v0.0.0-20220223132316-b832511892a9 | Apache-2.0 |
| [github.com/golang-sql/sqlexp](https://github.com/golang-sql/sqlexp) | v0.1.0 | BSD-3-Clause |
| [github.com/golang/mock](https://github.com/golang/mock) | v1.6.0 | Apache-2.0 |
| [github.com/golang/snappy](https://github.com/golang/snappy) | v0.0.4 | BSD-3-Clause |
| [github.com/google/uuid](https://github.com/google/uuid) | v1.6.0 | BSD-3-Clause |
| [github.com/gorilla/websocket](https://github.com/gorilla/websocket) | v1.5.3 | BSD-2-Clause |
| [github.com/grpc-ecosystem/go-grpc-middleware/v2](https://github.com/grpc-ecosystem/go-grpc-middleware) | v2.3.2 | Apache-2.0 |
| [github.com/grpc-ecosystem/grpc-gateway/v2](https://github.com/grpc-ecosystem/grpc-gateway) | v2.30.0 | BSD-3-Clause |
| [github.com/hashicorp/go-uuid](https://github.com/hashicorp/go-uuid) | v1.0.3 | MPL-2.0 |
| [github.com/jackc/pgpassfile](https://github.com/jackc/pgpassfile) | v1.0.0 | MIT |
| [github.com/jackc/pgservicefile](https://github.com/jackc/pgservicefile) | v0.0.0-20240606120523-5a60cdf6a761 | MIT |
| [github.com/jackc/pgx/v5](https://github.com/jackc/pgx) | v5.10.0 | MIT |
| [github.com/jackc/puddle/v2](https://github.com/jackc/puddle) | v2.2.2 | MIT |
| [github.com/jcmturner/aescts/v2](https://github.com/jcmturner/aescts) | v2.0.0 | Apache-2.0 |
| [github.com/jcmturner/dnsutils/v2](https://github.com/jcmturner/dnsutils) | v2.0.0 | Apache-2.0 |
| [github.com/jcmturner/gofork](https://github.com/jcmturner/gofork) | v1.7.6 | BSD-3-Clause |
| [github.com/jcmturner/gokrb5/v8](https://github.com/jcmturner/gokrb5) | v8.4.4 | Apache-2.0 |
| [github.com/jcmturner/rpc/v2](https://github.com/jcmturner/rpc) | v2.0.3 | Apache-2.0 |
| [github.com/klauspost/compress](https://github.com/klauspost/compress) | v1.19.1 | BSD-3-Clause |
| [github.com/klauspost/compress](https://github.com/klauspost/compress) | v1.19.2 | BSD-3-Clause |
| [github.com/leodido/go-urn](https://github.com/leodido/go-urn) | v1.4.0 | MIT |
| [github.com/linkedin/goavro/v2](https://github.com/linkedin/goavro) | v2.15.0 | Apache-2.0 |
| [github.com/mattn/go-isatty](https://github.com/mattn/go-isatty) | v0.0.20 | MIT |
| [github.com/microsoft/go-mssqldb](https://github.com/microsoft/go-mssqldb) | v1.11.0 | BSD-3-Clause |
| [github.com/moby/docker-image-spec](https://github.com/moby/docker-image-spec) | v1.3.1 | Apache-2.0 |
| [github.com/munnerz/goautoneg](https://github.com/munnerz/goautoneg) | v0.0.0-20191010083416-a7dc8b61c822 | BSD-3-Clause |
| [github.com/nexus-rpc/nexus-proto-annotations](https://github.com/nexus-rpc/api) | v0.1.0 | MIT |
| [github.com/nexus-rpc/sdk-go](https://github.com/nexus-rpc/sdk-go) | v0.7.0 | MIT |
| [github.com/opencontainers/go-digest](https://github.com/opencontainers/go-digest) | v1.0.0 | Apache-2.0 |
| [github.com/opencontainers/image-spec](https://github.com/opencontainers/image-spec) | v1.1.1 | Apache-2.0 |
| [github.com/pelletier/go-toml/v2](https://github.com/pelletier/go-toml) | v2.2.4 | MIT |
| [github.com/pierrec/lz4/v4](https://github.com/pierrec/lz4) | v4.1.27 | BSD-3-Clause |
| [github.com/pierrec/lz4/v4](https://github.com/pierrec/lz4) | v4.1.29 | BSD-3-Clause |
| [github.com/pkg/errors](https://github.com/pkg/errors) | v0.9.1 | BSD-2-Clause |
| [github.com/prometheus/client_golang](https://github.com/prometheus/client_golang) | v1.24.1 | Apache-2.0 |
| [github.com/prometheus/client_model](https://github.com/prometheus/client_model) | v0.6.2 | Apache-2.0 |
| [github.com/prometheus/common](https://github.com/prometheus/common) | v0.70.1 | Apache-2.0 |
| [github.com/prometheus/procfs](https://github.com/prometheus/procfs) | v0.21.1 | Apache-2.0 |
| [github.com/quic-go/qpack](https://github.com/quic-go/qpack) | v0.6.0 | MIT |
| [github.com/quic-go/quic-go](https://github.com/quic-go/quic-go) | v0.59.1 | MIT |
| [github.com/rcrowley/go-metrics](https://github.com/rcrowley/go-metrics) | v0.0.0-20250401214520-65e299d6c5c9 | BSD-2-Clause |
| [github.com/redis/go-redis/extra/rediscmd/v9](https://github.com/redis/go-redis) | v9.22.0 | BSD-2-Clause |
| [github.com/redis/go-redis/extra/redisotel/v9](https://github.com/redis/go-redis) | v9.22.0 | BSD-2-Clause |
| [github.com/redis/go-redis/v9](https://github.com/redis/go-redis) | v9.22.0 | BSD-2-Clause |
| [github.com/robfig/cron](https://github.com/robfig/cron) | v1.2.0 | MIT |
| [github.com/sagikazarmark/locafero](https://github.com/sagikazarmark/locafero) | v0.11.0 | MIT |
| [github.com/segmentio/kafka-go](https://github.com/segmentio/kafka-go) | v0.4.51 | MIT |
| [github.com/shopspring/decimal](https://github.com/shopspring/decimal) | v1.4.0 | MIT |
| [github.com/sijms/go-ora/v2](https://github.com/sijms/go-ora) | v2.9.0 | MIT |
| [github.com/sirupsen/logrus](https://github.com/sirupsen/logrus) | v1.10.2 | MIT |
| [github.com/sourcegraph/conc](https://github.com/sourcegraph/conc) | v0.3.1-0.20240121214520-5f936abd7ae8 | MIT |
| [github.com/spf13/afero](https://github.com/spf13/afero) | v1.15.0 | Apache-2.0 |
| [github.com/spf13/cast](https://github.com/spf13/cast) | v1.10.0 | MIT |
| [github.com/spf13/pflag](https://github.com/spf13/pflag) | v1.0.10 | BSD-3-Clause |
| [github.com/spf13/viper](https://github.com/spf13/viper) | v1.21.0 | MIT |
| [github.com/stretchr/objx](https://github.com/stretchr/objx) | v0.5.3 | MIT |
| [github.com/stretchr/testify](https://github.com/stretchr/testify) | v1.12.1 | MIT |
| [github.com/subosito/gotenv](https://github.com/subosito/gotenv) | v1.6.0 | MIT |
| [github.com/ugorji/go/codec](https://github.com/ugorji/go) | v1.3.1 | MIT |
| [github.com/xdg-go/scram](https://github.com/xdg-go/scram) | v1.2.0 | Apache-2.0 |
| [github.com/xdg-go/stringprep](https://github.com/xdg-go/stringprep) | v1.0.4 | Apache-2.0 |
| [go.mongodb.org/mongo-driver/v2](https://github.com/mongodb/mongo-go-driver) | v2.5.0 | Apache-2.0 |
| [go.opentelemetry.io/auto/sdk](https://github.com/open-telemetry/opentelemetry-go-instrumentation) | v1.2.1 | Apache-2.0 |
| [go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp](https://go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp) | v0.69.0 | Apache-2.0 |
| [go.opentelemetry.io/otel](https://github.com/open-telemetry/opentelemetry-go) | v1.44.0 | Apache-2.0 |
| [go.opentelemetry.io/otel](https://github.com/open-telemetry/opentelemetry-go) | v1.46.0 | Apache-2.0 |
| [go.opentelemetry.io/otel/exporters/otlp/otlptrace](https://github.com/open-telemetry/opentelemetry-go) | v1.46.0 | Apache-2.0 |
| [go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc](https://github.com/open-telemetry/opentelemetry-go) | v1.46.0 | Apache-2.0 |
| [go.opentelemetry.io/otel/metric](https://github.com/open-telemetry/opentelemetry-go) | v1.44.0 | Apache-2.0 |
| [go.opentelemetry.io/otel/metric](https://github.com/open-telemetry/opentelemetry-go) | v1.46.0 | Apache-2.0 |
| [go.opentelemetry.io/otel/sdk](https://github.com/open-telemetry/opentelemetry-go) | v1.46.0 | Apache-2.0 |
| [go.opentelemetry.io/otel/trace](https://github.com/open-telemetry/opentelemetry-go) | v1.44.0 | Apache-2.0 |
| [go.opentelemetry.io/otel/trace](https://github.com/open-telemetry/opentelemetry-go) | v1.46.0 | Apache-2.0 |
| [go.opentelemetry.io/proto/otlp](https://github.com/open-telemetry/opentelemetry-proto-go) | v1.11.0 | Apache-2.0 |
| [go.temporal.io/api](https://github.com/temporalio/api-go) | v1.63.5 | MIT |
| [go.temporal.io/sdk](https://github.com/temporalio/sdk-go) | v1.48.0 | MIT |
| [go.uber.org/atomic](https://github.com/uber-go/atomic) | v1.11.0 | MIT |
| [go.yaml.in/yaml/v3](https://github.com/yaml/go-yaml) | v3.0.5 | Apache-2.0 |
| [golang.org/x/crypto](https://cs.opensource.google/go/x/crypto) | v0.55.0 | BSD-3-Clause |
| [golang.org/x/net](https://cs.opensource.google/go/x/net) | v0.58.0 | BSD-3-Clause |
| [golang.org/x/sync](https://cs.opensource.google/go/x/sync) | v0.22.0 | BSD-3-Clause |
| [golang.org/x/sys](https://cs.opensource.google/go/x/sys) | v0.47.0 | BSD-3-Clause |
| [golang.org/x/text](https://cs.opensource.google/go/x/text) | v0.40.0 | BSD-3-Clause |
| [golang.org/x/text](https://cs.opensource.google/go/x/text) | v0.41.0 | BSD-3-Clause |
| [golang.org/x/time](https://cs.opensource.google/go/x/time) | v0.3.0 | BSD-3-Clause |
| [golang.org/x/time](https://cs.opensource.google/go/x/time) | v0.5.0 | BSD-3-Clause |
| [google.golang.org/genproto/googleapis/api](https://github.com/googleapis/go-genproto) | v0.0.0-20260819154853-08b0e4226688 | Apache-2.0 |
| [google.golang.org/genproto/googleapis/rpc](https://github.com/googleapis/go-genproto) | v0.0.0-20260819154853-08b0e4226688 | Apache-2.0 |
| [google.golang.org/grpc](https://github.com/grpc/grpc-go) | v1.83.2 | Apache-2.0 |
| [google.golang.org/protobuf](https://github.com/protocolbuffers/protobuf-go) | v1.36.12 | BSD-3-Clause |
| [gopkg.in/yaml.v3](https://github.com/go-yaml/yaml) | v3.0.1 | Apache-2.0 |

## Python packages

Runtime dependencies of the OSS lifecycle image (`requirements-oss.txt`) and every shipped
connector (`shared/mcp-connectors/**/requirements.txt`, uncommented lines only). Licenses are
SPDX identifiers from each package's published metadata. The OSS lifecycle image also requires the
`docker` Python SDK (`docker>=7.0.0`, **Apache-2.0**) — used by `/v1/deploy` to build/start
connector containers — and its transitive deps (`requests`, `urllib3`, `websocket-client`,
`packaging`; all permissive). The exact transitive closure is enumerated at release time.

| Component | Version | License |
|---|---|---|
| [azure-storage-blob](https://github.com/Azure/azure-sdk-for-python) | >=12.19.0 (azure-blob connector) | MIT |
| [boto3](https://github.com/boto/boto3) | >=1.26.0 (aws-s3, azure-blob, gcs connectors; also base_connector.py staging client) | Apache-2.0 |
| [botocore](https://github.com/boto/botocore) | transitive of boto3; imported directly in base_connector.py (botocore.config.Config) | Apache-2.0 |
| [cryptography](https://github.com/pyca/cryptography) | >=41.0.0 (mysql connector — PyMySQL caching_sha2_password auth) | Apache-2.0 OR BSD-3-Clause |
| [databricks-sql-connector](https://github.com/databricks/databricks-sql-python) | >=3.0.0 (databricks connector) | Apache-2.0 |
| [fastapi](https://github.com/fastapi/fastapi) | ==0.137.1 (OSS image); >=0.100.0 (connectors) | MIT |
| [fastavro](https://github.com/fastavro/fastavro) | >=1.8.0 (aws-s3 connector; base_connector.py avro writer) | MIT |
| [google-cloud-bigquery](https://github.com/googleapis/python-bigquery) | >=3.0.0 (bigquery connector) | Apache-2.0 |
| [google-cloud-storage](https://github.com/googleapis/python-storage) | >=2.10.0 (gcs connector) | Apache-2.0 |
| [httpx](https://github.com/encode/httpx) | >=0.26.0 (OSS); >=0.25.0 (google-sheets connector) | BSD-3-Clause |
| [lz4](https://github.com/python-lz4/python-lz4) | >=4.3.0 (aws-s3 connector; base_connector.py lz4.frame) | BSD-3-Clause |
| [openpyxl](https://foss.heptapod.net/openpyxl/openpyxl) | >=3.1.0 (aws-s3 connector; base_connector.py xlsx writer) | MIT |
| [oracledb](https://github.com/oracle/python-oracledb) | >=2.0.0 (oracle connector, thin mode) | UPL-1.0 OR Apache-2.0 |
| [psycopg2-binary](https://github.com/psycopg/psycopg2) | >=2.9.0 (postgresql + redshift connectors) | LGPL-3.0-or-later |
| [pyarrow](https://github.com/apache/arrow) | >=14.0.0 (aws-s3, azure-blob, gcs connectors; base_connector.py parquet/orc/feather) | Apache-2.0 |
| [pydantic](https://github.com/pydantic/pydantic) | >=2.6.0 (OSS); >=2.0.0 (connectors) | MIT |
| [pymongo](https://github.com/mongodb/mongo-python-driver) | >=4.6.0 (mongodb connector) | Apache-2.0 |
| [PyMySQL](https://github.com/PyMySQL/PyMySQL) | ==1.1.3 (mysql connector; pinned to the verified 1.1.x line — replaced GPLv2 mysql-connector-python per #499) | MIT |
| [pyodbc](https://github.com/mkleehammer/pyodbc) | >=5.0.0 (sqlserver connector) | MIT-0 |
| [python-dateutil](https://github.com/dateutil/dateutil) | base_connector.py lazy import (date parsing); ships transitively via boto3/botocore | Apache-2.0 OR BSD-3-Clause |
| [python-snappy](https://github.com/andrix/python-snappy) | >=0.6.0 (aws-s3 connector; base_connector.py snappy) | BSD-3-Clause |
| [pytz](https://github.com/stub42/pytz) | base_connector.py OPTIONAL lazy import (try/except tz path); not declared in any manifest — present only if pulled transitively | MIT |
| [requests](https://github.com/psf/requests) | >=2.28.0 (github-rest, petstore, shopify-admin-graphql, stripe, widgets-graphql; also base_connector.py lazy import) | Apache-2.0 |
| [snowflake-connector-python](https://github.com/snowflakedb/snowflake-connector-python) | >=3.0.0 (snowflake connector) | Apache-2.0 |
| [starlette](https://github.com/encode/starlette) | ==1.3.1 (OSS image; also fastapi transitive) | BSD-3-Clause |
| [uvicorn](https://github.com/encode/uvicorn) | >=0.27.0 [standard] (OSS); >=0.22.0 (connectors) | BSD-3-Clause |
| [zstandard](https://github.com/indygreg/python-zstandard) | >=0.21.0 (aws-s3 connector; base_connector.py zstd) | BSD-3-Clause |

## Frontend (npm, production dependencies)

Direct production dependencies from `frontend/package.json`. The full transitive tree (with
texts) is enumerated at release time by `scripts/gen-third-party-notices.sh` (npm ci +
license-checker). Notable transitive: `caniuse-lite` -- CC-BY-4.0 (attribution-only browser data).

| Component | Version | License |
|---|---|---|
| `@codemirror/autocomplete` | ^6.20.2 | MIT |
| `@codemirror/lang-sql` | ^6.10.0 | MIT |
| `@codemirror/state` | ^6.6.0 | MIT |
| `@codemirror/theme-one-dark` | ^6.1.3 | MIT |
| `@codemirror/view` | ^6.43.0 | MIT |
| `@hookform/resolvers` | ^5.2.2 | MIT |
| `@radix-ui/react-accordion` | ^1.2.12 | MIT |
| `@radix-ui/react-alert-dialog` | ^1.1.15 | MIT |
| `@radix-ui/react-avatar` | ^1.1.11 | MIT |
| `@radix-ui/react-checkbox` | ^1.3.3 | MIT |
| `@radix-ui/react-collapsible` | ^1.1.3 | MIT |
| `@radix-ui/react-dialog` | ^1.1.15 | MIT |
| `@radix-ui/react-dropdown-menu` | ^2.1.16 | MIT |
| `@radix-ui/react-icons` | ^1.3.2 | MIT |
| `@radix-ui/react-label` | ^2.1.8 | MIT |
| `@radix-ui/react-popover` | ^1.1.15 | MIT |
| `@radix-ui/react-radio-group` | ^1.2.0 | MIT |
| `@radix-ui/react-scroll-area` | ^1.2.10 | MIT |
| `@radix-ui/react-select` | ^2.2.6 | MIT |
| `@radix-ui/react-separator` | ^1.1.8 | MIT |
| `@radix-ui/react-slot` | ^1.2.4 | MIT |
| `@radix-ui/react-switch` | ^1.2.6 | MIT |
| `@radix-ui/react-tabs` | ^1.1.13 | MIT |
| `@radix-ui/react-tooltip` | ^1.2.8 | MIT |
| `@sentry/nextjs` | ^10.51.0 | MIT |
| `@tanstack/react-query` | ^5.90.11 | MIT |
| `@types/dagre` | ^0.7.54 | MIT |
| `@uiw/react-codemirror` | ^4.25.10 | MIT |
| `@xyflow/react` | ^12.10.2 | MIT |
| `axios` | ^1.16.0 | MIT |
| [class-variance-authority](https://www.npmjs.com/package/class-variance-authority) | ^0.7.1 | Apache-2.0 |
| `clsx` | ^2.1.1 | MIT |
| `dagre` | ^0.8.5 | MIT |
| `date-fns` | ^4.1.0 | MIT |
| `framer-motion` | ^12.23.24 | MIT |
| [lucide-react](https://www.npmjs.com/package/lucide-react) | ^0.555.0 | ISC |
| `next` | 16.2.10 | MIT |
| `next-themes` | ^0.4.6 | MIT |
| `react` | 19.2.7 | MIT |
| `react-dom` | 19.2.7 | MIT |
| `react-hook-form` | ^7.67.0 | MIT |
| `react-markdown` | ^10.1.0 | MIT |
| `remark-breaks` | ^4.0.0 | MIT |
| `remark-gfm` | ^4.0.1 | MIT |
| `sonner` | ^2.0.7 | MIT |
| `tailwind-merge` | ^3.4.0 | MIT |
| [xlsx](https://www.npmjs.com/package/xlsx) | ^0.18.5 | Apache-2.0 |
| `zod` | ^4.1.13 | MIT |
| `zustand` | ^5.0.9 | MIT |

## Container image / OS packages

All connector images build `FROM python:3.13-slim` (Debian trixie); its system packages are
under their respective Debian licenses (predominantly permissive, some LGPL). Every connector
apt-installs only `curl` + `ca-certificates` from the Debian repo. **One connector installs
non-Debian / non-permissive packages:**

- **`sqlserver`** — `ACCEPT_EULA=Y apt-get install msodbcsql18 unixodbc-dev` from Microsoft's apt
  repo ([Dockerfile](shared/mcp-connectors/public/database/sqlserver/versions/v1.0.0/Dockerfile)).
  `msodbcsql18` is **Microsoft-proprietary** (ODBC Driver 18 EULA); `unixodbc` is LGPL-2.1 (with
  GPL-2.0 CLI tools). See the attention table above. **The Microsoft EULA's redistribution terms
  must be reviewed before this image is published.** (Oracle uses `oracledb` thin mode — no
  proprietary Instant Client — so it carries no such package.)

The Go / Python / npm inventories above are the language-package layer only; the verbatim
OS-package manifests are reproduced from each built image at release time.

## How to regenerate

```bash
# one-time: install the generators
go install github.com/google/go-licenses@latest   # optional -- the script falls back to
                                                   # 'go list -deps' when go-licenses cannot
                                                   # analyze the toolchain (Go >= 1.25)
pipx install pip-licenses                          # or run inside the per-manifest venvs
# then:
scripts/gen-third-party-notices.sh                 # writes the full THIRD_PARTY_NOTICES.md
CHECK=1 scripts/gen-third-party-notices.sh         # regen to a temp file + diff (drift check)
```
