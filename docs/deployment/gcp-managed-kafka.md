# Google Managed Service for Apache Kafka

How to point rsync at a Google-managed Kafka cluster instead of the one in the
chart. Everything here was run against a live cluster; the commands are the ones
that worked, not a reconstruction from Google's docs.

Nothing in rsync is GCP-specific — this is the generic BYO-Kafka path
([`values.yaml`](../../deploy/helm/rsync-ai/values.yaml) `kafka.external`) filled
in with the settings Google's brokers want. For the ACL model itself, read
[Kafka ACLs for a customer-managed cluster](kafka-acls.md); this page covers only
what is different about Google's.

## Pick the authentication path first

Managed Kafka offers two listeners, and the choice is not cosmetic — one of them
cannot carry a long-running pipeline.

| | mutual TLS | SASL_SSL / PLAIN |
|---|---|---|
| Port | `9192` | `9092` |
| `securityProtocol` | `SSL` | `SASL_SSL` |
| Credential | client certificate from a CA Service pool | username + password |
| Expires | when the certificate does (you choose) | **~1 hour** |
| Use it for | **production, and anything that runs more than an hour** | a demo, a smoke test |

**Use mutual TLS.** The SASL password Google accepts is an OAuth access token
from `gcloud auth print-access-token`, and it is valid for about an hour. rsync
passes `kafka.external.saslPassword` through as a static value — there is no
refresh hook — while Google wants a valid token on *every new broker
connection*. So a SASL-configured pipeline works, keeps working for an hour, and
then starts failing in a way that reads like an intermittent auth problem rather
than an expiry. It is fine for proving connectivity and wrong for anything else.

> Google's own recommendation for the SASL listener is OAUTHBEARER with
> Application Default Credentials, which refreshes itself. rsync cannot use it
> yet: its `OAUTHBEARER` support is the OIDC client-credentials grant, so it
> requires a token endpoint plus a client id and secret, which ADC does not have.
> Mutual TLS is the supported answer today.

## Mutual TLS, end to end

Six steps. Substitute your own project, region, and names.

**1. Turn on Certificate Authority Service and create a CA pool.** The pool is
what the Kafka cluster will trust; it is billed monthly, prorated.

```bash
gcloud services enable privateca.googleapis.com --project MY_PROJECT
gcloud privateca pools create rsync-pool --location=us-central1 --tier=devops --project MY_PROJECT
```

**2. Create a root CA in that pool and enable it.**

```bash
gcloud privateca roots create rsync-root --pool=rsync-pool --location=us-central1 --project MY_PROJECT \
  --subject="CN=rsync-root,O=example" \
  --key-algorithm=ec-p256-sha256 --max-chain-length=2 --auto-enable
```

**3. Attach the pool to the Kafka cluster and tell it how to turn a certificate
into a principal.** The mapping rule is what makes `CN=rsync-client` arrive at
the broker as `User:rsync-client`; without it the principal is the whole
distinguished name and no ACL you write will match.

```bash
gcloud managed-kafka clusters update MY_CLUSTER --location=us-central1 --project MY_PROJECT \
  --mtls-ca-pools=projects/MY_PROJECT/locations/us-central1/caPools/rsync-pool \
  --ssl-principal-mapping-rules='RULE:^.*[Cc][Nn]=([a-zA-Z0-9.-]*).*$/$1/L,DEFAULT'
```

This restarts the brokers one at a time and **takes well over ten minutes** —
budget for it and do not interrupt it. If the update is rejected outright, the
cluster predates Managed Kafka's mTLS support; create a new one.

**4. Issue a client key and certificate.** Generate the key yourself and send a
CSR — `gcloud privateca certificates create --generate-key` depends on a Python
cryptography library that is missing from many gcloud installs, and the error it
prints does not say so.

```bash
openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout client-key.pem -out client.csr \
  -subj "/CN=rsync-client/O=example"

gcloud privateca certificates create --project=MY_PROJECT \
  --issuer-location=us-central1 --issuer-pool=rsync-pool --ca=rsync-root \
  --csr=client.csr --cert-output-file=client-cert.pem --validity=P30D
```

Pick `--validity` deliberately: this certificate is the credential, and nothing
rotates it for you. Put its expiry on a calendar.

**5. Grant the principal its ACLs.** `Describe` on the cluster plus rights on the
topics — see [kafka-acls.md](kafka-acls.md) for the full prefix model, which
applies here unchanged.

```bash
gcloud managed-kafka acls add-acl-entry cluster \
  --cluster=MY_CLUSTER --location=us-central1 --project MY_PROJECT \
  --principal='User:rsync-client' --operation=DESCRIBE --permission-type=ALLOW --host='*'
```

**6. Install with the certificate and an empty `caCert`.**

```yaml
kafka:
  enabled: false
  external:
    bootstrapServers: "bootstrap.MY_CLUSTER.us-central1.managedkafka.MY_PROJECT.cloud.goog:9192"
    securityProtocol: SSL
    tls:
      caCert: ""                    # MUST stay empty — see below
      clientCert: |
        -----BEGIN CERTIFICATE-----
        …contents of client-cert.pem…
        -----END CERTIFICATE-----
      clientKey: |
        -----BEGIN PRIVATE KEY-----
        …contents of client-key.pem…
        -----END PRIVATE KEY-----
```

Prefer `tls.existingSecret` for a Secret you manage yourself; when you do, set
`clientCertKey` and `clientKeyKey`, and `clientPemKey` as well if the CDC plane
is enabled — Kafka Connect needs the certificate and key as one combined PEM
file, which the chart builds for you only when it holds the values itself.

## Four things that go wrong

- **`tls.caCert` must be empty, and the natural reading is the wrong one.** The
  broker's *server* certificate chains to Google Trust Services, a public root the
  images already trust. Your CA Service root signs the *client* certificate and has
  nothing to do with verifying the broker. Putting it in `caCert` **replaces** the
  default trust store rather than adding to it, and every client then fails the
  handshake against a broker certificate it can no longer verify.
- **Adding your first ACL silently changes the rules for everyone.** While a
  cluster has no ACLs at all, authenticated principals are allowed. Create one ACL
  — for the mTLS principal, say — and authorization becomes ACL-driven for *every*
  principal, including the IAM/SASL identity that worked five minutes earlier. It
  starts failing with `TopicAuthorizationFailedError` until it gets entries of its
  own. Grant the identities you still need before you cut over.
- **Reaching the cluster at all requires being inside the VPC.** The bootstrap
  address resolves to a private Private Service Connect endpoint. From outside the
  attached network it does not resolve, which looks like a hang rather than a
  networking error.
- **A bad credential can appear to work on the SASL listener.** Once a principal
  has authenticated successfully, Google will accept later connections from it
  carrying a token that is merely *shaped* like one. If you are testing that
  authentication is enforced, test with a principal that has never connected, or
  you will prove nothing.

## SASL_SSL, for a short-lived test only

```yaml
kafka:
  enabled: false
  external:
    bootstrapServers: "bootstrap.MY_CLUSTER.us-central1.managedkafka.MY_PROJECT.cloud.goog:9092"
    securityProtocol: SASL_SSL
    saslMechanism: PLAIN
    saslUsername: "you@example.com"          # the principal, verbatim
    saslPassword: "<gcloud auth print-access-token>"
    tls: { caCert: "" }                      # empty, same reason as above
```

The password is the raw token — do not base64-encode it, Google does that
itself. A token with a trailing newline fails, so use
`printf '%s' "$(gcloud auth print-access-token)"` when writing it into a file or
a Secret. Expect it to stop working within the hour.

## Verifying

Any Kafka client that can reach the VPC will do. rsync's own clients — the Go
orchestrator, the Go sink worker, the Python service, and the JVM client inside
Kafka Connect — all read the same settings the chart writes above, so a
successful install exercising a pipeline end to end is the real check. A failure
during a broker handshake is logged with the cause; a certificate rejected for
authorization reports that it is not authorized for the topic, which means the
ACL in step 5 is missing or the principal mapping rule in step 3 did not match.
