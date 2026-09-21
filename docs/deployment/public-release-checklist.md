# Public release checklist

The manual steps that make the public repository, the README, the website and the docs tell
one consistent story about one release — and what each "downloads" or "users" number does
and does not mean. Nothing in this file is automated except where it says so, and none of
these steps is performed by a code change: GitHub settings, releases and the website are
changed by a maintainer.

The public repository is [`rsync-ai/rsync`](https://github.com/rsync-ai/rsync).

## 1. What each number measures

Four different things get called "downloads". They are not interchangeable, and only the
last two are evidence that someone used the product.

| Signal | Where to read it | What it actually counts | What it does not tell you |
|---|---|---|---|
| **Git clone traffic** | Repository → Insights → Traffic, or `gh api repos/rsync-ai/rsync/traffic/clones` | `git clone`/fetch operations against the repository, over a **rolling 14-day window** only. Includes bots, mirrors, CI runs and repeat clones. | People. It is not a user count, not an install count and not a download count. **Do not market clone counts as user downloads.** |
| **Release asset downloads** | `gh api repos/rsync-ai/rsync/releases --jq '.[] \| {tag: .tag_name, assets: [.assets[] \| {name, download_count}]}'` | Exact per-file downloads of files **attached to a GitHub Release**, since the release was published. Only covers files you attach. | Whether the file was run, or succeeded. Downloads by CI, scanners and scripted re-downloads are included. |
| **Completed installs** | Nowhere today. | — | An install that reached a running stack leaves no trace on GitHub. `raw.githubusercontent.com` (where `install.sh` and the compose file are fetched from) keeps no counter, and image pulls from GHCR are visible only as per-package numbers, at roughly ten pulls per install. |
| **First successful pipeline runs** | Nowhere today. | — | This is the number that would show real use, and it is invisible without a usage report from the installation itself. rsync.ai sends none, and none may be added without an explicit decision. |

Also available, and useful for context only: **referrers** (Insights → Traffic → Referring
sites), **stars**, **forks** and **issues opened**.

**What you may say publicly.** You may quote release-asset download counts as "downloads of
`<file>`" with the file named, and clone counts as "clones in the last 14 days" with the
window named — and only with that wording. You may not say "users", "installs" or "active
deployments" from any number in this table.

### Keep a traffic history

GitHub keeps clone and view traffic for only 14 days. Capture it on a schedule, or it is
gone; this needs push access to the repository.

```bash
gh api repos/rsync-ai/rsync/traffic/clones   > "clones-$(date +%F).json"
gh api repos/rsync-ai/rsync/traffic/views    > "views-$(date +%F).json"
gh api repos/rsync-ai/rsync/traffic/popular/referrers > "referrers-$(date +%F).json"
```

Store the files somewhere that is not the repository itself.

## 2. Repository settings (manual, GitHub → Settings)

None of these can be set from a commit.

- [ ] **Description.** Suggested text — it states the licence and claims only what the
  README claims:

  > Self-hosted, source-available AI data platform for batch pipelines, CDC, scheduled models, and lineage.

  The description in place when this checklist was written is older wording ("AI-native data
  pipelines for databases, APIs, warehouses and CDC…"); replace it so the repository, README
  and website open with the same sentence.
- [ ] **Website.** `https://rsync.ai` (this was already set when this checklist was written).
- [ ] **Topics.** GitHub allows 20 and the repository was at 20 when this was written, so
  adding one means removing one. Candidates to add, each backed by a shipped feature:
  `data-lineage`, `data-observability`, `temporal`, `source-available`. Candidates to drop
  to make room, being the least specific: `ai`, `llm`, `helm`, `kubernetes`,
  `data-integration`. Do not add a topic for a capability the README does not claim.
- [ ] **Social preview image.** Settings → General → Social preview, 1280×640. Use the
  real product name and positioning line only — no invented metrics, customer logos or
  screenshots that are not of the shipped product.
- [ ] **Licence display.** The repository API reports the licence as `NOASSERTION`, because
  GitHub does not detect the Elastic License 2.0 from the `LICENSE` file. That is expected;
  the README's licence section is the plain-English statement. Call it "source-available",
  never "open source".
- [ ] **Discussions.** Currently disabled. If you enable it (Settings → General → Features),
  create at least *Q&A* and *Ideas* categories, then uncomment the "Questions and ideas"
  contact link at the bottom of `.github/ISSUE_TEMPLATE/config.yml` in a follow-up change —
  the file explains why the link is held back until then (a link to a disabled tab 404s).
- [ ] **Issue templates.** `bug_report.md`, `feature_request.md` and the chooser
  configuration already exist under `.github/ISSUE_TEMPLATE/`; confirm they render at
  *Issues → New issue* after any change to them.

## 3. Cutting a release

A GitHub Release with attached files is the only thing that produces an exact download
count, and it lets a user verify what they ran.

### Before you tag

- [ ] The README, the docs and the website all name the release you are about to publish.
  As of writing, several places name an older one — see [section 5](#5-places-that-must-agree).
- [ ] `RSYNC_REF` in `install.sh` defaults to the release the installer should install.
  It pairs the compose file with the image tag, so a default that trails the release means
  new users install the old version.
- [ ] Keep the `[Unreleased]` heading in [CHANGELOG.md](../../CHANGELOG.md) in the release PR.
  A test fails if a `## [x.y.z]` heading names a tag that does not exist, so the heading
  cannot merge before the tag; retitle it in the follow-up PR below, once the tag exists.
- [ ] The images and the Helm chart are published by `docker-publish.yml` when the
  `v*.*.*` tag is pushed; it derives the chart version and image tag from the tag, so the
  version in `deploy/helm/rsync-ai/Chart.yaml` is not what ships. The README's "Which code
  you get" note names the release: update it in the release PR. Its `helm install --version`
  example moves in the follow-up PR below, because it must name a chart that exists.

### After you tag: the follow-up PR

Three checks read the tag, so they can only pass once it exists: the changelog heading,
`Chart.yaml` `version`/`appVersion` (a test requires the tag it names to have built every
image), and `RSYNC_CHART_VERSION` in `install-k8s.sh` (a test requires it to equal
`appVersion`). So the release PR bumps `install.sh` `RSYNC_REF` and the README "Which code
you get" note only, and one small PR after the tag moves the rest:

- [ ] `Chart.yaml` `version` and `appVersion`, `install-k8s.sh` `RSYNC_CHART_VERSION`
- [ ] README `helm install --version` and the `.Chart.AppVersion` sentence, the version
  lines in `deploy/helm/rsync-ai/README.md` and `docs/deployment/kubernetes.md`
- [ ] `CHANGELOG.md`: `[Unreleased]` becomes `## [x.y.z]`, with a fresh `[Unreleased]` above it

### Publish (manual)

Create the Release from the tag in the GitHub UI (or `gh release create`), with release
notes taken from the CHANGELOG section. This is a deliberate human step; nothing creates a
release automatically.

### Attached files (automated once a release is published)

`.github/workflows/release-assets.yml` runs when a release is **published**. It attaches:

| File | Why |
|---|---|
| `install.sh` | The installer, as it was at the tag. Its download count is the exact figure for "people who fetched the installer from the release". |
| `docker-compose.quickstart.yml` | The compose file that installer starts. |
| `SHA256SUMS` | Checksums of the two files above, so a user can check them. |

After publishing, verify (the workflow needs a minute or two):

```bash
gh release view <tag> --repo rsync-ai/rsync --json assets --jq '.assets[].name'
# Expect: install.sh, docker-compose.quickstart.yml, SHA256SUMS
```

A release is not "real" until its assets and checksums are attached: a tag with no assets
gives no download count. Releases published before the workflow existed have none, and the
workflow does not backfill them.

### After the first release with assets

- [ ] Optionally point the README's one-line install at the latest release's asset —
  `curl -sSL https://github.com/rsync-ai/rsync/releases/latest/download/install.sh | bash` —
  so installs are counted. **Only do this once the latest release actually carries
  `install.sh`.** Until then the URL 404s and the documented install breaks. Some tests pin
  the documented install command, so change the README, the docs and those tests together.
- [ ] Publish the checksum command next to the install command:
  `sha256sum -c SHA256SUMS` after downloading the files.

## 4. What is deliberately not here

- **A usage ping in the installer or product.** Adding it would report installs and first
  successful runs, and it is the only way to get those two numbers. It is not built. Any
  telemetry needs an explicit decision, must be opt-out documented in the README, and must
  default to what that decision says.
- **A vanity install URL** (for example `get.rsync.ai`) that redirects to `install.sh`. It
  would give a request count, but it adds a service to run and only counts fetches.
- **Marketing site changes.** The website is not in this repository. Its titles, meta
  descriptions and structured data are managed where it is built.

## 5. Places that must agree

Before announcing a release, check that each of these names the same version and describes
the product with the same sentence: *a self-hosted data platform for batch pipelines, CDC,
scheduled data models, and lineage.*

| Place | Check |
|---|---|
| GitHub repository description and topics | Section 2 |
| `README.md` — headline, intro, "Which code you get" note, Helm chart `--version` | Version and positioning |
| `install.sh` — `RSYNC_REF` default | Version |
| `deploy/helm/rsync-ai/Chart.yaml` — `version` and `appVersion` (checked-in defaults; the published chart takes its version from the tag) | Version |
| `docs/README.md` and `docs/solutions/README.md` | Positioning; no page claims more than its *Verification status* section |
| `CHANGELOG.md` | A heading for the release |
| GitHub Release notes | Copied from the CHANGELOG, not rewritten |
| `https://rsync.ai` (website) | Same positioning sentence and same version; no capability the README does not claim |
| Social preview image | Product name and positioning only |

## 6. Claims not to make

Do not publish, on any of these surfaces, a capability, scale, uptime, connector-count,
pricing, security or "works with any API" claim that the repository cannot back with a test
or a documented run. In particular:

- Connector counts and CDC families come from the generated
  [connector reference](../connectors/reference.md) — quote that, not a memory of it.
- Feature pages carry a *Verification status* section; do not summarise a page as more
  verified than it says.
- Do not call the project "open source". It is source-available under the Elastic
  License 2.0.
- Do not describe reverse ETL, column-level lineage or hosted/cloud availability as
  features: the first two are not shipped, and there is no hosted offering.
