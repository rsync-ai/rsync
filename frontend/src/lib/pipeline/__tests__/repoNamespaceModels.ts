// Test helper: the namespace models the repo's connector metadata declares,
// built the way the gateway's GET /api/v1/connectors/namespace-models builds
// them (backend-orchestrator pkg/namespacemodel Load): every connector root is
// a directory with latest.json, its metadata.json lives under
// versions/<current_version>/, ids are indexed before aliases, and an empty
// field takes the default. Not a test file itself (no .test. in the name).
import fs from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { DEFAULT_NAMESPACE_MODEL, namespaceKey, type NamespaceModel, type NamespaceModels } from "../namespaceModel"

const REPO_SHARED = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../../../../shared")
export const REPO_CONNECTORS = path.join(REPO_SHARED, "mcp-connectors")
export const NAMESPACE_MODEL_GOLDEN = path.join(REPO_SHARED, "namespace_model_golden.json")

type Metadata = {
  id?: string
  connector_type?: string
  aliases?: string[]
  namespace_model?: Partial<NamespaceModel>
}

function connectorMetadata(dir: string, out: Metadata[]): void {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    if (e.isDirectory()) {
      if (e.name !== "versions" && e.name !== "node_modules" && !e.name.startsWith(".")) {
        connectorMetadata(path.join(dir, e.name), out)
      }
      continue
    }
    if (e.name !== "latest.json") continue
    const cv = JSON.parse(fs.readFileSync(path.join(dir, e.name), "utf8")).current_version
    const mdPath = path.join(dir, "versions", String(cv), "metadata.json")
    if (!cv || !fs.existsSync(mdPath)) continue
    const md: Metadata = JSON.parse(fs.readFileSync(mdPath, "utf8"))
    md.id = (md.id || md.connector_type || path.basename(dir)).trim()
    out.push(md)
  }
}

export function repoNamespaceModels(): NamespaceModels {
  const all: Metadata[] = []
  connectorMetadata(REPO_CONNECTORS, all)
  const declared = all.filter((md) => md.namespace_model)
  const models: Record<string, NamespaceModel> = {}
  const model = (md: Metadata): NamespaceModel => ({
    table_namespace: md.namespace_model?.table_namespace || DEFAULT_NAMESPACE_MODEL.table_namespace,
    destination_namespace: md.namespace_model?.destination_namespace || DEFAULT_NAMESPACE_MODEL.destination_namespace,
    destination_default: md.namespace_model?.destination_default ?? DEFAULT_NAMESPACE_MODEL.destination_default,
  })
  // Go namespacemodel.Listing: a connector lists namespaces when its raw block
  // declares a table_namespace (object stores declare only a destination).
  const lists = new Set<string>()
  const claim = (k: string, md: Metadata) => {
    models[k] = model(md)
    if (md.namespace_model?.table_namespace?.trim()) lists.add(k)
  }
  for (const md of declared) claim(namespaceKey(md.id), md)
  for (const md of declared) {
    for (const a of md.aliases || []) {
      const k = namespaceKey(a)
      if (k && !(k in models)) claim(k, md)
    }
  }
  return { models, defaults: { ...DEFAULT_NAMESPACE_MODEL }, lists_namespaces: [...lists].sort() }
}
