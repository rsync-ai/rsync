import { describe, it, expect, vi, beforeEach, type Mock } from "vitest"
import fs from "node:fs"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

import { authFetch } from "@/lib/api/auth-fetch"
import {
  DEFAULT_NAMESPACE_MODEL,
  listsNamespaces,
  loadNamespaceModels,
  namespaceKey,
  namespaceModelFor,
  namespaceModelsSnapshot,
  primeNamespaceModels,
  type NamespaceModel,
} from "../namespaceModel"
import { NAMESPACE_MODEL_GOLDEN, repoNamespaceModels } from "./repoNamespaceModels"

// The same golden backend-orchestrator/pkg/namespacemodel/model_test.go reads:
// the repo's connector metadata, looked up the way the frontend looks it up,
// must give exactly these answers.
const golden = JSON.parse(fs.readFileSync(NAMESPACE_MODEL_GOLDEN, "utf8")) as {
  defaults: NamespaceModel
  cases: Record<string, NamespaceModel>
}

describe("namespace models from the repo's connector metadata", () => {
  const models = repoNamespaceModels()

  it("reads every database and storage connector", () => {
    // 14 connectors declare the block, most with aliases.
    expect(Object.keys(models.models).length).toBeGreaterThanOrEqual(14)
  })

  it("resolves every golden name", () => {
    expect(Object.keys(golden.cases).length).toBeGreaterThanOrEqual(50)
    for (const [name, want] of Object.entries(golden.cases)) {
      expect(namespaceModelFor(models, name), name).toEqual(want)
    }
  })

  it("an unknown name gets the golden defaults", () => {
    expect(DEFAULT_NAMESPACE_MODEL).toEqual(golden.defaults)
    expect(namespaceModelFor(models, "some-new-saas")).toEqual(golden.defaults)
    expect(namespaceModelFor(undefined, "postgresql")).toEqual(golden.defaults)
  })
})

describe("listsNamespaces", () => {
  const models = repoNamespaceModels()

  it("is true for the connectors whose databases or schemas the Scope step narrows", () => {
    for (const t of ["mysql", "MongoDB", "clickhouse", "postgresql", "aurora_mysql"]) {
      expect(listsNamespaces(models, t), t).toBe(true)
    }
  })

  it("is false for object stores, unknown names and before the models load", () => {
    for (const t of ["gcs", "aws-s3", "azure-blob", "some-new-saas", ""]) {
      expect(listsNamespaces(models, t), t).toBe(false)
    }
    expect(listsNamespaces(undefined, "mysql")).toBe(false)
    // An older gateway sends no list: nothing is listed, the Scope step stays hidden.
    expect(listsNamespaces({ models: models.models, defaults: models.defaults }, "mysql")).toBe(false)
  })
})

describe("namespaceKey", () => {
  it("folds case, spaces, hyphens and underscores like the Go Key", () => {
    expect(namespaceKey(" Aurora_MySQL ")).toBe("auroramysql")
    expect(namespaceKey("AWS S3")).toBe("awss3")
    expect(namespaceKey("azure-blob-storage")).toBe("azureblobstorage")
    expect(namespaceKey(undefined)).toBe("")
  })
})

describe("loadNamespaceModels", () => {
  const body = {
    models: { postgresql: { table_namespace: "schema", destination_namespace: "schema", destination_default: "public" } },
    defaults: { ...DEFAULT_NAMESPACE_MODEL },
  }

  beforeEach(() => {
    primeNamespaceModels(undefined)
    ;(authFetch as Mock).mockReset()
  })

  it("fetches the models once and shares them", async () => {
    ;(authFetch as Mock).mockResolvedValue({ ok: true, json: async () => body })
    const [a, b] = await Promise.all([loadNamespaceModels(), loadNamespaceModels()])
    expect(a).toEqual(body)
    expect(b).toEqual(body)
    expect(await loadNamespaceModels()).toEqual(body)
    expect(authFetch).toHaveBeenCalledTimes(1)
    expect(String((authFetch as Mock).mock.calls[0][0])).toMatch(/\/api\/v1\/connectors\/namespace-models$/)
    expect(namespaceModelFor(namespaceModelsSnapshot(), "PostgreSQL").destination_default).toBe("public")
  })

  it("a failed fetch keeps the defaults and is tried again next time", async () => {
    ;(authFetch as Mock).mockResolvedValueOnce({ ok: false, status: 502, json: async () => ({}) })
    expect(await loadNamespaceModels()).toBeUndefined()
    expect(namespaceModelsSnapshot()).toBeUndefined()

    ;(authFetch as Mock).mockRejectedValueOnce(new Error("network down"))
    expect(await loadNamespaceModels()).toBeUndefined()

    ;(authFetch as Mock).mockResolvedValueOnce({ ok: true, json: async () => ({ unexpected: true }) })
    expect(await loadNamespaceModels()).toBeUndefined()

    ;(authFetch as Mock).mockResolvedValueOnce({ ok: true, json: async () => body })
    expect(await loadNamespaceModels()).toEqual(body)
    expect(authFetch).toHaveBeenCalledTimes(4)
  })
})
