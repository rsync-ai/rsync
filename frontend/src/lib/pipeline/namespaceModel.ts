/**
 * What each connector calls the level above a table, read from the
 * "namespace_model" block of its metadata.json through
 * GET /api/v1/connectors/namespace-models (backend-orchestrator
 * pkg/namespacemodel). It replaces the per-type switches this file's callers
 * used to carry: a new connector declares the block and the UI picks it up.
 *
 * shared/namespace_model_golden.json pins the answer for every name the old
 * switches knew; __tests__/namespaceModel.test.ts checks this module against it.
 */

import { useEffect, useSyncExternalStore } from "react"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"

export type NamespaceModel = {
  // As a source: what holds its tables ("schema", "database" or "dataset").
  table_namespace: string
  // As a destination: the kind of name the user gives ("schema", "database",
  // "dataset", "prefix" or "path").
  destination_namespace: string
  // The engine's own default namespace ("public" on PostgreSQL), "" for none.
  destination_default: string
}

export type NamespaceModels = {
  // Keyed by namespaceKey(id) and namespaceKey(alias).
  models: Record<string, NamespaceModel>
  defaults: NamespaceModel
  // The keys of the connectors that can list their databases or schemas
  // (list_namespaces): those that declare a table_namespace. Object stores and
  // SaaS connectors are absent. Missing from an older gateway: none.
  lists_namespaces?: string[]
}

// What a name no connector claims reads as, and what everything reads as until
// the models arrive. Same as the Go defaults.
export const DEFAULT_NAMESPACE_MODEL: NamespaceModel = {
  table_namespace: "schema",
  destination_namespace: "schema",
  destination_default: "",
}

// Folds "aurora_mysql", "Aurora-MySQL" and "AWS S3" onto one key, as the Go
// namespacemodel.Key does: lower case, spaces, hyphens and underscores removed.
export function namespaceKey(name?: string): string {
  return (name || "").trim().toLowerCase().replace(/[\s_-]/g, "")
}

export function namespaceModelFor(models: NamespaceModels | undefined, connectorType?: string): NamespaceModel {
  const m = models?.models[namespaceKey(connectorType)]
  return m ?? models?.defaults ?? DEFAULT_NAMESPACE_MODEL
}

// listsNamespaces reports whether a connector can list the databases or
// schemas a connection reaches, which is what the connection modal's Scope step
// narrows. False until the models arrive.
export function listsNamespaces(models: NamespaceModels | undefined, connectorType?: string): boolean {
  const key = namespaceKey(connectorType)
  return !!key && Array.isArray(models?.lists_namespaces) && models.lists_namespaces.includes(key)
}

// One copy per page: the models change only when a connector is added, so they
// are fetched once and shared by every component that reads them.
let snapshot: NamespaceModels | undefined
let inflight: Promise<NamespaceModels | undefined> | undefined
const listeners = new Set<() => void>()

export function namespaceModelsSnapshot(): NamespaceModels | undefined {
  return snapshot
}

// primeNamespaceModels sets the shared copy; tests use it to load the models
// the repo's metadata declares.
export function primeNamespaceModels(models: NamespaceModels | undefined): void {
  snapshot = models
  listeners.forEach((l) => l())
}

function isModels(v: unknown): v is NamespaceModels {
  const o = v as NamespaceModels | undefined
  return !!o && typeof o.models === "object" && o.models !== null && typeof o.defaults === "object" && o.defaults !== null
}

// loadNamespaceModels fetches the models once. A failed fetch is not kept, so
// the next caller tries again; until then every connector reads as the defaults.
export function loadNamespaceModels(): Promise<NamespaceModels | undefined> {
  if (snapshot) return Promise.resolve(snapshot)
  if (!inflight) {
    inflight = (async () => {
      try {
        const res = await authFetch(API_ENDPOINTS.CONNECTORS.NAMESPACE_MODELS)
        if (!res?.ok) return undefined
        const body: unknown = await res.json()
        if (!isModels(body)) return undefined
        primeNamespaceModels(body)
        return body
      } catch {
        return undefined
      } finally {
        inflight = undefined
      }
    })()
  }
  return inflight
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener)
  return () => {
    listeners.delete(listener)
  }
}

// useNamespaceModels returns the models, or undefined while they load. The
// component re-renders when they arrive.
export function useNamespaceModels(): NamespaceModels | undefined {
  const models = useSyncExternalStore(subscribe, namespaceModelsSnapshot, namespaceModelsSnapshot)
  useEffect(() => {
    if (!models) void loadNamespaceModels()
  }, [models])
  return models
}
