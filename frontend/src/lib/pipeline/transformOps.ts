/**
 * The bridge between the Transform Builder's vocabulary and the execution engine's.
 *
 * It lives outside the page component because it is the part that was wrong: the
 * builder offered 17 operations, the engine has cases for 8 of them, and nothing in
 * between checked. Pulling the mapping out makes each direction testable on its own.
 */

export type TransformOperation =
  | "filter" | "select" | "exclude" | "rename" | "mask" | "hash"
  | "type_convert" | "null_handle" | "truncate"
  | "aggregate" | "join" | "enrich" | "deduplicate" | "sort" | "limit" | "sql" | "python_udf";

export interface TransformRule {
  id: string;
  order: number;
  type: "producer" | "consumer";
  operation: TransformOperation;
  enabled: boolean;
  config: Record<string, any>;
  description?: string;
}

/**
 * The operations some engine can actually execute — the cases in
 * SimpleTransformEngine.CanHandle (shared/go/transforms/engine.go), in the builder's
 * own names. `hash` qualifies only because toEngineTransform rewrites it as mask_pii.
 *
 * Everything else — aggregate, join, enrich, deduplicate, sort, limit, sql, python_udf
 * — has no engine case at all; the Tier-2 (DuckDB) engine meant to run them still
 * returns "not implemented yet (Phase 2)". That gap was not cosmetic: the CDC sink
 * validates in NormalizeModeCDC and fail-closes the WHOLE batch to the DLQ, so a
 * single saved `aggregate` stopped delivery for that table with nothing in the UI
 * to say so.
 */
export const SUPPORTED_OPERATIONS: ReadonlySet<TransformOperation> = new Set<TransformOperation>([
  "filter", "select", "exclude", "rename", "mask", "hash",
  "type_convert", "null_handle", "truncate",
]);

export const isSupportedOperation = (op: string): op is TransformOperation =>
  SUPPORTED_OPERATIONS.has(op as TransformOperation);

/**
 * Builder rule → the shape the preview endpoint and the engine expect.
 * Returns null for a disabled rule, and for any operation no engine can run.
 */
export function toEngineTransform(t: TransformRule): Record<string, any> | null {
  if (!t.enabled) return null;

  const cfg: Record<string, any> = { ...(t.config || {}) };
  const op = t.operation;

  let type = "";
  switch (op) {
    case "filter":
      type = "filter";
      break;
    case "select":
      type = "select_columns";
      break;
    case "exclude":
      type = "exclude_columns";
      break;
    case "rename":
      type = "rename_columns";
      break;
    case "mask":
      type = "mask_pii";
      break;
    case "hash":
      // The engine has no `hash` type — hashing is mask_pii with mask_type: hash,
      // exactly what the Mask card's own "Hash (SHA256)" option produces. Without
      // this case the switch fell through to `return null`, so the rule vanished
      // from Preview with no warning and failed on save as
      // `unknown transform type "hash"`.
      type = "mask_pii";
      cfg["mask_type"] = "hash";
      // hash_function passes straight through: mask_nested.go reads that exact key.
      break;
    case "type_convert":
      type = "type_convert";
      // UI uses to_type; engine expects to
      if (cfg["to"] == null && cfg["to_type"] != null) cfg["to"] = cfg["to_type"];
      delete cfg["to_type"];
      break;
    case "null_handle":
      type = "null_handle";
      // UI uses action; engine expects strategy
      if (cfg["strategy"] == null && cfg["action"] != null) {
        const action = String(cfg["action"] || "").toLowerCase();
        if (action === "default") cfg["strategy"] = "default";
        else cfg["strategy"] = "drop_row";
      }
      delete cfg["action"];
      break;
    case "truncate":
      type = "truncate";
      break;
    default:
      return null;
  }

  // Normalize a few config field shapes to match backend validator/engine expectations.
  if (type === "select_columns" || type === "exclude_columns") {
    if (typeof cfg["columns"] === "string") {
      cfg["columns"] = String(cfg["columns"])
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
    }
  }

  if (type === "rename_columns") {
    // UI captures mappings as "a:b, c:d" string; engine expects map.
    if (typeof cfg["mappings"] === "string") {
      const mappings: Record<string, string> = {};
      for (const pair of String(cfg["mappings"]).split(",")) {
        const p = pair.trim();
        if (!p) continue;
        const [from, to] = p.split(":").map((s) => s.trim());
        if (from && to) mappings[from] = to;
      }
      cfg["mappings"] = mappings;
    }
  }

  if (type === "truncate") {
    if (typeof cfg["max_length"] === "string") {
      const n = Number(cfg["max_length"]);
      if (!Number.isNaN(n)) cfg["max_length"] = n;
    }
  }

  return {
    id: t.id,
    order: t.order,
    enabled: t.enabled,
    type,
    config: cfg,
  };
}

/**
 * POST /api/v1/transforms/parse answers in the DB's own shape — the Go
 * TransformDefinition struct, i.e. `transform_type` plus a `transform_config` whose
 * `operation` key carries the verb:
 *
 *   {"transform_type":"producer","transform_order":0,
 *    "transform_config":{"operation":"filter","condition":"amount > 100"},"enabled":true}
 *
 * The builder's state is a TransformRule: `type`, `operation`, `config`. Generate used
 * to store the raw rows, so every card was filtered out by `t.type === "producer"` and
 * the operator watched the request succeed and nothing appear.
 */
export function fromApiTransforms(raw: unknown, newId: () => string = () => crypto.randomUUID()): TransformRule[] {
  if (!Array.isArray(raw)) return [];

  return raw.flatMap((row: any, i: number): TransformRule[] => {
    const cfg = { ...(row?.transform_config ?? row?.config ?? {}) };
    const operation = String(cfg.operation ?? row?.operation ?? "");
    delete cfg.operation;

    // Drop anything no engine can run, rather than letting the parser reintroduce
    // through the back door exactly what the dialog now refuses to offer.
    if (!isSupportedOperation(operation)) return [];

    return [{
      id: typeof row?.id === "string" && row.id ? row.id : newId(),
      order: typeof row?.transform_order === "number" ? row.transform_order : i,
      type: row?.transform_type === "consumer" ? "consumer" : "producer",
      operation,
      enabled: row?.enabled !== false,
      config: cfg,
    }];
  });
}
