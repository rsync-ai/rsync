import { describe, expect, it } from "vitest";
import {
  fromApiTransforms,
  isSupportedOperation,
  SUPPORTED_OPERATIONS,
  toEngineTransform,
  type TransformRule,
} from "./transformOps";

const rule = (over: Partial<TransformRule>): TransformRule => ({
  id: "r1",
  order: 0,
  type: "producer",
  operation: "filter",
  enabled: true,
  config: {},
  ...over,
});

describe("toEngineTransform", () => {
  it("maps Hash Column onto mask_pii, the only type the engine has for hashing", () => {
    // Before this case existed the switch fell through to `return null`: the rule
    // disappeared from Preview with no warning, and save failed far away with
    // `unknown transform type "hash"`.
    const out = toEngineTransform(
      rule({ operation: "hash", config: { column: "email", hash_function: "sha256" } })
    );

    expect(out).not.toBeNull();
    expect(out!.type).toBe("mask_pii");
    expect(out!.config.mask_type).toBe("hash");
    // mask_nested.go reads `hash_function` verbatim, so it must survive unrenamed.
    expect(out!.config.hash_function).toBe("sha256");
    expect(out!.config.column).toBe("email");
  });

  it("produces the same engine transform for Hash Column as for Mask + mask_type hash", () => {
    const viaHash = toEngineTransform(rule({ operation: "hash", config: { column: "email" } }));
    const viaMask = toEngineTransform(
      rule({ operation: "mask", config: { column: "email", mask_type: "hash" } })
    );
    expect(viaHash!.type).toBe(viaMask!.type);
    expect(viaHash!.config.mask_type).toBe(viaMask!.config.mask_type);
  });

  it("returns null for every operation no engine can run", () => {
    for (const op of ["aggregate", "join", "enrich", "deduplicate", "sort", "limit", "sql", "python_udf"] as const) {
      expect(toEngineTransform(rule({ operation: op })), op).toBeNull();
    }
  });

  it("emits an engine type for every operation the builder offers", () => {
    // The dialog's cards come from SUPPORTED_OPERATIONS. If one of them has no case
    // here it vanishes silently from Preview, which is exactly how `hash` was lost.
    for (const op of SUPPORTED_OPERATIONS) {
      const out = toEngineTransform(rule({ operation: op }));
      expect(out, `offered operation ${op} maps to nothing`).not.toBeNull();
      expect(out!.type, op).not.toBe("");
    }
  });

  it("does not mutate the rule's own config", () => {
    const config = { column: "email", hash_function: "sha256" };
    toEngineTransform(rule({ operation: "hash", config }));
    expect(config).toEqual({ column: "email", hash_function: "sha256" });
  });
});

describe("fromApiTransforms", () => {
  // The exact body POST /api/v1/transforms/parse returned for
  // "filter orders where amount > 100", recorded against app.rsync.ai.
  const apiResponse = [
    {
      id: "bb317d87-0000-0000-0000-000000000000",
      pipeline_id: "",
      transform_type: "producer",
      transform_order: 0,
      transform_config: { condition: "amount > 100", operation: "filter" },
      enabled: true,
    },
  ];

  it("translates the API's DB shape into the shape the builder renders", () => {
    const [r] = fromApiTransforms(apiResponse);

    // The page filters cards with `t.type === "producer"` and reads `t.operation`.
    // Storing the raw row left both undefined, so Generate returned 200 and drew
    // nothing at all.
    expect(r.type).toBe("producer");
    expect(r.operation).toBe("filter");
    expect(r.config).toEqual({ condition: "amount > 100" });
    expect(r.enabled).toBe(true);
    expect(r.order).toBe(0);
    // `operation` belongs in the rule, not the config — transformPlan.toDefinitions
    // puts it back on the way out, and a duplicate would survive the round trip.
    expect(r.config.operation).toBeUndefined();
  });

  it("routes a consumer row to the CDC side", () => {
    const [r] = fromApiTransforms([{ ...apiResponse[0], transform_type: "consumer" }]);
    expect(r.type).toBe("consumer");
  });

  it("drops a parsed rule no engine can run", () => {
    // The parser is not allowed to reintroduce through the back door what the
    // dialog now refuses to offer.
    const out = fromApiTransforms([
      { transform_type: "consumer", transform_config: { operation: "aggregate", group_by: "region" } },
    ]);
    expect(out).toEqual([]);
  });

  it("returns an empty list for a body with no transforms", () => {
    expect(fromApiTransforms(undefined)).toEqual([]);
    expect(fromApiTransforms(null)).toEqual([]);
    expect(fromApiTransforms({})).toEqual([]);
    expect(fromApiTransforms([])).toEqual([]);
  });

  it("mints an id when the API sends none, so React keys stay stable", () => {
    const [r] = fromApiTransforms(
      [{ transform_type: "producer", transform_config: { operation: "filter" } }],
      () => "generated-id"
    );
    expect(r.id).toBe("generated-id");
  });
});

describe("isSupportedOperation", () => {
  it("rejects the Tier-2 operations and anything unknown", () => {
    expect(isSupportedOperation("sql")).toBe(false);
    expect(isSupportedOperation("python_udf")).toBe(false);
    expect(isSupportedOperation("")).toBe(false);
    expect(isSupportedOperation("not_a_real_operation")).toBe(false);
  });
});
