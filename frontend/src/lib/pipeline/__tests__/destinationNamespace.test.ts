import { describe, it, expect } from "vitest"
import {
  validateNamespace,
  namespaceKindForType,
  destDefaultSchemaName,
  kindMeta,
} from "../destinationNamespace"

// These tests pin the destination-namespace contract behind the multi-schema
// "Select entire database" fix: a blank namespace must be VALID when the caller
// says it is optional (multi-schema selections mirror each source schema at the
// destination; path/prefix destinations write to root), but still REQUIRED by
// default so single-schema relational destinations keep prompting for a name.
describe("validateNamespace required flag", () => {
  it("rejects empty by default (backward compatible)", () => {
    expect(validateNamespace("")).toBe("Enter a name.")
  })

  it("rejects empty when explicitly required", () => {
    expect(validateNamespace("", { required: true })).toBe("Enter a name.")
  })

  it("accepts empty when not required (multi-schema / path destinations)", () => {
    expect(validateNamespace("", { required: false })).toBe("")
    expect(validateNamespace("   ", { required: false })).toBe("")
  })

  it("still format-validates a non-empty value even when optional", () => {
    // A digit-leading name is invalid regardless of the required flag — the
    // optionality only governs the empty case.
    expect(validateNamespace("1bad", { required: false })).toBe("Name must not start with a digit.")
    expect(validateNamespace("warehouse", { required: false })).toBe("")
  })
})

// aws-s3 is the real connector id (metadata.json connector_type: "aws-s3"), but
// the kind/default maps historically listed only "s3" — so S3 destinations fell
// through to the "schema" default and mislabeled the field as a required
// "Schema name". These pin aws-s3 to the object-storage (path) behavior.
describe("aws-s3 is recognized as object storage", () => {
  it("maps aws-s3 to a path namespace kind", () => {
    expect(namespaceKindForType("aws-s3")).toBe("path")
    expect(namespaceKindForType("s3")).toBe("path")
    expect(namespaceKindForType("gcs")).toBe("path")
  })

  it("has no default schema for aws-s3 (path-style, no schema concept)", () => {
    expect(destDefaultSchemaName("aws-s3")).toBe("")
  })

  it("labels the aws-s3 namespace field as a non-createable Path prefix", () => {
    const meta = kindMeta(namespaceKindForType("aws-s3"))
    expect(meta.noun).toBe("Path prefix")
    expect(meta.createable).toBe(false)
  })

})
