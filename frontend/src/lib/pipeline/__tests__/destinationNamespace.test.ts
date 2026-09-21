import { describe, it, expect, beforeAll } from "vitest"
import fs from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"
import {
  validateNamespace,
  namespaceKindForType,
  destDefaultSchemaName,
  defaultNamespaceForTypes,
  kindMeta,
  isLayoutV2Destination,
  validatePipelinePrefix,
} from "../destinationNamespace"

import { primeNamespaceModels } from "../namespaceModel"
import { repoNamespaceModels } from "./repoNamespaceModels"

// Connector types resolve through the namespace models the repo's metadata
// declares, as they do once the page has fetched them from the gateway.
beforeAll(() => primeNamespaceModels(repoNamespaceModels()))

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

// #13: the table-selection dialog pre-fills the namespace with this function
// whenever the stored value is "", which the backend stores on purpose for object
// storage. Returning the source slug put every MongoDB collection under a
// "mongodb/" folder instead of the database's own folder.
describe("object-storage destinations pre-fill an empty path prefix", () => {
  it("never seeds a source name for a path destination", () => {
    // "object-storage" is not a connector name (see shared/namespace_model_golden.json).
    for (const dest of ["gcs", "aws-s3", "s3", "azure-blob", "minio"]) {
      for (const src of ["mongodb", "sqlserver", "snowflake", "postgresql", "mysql", "shopify"]) {
        expect(defaultNamespaceForTypes(src, dest)).toBe("")
      }
    }
  })

  it("keeps the relational pre-fills", () => {
    expect(defaultNamespaceForTypes("mysql", "postgresql")).toBe("public")
    expect(defaultNamespaceForTypes("postgresql", "mysql")).toBe("default")
    expect(defaultNamespaceForTypes("sqlserver", "postgresql")).toBe("sqlserver")
  })
})

// Object layout v2 (gcs, aws-s3, azure-blob): <prefix>/<database>/[<schema>/]<table>/.
// The prefix is required and must satisfy both the server's namespace check and the
// layout v2 prefix rule, or the pipeline silently falls back to the pipeline-id layout.
describe("layout v2 path prefix (GCS, S3, Azure Blob)", () => {
  it("applies to gcs, aws-s3 and azure-blob, matching the orchestrator's gate", () => {
    for (const t of ["gcs", " GCS ", "aws-s3", "AWS_S3", "azure-blob", "Azure_Blob"]) {
      expect(isLayoutV2Destination(t)).toBe(true)
    }
    for (const t of ["s3", "minio", "google-cloud-storage", "postgresql", "", undefined]) {
      expect(isLayoutV2Destination(t)).toBe(false)
    }
  })

  it("matches v2_destinations in the shared golden the sink and orchestrator also read", () => {
    const goldenPath = path.resolve(
      path.dirname(fileURLToPath(import.meta.url)),
      "../../../../../shared/object_layout_golden.json",
    )
    const golden = JSON.parse(fs.readFileSync(goldenPath, "utf8"))
    const { eligible, not_eligible: notEligible } = golden.v2_destinations as {
      eligible: string[]
      not_eligible: string[]
    }
    expect(eligible.length).toBeGreaterThan(0)
    expect(notEligible.length).toBeGreaterThan(0)
    for (const t of eligible) expect(isLayoutV2Destination(t), t).toBe(true)
    for (const t of notEligible) expect(isLayoutV2Destination(t), t).toBe(false)
  })

  it("requires a prefix", () => {
    expect(validatePipelinePrefix("")).toMatch(/Enter a path prefix/)
    expect(validatePipelinePrefix("   ")).toMatch(/Enter a path prefix/)
  })

  it("accepts lowercase letters, digits and underscores starting with a letter", () => {
    for (const ok of ["sales", "sales_orders", "s", "crm2", " sales "]) {
      expect(validatePipelinePrefix(ok)).toBe("")
    }
    expect(validatePipelinePrefix("a".repeat(63))).toBe("")
  })

  it("rejects what either the server or the layout v2 rule would reject", () => {
    // Upper case: layout v2 rule. Hyphen: server namespace check. Leading digit:
    // server. Leading underscore: layout v2. Slash/dot: both.
    for (const bad of ["Sales", "sales-eu", "1sales", "_sales", "a/b", "a.b", "sales orders"]) {
      expect(validatePipelinePrefix(bad)).toMatch(/lowercase letters, digits and underscores/)
    }
    expect(validatePipelinePrefix("a".repeat(64))).toMatch(/too long/)
    expect(validatePipelinePrefix("the")).toMatch(/reserved/)
    expect(validatePipelinePrefix("default")).toMatch(/reserved/)
  })
})
