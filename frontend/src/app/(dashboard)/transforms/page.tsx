"use client";

import React, { useState, useEffect, useRef } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { 
  Plus, 
  Trash2, 
  GripVertical, 
  Filter, 
  Table2, 
  Hash, 
  ArrowRight,
  Play,
  Save,
  Code,
  Wand2,
  Sparkles,
  Settings,
  ChevronDown,
  ChevronUp,
  Eye,
  EyeOff,
  AlertTriangle,
  Copy,
  RefreshCw
} from "lucide-react";
import { API_ENDPOINTS } from "@/lib/config/api";
import { authFetch } from "@/lib/api/auth-fetch";
import { useWorkspaceRole } from "@/contexts/WorkspaceContext";
import { meetsRole } from "@/lib/workspace/roles";
import {
  fromApiTransforms,
  isSupportedOperation,
  SUPPORTED_OPERATIONS,
  toEngineTransform,
  type TransformOperation,
  type TransformRule,
} from "@/lib/pipeline/transformOps";
import {
  describeReplace,
  isPlanDirty,
  loadPipelineTransformPlan,
  planFingerprint,
  savePipelineTransformPlan,
  type PlanRule,
} from "@/lib/pipeline/transformPlan";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { toast } from "sonner";

// Types
interface PipelineOption {
  id: string;
  name: string;
}

interface TransformOperationConfig {
  name: string;
  description: string;
  type: "producer" | "consumer";
  icon: React.ReactNode;
  fields: TransformField[];
}

interface TransformField {
  name: string;
  label: string;
  type: "text" | "textarea" | "select" | "multiselect" | "number" | "boolean";
  options?: { value: string; label: string }[];
  placeholder?: string;
  required?: boolean;
}

// Transform operation configurations
const TRANSFORM_OPERATIONS: Record<TransformOperation, TransformOperationConfig> = {
  filter: {
    name: "Filter Rows",
    description: "Filter rows based on a condition",
    type: "producer",
    icon: <Filter className="w-4 h-4" />,
    fields: [
      { name: "condition", label: "Filter Condition", type: "text", placeholder: "amount > 100 AND status = 'active'", required: true },
    ],
  },
  select: {
    name: "Select Columns",
    description: "Include only specific columns",
    type: "producer",
    icon: <Table2 className="w-4 h-4" />,
    fields: [
      { name: "columns", label: "Columns (comma-separated)", type: "text", placeholder: "id, name, email", required: true },
    ],
  },
  exclude: {
    name: "Exclude Columns",
    description: "Remove specific columns",
    type: "producer",
    icon: <EyeOff className="w-4 h-4" />,
    fields: [
      { name: "columns", label: "Columns to Exclude", type: "text", placeholder: "password, secret_key" },
    ],
  },
  rename: {
    name: "Rename Columns",
    description: "Rename column names",
    type: "producer",
    icon: <Settings className="w-4 h-4" />,
    fields: [
      { name: "mappings", label: "Mappings (old:new, comma-separated)", type: "text", placeholder: "email:user_email, name:full_name" },
    ],
  },
  mask: {
    name: "Mask PII",
    description: "Apply PII masking to columns",
    type: "producer",
    icon: <Hash className="w-4 h-4" />,
    fields: [
      { name: "column", label: "Column", type: "text", required: true },
      { name: "mask_type", label: "Mask Type", type: "select", options: [
        { value: "hash", label: "Hash (SHA256)" },
        { value: "redact", label: "Redact" },
        { value: "partial_mask", label: "Partial Mask" },
        { value: "remove", label: "Remove" },
      ]},
      // Only the digests hashValue() implements (shared/go/transforms/engine.go).
      // Its default branch silently falls back to SHA-256, so SHA-512, BLAKE2 and
      // "Custom" used to claim an algorithm that was never applied.
      { name: "hash_function", label: "Hash Function", type: "select", options: [
        { value: "sha256", label: "SHA-256" },
        { value: "hmac_sha256", label: "HMAC-SHA256 (keyed)" },
        { value: "md5", label: "MD5 (legacy)" },
      ]},
    ],
  },
  hash: {
    name: "Hash Column",
    description: "Apply hash function to a column",
    type: "producer",
    icon: <Hash className="w-4 h-4" />,
    fields: [
      { name: "column", label: "Column", type: "text", required: true },
      // Only the digests hashValue() implements (shared/go/transforms/engine.go).
      // Its default branch silently falls back to SHA-256, so SHA-512, BLAKE2 and
      // "Custom" used to claim an algorithm that was never applied.
      { name: "hash_function", label: "Hash Function", type: "select", options: [
        { value: "sha256", label: "SHA-256" },
        { value: "hmac_sha256", label: "HMAC-SHA256 (keyed)" },
        { value: "md5", label: "MD5 (legacy)" },
      ]},
    ],
  },
  type_convert: {
    name: "Type Conversion",
    description: "Convert column data types",
    type: "producer",
    icon: <ArrowRight className="w-4 h-4" />,
    fields: [
      { name: "column", label: "Column", type: "text", required: true },
      // Only the targets convertValue implements (shared/go/transforms/engine.go).
      // "Date" used to sit here and had no case at all: validateConfig rejected
      // it outright ("unsupported target type"), so the option could be picked
      // and never saved. Its companion "Format (for dates)" field was read by
      // nothing in either engine and went with it.
      { name: "to_type", label: "Target Type", type: "select", options: [
        { value: "string", label: "String" },
        { value: "integer", label: "Integer" },
        { value: "float", label: "Float" },
        { value: "boolean", label: "Boolean" },
      ]},
    ],
  },
  null_handle: {
    name: "Handle Nulls",
    description: "Handle null values in columns",
    type: "producer",
    icon: <Settings className="w-4 h-4" />,
    fields: [
      { name: "column", label: "Column", type: "text", required: true },
      // Only the two strategies the engine actually has (shared/go/transforms/
      // engine.go applyNullHandle: "default"/"fill"/"fill_default", or "drop_row").
      // A third option, "Raise Error", used to sit here and was mapped to drop_row
      // on the way out — it silently dropped the row it promised to complain about.
      { name: "action", label: "Action", type: "select", options: [
        { value: "default", label: "Replace with Default" },
        { value: "skip", label: "Drop Row" },
      ]},
      // Required because strategy=default without it is rejected by the validator.
      { name: "default_value", label: "Default Value", type: "text", required: true },
    ],
  },
  truncate: {
    name: "Truncate",
    description: "Truncate string values",
    type: "producer",
    icon: <Settings className="w-4 h-4" />,
    fields: [
      { name: "column", label: "Column", type: "text", required: true },
      { name: "max_length", label: "Max Length", type: "number", placeholder: "255" },
    ],
  },
  aggregate: {
    name: "Aggregate",
    description: "Group and aggregate data",
    type: "consumer",
    icon: <Table2 className="w-4 h-4" />,
    fields: [
      { name: "group_by", label: "Group By (comma-separated)", type: "text", placeholder: "region, month" },
      { name: "aggregations", label: "Aggregations (column:func, comma-separated)", type: "text", placeholder: "amount:SUM, orders:COUNT" },
    ],
  },
  join: {
    name: "Join",
    description: "Join with another table",
    type: "consumer",
    icon: <ArrowRight className="w-4 h-4" />,
    fields: [
      { name: "lookup_table", label: "Lookup Table", type: "text", required: true },
      { name: "join_key", label: "Join Key", type: "text", required: true },
      { name: "lookup_key", label: "Lookup Key", type: "text" },
      { name: "join_type", label: "Join Type", type: "select", options: [
        { value: "left", label: "Left Join" },
        { value: "inner", label: "Inner Join" },
        { value: "right", label: "Right Join" },
      ]},
    ],
  },
  enrich: {
    name: "Enrich",
    description: "Enrich data from external API",
    type: "consumer",
    icon: <Sparkles className="w-4 h-4" />,
    fields: [
      { name: "api_endpoint", label: "API Endpoint", type: "text", required: true },
      { name: "input_mapping", label: "Input Mapping (col:field)", type: "text" },
      { name: "output_mapping", label: "Output Mapping (field:col)", type: "text" },
      { name: "cache_enabled", label: "Enable Cache", type: "boolean" },
    ],
  },
  deduplicate: {
    name: "Deduplicate",
    description: "Remove duplicate rows",
    type: "consumer",
    icon: <Copy className="w-4 h-4" />,
    fields: [
      { name: "columns", label: "Dedup Columns (comma-separated)", type: "text", placeholder: "Leave empty for all columns" },
    ],
  },
  sort: {
    name: "Sort",
    description: "Sort rows by columns",
    type: "consumer",
    icon: <ArrowRight className="w-4 h-4" />,
    fields: [
      { name: "columns", label: "Sort Columns (comma-separated)", type: "text", required: true },
      { name: "descending", label: "Descending", type: "boolean" },
    ],
  },
  limit: {
    name: "Limit",
    description: "Limit number of rows",
    type: "consumer",
    icon: <Filter className="w-4 h-4" />,
    fields: [
      { name: "limit", label: "Limit", type: "number", placeholder: "1000", required: true },
      { name: "offset", label: "Offset", type: "number", placeholder: "0" },
    ],
  },
  sql: {
    name: "SQL Query",
    description: "Custom SQL transformation",
    type: "consumer",
    icon: <Code className="w-4 h-4" />,
    fields: [
      { name: "query", label: "SQL Query", type: "textarea", placeholder: "SELECT * FROM data WHERE amount > 100", required: true },
    ],
  },
  python_udf: {
    name: "Python UDF",
    description: "Custom Python function",
    type: "consumer",
    icon: <Code className="w-4 h-4" />,
    fields: [
      { name: "name", label: "Function Name", type: "text", required: true },
      { name: "code", label: "Python Code", type: "textarea", placeholder: "def transform(row):\n    return row", required: true },
      { name: "input_cols", label: "Input Columns", type: "text" },
      { name: "output_cols", label: "Output Columns", type: "text" },
    ],
  },
};

// Producer vs consumer is WHERE a rule runs, not WHAT it can do. The batch executor
// reads the producer rows and the CDC sink reads the consumer rows, but both build the
// same engine (kafka-sink-worker/main.go: NewSimpleTransformEngine +
// NewTransformCoordinator). So every supported operation is offered on both sides —
// the old split, which reserved the "complex" operations for the consumer tab, left a
// CDC-only pipeline with zero transforms it could actually author.
const SUPPORTED_OPERATION_LIST = (Object.keys(TRANSFORM_OPERATIONS) as TransformOperation[])
  .filter(isSupportedOperation);

// Uses the set rather than isSupportedOperation, whose `op is TransformOperation`
// guard would narrow the negative branch to never.
const UNSUPPORTED_OPERATION_LIST = (Object.keys(TRANSFORM_OPERATIONS) as TransformOperation[])
  .filter((op) => !SUPPORTED_OPERATIONS.has(op));

export default function TransformBuilderPage() {
  const [transforms, setTransforms] = useState<TransformRule[]>([]);
  const [nlQuery, setNlQuery] = useState("");
  const [showAddDialog, setShowAddDialog] = useState(false);
  const [selectedOperation, setSelectedOperation] = useState<TransformOperation | null>(null);
  const [editingTransform, setEditingTransform] = useState<TransformRule | null>(null);
  const [previewData, setPreviewData] = useState<Record<string, any>[] | null>(null);
  const [previewSampleJson, setPreviewSampleJson] = useState(
    JSON.stringify(
      [
        { id: 1, email: "alice@example.com", name: "Alice", password: "secret", age: "29" },
        { id: 2, email: "bob@example.com", name: "Bob", password: "hunter2", age: null },
      ],
      null,
      2
    )
  );
  const [previewWarnings, setPreviewWarnings] = useState<string[]>([]);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  // WHICH PIPELINE THIS PLAN BELONGS TO.
  // The builder had no answer to that question, which is why "Save Plan" had
  // nowhere to POST and shipped with no onClick at all: the save endpoint is
  // per-pipeline (POST /api/v1/transforms/pipeline/:pipeline_id).
  const [pipelines, setPipelines] = useState<PipelineOption[]>([]);
  // Arriving from a pipeline's Transforms tab, which links here as
  // /transforms?pipeline=<id>. Held until the pipeline list arrives, then applied
  // by the effect below — and only if the id is really in that list, so a
  // hand-edited URL cannot aim the builder at a pipeline in another workspace.
  const [wantedPipeline] = useState<string | null>(() =>
    typeof window === "undefined"
      ? null
      : new URLSearchParams(window.location.search).get("pipeline")
  );
  // A ref, not state: this is a one-shot latch, and flipping state inside the
  // effect below would just cause another render to do nothing with.
  const appliedWantedPipeline = useRef(false);
  const [pipelineId, setPipelineId] = useState("");
  const [existingRules, setExistingRules] = useState<PlanRule[]>([]);
  const [planError, setPlanError] = useState<string | null>(null);
  const [planLoading, setPlanLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);
  // The plan as the server last confirmed it. Everything built since is unsaved,
  // and the builder used to have no way to know that -- so it discarded work on
  // a reload, a back button, or a change of pipeline without ever saying so.
  const [savedFingerprint, setSavedFingerprint] = useState(() => planFingerprint([]));
  // A pipeline the operator picked while the builder was dirty, held until they
  // answer whether the unsaved plan may be thrown away.
  const [pendingPipelineId, setPendingPipelineId] = useState<string | null>(null);

  const { role } = useWorkspaceRole();
  // Mirrors requirePipelineWorkspaceRole(..., security.WSMember) on the save
  // handler. roleRank returns 0 for an unknown role, so this is false while the
  // workspace context is still loading rather than briefly true.
  const canSave = meetsRole(role, "member");

  const producerTransforms = transforms.filter(t => t.type === "producer");
  const consumerTransforms = transforms.filter(t => t.type === "consumer");

  // `side` comes from the tab the operator was on, not from the operation, because the
  // same operation is legal on both paths — see SUPPORTED_OPERATION_LIST above.
  const addTransform = (operation: TransformOperation, side: "producer" | "consumer") => {
    if (!isSupportedOperation(operation)) return;
    const config = TRANSFORM_OPERATIONS[operation];
    const newTransform: TransformRule = {
      id: crypto.randomUUID(),
      order: transforms.length,
      type: side,
      operation,
      enabled: true,
      config: {},
      description: config.description,
    };
    setTransforms([...transforms, newTransform]);
    setEditingTransform(newTransform);
    setShowAddDialog(false);
  };

  const updateTransform = (id: string, updates: Partial<TransformRule>) => {
    setTransforms(transforms.map(t => 
      t.id === id ? { ...t, ...updates } : t
    ));
  };

  const removeTransform = (id: string) => {
    setTransforms(transforms.filter(t => t.id !== id));
  };

  const moveTransform = (id: string, direction: "up" | "down") => {
    const index = transforms.findIndex(t => t.id === id);
    if (index === -1) return;

    const newIndex = direction === "up" ? index - 1 : index + 1;
    if (newIndex < 0 || newIndex >= transforms.length) return;

    const newTransforms = [...transforms];
    [newTransforms[index], newTransforms[newIndex]] = [newTransforms[newIndex], newTransforms[index]];
    
    // Update order numbers
    newTransforms.forEach((t, i) => t.order = i);
    setTransforms(newTransforms);
  };

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const res = await authFetch(`${API_ENDPOINTS.PIPELINES.LIST}?limit=200&offset=0`, {
          cache: "no-store",
        });
        if (cancelled || !res.ok) return;
        const data = await res.json();
        if (cancelled) return;
        const list = Array.isArray(data) ? data : data.pipelines || [];
        const options: PipelineOption[] = list.map((p: Record<string, unknown>) => ({
          id: String(p.id ?? ""),
          name: String(p.name ?? p.id ?? ""),
        })).filter((p: PipelineOption) => p.id);
        setPipelines(options);
      } catch {
        // The selector stays empty and Save Plan stays disabled. Building and
        // previewing a plan still works, which is what the page did before.
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const isRenderable = (operation: string) =>
    Object.prototype.hasOwnProperty.call(TRANSFORM_OPERATIONS, operation);

  // Rules the builder has no card for — most importantly `mask_pii`, which the
  // natural-language pipeline setup materializes and which is NOT one of this
  // page's operations. They are carried through the save untouched instead of
  // being dropped, because the save is a whole-plan replace: anything left out
  // of the request is deleted, and deleting a mask makes the next run copy
  // those columns to the destination in the clear.
  const carryOver = existingRules.filter((r) => !isRenderable(r.operation));
  const nextRules: PlanRule[] = [...transforms, ...carryOver];

  // Choosing a pipeline REPLACES the builder contents with that pipeline's
  // current plan. Editing has to start from what is really stored, or the first
  // save silently deletes rows the operator never saw.
  const selectPipeline = async (id: string) => {
    setPipelineId(id);
    setPlanError(null);
    setPlanLoading(true);
    try {
      const out = await loadPipelineTransformPlan(id);
      if (!out.ok) {
        setPlanError(out.error);
        setExistingRules([]);
        return;
      }
      setExistingRules(out.data);
      const loaded = 
        out.data
          .filter((r) => isRenderable(r.operation))
          .map((r, i) => ({
            id: r.id,
            order: i,
            type: r.type,
            operation: r.operation as TransformOperation,
            enabled: r.enabled,
            config: r.config as Record<string, any>,
            description: r.description,
          }));
      setTransforms(loaded);
      // What is on screen now IS what is stored, so this is the baseline every
      // later edit is measured against.
      setSavedFingerprint(planFingerprint(loaded));
      setEditingTransform(null);
    } finally {
      setPlanLoading(false);
    }
  };

  // Picking a pipeline REPLACES the builder contents. That is correct -- editing
  // has to start from what is really stored -- but doing it over unsaved work
  // without asking is how a freshly generated plan disappeared.
  const requestSelectPipeline = (id: string) => {
    if (id === pipelineId) return;
    if (isPlanDirty(savedFingerprint, transforms)) {
      setPendingPipelineId(id);
      return;
    }
    void selectPipeline(id);
  };

  useEffect(() => {
    if (appliedWantedPipeline.current || !wantedPipeline) return;
    if (!pipelines.some((p) => p.id === wantedPipeline)) return;
    appliedWantedPipeline.current = true;
    // selectPipeline sets state, which is the point: the builder has to be loaded
    // with that pipeline's stored plan on arrival. The latch keeps it to one run.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    void selectPipeline(wantedPipeline);
    // selectPipeline is re-created on every render, so naming it as a dependency
    // would re-fire this on every render; the latch above makes that moot either
    // way, and the operator's own selection must not be overwritten afterwards.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [wantedPipeline, pipelines]);

  const dirty = isPlanDirty(savedFingerprint, transforms);

  // The browser's own guard. It is the only thing that can stop a reload or a
  // closed tab, and the builder had none: everything since the last save went
  // without a prompt. The message is the browser's -- Chrome and Firefox ignore
  // a custom one -- so the visible "Unsaved changes" marker below carries the
  // detail.
  useEffect(() => {
    if (!dirty) return;
    const warn = (e: BeforeUnloadEvent) => {
      e.preventDefault();
      e.returnValue = "";
    };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [dirty]);

  const savePlan = async () => {
    if (!pipelineId) return;
    setSaving(true);
    try {
      const out = await savePipelineTransformPlan(pipelineId, nextRules);
      if (!out.ok) {
        toast.error(out.error);
        return;
      }
      setConfirmOpen(false);
      toast.success(
        `Saved ${out.data.count} ${out.data.count === 1 ? "transform" : "transforms"} to this pipeline.`
      );
      // Re-read: the server mints ids for new rows and renumbers transform_order,
      // so the builder would otherwise be one save behind the stored plan.
      await selectPipeline(pipelineId);
    } finally {
      setSaving(false);
    }
  };

  const parseNaturalLanguage = async () => {
    if (!nlQuery.trim()) return;
    
    setLoading(true);
    try {
      // authFetch, not fetch: /api/v1 is behind AuthRequiredMiddleware and
      // CSRFMiddleware, so a bare POST here was a 401/403 every time — and the
      // `if (res.ok)` below turned that into a silent no-op.
      const res = await authFetch(`/api/v1/transforms/parse`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ natural_language: nlQuery }),
      });

      if (res.ok) {
        const data = await res.json();
        const parsed = fromApiTransforms(data?.transforms);
        if (parsed.length > 0) {
          // Append, don't replace: the old code overwrote whatever the operator had
          // already built, and Generate is a starting point, not a whole plan.
          setTransforms((prev) => [
            ...prev,
            ...parsed.map((r, i) => ({
              ...r,
              order: prev.length + i,
              description: TRANSFORM_OPERATIONS[r.operation]?.description,
            })),
          ]);
          setNlQuery("");
        } else {
          toast.error(
            "Nothing recognizable in that sentence. Try naming a column and one of: filter, mask, hash, rename, select, exclude."
          );
        }
      } else {
        const body = await res.json().catch(() => null);
        toast.error(body?.error || body?.message || `Could not parse that (HTTP ${res.status}).`);
      }
    } catch (error) {
      toast.error("The API is unreachable.");
      console.error("Failed to parse NL query:", error);
    } finally {
      setLoading(false);
    }
  };

  const previewTransformations = async () => {
    setLoading(true);
    setPreviewError(null);
    setPreviewWarnings([]);
    try {
      let sampleRows: unknown = [];
      try {
        sampleRows = JSON.parse(previewSampleJson || "[]");
      } catch {
        throw new Error("Preview sample data must be valid JSON (an array of row objects).");
      }

      if (!Array.isArray(sampleRows)) {
        throw new Error("Preview sample data must be a JSON array of row objects.");
      }

      const engineTransforms = transforms
        .map(toEngineTransform)
        .filter((t0): t0 is Record<string, any> => Boolean(t0));

      // Same gate as /transforms/parse above — this needs the auth and CSRF
      // headers authFetch attaches.
      const res = await authFetch(`/api/v1/transforms/preview`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ transforms: engineTransforms, sample_data: sampleRows }),
      });

      if (res.ok) {
        const data = await res.json();
        setPreviewData(data.preview || []);
        setPreviewWarnings(Array.isArray(data.warnings) ? data.warnings : []);
      } else {
        const body = await res.json().catch(() => null);
        const msg = body?.error || body?.message || "Failed to preview transformations.";
        throw new Error(msg);
      }
    } catch (error) {
      const msg = error instanceof Error ? error.message : "Failed to preview transformations.";
      setPreviewError(msg);
      console.error("Failed to preview transformations:", error);
    } finally {
      setLoading(false);
    }
  };

  const renderTransformCard = (transform: TransformRule, index: number) => {
    const config = TRANSFORM_OPERATIONS[transform.operation];
    const isExpanded = editingTransform?.id === transform.id;

    return (
      <Card 
        key={transform.id} 
        className={`mb-3 ${transform.enabled ? "" : "opacity-50"} ${isExpanded ? "ring-2 ring-blue-500" : ""}`}
      >
        <CardContent className="pt-4">
          <div className="flex items-center gap-3" data-testid="transform-card-row">
            <div className="cursor-move text-muted-foreground shrink-0">
              <GripVertical className="w-5 h-5" />
            </div>
            
            {/* min-w-0 is load-bearing, not tidying. A flex child defaults to
                min-width:auto, so this block could not shrink below the
                intrinsic width of the badge + operation name, and the button
                group to its right was pushed clean out of the card: measured on
                app.rsync.ai, "Rename Columns" overflowed by 125px, which put
                its settings and delete buttons past the card edge where no
                pointer could reach them. The name truncates; the controls stay. */}
            <div className="flex-1 min-w-0" data-testid="transform-card-title">
              {/* The name has its line to itself. With the badges beside it, every
                  sibling was shrink-0, so the name was the only thing that could
                  give way and on a narrow card it reached 0px — worst on the card
                  whose badge says to remove it. The badges wrap on the line below;
                  a floor on the name instead would push the controls off-card
                  again (#1166). The full name is on hover too. */}
              <div className="flex items-center gap-2 min-w-0 font-medium" data-testid="transform-card-name">
                <span className="shrink-0">{config.icon}</span>
                <span className="truncate" title={config.name}>{config.name}</span>
              </div>
              <div className="mt-1 flex flex-wrap items-center gap-2" data-testid="transform-card-badges">
                <Badge variant="outline" className={transform.type === "producer" ? "border-blue-500 text-blue-500" : "border-green-500 text-green-500"}>
                  {transform.type}
                </Badge>
                {/* A plan saved before the dialog stopped offering these still
                    loads here, and the save now refuses the whole plan while one
                    is present. Say which card is the problem instead of leaving
                    the operator to guess at a 400. */}
                {!isSupportedOperation(transform.operation) && (
                  <Badge variant="destructive" className="text-[10px]">
                    No engine can run this — remove it to save
                  </Badge>
                )}
              </div>
              {!isExpanded && Object.keys(transform.config).length > 0 && (
                <p className="text-sm text-muted-foreground mt-1 truncate">
                  {JSON.stringify(transform.config)}
                </p>
              )}
            </div>

            {/* Every control here is icon-only, so without a label it has no
                accessible name at all: a screen reader announced five bare
                "button"s per card, and with several cards open nothing said
                which rule any of them belonged to. The name goes IN the label
                rather than relying on the adjacent title cell, because that
                cell truncates. `title` carries the same string so the meaning
                is available on hover too, not only to assistive tech. */}
            <div className="flex items-center gap-2 shrink-0" data-testid="transform-card-controls">
              <Switch
                checked={transform.enabled}
                onCheckedChange={(checked) => updateTransform(transform.id, { enabled: checked })}
                aria-label={`${transform.enabled ? "Disable" : "Enable"} ${config.name}`}
                title={`${transform.enabled ? "Disable" : "Enable"} ${config.name}`}
              />
              <Button
                variant="ghost"
                size="icon"
                onClick={() => moveTransform(transform.id, "up")}
                disabled={index === 0}
                aria-label={`Move ${config.name} earlier`}
                title={`Move ${config.name} earlier`}
              >
                <ChevronUp className="w-4 h-4" />
              </Button>
              <Button
                variant="ghost"
                size="icon"
                onClick={() => moveTransform(transform.id, "down")}
                disabled={index === transforms.filter(t => t.type === transform.type).length - 1}
                aria-label={`Move ${config.name} later`}
                title={`Move ${config.name} later`}
              >
                <ChevronDown className="w-4 h-4" />
              </Button>
              <Button
                variant="ghost"
                size="icon"
                onClick={() => setEditingTransform(isExpanded ? null : transform)}
                aria-expanded={isExpanded}
                aria-label={isExpanded ? `Hide ${config.name} settings` : `Edit ${config.name} settings`}
                title={isExpanded ? `Hide ${config.name} settings` : `Edit ${config.name} settings`}
              >
                <Settings className="w-4 h-4" />
              </Button>
              <Button
                variant="ghost"
                size="icon"
                className="text-red-500 hover:text-red-600"
                onClick={() => removeTransform(transform.id)}
                aria-label={`Remove ${config.name}`}
                title={`Remove ${config.name}`}
              >
                <Trash2 className="w-4 h-4" />
              </Button>
            </div>
          </div>

          {isExpanded && (
            <div className="mt-4 space-y-4 pt-4 border-t">
              {config.fields.map((field) => (
                <div key={field.name} className="space-y-2">
                  <Label>{field.label}</Label>
                  {field.type === "textarea" ? (
                    <Textarea
                      value={transform.config[field.name] || ""}
                      onChange={(e) => updateTransform(transform.id, {
                        config: { ...transform.config, [field.name]: e.target.value }
                      })}
                      placeholder={field.placeholder}
                      className="font-mono"
                      rows={4}
                    />
                  ) : field.type === "select" ? (
                    <Select
                      value={transform.config[field.name] || ""}
                      onValueChange={(value) => updateTransform(transform.id, {
                        config: { ...transform.config, [field.name]: value }
                      })}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder={`Select ${field.label}`} />
                      </SelectTrigger>
                      <SelectContent>
                        {field.options?.map((opt) => (
                          <SelectItem key={opt.value} value={opt.value}>
                            {opt.label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  ) : field.type === "boolean" ? (
                    <Switch
                      checked={transform.config[field.name] || false}
                      onCheckedChange={(checked) => updateTransform(transform.id, {
                        config: { ...transform.config, [field.name]: checked }
                      })}
                    />
                  ) : field.type === "number" ? (
                    <Input
                      type="number"
                      value={transform.config[field.name] || ""}
                      onChange={(e) => updateTransform(transform.id, {
                        config: { ...transform.config, [field.name]: parseInt(e.target.value) || 0 }
                      })}
                      placeholder={field.placeholder}
                    />
                  ) : (
                    <Input
                      value={transform.config[field.name] || ""}
                      onChange={(e) => updateTransform(transform.id, {
                        config: { ...transform.config, [field.name]: e.target.value }
                      })}
                      placeholder={field.placeholder}
                    />
                  )}
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    );
  };

  return (
    <div className="container mx-auto p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold flex items-center gap-2">
            <Wand2 className="w-8 h-8 text-purple-500" />
            Transform Builder
          </h1>
          <p className="text-muted-foreground mt-1">
            Define data transformations using natural language or visual builder
          </p>
        </div>
        <div className="flex items-center gap-2">
          {dirty && (
            <span
              className="text-xs text-amber-600 dark:text-amber-500 flex items-center gap-1"
              role="status"
            >
              <AlertTriangle className="w-3.5 h-3.5" />
              Unsaved changes
            </span>
          )}
          <Button variant="outline" onClick={previewTransformations} disabled={loading}>
            <Eye className="w-4 h-4 mr-2" />
            Preview
          </Button>
          <Button
            disabled={loading || saving || planLoading || !pipelineId || !canSave}
            onClick={() => setConfirmOpen(true)}
            title={
              !canSave
                ? "Only workspace members and admins can change a pipeline's transforms."
                : !pipelineId
                  ? "Pick a pipeline first — transforms are saved to one pipeline."
                  : undefined
            }
          >
            {saving ? <RefreshCw className="w-4 h-4 mr-2 animate-spin" /> : <Save className="w-4 h-4 mr-2" />}
            Save Plan
          </Button>
        </div>
      </div>

      {/* Pipeline selector — a plan is saved to one pipeline, so this is what
          turns the builder from a scratchpad into something that persists. */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Pipeline</CardTitle>
          <CardDescription>
            Transforms are stored per pipeline. Picking one loads its current plan into the builder — and saving
            replaces that plan with whatever is on screen.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <Select value={pipelineId} onValueChange={requestSelectPipeline}>
            <SelectTrigger className="max-w-md">
              <SelectValue placeholder={pipelines.length === 0 ? "No pipelines available" : "Select a pipeline…"} />
            </SelectTrigger>
            <SelectContent>
              {pipelines.map((p) => (
                <SelectItem key={p.id} value={p.id}>
                  {p.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          {planLoading && (
            <p className="text-sm text-muted-foreground flex items-center gap-2">
              <RefreshCw className="w-4 h-4 animate-spin" />
              Loading this pipeline&apos;s transforms…
            </p>
          )}

          {planError && (
            <Alert variant="destructive">
              <AlertTriangle className="w-4 h-4" />
              <AlertDescription>{planError}</AlertDescription>
            </Alert>
          )}

          {!planLoading && !planError && pipelineId && (
            <p className="text-sm text-muted-foreground">
              {existingRules.length === 0
                ? "This pipeline has no transforms yet."
                : `${existingRules.length} ${existingRules.length === 1 ? "rule is" : "rules are"} stored on this pipeline.`}
            </p>
          )}

          {carryOver.length > 0 && (
            <Alert>
              <AlertTriangle className="w-4 h-4" />
              <AlertDescription>
                {carryOver.length} {carryOver.length === 1 ? "rule uses an operation" : "rules use operations"} this
                builder cannot display ({[...new Set(carryOver.map((r) => r.operation))].join(", ")}) — typically
                masking rules created by the pipeline&apos;s natural-language setup. They are not editable here and are
                kept unchanged when you save.
              </AlertDescription>
            </Alert>
          )}

          {!canSave && (
            <p className="text-sm text-muted-foreground">
              You can build and preview a plan, but only workspace members and admins can save one to a pipeline.
            </p>
          )}
        </CardContent>
      </Card>

      {/* Natural Language Input */}
      <Card className="bg-gradient-to-br from-purple-500/10 to-blue-500/10 border-purple-500/20">
        <CardContent className="pt-6">
          <div className="flex gap-4">
            <div className="flex-1">
              <Label className="flex items-center gap-2 mb-2">
                <Sparkles className="w-4 h-4 text-purple-500" />
                Describe your transformations in natural language
              </Label>
              <Textarea
                value={nlQuery}
                onChange={(e) => setNlQuery(e.target.value)}
                placeholder="e.g., Filter orders where amount > 100, mask all email addresses"
                className="min-h-[100px]"
              />
            </div>
            <div className="flex flex-col justify-end">
              <Button 
                onClick={parseNaturalLanguage} 
                disabled={loading || !nlQuery.trim()}
                className="bg-purple-600 hover:bg-purple-700"
              >
                {loading ? <RefreshCw className="w-4 h-4 mr-2 animate-spin" /> : <Wand2 className="w-4 h-4 mr-2" />}
                Generate
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Preview sample rows (used by backend preview endpoint) */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between gap-4">
            <div>
              <CardTitle className="text-base">Preview sample data</CardTitle>
              <CardDescription>Paste a JSON array of row objects to preview transforms.</CardDescription>
            </div>
            <Button variant="outline" onClick={previewTransformations} disabled={loading}>
              {loading ? <RefreshCw className="w-4 h-4 mr-2 animate-spin" /> : <Eye className="w-4 h-4 mr-2" />}
              Preview
            </Button>
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
          {previewError && (
            <Alert>
              <AlertDescription>{previewError}</AlertDescription>
            </Alert>
          )}
          {previewWarnings.length > 0 && (
            <Alert>
              <AlertDescription>
                <div className="font-medium mb-1">Preview warnings</div>
                <ul className="list-disc pl-5 space-y-1">
                  {previewWarnings.map((w, i) => (
                    <li key={i}>{w}</li>
                  ))}
                </ul>
              </AlertDescription>
            </Alert>
          )}
          <Textarea
            value={previewSampleJson}
            onChange={(e) => setPreviewSampleJson(e.target.value)}
            className="min-h-[140px] font-mono text-xs"
          />
        </CardContent>
      </Card>

      {/* Transform Pipeline */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Producer Transforms */}
        <Card>
          <CardHeader>
            <div className="flex justify-between items-center">
              <div>
                <CardTitle className="flex items-center gap-2">
                  <Badge className="bg-blue-500">Producer</Badge>
                  Pre-Kafka Transforms
                </CardTitle>
                <CardDescription>Run in the batch executor, on each scheduled run</CardDescription>
              </div>
              <Button size="sm" variant="outline" onClick={() => { setShowAddDialog(true); setSelectedOperation(null); }}>
                <Plus className="w-4 h-4 mr-2" />
                Add
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            {producerTransforms.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground border-2 border-dashed rounded-lg">
                <Filter className="w-10 h-10 mx-auto mb-2 opacity-50" />
                <p>No batch transforms</p>
                <p className="text-sm">Add filters, masks, or column selections</p>
              </div>
            ) : (
              producerTransforms.map((t, i) => renderTransformCard(t, i))
            )}
          </CardContent>
        </Card>

        {/* Consumer Transforms */}
        <Card>
          <CardHeader>
            <div className="flex justify-between items-center">
              <div>
                <CardTitle className="flex items-center gap-2">
                  <Badge className="bg-green-500">Consumer</Badge>
                  Post-Kafka Transforms
                </CardTitle>
                <CardDescription>Run in the CDC sink, on every change event</CardDescription>
              </div>
              <Button size="sm" variant="outline" onClick={() => { setShowAddDialog(true); setSelectedOperation(null); }}>
                <Plus className="w-4 h-4 mr-2" />
                Add
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            {consumerTransforms.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground border-2 border-dashed rounded-lg">
                <Table2 className="w-10 h-10 mx-auto mb-2 opacity-50" />
                <p>No CDC transforms</p>
                <p className="text-sm">Add filters, masks, or column selections</p>
              </div>
            ) : (
              consumerTransforms.map((t, i) => renderTransformCard(t, i))
            )}
          </CardContent>
        </Card>
      </div>

      {/* Preview Panel */}
      {previewData && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Eye className="w-5 h-5" />
              Transform Preview
            </CardTitle>
            <CardDescription>Sample of transformed data</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b">
                    {previewData.length > 0 && Object.keys(previewData[0]).map(key => (
                      <th key={key} className="text-left p-2 font-medium">{key}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {previewData.slice(0, 5).map((row, i) => (
                    <tr key={i} className="border-b">
                      {Object.values(row).map((val, j) => (
                        <td key={j} className="p-2 font-mono text-xs">{String(val)}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Add Transform Dialog */}
      <Dialog open={showAddDialog} onOpenChange={setShowAddDialog}>
        <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Add Transformation</DialogTitle>
            <DialogDescription>Choose a transformation to add to your pipeline</DialogDescription>
          </DialogHeader>
          
          <Tabs defaultValue="producer" className="mt-4">
            <TabsList className="grid grid-cols-2">
              <TabsTrigger value="producer">Batch (Producer)</TabsTrigger>
              <TabsTrigger value="consumer">CDC (Consumer)</TabsTrigger>
            </TabsList>

            {/* Both tabs offer the SAME operations. The tab chooses where the rule
                runs — the batch executor reads the producer rows, the CDC sink reads
                the consumer rows — and both build the same engine, so an operation
                legal on one path is legal on the other. */}
            {(["producer", "consumer"] as const).map((side) => (
              <TabsContent key={side} value={side} className="mt-4">
                <p className="text-xs text-muted-foreground mb-3">
                  {side === "producer"
                    ? "Runs in the batch executor, on each scheduled run."
                    : "Runs in the CDC sink, on every change event."}
                </p>
                <div className="grid grid-cols-2 gap-3">
                  {SUPPORTED_OPERATION_LIST.map((op) => {
                    const config = TRANSFORM_OPERATIONS[op];
                    return (
                      <Button
                        key={op}
                        variant="outline"
                        className="h-auto p-4 flex flex-col items-start gap-2"
                        onClick={() => addTransform(op, side)}
                      >
                        <div className="flex items-center gap-2">
                          {config.icon}
                          <span className="font-medium">{config.name}</span>
                        </div>
                        <p className="text-xs text-muted-foreground text-left">{config.description}</p>
                      </Button>
                    );
                  })}
                </div>

                {/* Still listed, because they are a real roadmap and hiding them
                    outright would look like a regression to anyone who used them —
                    but not clickable, because no engine can run them. They are the
                    Tier-2 (DuckDB) operations; that engine is a stub today. */}
                {UNSUPPORTED_OPERATION_LIST.length > 0 && (
                  <div className="mt-6">
                    <p className="text-xs font-medium text-muted-foreground mb-2">
                      Not available yet — no execution engine
                    </p>
                    <div className="grid grid-cols-2 gap-3">
                      {UNSUPPORTED_OPERATION_LIST.map((op) => {
                        const config = TRANSFORM_OPERATIONS[op];
                        return (
                          <div
                            key={op}
                            className="h-auto p-4 flex flex-col items-start gap-2 rounded-md border border-dashed opacity-60"
                            aria-disabled="true"
                          >
                            <div className="flex items-center gap-2">
                              {config.icon}
                              <span className="font-medium text-sm">{config.name}</span>
                              <Badge variant="secondary" className="text-[10px]">Coming soon</Badge>
                            </div>
                            <p className="text-xs text-muted-foreground text-left">{config.description}</p>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}
              </TabsContent>
            ))}
          </Tabs>
        </DialogContent>
      </Dialog>

      {/* Saving is a REPLACE, not an append: the handler deletes every transform
          on the pipeline inside the transaction and re-inserts the request body.
          The dialog says so in counts, and names any masking rule that is about
          to disappear. */}
      {/* Switching pipelines throws the builder contents away. Over unsaved work
          that is a deletion, so it is asked rather than done. */}
      <AlertDialog
        open={pendingPipelineId !== null}
        onOpenChange={(open) => {
          if (!open) setPendingPipelineId(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Discard the unsaved plan?</AlertDialogTitle>
            <AlertDialogDescription>
              {transforms.length} {transforms.length === 1 ? "rule is" : "rules are"} on screen that
              {pipelineId ? " differ from what is stored on this pipeline" : " have never been saved"}. Loading another
              pipeline replaces them, and they cannot be recovered.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Keep editing</AlertDialogCancel>
            <AlertDialogAction
              onClick={(e) => {
                e.preventDefault();
                const next = pendingPipelineId;
                setPendingPipelineId(null);
                if (next) void selectPipeline(next);
              }}
            >
              Discard and load
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Replace this pipeline&apos;s transforms?</AlertDialogTitle>
            <AlertDialogDescription>{describeReplace(existingRules, nextRules)}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={saving}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={(e) => {
                e.preventDefault();
                void savePlan();
              }}
              disabled={saving}
            >
              {saving ? "Saving…" : "Replace and save"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
