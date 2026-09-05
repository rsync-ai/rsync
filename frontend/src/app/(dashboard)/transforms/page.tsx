"use client";

import React, { useState, useEffect } from "react";
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
  Copy,
  RefreshCw
} from "lucide-react";
import { API_ENDPOINTS } from "@/lib/config/api";

// Types
interface TransformRule {
  id: string;
  order: number;
  type: "producer" | "consumer";
  operation: TransformOperation;
  enabled: boolean;
  config: Record<string, any>;
  description?: string;
}

type TransformOperation = 
  | "filter" | "select" | "exclude" | "rename" | "mask" | "hash" 
  | "type_convert" | "null_handle" | "truncate"
  | "aggregate" | "join" | "enrich" | "deduplicate" | "sort" | "limit" | "sql" | "python_udf";

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
      { name: "hash_function", label: "Hash Function", type: "select", options: [
        { value: "sha256", label: "SHA-256" },
        { value: "sha512", label: "SHA-512" },
        { value: "md5", label: "MD5 (Legacy)" },
        { value: "blake2", label: "BLAKE2" },
        { value: "hmac_sha256", label: "HMAC-SHA256" },
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
      { name: "hash_function", label: "Hash Function", type: "select", options: [
        { value: "sha256", label: "SHA-256" },
        { value: "sha512", label: "SHA-512" },
        { value: "custom", label: "Custom" },
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
      { name: "to_type", label: "Target Type", type: "select", options: [
        { value: "string", label: "String" },
        { value: "integer", label: "Integer" },
        { value: "float", label: "Float" },
        { value: "boolean", label: "Boolean" },
        { value: "date", label: "Date" },
      ]},
      { name: "format", label: "Format (for dates)", type: "text", placeholder: "YYYY-MM-DD" },
    ],
  },
  null_handle: {
    name: "Handle Nulls",
    description: "Handle null values in columns",
    type: "producer",
    icon: <Settings className="w-4 h-4" />,
    fields: [
      { name: "column", label: "Column", type: "text", required: true },
      { name: "action", label: "Action", type: "select", options: [
        { value: "default", label: "Replace with Default" },
        { value: "skip", label: "Skip Row" },
        { value: "error", label: "Raise Error" },
      ]},
      { name: "default_value", label: "Default Value", type: "text" },
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

  const producerTransforms = transforms.filter(t => t.type === "producer");
  const consumerTransforms = transforms.filter(t => t.type === "consumer");

  const addTransform = (operation: TransformOperation) => {
    const config = TRANSFORM_OPERATIONS[operation];
    const newTransform: TransformRule = {
      id: crypto.randomUUID(),
      order: transforms.length,
      type: config.type,
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

  const parseNaturalLanguage = async () => {
    if (!nlQuery.trim()) return;
    
    setLoading(true);
    try {
      const res = await fetch(`${API_ENDPOINTS.API_GATEWAY_URL}/api/v1/transforms/parse`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ natural_language: nlQuery }),
      });

      if (res.ok) {
        const data = await res.json();
        if (data.transforms) {
          setTransforms(data.transforms);
        }
      }
    } catch (error) {
      console.error("Failed to parse NL query:", error);
    } finally {
      setLoading(false);
    }
  };

  const toEngineTransform = (t: TransformRule): Record<string, any> | null => {
    if (!t.enabled) return null;

    const cfg: Record<string, any> = { ...(t.config || {}) };
    const op = t.operation;

    // Map UI operations → engine transform types.
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

      const res = await fetch(`${API_ENDPOINTS.API_GATEWAY_URL}/api/v1/transforms/preview`, {
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
          <div className="flex items-center gap-3">
            <div className="cursor-move text-muted-foreground">
              <GripVertical className="w-5 h-5" />
            </div>
            
            <div className="flex-1">
              <div className="flex items-center gap-2">
                <Badge variant="outline" className={transform.type === "producer" ? "border-blue-500 text-blue-500" : "border-green-500 text-green-500"}>
                  {transform.type}
                </Badge>
                <span className="font-medium flex items-center gap-2">
                  {config.icon}
                  {config.name}
                </span>
              </div>
              {!isExpanded && Object.keys(transform.config).length > 0 && (
                <p className="text-sm text-muted-foreground mt-1 truncate">
                  {JSON.stringify(transform.config).slice(0, 50)}...
                </p>
              )}
            </div>

            <div className="flex items-center gap-2">
              <Switch
                checked={transform.enabled}
                onCheckedChange={(checked) => updateTransform(transform.id, { enabled: checked })}
              />
              <Button
                variant="ghost"
                size="icon"
                onClick={() => moveTransform(transform.id, "up")}
                disabled={index === 0}
              >
                <ChevronUp className="w-4 h-4" />
              </Button>
              <Button
                variant="ghost"
                size="icon"
                onClick={() => moveTransform(transform.id, "down")}
                disabled={index === transforms.filter(t => t.type === transform.type).length - 1}
              >
                <ChevronDown className="w-4 h-4" />
              </Button>
              <Button
                variant="ghost"
                size="icon"
                onClick={() => setEditingTransform(isExpanded ? null : transform)}
              >
                <Settings className="w-4 h-4" />
              </Button>
              <Button
                variant="ghost"
                size="icon"
                className="text-red-500 hover:text-red-600"
                onClick={() => removeTransform(transform.id)}
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
        <div className="flex gap-2">
          <Button variant="outline" onClick={previewTransformations} disabled={loading}>
            <Eye className="w-4 h-4 mr-2" />
            Preview
          </Button>
          <Button disabled={loading}>
            <Save className="w-4 h-4 mr-2" />
            Save Plan
          </Button>
        </div>
      </div>

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
                placeholder="e.g., Filter orders where amount > 100, mask all email addresses, aggregate sales by region and month"
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
                <CardDescription>Lightweight transforms applied before Kafka</CardDescription>
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
                <p>No producer transforms</p>
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
                <CardDescription>Complex transforms applied after Kafka</CardDescription>
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
                <p>No consumer transforms</p>
                <p className="text-sm">Add aggregations, joins, or enrichments</p>
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
              <TabsTrigger value="producer">Producer (Pre-Kafka)</TabsTrigger>
              <TabsTrigger value="consumer">Consumer (Post-Kafka)</TabsTrigger>
            </TabsList>
            
            <TabsContent value="producer" className="mt-4">
              <div className="grid grid-cols-2 gap-3">
                {Object.entries(TRANSFORM_OPERATIONS)
                  .filter(([_, config]) => config.type === "producer")
                  .map(([op, config]) => (
                    <Button
                      key={op}
                      variant="outline"
                      className="h-auto p-4 flex flex-col items-start gap-2"
                      onClick={() => addTransform(op as TransformOperation)}
                    >
                      <div className="flex items-center gap-2">
                        {config.icon}
                        <span className="font-medium">{config.name}</span>
                      </div>
                      <p className="text-xs text-muted-foreground text-left">{config.description}</p>
                    </Button>
                  ))}
              </div>
            </TabsContent>
            
            <TabsContent value="consumer" className="mt-4">
              <div className="grid grid-cols-2 gap-3">
                {Object.entries(TRANSFORM_OPERATIONS)
                  .filter(([_, config]) => config.type === "consumer")
                  .map(([op, config]) => (
                    <Button
                      key={op}
                      variant="outline"
                      className="h-auto p-4 flex flex-col items-start gap-2"
                      onClick={() => addTransform(op as TransformOperation)}
                    >
                      <div className="flex items-center gap-2">
                        {config.icon}
                        <span className="font-medium">{config.name}</span>
                      </div>
                      <p className="text-xs text-muted-foreground text-left">{config.description}</p>
                    </Button>
                  ))}
              </div>
            </TabsContent>
          </Tabs>
        </DialogContent>
      </Dialog>
    </div>
  );
}

