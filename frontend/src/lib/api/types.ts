/**
 * API Response Types
 */

// Pipeline types
export interface Pipeline {
  id: string
  name: string
  description?: string
  nodes: PipelineNode[]
  edges: PipelineEdge[]
  status: PipelineStatus
  userId: string
  schedule?: string
  nextRun?: string
  lastRun?: string
  createdAt: string
  updatedAt: string
}

export interface PipelineNode {
  id: string
  type: "source" | "transform" | "destination" | "llm"
  position: { x: number; y: number }
  data: {
    label: string
    connector?: string
    config?: Record<string, unknown>
  }
}

export interface PipelineEdge {
  id: string
  source: string
  target: string
  sourceHandle?: string
  targetHandle?: string
}

export type PipelineStatus = "draft" | "active" | "paused" | "archived"

// Execution types
export interface Execution {
  id: string
  pipelineId: string
  status: ExecutionStatus
  startedAt: string
  finishedAt?: string
  error?: string
  nodeResults: NodeResult[]
  inputData?: Record<string, unknown>
  outputData?: Record<string, unknown>
  userId: string
}

export type ExecutionStatus = "pending" | "running" | "success" | "failed" | "cancelled"

export interface NodeResult {
  nodeId: string
  status: "pending" | "running" | "success" | "failed"
  startedAt?: string
  finishedAt?: string
  output?: unknown
  error?: string
}

// Note: Connector type moved to @/lib/types/mcp-connector.ts as MCPConnector

export interface Credential {
  id: string
  name: string
  connectorType: string
  createdAt: string
  updatedAt: string
}

// Chat types
export interface ChatMessage {
  id: string
  role: "user" | "assistant" | "system"
  content: string
  createdAt: string
  metadata?: {
    model?: string
    tokens?: number
    latency?: number
  }
}

export interface ChatSession {
  id: string
  title: string
  messages: ChatMessage[]
  createdAt: string
  updatedAt: string
}

// Query types
export interface QueryHistory {
  id: string
  query: string
  generatedSql?: string
  dialect: string
  resultCount?: number
  executionTimeMs?: number
  createdAt: string
}

// User types
export interface UserProfile {
  id: string
  email: string
  name?: string
  avatarUrl?: string
  settings: UserSettings
  createdAt: string
}

export interface UserSettings {
  theme: "light" | "dark" | "system"
  notifications: boolean
  defaultConnector?: string
}

// Notification types
export interface Notification {
  id: string
  type: "info" | "success" | "warning" | "error"
  title: string
  message: string
  read: boolean
  createdAt: string
  data?: Record<string, unknown>
}

// CDC Agentic Pipeline Types
export interface CDCPipelineSession {
  session_id: string
  stage: CDCPipelineStage
  messages: CDCMessage[]
  cdc_ready: boolean
  // Schema Discovery
  discovered_schema?: CDCDiscoveredSchema
  cdc_validation?: CDCValidation
  // Recommendations
  recommended_tables?: CDCTableRecommendation[]
  recommended_sync_mode?: string
  recommendations?: string[]
  warnings?: string[]
  pipeline_preview?: CDCPipelinePreview
  // Configuration
  pipeline_config?: CDCPipelineConfig
  deployed: boolean
  error?: string
}

export interface CDCValidation {
  is_ready: boolean
  wal_level?: string
  replication_slots_available?: boolean
  publication_exists?: boolean
  issues?: string[]
}

export interface CDCDiscoveredSchema {
  tables: CDCTableInfo[]
  relationships?: CDCRelationship[]
  total_tables?: number
  total_rows?: number
  total_size_human?: string
}

export interface CDCTableInfo {
  name: string
  full_name?: string
  schema?: string
  estimated_rows?: number
  size_bytes?: number
  size_human?: string
  has_primary_key?: boolean
  primary_keys?: string[]
  columns?: CDCColumnInfo[]
  description?: string
}

export interface CDCColumnInfo {
  name: string
  data_type: string
  is_nullable: boolean
  is_primary_key?: boolean
  default_value?: string
}

export interface CDCRelationship {
  from_table: string
  from_column: string
  to_table: string
  to_column: string
}

export interface CDCTableRecommendation {
  name: string
  reason?: string
  priority?: "high" | "medium" | "low"
  estimated_rows?: number
}

export interface CDCPipelinePreview {
  source_type?: string
  sync_mode?: string
  tables?: string[]
  estimated_initial_sync?: string
}

export type CDCPipelineStage = 
  | "init" 
  | "discovery" 
  | "recommendation" 
  | "configuration" 
  | "deployment" 
  | "complete" 
  | "error"

export interface CDCMessage {
  role: "user" | "assistant"
  content: string
}

export interface CDCPipelineConfig {
  source: {
    connection_id: string
    type: string
    tables: string[]
  }
  destination?: {
    connection_id: string
    type: string
    config?: Record<string, unknown>
  }
  debezium?: Record<string, unknown>
  kafka_topics?: string[]
  sync_mode: string
  name?: string
}

export interface CDCConnection {
  id: string
  name: string
  database_type?: string
  destination_type?: string
  hostname?: string
  port?: number
  database?: string
  bucket_name?: string
  region?: string
  is_connected: boolean
  created_at: string
}

export interface CDCConnector {
  id: string
  name: string
  type: "source" | "destination"
  category: string
  description: string
  supports_cdc?: boolean
  snapshot_modes?: string[]
  default_port?: number
  capabilities?: Record<string, boolean>
}

// SSE Event Types
export interface CDCSSEEvent {
  event: string
  data: Record<string, unknown>
}

// API Response wrappers
export interface PaginatedResponse<T> {
  data: T[]
  total: number
  page: number
  pageSize: number
  hasMore: boolean
}

export interface ApiResponse<T> {
  data: T
  message?: string
}

// ─── Pipeline State (GET /api/v1/pipelines/:id/state) ─────────────────────────

export interface BlockingReasonDetails {
  source_connection_id?: string
  destination_connection_id?: string
  source_type?: string
  destination_type?: string
  node_id?: string
  user_request?: string
  available_tables?: Array<Record<string, unknown>>
  validation_errors?: Record<string, unknown>
  missing_connections?: Array<Record<string, unknown>>
  missing_connectors?: Array<Record<string, unknown>>
  selected_tables?: string[]
  ui_hints?: { widget?: string; [key: string]: unknown }
  source_connector?: string
  destination_connector?: string
  required_connectors?: { source?: string; destination?: string; [key: string]: unknown }
  input_schema?: Record<string, unknown>
  [key: string]: unknown
}

export interface PipelineStateResponse {
  pipeline_id: string
  execution_id?: string
  trace_id?: string
  schema_version?: number
  status: string
  current_stage?: string
  stage_group?: string
  state?: string
  message?: string
  summary?: string
  error_message?: string
  started_at?: string
  last_heartbeat_at?: string
  duration_ms?: number
  attempt?: number
  max_attempts?: number
  created_at?: string
  updated_at?: string
  is_stale?: boolean
  stale_reason?: string
  stale_elapsed_seconds?: number
  cancel_recommended?: boolean
  blocking_reason_type?: string
  progress?: {
    percent?: number
    current_step?: number
    total_steps?: number
    stage?: string
  }
  blocking_reason?: {
    type?: string
    description?: string
    estimated_seconds?: number
    available_tables?: Array<Record<string, unknown>>
    details?: BlockingReasonDetails
  }
  execution_plan?: {
    mode?: string
    metadata?: Record<string, unknown>
    stages?: Array<{
      id?: string
      display_name?: string
      description?: string
      icon?: string
      status?: string
      progress?: number
      started_at?: string
      completed_at?: string
    }>
  }
}

// ─── API Error Response ────────────────────────────────────────────────────────

export interface ApiErrorBody {
  error?: string
  message?: string
  details?: unknown
}

// ─── WebSocket Domain Event Data ──────────────────────────────────────────────

export interface DomainEventData {
  event_type?: string
  eventType?: string
  type?: string
  pipeline_id?: string
  execution_id?: string
  workflow_id?: string
  status?: string
  state?: string
  stage?: string
  current_stage?: string
  message?: string
  summary?: string
  error?: string
  timestamp?: string | number
  attempt?: number
  max_attempts?: number
  progress?: {
    percent?: number
    stage?: string
    current_step?: number
    total_steps?: number
  }
  blocking_reason?: {
    type?: string
    description?: string
    estimated_seconds?: number
    details?: BlockingReasonDetails
    available_tables?: Array<Record<string, unknown>>
  }
  blockingReason?: {
    type?: string
    description?: string
    estimated_seconds?: number
    details?: BlockingReasonDetails
    available_tables?: Array<Record<string, unknown>>
  }
  [key: string]: unknown
}

