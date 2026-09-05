import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import { extractErrorMessage } from "@/lib/utils/error-handling"

export type ConnectionTableMetadata = {
  name: string
  schema?: string
  row_count?: number
  is_exact_count?: boolean
  columns?: number
  // columns metadata exists too, but we intentionally omit it in UI calls (include_columns=false)
}

export type ConnectionMetadataResponse = {
  connection_id: string
  connector_type: string
  tables: ConnectionTableMetadata[]
  total: number
  limit: number
  offset: number
  discovered_at?: string
  schema_version?: number
  connector_version?: string
  database_version?: string
}

export async function getConnectionMetadata(
  connectionId: string,
  opts?: {
    refresh?: boolean
    search?: string
    limit?: number
    offset?: number
    tables?: string[]
  }
): Promise<ConnectionMetadataResponse> {
  const params = new URLSearchParams()
  if (opts?.refresh) params.set("refresh", "true")
  if (opts?.search) params.set("search", opts.search)
  params.set("include_columns", "false")
  params.set("sort_by", "name")
  params.set("limit", String(opts?.limit ?? 5000))
  params.set("offset", String(opts?.offset ?? 0))
  if (opts?.tables && opts.tables.length > 0) {
    params.set("tables", opts.tables.join(","))
  }

  const url = `${API_ENDPOINTS.CONNECTIONS.GET(connectionId)}/metadata?${params.toString()}`
  const res = await authFetch(url, { cache: "no-store" })
  if (!res.ok) {
    const text = await res.text().catch(() => "")
    throw new Error(text || `Failed to load connection metadata (${res.status})`)
  }
  return (await res.json()) as ConnectionMetadataResponse
}

/**
 * API functions for managing CDC connections (sources and destinations)
 */

// =============================================================================
// TYPES
// =============================================================================

export interface SourceConnectionConfig {
  name: string
  description?: string
  // Connector id/type backing the source (e.g. "postgresql", "mysql", "sqlserver", "mongodb", ...)
  database_type: string
  hostname: string
  port: number
  database: string
  schema?: string
  username: string
  password: string
  ssl_mode?: string
  tables?: string[]
  snapshot_mode?: string
}

export interface DestinationConnectionConfig {
  name: string
  description?: string
  // Connector id/type backing the destination (e.g. "aws-s3", "snowflake", "bigquery", "postgresql", ...)
  destination_type: string
  
  // S3 settings
  bucket_name?: string
  region?: string
  access_key_id?: string
  secret_access_key?: string
  endpoint?: string
  path_prefix?: string
  output_format?: "json" | "jsonl" | "csv" | "parquet" | "avro"
  compression?: "none" | "gzip" | "snappy" | "zstd" | "lz4"
  
  // Snowflake settings
  snowflake_url?: string
  snowflake_user?: string
  snowflake_password?: string
  snowflake_database?: string
  snowflake_schema?: string
  snowflake_warehouse?: string
}

export interface Connection {
  id: string
  name: string
  description: string
  type: "source" | "destination"
  connector_type: string
  is_connected: boolean
  last_tested_at?: string
  connection_error?: string
  created_at: string
  updated_at: string
}

export interface TestConnectionResult {
  success: boolean
  message: string
}

// API Error response structure
export interface APIError {
  code: "DUPLICATE_NAME" | "VALIDATION_FAILED" | "NOT_FOUND" | "INTERNAL_ERROR"
  message: string
  details?: Record<string, string>
  suggested_names?: string[]
}

// Name availability check response
export interface NameAvailabilityResult {
  available: boolean
  name: string
  type: "source" | "destination"
  existing_id?: string
  message?: string
  suggested_names?: string[]
}

// Custom error class with structured error info
export class ConnectionAPIError extends Error {
  code: string
  details?: Record<string, string>
  suggestedNames?: string[]

  constructor(error: APIError) {
    super(error.message)
    this.name = "ConnectionAPIError"
    this.code = error.code
    this.details = error.details
    this.suggestedNames = error.suggested_names
  }

  isDuplicateName(): boolean {
    return this.code === "DUPLICATE_NAME"
  }
}

// =============================================================================
// API FUNCTIONS
// =============================================================================

/**
 * Check if a connection name is available
 */
export async function checkNameAvailability(
  name: string,
  type: "source" | "destination"
): Promise<NameAvailabilityResult> {
  const params = new URLSearchParams({ name, type })
  const response = await authFetch(`${API_ENDPOINTS.CONNECTIONS.LIST}/check-name?${params.toString()}`)

  if (!response.ok) {
    // Default to available if check fails (let server-side validation catch it)
    return { available: true, name, type }
  }

  return response.json()
}

/**
 * Create a new source connection (PostgreSQL, MySQL)
 */
export async function createSourceConnection(
  config: SourceConnectionConfig
): Promise<Connection> {
  const response = await authFetch(API_ENDPOINTS.CONNECTIONS.CREATE, {
    method: "POST",
    body: JSON.stringify(config),
  })

  if (!response.ok) {
    const errorData = await response.json()
    
    // Check if it's a structured API error
    if (errorData.code) {
      throw new ConnectionAPIError(errorData as APIError)
    }
    
    throw new Error(extractErrorMessage(errorData) || "Failed to create source connection")
  }

  return response.json()
}

/**
 * Create a new destination connection (S3, Snowflake)
 */
export async function createDestinationConnection(
  config: DestinationConnectionConfig
): Promise<Connection> {
  const response = await authFetch(API_ENDPOINTS.CONNECTIONS.CREATE, {
    method: "POST",
    body: JSON.stringify(config),
  })

  if (!response.ok) {
    const errorData = await response.json()
    
    // Check if it's a structured API error
    if (errorData.code) {
      throw new ConnectionAPIError(errorData as APIError)
    }
    
    throw new Error(extractErrorMessage(errorData) || "Failed to create destination connection")
  }

  return response.json()
}

/**
 * Test a source connection before saving
 */
export async function testSourceConnection(
  config: SourceConnectionConfig
): Promise<TestConnectionResult> {
  const response = await authFetch(API_ENDPOINTS.CONNECTIONS.TEST, {
    method: "POST",
    body: JSON.stringify(config),
  })

  if (!response.ok) {
    return {
      success: false,
      message: "Failed to test connection",
    }
  }

  return response.json()
}

/**
 * Test a destination connection before saving
 */
export async function testDestinationConnection(
  config: DestinationConnectionConfig
): Promise<TestConnectionResult> {
  const response = await authFetch(API_ENDPOINTS.CONNECTIONS.TEST, {
    method: "POST",
    body: JSON.stringify(config),
  })

  if (!response.ok) {
    return {
      success: false,
      message: "Failed to test connection",
    }
  }

  return response.json()
}

/**
 * List all connections
 */
export async function listConnections(
  type?: "source" | "destination"
): Promise<{ connections: Connection[]; total: number }> {
  const params = new URLSearchParams()
  if (type) {
    params.set("type", type)
  }

  const response = await authFetch(`${API_ENDPOINTS.CONNECTIONS.LIST}?${params.toString()}`)

  if (!response.ok) {
    throw new Error(extractErrorMessage(await response.text()) || "Failed to list connections")
  }

  return response.json()
}

/**
 * Get a connection by ID
 */
export async function getConnection(
  id: string,
  type: "source" | "destination"
): Promise<Connection> {
  const response = await authFetch(API_ENDPOINTS.CONNECTIONS.GET(id))

  if (!response.ok) {
    throw new Error(extractErrorMessage(await response.text()) || "Connection not found")
  }

  return response.json()
}

/**
 * Delete a connection
 */
export async function deleteConnection(
  id: string,
  type: "source" | "destination"
): Promise<void> {
  const response = await authFetch(API_ENDPOINTS.CONNECTIONS.DELETE(id), { method: "DELETE" })

  if (!response.ok) {
    throw new Error(extractErrorMessage(await response.text()) || "Failed to delete connection")
  }
}

// =============================================================================
// OAUTH HELPER FUNCTIONS
// =============================================================================

export interface OAuthProvider {
  name: string
  display_name: string
  category: string
  enabled: boolean
  message?: string
}

export async function fetchOAuthProviders(): Promise<OAuthProvider[]> {
  const response = await authFetch(API_ENDPOINTS.OAUTH.PROVIDERS)

  if (!response.ok) {
    throw new Error(extractErrorMessage(await response.text()) || "Failed to fetch OAuth providers")
  }

  const data = await response.json()
  return data.providers
}
