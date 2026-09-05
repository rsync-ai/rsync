/**
 * Executions API functions
 */

import type { Execution, PaginatedResponse } from "./types"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import { extractErrorMessage } from "@/lib/utils/error-handling"

export interface ExecutionListParams {
  pipelineId?: string
  status?: string
  page?: number
  limit?: number
}

export interface ExecutionWithPipeline extends Execution {
  pipelineName?: string
}

export interface ExecutionStats {
  running: number
  success: number
  failed: number
}

export interface ExecutionListResponse {
  executions: ExecutionWithPipeline[]
  total: number
  stats: ExecutionStats
}

function getLocalAuthHeaders(): HeadersInit {
  // Cookie-based auth: no custom identity headers needed.
  return { "Content-Type": "application/json" }
}

/**
 * List executions and include stats/total (preferred for UI pages).
 * Backend returns `{ executions, total, stats }`.
 */
export async function listExecutionsResponse(
  params?: ExecutionListParams
): Promise<ExecutionListResponse> {
  const searchParams = new URLSearchParams()

  if (params?.pipelineId) {
    searchParams.set("pipeline_id", params.pipelineId)
  }
  if (params?.status) {
    searchParams.set("status", params.status)
  }
  if (params?.page) {
    searchParams.set("page", params.page.toString())
  }
  if (params?.limit) {
    searchParams.set("limit", params.limit.toString())
  }

  const queryString = searchParams.toString()
  const url = `${API_ENDPOINTS.EXECUTIONS.LIST}${queryString ? `?${queryString}` : ""}`

  const response = await authFetch(url, {
    method: "GET",
    headers: getLocalAuthHeaders(),
  })

  if (!response.ok) {
    const error = await response
      .json()
      .catch(() => ({ error: "Failed to fetch executions" }))
    throw new Error(extractErrorMessage(error) || "Failed to fetch executions")
  }

  const data = await response.json().catch(() => null)

  // Backwards compatibility: some backends/callers may return a plain array.
  if (Array.isArray(data)) {
    const executions = data as ExecutionWithPipeline[]
    return {
      executions,
      total: executions.length,
      stats: {
        running: executions.filter((e: any) => e.status === "running").length,
        success: executions.filter((e: any) => e.status === "success" || e.status === "completed").length,
        failed: executions.filter((e: any) => e.status === "failed").length,
      },
    }
  }

  const executions = (data?.executions || []) as ExecutionWithPipeline[]
  const stats = (data?.stats || { running: 0, success: 0, failed: 0 }) as ExecutionStats
  const total = typeof data?.total === "number" ? data.total : executions.length

  return { executions, stats, total }
}

/**
 * List all executions with optional filters
 */
export async function listExecutions(params?: ExecutionListParams): Promise<ExecutionWithPipeline[]> {
  const searchParams = new URLSearchParams()
  
  if (params?.pipelineId) {
    searchParams.set("pipeline_id", params.pipelineId)
  }
  if (params?.status) {
    searchParams.set("status", params.status)
  }
  if (params?.page) {
    searchParams.set("page", params.page.toString())
  }
  if (params?.limit) {
    searchParams.set("limit", params.limit.toString())
  }
  
  const queryString = searchParams.toString()
  const url = `${API_ENDPOINTS.EXECUTIONS.LIST}${queryString ? `?${queryString}` : ""}`
  
  const response = await authFetch(url, {
    method: "GET",
    headers: getLocalAuthHeaders(),
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: "Failed to fetch executions" }))
    throw new Error(extractErrorMessage(error) || "Failed to fetch executions")
  }

  const data = await response.json().catch(() => null)
  // Backend returns { executions, total, stats }. Older callers may expect an array.
  if (Array.isArray(data)) return data
  if (data && Array.isArray(data.executions)) return data.executions
  return []
}

/**
 * Get a single execution by ID
 */
export async function getExecution(id: string): Promise<ExecutionWithPipeline> {
  const response = await authFetch(API_ENDPOINTS.EXECUTIONS.GET(id), {
    method: "GET",
    headers: getLocalAuthHeaders(),
  })

  if (!response.ok) {
    if (response.status === 404) {
      throw new Error("Execution not found")
    }
    const error = await response.json().catch(() => ({ error: "Failed to fetch execution" }))
    throw new Error(extractErrorMessage(error) || "Failed to fetch execution")
  }

  return response.json()
}

/**
 * Cancel a running execution
 */
export async function cancelExecution(id: string): Promise<void> {
  const response = await authFetch(`${API_ENDPOINTS.EXECUTIONS.GET(id)}/cancel`, {
    method: "POST",
    headers: getLocalAuthHeaders(),
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: "Failed to cancel execution" }))
    throw new Error(extractErrorMessage(error) || "Failed to cancel execution")
  }
}

/**
 * Get executions for a specific pipeline
 */
export async function getPipelineExecutions(pipelineId: string, limit = 10): Promise<ExecutionWithPipeline[]> {
  return listExecutions({ pipelineId, limit })
}

/**
 * Get recent executions (last 20)
 */
export async function getRecentExecutions(): Promise<ExecutionWithPipeline[]> {
  return listExecutions({ limit: 20 })
}

/**
 * Poll execution status until it completes
 */
export async function pollExecutionStatus(
  id: string,
  onUpdate: (execution: ExecutionWithPipeline) => void,
  intervalMs = 2000
): Promise<ExecutionWithPipeline> {
  return new Promise((resolve, reject) => {
    const poll = async () => {
      try {
        const execution = await getExecution(id)
        onUpdate(execution)
        
        if (["success", "failed", "cancelled"].includes(execution.status)) {
          resolve(execution)
        } else {
          setTimeout(poll, intervalMs)
        }
      } catch (error) {
        reject(error)
      }
    }
    
    poll()
  })
}
