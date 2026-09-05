/**
 * API Error handling utilities
 * Provides user-friendly error messages for all API errors
 */
import { parseApiError as parseApiErrorGeneric } from "@/lib/utils/error-handling"

// Error codes returned by backend
export const ErrorCodes = {
  DUPLICATE_NAME: "DUPLICATE_NAME",
  CONNECTION_FAILED: "CONNECTION_FAILED",
  VALIDATION_FAILED: "VALIDATION_FAILED",
  NOT_FOUND: "NOT_FOUND",
  UNAUTHORIZED: "UNAUTHORIZED",
  DATABASE_ERROR: "DATABASE_ERROR",
  ENCRYPTION_FAILED: "ENCRYPTION_FAILED",
  CONNECTOR_NOT_FOUND: "CONNECTOR_NOT_FOUND",
  NETWORK_ERROR: "NETWORK_ERROR",
  TIMEOUT: "TIMEOUT",
  UNKNOWN: "UNKNOWN",
} as const

export type ErrorCode = (typeof ErrorCodes)[keyof typeof ErrorCodes]

// Structured API error
export interface APIError {
  code: ErrorCode
  message: string
  details?: string
  field?: string
  suggestion?: string
}

// Parse raw error response into structured APIError
export function parseAPIError(error: unknown): APIError {
  // Prefer the centralized generic parser (it understands more backend formats),
  // then map back into our APIError shape for backwards compatibility.
  const normalized = parseApiErrorGeneric(error)
  if (normalized && normalized.message) {
    const inferredCode =
      (normalized.code as ErrorCode) ||
      inferErrorCode(normalized.message, normalized.statusCode)
    const friendly = getUserFriendlyMessage(inferredCode, normalized.message, normalized.details)
    return {
      code: inferredCode,
      // Prefer the friendly message when we can infer a known code.
      message: friendly.message || normalized.message,
      details: normalized.details,
      field: normalized.field || friendly.field,
      suggestion: normalized.suggestion || friendly.suggestion,
    }
  }

  // Network errors
  if (error instanceof TypeError) {
    if (error.message.includes("Failed to fetch") || error.message.includes("NetworkError")) {
      return {
        code: ErrorCodes.NETWORK_ERROR,
        message: "Unable to connect to the server",
        suggestion: "Please check your internet connection and try again.",
      }
    }
  }

  // String errors (from HTTP response text)
  if (typeof error === "string") {
    return parseErrorString(error)
  }

  // Error objects with message
  if (error instanceof Error) {
    return parseErrorString(error.message)
  }

  // Unknown errors
  return {
    code: ErrorCodes.UNKNOWN,
    message: "An unexpected error occurred",
    suggestion: "Please try again or contact support if the issue persists.",
  }
}

// Parse error string (may be JSON or plain text)
function parseErrorString(errorStr: string): APIError {
  // Try to extract JSON from "HTTP XXX: {...}" format
  // Avoid relying on the ES2018 RegExp dotAll flag.
  const httpMatch = errorStr.match(/^HTTP (\d+): ([\s\S]+)$/)
  if (httpMatch) {
    const statusCode = parseInt(httpMatch[1])
    const body = httpMatch[2]

    try {
      const json = JSON.parse(body)
      return parseJSONError(json, statusCode)
    } catch {
      // Not JSON, use raw message
      return createErrorFromStatus(statusCode, body)
    }
  }

  // Try direct JSON parse
  try {
    const json = JSON.parse(errorStr)
    return parseJSONError(json)
  } catch {
    // Plain text error
    return {
      code: ErrorCodes.UNKNOWN,
      message: errorStr || "An error occurred",
    }
  }
}

// Parse JSON error response from backend
function parseJSONError(json: Record<string, unknown>, statusCode?: number): APIError {
  const error = (json.error as string) || "An error occurred"
  const code = (json.code as ErrorCode) || inferErrorCode(error, statusCode)
  const details = json.details as string | undefined

  // Generate user-friendly message based on error code
  const userFriendly = getUserFriendlyMessage(code, error, details)

  return {
    code,
    message: userFriendly.message,
    details,
    suggestion: userFriendly.suggestion,
    field: userFriendly.field,
  }
}

// Infer error code from error message if not provided
function inferErrorCode(error: string, statusCode?: number): ErrorCode {
  const errorLower = error.toLowerCase()

  if (errorLower.includes("duplicate") || errorLower.includes("already exists")) {
    return ErrorCodes.DUPLICATE_NAME
  }
  if (errorLower.includes("not found")) {
    return ErrorCodes.NOT_FOUND
  }
  if (errorLower.includes("unauthorized") || errorLower.includes("authentication")) {
    return ErrorCodes.UNAUTHORIZED
  }
  if (errorLower.includes("connection") && (errorLower.includes("failed") || errorLower.includes("refused"))) {
    return ErrorCodes.CONNECTION_FAILED
  }
  if (errorLower.includes("validation") || errorLower.includes("invalid") || errorLower.includes("required")) {
    return ErrorCodes.VALIDATION_FAILED
  }
  if (errorLower.includes("encrypt")) {
    return ErrorCodes.ENCRYPTION_FAILED
  }
  if (errorLower.includes("connector") && errorLower.includes("not found")) {
    return ErrorCodes.CONNECTOR_NOT_FOUND
  }

  // Infer from status code
  if (statusCode) {
    if (statusCode === 401) return ErrorCodes.UNAUTHORIZED
    if (statusCode === 404) return ErrorCodes.NOT_FOUND
    if (statusCode === 409) return ErrorCodes.DUPLICATE_NAME
    if (statusCode === 400) return ErrorCodes.VALIDATION_FAILED
  }

  return ErrorCodes.DATABASE_ERROR
}

// Create error from HTTP status code
function createErrorFromStatus(statusCode: number, body: string): APIError {
  switch (statusCode) {
    case 400:
      return {
        code: ErrorCodes.VALIDATION_FAILED,
        message: "Invalid request",
        details: body,
        suggestion: "Please check your input and try again.",
      }
    case 401:
      return {
        code: ErrorCodes.UNAUTHORIZED,
        message: "You are not authorized to perform this action",
        suggestion: "Please sign in and try again.",
      }
    case 403:
      return {
        code: ErrorCodes.UNAUTHORIZED,
        message: "You don't have permission to perform this action",
        suggestion: "Contact your administrator if you need access.",
      }
    case 404:
      return {
        code: ErrorCodes.NOT_FOUND,
        message: "The requested resource was not found",
      }
    case 409:
      return {
        code: ErrorCodes.DUPLICATE_NAME,
        message: "A resource with this name already exists",
        suggestion: "Please choose a different name.",
      }
    case 500:
    case 502:
    case 503:
      return {
        code: ErrorCodes.DATABASE_ERROR,
        message: "Server error occurred",
        details: body,
        suggestion: "Please try again later.",
      }
    default:
      return {
        code: ErrorCodes.UNKNOWN,
        message: body || `Request failed with status ${statusCode}`,
      }
  }
}

// Get user-friendly message for error code
function getUserFriendlyMessage(
  code: ErrorCode,
  originalError: string,
  details?: string
): { message: string; suggestion?: string; field?: string } {
  switch (code) {
    case ErrorCodes.DUPLICATE_NAME:
      // Try to extract the name from the error
      const nameMatch = originalError.match(/["']([^"']+)["']/) || 
                       details?.match(/["']([^"']+)["']/)
      const name = nameMatch ? nameMatch[1] : "this name"
      return {
        message: `A connection named "${name}" already exists`,
        suggestion: "Please choose a different name for your connection.",
        field: "name",
      }

    case ErrorCodes.CONNECTION_FAILED:
      // Parse connection failure details
      if (details?.includes("connection refused") || details?.includes("ECONNREFUSED")) {
        return {
          message: "Cannot connect to the database server",
          suggestion: "Please verify the host and port are correct, and the server is running.",
        }
      }
      if (details?.includes("timeout") || details?.includes("ETIMEDOUT")) {
        return {
          message: "Connection timed out",
          suggestion: "The server is taking too long to respond. Check your network or server status.",
        }
      }
      if (details?.includes("authentication") || details?.includes("Access denied") || details?.includes("password")) {
        return {
          message: "Authentication failed",
          suggestion: "Please verify your username and password are correct.",
          field: "password",
        }
      }
      if (details?.includes("Unknown database") || details?.includes("does not exist")) {
        return {
          message: "Database not found",
          suggestion: "Please verify the database name exists on the server.",
          field: "database",
        }
      }
      return {
        message: "Connection test failed",
        suggestion: "Please verify your connection settings and try again.",
      }

    case ErrorCodes.VALIDATION_FAILED:
      // Try to extract field name
      const fieldMatch = details?.match(/field ["']?(\w+)["']?/) ||
                        originalError.match(/["'](\w+)["'] is required/)
      return {
        message: originalError || "Please fill in all required fields",
        suggestion: fieldMatch ? `The "${fieldMatch[1]}" field needs attention.` : undefined,
        field: fieldMatch ? fieldMatch[1] : undefined,
      }

    case ErrorCodes.NOT_FOUND:
      return {
        message: "Resource not found",
        suggestion: "The item you're looking for may have been deleted.",
      }

    case ErrorCodes.UNAUTHORIZED:
      return {
        message: "Please sign in to continue",
        suggestion: "Your session may have expired.",
      }

    case ErrorCodes.CONNECTOR_NOT_FOUND:
      return {
        message: "Connector not available",
        suggestion: "This connector may not be installed. Try refreshing the page.",
      }

    case ErrorCodes.ENCRYPTION_FAILED:
      return {
        message: "Failed to secure your credentials",
        suggestion: "Please try again. Contact support if this persists.",
      }

    case ErrorCodes.DATABASE_ERROR:
      return {
        message: "A server error occurred",
        suggestion: "Please try again in a moment.",
      }

    case ErrorCodes.NETWORK_ERROR:
      return {
        message: "Unable to connect to the server",
        suggestion: "Please check your internet connection.",
      }

    case ErrorCodes.TIMEOUT:
      return {
        message: "Request timed out",
        suggestion: "The server is taking too long. Please try again.",
      }

    default:
      return {
        message: originalError || "An unexpected error occurred",
        suggestion: "Please try again or contact support.",
      }
  }
}

// Format error for display in toast/alert
export function formatErrorForDisplay(error: APIError): {
  title: string
  description: string
} {
  return {
    title: error.message,
    description: error.suggestion || error.details || "",
  }
}

// Custom error class for API errors
export class APIRequestError extends Error {
  public readonly apiError: APIError

  constructor(error: unknown) {
    const apiError = parseAPIError(error)
    super(apiError.message)
    this.name = "APIRequestError"
    this.apiError = apiError
  }

  get code(): ErrorCode {
    return this.apiError.code
  }

  get suggestion(): string | undefined {
    return this.apiError.suggestion
  }

  get field(): string | undefined {
    return this.apiError.field
  }

  get details(): string | undefined {
    return this.apiError.details
  }

  toDisplayFormat(): { title: string; description: string } {
    return formatErrorForDisplay(this.apiError)
  }
}
