"use client"

import { useState, useRef, useEffect } from "react"
import { useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import {
  Send,
  Loader2,
  Database,
  ArrowRight,
  RefreshCw,
  Sparkles,
  CheckCircle2,
  AlertCircle,
} from "lucide-react"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import { captureWorkspace, onActiveWorkspaceChange } from "@/lib/workspace/active-workspace"

type CDCConnection = {
  id: string
  name: string
  type?: string
  connection_type?: string
  connector_type?: string
  status: string
  supports_source?: boolean
  supports_destination?: boolean
  is_expired?: boolean
  expires_at?: string | null
}

type AgentMessage = {
  id: string
  role: "user" | "assistant" | "system"
  content: string
  timestamp: Date
  isLoading?: boolean
}

const quickPrompts = [
  {
    label: "PostgreSQL → S3",
    prompt: "Create a pipeline from PostgreSQL to S3",
    icon: Database,
  },
  {
    label: "MySQL → S3",
    prompt: "Create a pipeline from MySQL to S3",
    icon: Database,
  },
  {
    label: "Real-time Sync",
    prompt: "Set up real-time CDC sync from my database to S3",
    icon: RefreshCw,
  },
]

export function HomeAgentWidget() {
  const router = useRouter()
  const [input, setInput] = useState("")
  const [isLoading, setIsLoading] = useState(false)
  const [connections, setConnections] = useState<CDCConnection[]>([])
  const [messages, setMessages] = useState<AgentMessage[]>([])
  const [showFullChat, setShowFullChat] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)

  // Fetch on mount, and again whenever the active workspace changes — connections
  // are workspace-scoped, so the previous workspace's list must not stay on the
  // dashboard under the new workspace's name.
  useEffect(() => {
    void fetchConnections()
    return onActiveWorkspaceChange(() => {
      setConnections([])
      void fetchConnections()
    })
  }, [])

  const fetchConnections = async () => {
    const isStale = captureWorkspace()
    try {
      // Home widget should reflect the same source of truth as the rest of the UI (API Gateway).
      const res = await authFetch(API_ENDPOINTS.CONNECTIONS.LIST, { cache: "no-store" })
      if (isStale()) return
      if (res.ok) {
        const data = await res.json()
        if (isStale()) return
        setConnections(data.connections || [])
      }
    } catch (error) {
      console.error("Failed to fetch connections:", error)
    }
  }

  const handleSubmit = async (prompt?: string) => {
    const message = prompt || input.trim()
    if (!message || isLoading) return

    setInput("")
    setShowFullChat(true)
    
    // Add user message
    const userMessage: AgentMessage = {
      id: `user-${Date.now()}`,
      role: "user",
      content: message,
      timestamp: new Date(),
    }
    setMessages(prev => [...prev, userMessage])

    // Add loading message
    const loadingMessage: AgentMessage = {
      id: `loading-${Date.now()}`,
      role: "assistant",
      content: "Analyzing your request...",
      timestamp: new Date(),
      isLoading: true,
    }
    setMessages(prev => [...prev, loadingMessage])
    setIsLoading(true)

    // Redirect to the main agentic builder (supports batch + CDC via planner routing)
    // and pass the prompt via query string.
    setTimeout(() => {
      router.push(`/chat?prompt=${encodeURIComponent(message)}&autosend=1`)
    }, 250)
  }

  const handleQuickPrompt = (prompt: string) => {
    handleSubmit(prompt)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault()
      handleSubmit()
    }
  }

  // Filter connections by type - use generic supports_source/supports_destination flags
  const sourceConnections = connections.filter(c => 
    c.type === "source" || c.connection_type === "source" || c.supports_source
  )
  const destConnections = connections.filter(c => 
    c.type === "destination" || c.connection_type === "destination" || c.supports_destination
  )

  if (showFullChat) {
    return (
      <div className="p-6 space-y-4">
        {/* Messages */}
        <div className="space-y-4 min-h-[100px]">
          {messages.map((msg) => (
            <div
              key={msg.id}
              className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
            >
              <div
                className={`max-w-[80%] rounded-lg px-4 py-3 ${
                  msg.role === "user"
                    ? "bg-violet-600 text-white"
                    : "bg-zinc-100 dark:bg-zinc-800"
                }`}
              >
                {msg.isLoading ? (
                  <div className="flex items-center gap-2">
                    <Loader2 className="h-4 w-4 animate-spin" />
                    <span>{msg.content}</span>
                  </div>
                ) : (
                  <p className="text-sm">{msg.content}</p>
                )}
              </div>
            </div>
          ))}
        </div>

        {/* Redirecting indicator */}
        <div className="flex items-center justify-center gap-2 text-sm text-zinc-500">
          <Loader2 className="h-4 w-4 animate-spin" />
          <span>Opening pipeline creator...</span>
        </div>
      </div>
    )
  }

  return (
    <div className="p-6 space-y-6">
      {/* Quick Actions */}
      <div className="space-y-3">
        <p className="text-sm font-medium text-zinc-500 dark:text-zinc-400">
          Quick Start
        </p>
        <div className="flex flex-wrap gap-2">
          {quickPrompts.map((item) => (
            <Button
              key={item.label}
              variant="outline"
              size="sm"
              className="gap-2"
              onClick={() => handleQuickPrompt(item.prompt)}
              disabled={isLoading}
            >
              <item.icon className="h-4 w-4" />
              {item.label}
            </Button>
          ))}
        </div>
      </div>

      {/* Available Connections */}
      {connections.length > 0 && (
        <div className="space-y-3">
          <p className="text-sm font-medium text-zinc-500 dark:text-zinc-400">
            Available Connections
          </p>
          <div className="flex flex-wrap gap-2">
            {sourceConnections.slice(0, 3).map((conn) => (
              <Badge
                key={conn.id}
                variant="secondary"
                className={`gap-1 ${conn.is_expired ? "border border-amber-400 text-amber-700 dark:text-amber-400" : ""}`}
                title={conn.is_expired ? "OAuth token expired — re-authenticate this connection" : undefined}
              >
                <Database className="h-3 w-3" />
                {conn.name}
                {conn.is_expired ? (
                  <AlertCircle className="h-3 w-3 text-amber-500" />
                ) : conn.status === "active" || conn.status === "connected" ? (
                  <CheckCircle2 className="h-3 w-3 text-emerald-500" />
                ) : null}
              </Badge>
            ))}
            {destConnections.slice(0, 2).map((conn) => (
              <Badge key={conn.id} variant="outline" className="gap-1">
                <ArrowRight className="h-3 w-3" />
                {conn.name}
              </Badge>
            ))}
            {connections.length > 5 && (
              <Badge variant="outline">+{connections.length - 5} more</Badge>
            )}
          </div>
        </div>
      )}

      {/* No connections warning */}
      {connections.length === 0 && (
        <div className="flex items-center gap-3 p-4 rounded-lg bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-900">
          <AlertCircle className="h-5 w-5 text-amber-600" />
          <div className="flex-1">
            <p className="text-sm font-medium text-amber-700 dark:text-amber-400">
              No connections configured
            </p>
            <p className="text-xs text-amber-600 dark:text-amber-500">
              Add a database connection to create pipelines
            </p>
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={() => router.push("/connections")}
          >
            Add Connection
          </Button>
        </div>
      )}

      {/* Input */}
      <div className="flex gap-2">
        <div className="flex-1 relative">
          <Sparkles className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-violet-500" />
          <Input
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Describe what you want to build..."
            className="pl-10 pr-4"
            disabled={isLoading}
          />
        </div>
        <Button 
          onClick={() => handleSubmit()}
          disabled={!input.trim() || isLoading}
          className="gap-2"
        >
          {isLoading ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Send className="h-4 w-4" />
          )}
          Create
        </Button>
      </div>

      {/* Helper text */}
      <p className="text-xs text-zinc-400 dark:text-zinc-500 text-center">
        Try: "Sync my orders table from PostgreSQL to S3 in real-time"
      </p>
    </div>
  )
}

