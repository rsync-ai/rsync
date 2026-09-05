"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion"
import {
  Settings,
  Database,
  Cloud,
  Rocket,
  Server,
  Key,
  CheckCircle2,
  Copy,
  Eye,
  EyeOff,
} from "lucide-react"
import { useState } from "react"

interface ConfigSection {
  title: string
  icon: React.ReactNode
  items: Array<{ label: string; value: string; sensitive?: boolean }>
}

interface PipelineConfig {
  source_config?: Record<string, any>
  sink_config?: Record<string, any>
  cdc_config?: Record<string, string> // Generic CDC config (was debezium_config)
  debezium_config?: Record<string, string> // Backward compatibility
  connector_name?: string
  pipeline_name?: string
}

interface ConfigPreviewProps {
  config: PipelineConfig
  onDeploy?: () => void
  onAdjust?: () => void
  isDeploying?: boolean
}

function maskSensitiveValue(value: string): string {
  if (value.length <= 4) return "****"
  return value.slice(0, 2) + "****" + value.slice(-2)
}

export function ConfigPreview({
  config,
  onDeploy,
  onAdjust,
  isDeploying = false,
}: ConfigPreviewProps) {
  const [showSensitive, setShowSensitive] = useState(false)
  const [copied, setCopied] = useState(false)

  // Extract config sections
  const sourceConfig = config.source_config || {}
  const sinkConfig = config.sink_config || {}
  // Use cdc_config if available, otherwise fallback to debezium_config
  const cdcConfig = config.cdc_config || config.debezium_config || {}

  const sections: ConfigSection[] = [
    {
      title: "Source Configuration",
      icon: <Database className="h-4 w-4 text-violet-500" />,
      items: [
        { label: "Connector Name", value: cdcConfig["name"] || config.connector_name || "N/A" },
        { label: "Database Server", value: cdcConfig["database.server.name"] || sourceConfig.hostname || "N/A" },
        { label: "Database", value: cdcConfig["database.dbname"] || sourceConfig.database || "N/A" },
        { label: "Schema", value: cdcConfig["schema.include.list"] || sourceConfig.schema || "public" },
        { label: "Username", value: cdcConfig["database.user"] || sourceConfig.username || "N/A" },
        { label: "Password", value: cdcConfig["database.password"] || sourceConfig.password || "****", sensitive: true },
        // Generic slot/publication (might be specific to Postgres, but okay to keep if present)
        { label: "Slot Name", value: cdcConfig["slot.name"] || "N/A" },
        { label: "Publication", value: cdcConfig["publication.name"] || "N/A" },
      ].filter(item => item.value && item.value !== "N/A"),
    },
    {
      title: "CDC Settings",
      icon: <Settings className="h-4 w-4 text-blue-500" />,
      items: [
        { label: "Connector Class", value: cdcConfig["connector.class"]?.split(".").pop() || "Generic CDC" },
        { label: "Plugin Name", value: cdcConfig["plugin.name"] || "N/A" },
        { label: "Snapshot Mode", value: cdcConfig["snapshot.mode"] || cdcConfig["cdc_mode"] || "initial" },
        { label: "Tables", value: cdcConfig["table.include.list"] || cdcConfig["tables"] || "All selected" },
        { label: "Key Converter", value: cdcConfig["key.converter"]?.split(".").pop() || "JSON" },
        { label: "Value Converter", value: cdcConfig["value.converter"]?.split(".").pop() || "JSON" },
      ].filter(item => item.value && item.value !== "N/A"),
    },
    {
      title: "Destination (S3)",
      icon: <Cloud className="h-4 w-4 text-amber-500" />,
      items: [
        { label: "Bucket", value: sinkConfig.bucket_name || sinkConfig.s3_bucket || "N/A" },
        { label: "Region", value: sinkConfig.region || sinkConfig.s3_region || "N/A" },
        { label: "Path Prefix", value: sinkConfig.path_prefix || sinkConfig.s3_path || "/" },
        { label: "Format", value: sinkConfig.format || sinkConfig.output_format || "JSON" },
        { label: "Compression", value: sinkConfig.compression || "None" },
      ].filter(item => item.value && item.value !== "N/A"),
    },
  ]

  const handleCopyConfig = async () => {
    const configText = JSON.stringify(config, null, 2)
    await navigator.clipboard.writeText(configText)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <Card className="border-zinc-200 dark:border-zinc-800">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base flex items-center gap-2">
            <Settings className="h-5 w-5 text-zinc-500" />
            Pipeline Configuration
          </CardTitle>
          <div className="flex items-center gap-2">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setShowSensitive(!showSensitive)}
            >
              {showSensitive ? (
                <EyeOff className="h-4 w-4" />
              ) : (
                <Eye className="h-4 w-4" />
              )}
            </Button>
            <Button variant="ghost" size="sm" onClick={handleCopyConfig}>
              {copied ? (
                <CheckCircle2 className="h-4 w-4 text-green-500" />
              ) : (
                <Copy className="h-4 w-4" />
              )}
            </Button>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Config Sections */}
        <Accordion type="multiple" defaultValue={["source", "cdc", "destination"]}>
          {sections.map((section, index) => (
            <AccordionItem
              key={section.title}
              value={["source", "cdc", "destination"][index]}
            >
              <AccordionTrigger className="text-sm">
                <div className="flex items-center gap-2">
                  {section.icon}
                  {section.title}
                  <Badge variant="secondary" className="ml-2">
                    {section.items.length} settings
                  </Badge>
                </div>
              </AccordionTrigger>
              <AccordionContent>
                <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 overflow-hidden">
                  <div className="divide-y divide-zinc-200 dark:divide-zinc-800">
                    {section.items.map((item) => (
                      <div
                        key={item.label}
                        className="flex items-center justify-between px-4 py-3 bg-white dark:bg-zinc-900"
                      >
                        <span className="text-sm text-zinc-500">{item.label}</span>
                        <span className="text-sm font-mono text-zinc-900 dark:text-white">
                          {item.sensitive && !showSensitive
                            ? maskSensitiveValue(item.value)
                            : item.value}
                          {item.sensitive && (
                            <Key className="h-3 w-3 inline ml-1 text-amber-500" />
                          )}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              </AccordionContent>
            </AccordionItem>
          ))}
        </Accordion>

        {/* Pipeline Name */}
        {config.pipeline_name && (
          <div className="p-4 rounded-lg bg-gradient-to-r from-violet-50 to-indigo-50 dark:from-violet-950/20 dark:to-indigo-950/20 border border-violet-200 dark:border-violet-800">
            <div className="flex items-center gap-3">
              <Server className="h-5 w-5 text-violet-500" />
              <div>
                <p className="text-sm text-zinc-500">Pipeline Name</p>
                <p className="font-medium text-zinc-900 dark:text-white">
                  {config.pipeline_name}
                </p>
              </div>
            </div>
          </div>
        )}

        {/* Action Buttons */}
        {(onDeploy || onAdjust) && (
          <div className="flex gap-3 pt-2">
            {onDeploy && (
              <Button
                onClick={onDeploy}
                disabled={isDeploying}
                className="flex-1 bg-gradient-to-r from-green-600 to-emerald-600 hover:from-green-700 hover:to-emerald-700"
              >
                {isDeploying ? (
                  <>
                    <span className="animate-spin mr-2">⏳</span>
                    Deploying...
                  </>
                ) : (
                  <>
                    <Rocket className="h-4 w-4 mr-2" />
                    Deploy Pipeline
                  </>
                )}
              </Button>
            )}
            {onAdjust && (
              <Button variant="outline" onClick={onAdjust} disabled={isDeploying}>
                Adjust Config
              </Button>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

