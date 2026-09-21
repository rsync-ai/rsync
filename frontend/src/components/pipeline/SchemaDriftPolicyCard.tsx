"use client"

/**
 * SchemaDriftPolicyCard — "which source schema changes should this pipeline
 * stop and ask me about?"
 *
 * Reads and writes pipelines.config.schema_drift_policy through
 * GET/PUT /pipelines/:id/schema-drift-policy (api-gateway schema_evolution.go).
 * The orchestrator's batch detector applies it (schema_drift.go
 * filterChangesByPolicy): the switch turns detection off for this pipeline, the
 * two boxes drop additions or drops from what is filed for review, and a type
 * change is always filed, because it can silently break writes.
 *
 * Every toggle saves at once, optimistically, and rolls back with a toast if the
 * save fails. Saves are serialized (controls lock while one is in flight) so two
 * quick clicks can never land out of order.
 */

import { useCallback, useEffect, useState } from "react"
import { AlertTriangle, BellRing, Info, Lock } from "lucide-react"
import { toast } from "sonner"

import { Alert, AlertDescription } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
import { Switch } from "@/components/ui/switch"
import { useWorkspaceRole } from "@/contexts/WorkspaceContext"
import {
  getSchemaDriftPolicy,
  updateSchemaDriftPolicy,
  type SchemaDriftPolicy,
  type SchemaDriftPolicyState,
} from "@/lib/api/schema-changes"
import { cn } from "@/lib/utils"

type SaveStatus = "idle" | "saving" | "saved"

export function SchemaDriftPolicyCard({ pipelineId, isCDC }: { pipelineId: string; isCDC: boolean }) {
  const { meets, isLoading: roleLoading } = useWorkspaceRole()
  // PUT is WSMember-gated (schema_evolution.go UpdatePipelineSchemaDriftPolicy).
  const canEdit = !roleLoading && meets("member")

  const [state, setState] = useState<SchemaDriftPolicyState | null>(null)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [status, setStatus] = useState<SaveStatus>("idle")

  const load = useCallback(async () => {
    setLoadError(null)
    try {
      setState(await getSchemaDriftPolicy(pipelineId))
    } catch (e) {
      setLoadError(e instanceof Error ? e.message : "Failed to load alert settings")
    }
  }, [pipelineId])

  useEffect(() => {
    void load()
  }, [load])

  const save = useCallback(
    async (next: SchemaDriftPolicy) => {
      if (!state) return
      const previous = state
      setState({ ...state, policy: next })
      setStatus("saving")
      try {
        setState(await updateSchemaDriftPolicy(pipelineId, next))
        setStatus("saved")
      } catch (e) {
        setState(previous)
        setStatus("idle")
        toast.error(e instanceof Error ? e.message : "Couldn't save the alert settings.")
      }
    },
    [pipelineId, state]
  )

  if (loadError) {
    return (
      <Card data-testid="schema-drift-policy">
        <CardContent className="flex items-center justify-between gap-3 py-4">
          <p className="text-sm text-muted-foreground">Couldn&apos;t load the alert settings.</p>
          <Button variant="outline" size="sm" onClick={() => void load()}>
            Retry
          </Button>
        </CardContent>
      </Card>
    )
  }
  if (!state) return null

  const { policy, detectorEnabled } = state
  const locked = !canEdit || status === "saving"
  const optionsLocked = locked || !policy.enabled

  return (
    <Card data-testid="schema-drift-policy">
      <CardHeader className="px-4 py-3 pb-2">
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <CardTitle className="flex items-center gap-2 text-sm font-semibold">
              <BellRing className="h-4 w-4 text-blue-600 dark:text-blue-400" />
              Schema change alerts
              <span aria-live="polite" className="text-xs font-normal text-muted-foreground">
                {status === "saving" ? "Saving…" : status === "saved" ? "Saved" : ""}
              </span>
            </CardTitle>
            <p className="mt-1 text-xs text-muted-foreground">
              Choose which changes to a source table&apos;s columns are listed below for review.
            </p>
          </div>
          <Switch
            id={`drift-enabled-${pipelineId}`}
            aria-label="Watch for schema changes"
            checked={policy.enabled}
            disabled={locked}
            onCheckedChange={(v) => void save({ ...policy, enabled: v })}
          />
        </div>
      </CardHeader>
      <CardContent className="space-y-3 px-4 pb-4 pt-0">
        {detectorEnabled === false && (
          <Alert className="border-amber-300 bg-amber-50 py-2 text-amber-900 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-200">
            <AlertTriangle className="h-4 w-4 !text-amber-600" />
            <AlertDescription className="text-xs">
              Schema change detection is off for this installation, so nothing is checked yet. These settings
              are saved and take effect once an administrator sets{" "}
              <code className="font-mono">RSYNC_SCHEMA_DRIFT_ENABLED=true</code>.
            </AlertDescription>
          </Alert>
        )}

        <fieldset disabled={optionsLocked} className={cn("space-y-2", !policy.enabled && "opacity-60")}>
          <legend className="mb-1 text-xs font-medium text-muted-foreground">
            {policy.enabled ? "Ask me about" : "Off for this pipeline: no changes are checked."}
          </legend>
          <PolicyOption
            id={`drift-add-${pipelineId}`}
            label="New tables and columns"
            checked={policy.notify_on_add}
            disabled={optionsLocked}
            onChange={(v) => void save({ ...policy, notify_on_add: v })}
          />
          <PolicyOption
            id={`drift-drop-${pipelineId}`}
            label="Dropped tables and columns"
            checked={policy.notify_on_drop}
            disabled={optionsLocked}
            onChange={(v) => void save({ ...policy, notify_on_drop: v })}
          />
          <div className="flex items-center gap-2 text-sm" data-testid="drift-type-change-row">
            <Checkbox checked disabled aria-label="Column type changes (always reported)" />
            <span>Column type changes</span>
            <span className="flex items-center gap-1 text-xs text-muted-foreground">
              <Lock className="h-3 w-3" />
              always reported, since a type change can break writes to the destination
            </span>
          </div>
        </fieldset>

        {isCDC && (
          <p className="flex gap-1.5 text-xs text-muted-foreground" data-testid="drift-cdc-note">
            <Info className="mt-0.5 h-3.5 w-3.5 shrink-0" />
            <span>
              For a CDC pipeline these settings apply only to a batch initial load. While it streams, new columns
              are added to the destination automatically and dropped tables or columns are always reported.
            </span>
          </p>
        )}

        {!roleLoading && !canEdit && (
          <p className="text-xs text-muted-foreground" data-testid="drift-policy-readonly">
            Needs member access to change.
          </p>
        )}
      </CardContent>
    </Card>
  )
}

function PolicyOption(props: {
  id: string
  label: string
  checked: boolean
  disabled: boolean
  onChange: (v: boolean) => void
}) {
  return (
    <div className="flex items-center gap-2">
      <Checkbox
        id={props.id}
        checked={props.checked}
        disabled={props.disabled}
        onCheckedChange={(v) => props.onChange(v === true)}
      />
      <label
        htmlFor={props.id}
        className={cn("text-sm", props.disabled ? "cursor-not-allowed" : "cursor-pointer")}
      >
        {props.label}
      </label>
    </div>
  )
}
