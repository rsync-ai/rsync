"use client"

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Check, Minus, ShieldCheck } from "lucide-react"
import { cn } from "@/lib/utils"
import {
  WORKSPACE_ROLES,
  WORKSPACE_CAPABILITY_ROWS,
  ROLE_LABELS,
  ROLE_DESCRIPTIONS,
  roleHasCapability,
} from "@/lib/workspace/roles"

interface WorkspaceRolesMatrixProps {
  /** The caller's role; its column is highlighted so they can see their own access. */
  currentRole?: string
}

/**
 * Read-only Roles & permissions matrix (capability rows x role columns). Pure render
 * from the shared roles module — the single source of truth — so the displayed
 * matrix can never drift from the actual gating in can()/meetsRole().
 */
export function WorkspaceRolesMatrix({ currentRole }: WorkspaceRolesMatrixProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ShieldCheck className="h-4 w-4 text-violet-500" />
          Roles &amp; permissions
        </CardTitle>
        <CardDescription>
          What each role can do in this workspace. Roles are assigned per member on the Members tab.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="min-w-[14rem]">Capability</TableHead>
                {WORKSPACE_ROLES.map((role) => {
                  const isCurrent = role === currentRole
                  return (
                    <TableHead
                      key={role}
                      data-testid={`role-col-${role}`}
                      data-current={String(isCurrent)}
                      className={cn(
                        "text-center",
                        isCurrent && "text-violet-600 dark:text-violet-300",
                      )}
                    >
                      {ROLE_LABELS[role]}
                      {isCurrent && <span className="ml-1 text-xs font-normal text-zinc-500">(you)</span>}
                    </TableHead>
                  )
                })}
              </TableRow>
            </TableHeader>
            <TableBody>
              {WORKSPACE_CAPABILITY_ROWS.map((cap) => (
                <TableRow key={cap.key}>
                  <TableCell className="font-medium">{cap.label}</TableCell>
                  {WORKSPACE_ROLES.map((role) => {
                    const allowed = roleHasCapability(role, cap.minRole)
                    return (
                      <TableCell
                        key={role}
                        data-testid={`cap-${cap.key}-${role}`}
                        className={cn("text-center", role === currentRole && "bg-violet-50 dark:bg-violet-950/20")}
                      >
                        {allowed ? (
                          <Check className="mx-auto h-4 w-4 text-emerald-600 dark:text-emerald-400" aria-hidden />
                        ) : (
                          <Minus className="mx-auto h-4 w-4 text-zinc-300 dark:text-zinc-600" aria-hidden />
                        )}
                        <span className="sr-only">{allowed ? "Allowed" : "Not allowed"}</span>
                      </TableCell>
                    )
                  })}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>

        <dl className="grid gap-3 sm:grid-cols-2">
          {WORKSPACE_ROLES.map((role) => (
            <div key={role} className="rounded-md border border-zinc-200 p-3 dark:border-zinc-800">
              <dt className="text-sm font-semibold">{ROLE_LABELS[role]}</dt>
              <dd className="mt-0.5 text-xs text-zinc-500 dark:text-zinc-400">{ROLE_DESCRIPTIONS[role]}</dd>
            </div>
          ))}
        </dl>
      </CardContent>
    </Card>
  )
}
