/**
 * Generic HITL Form
 * 
 * Renders a basic form for unknown HITL types when input_schema is provided.
 * Supports a minimal subset of JSON Schema:
 * - object with properties
 * - required fields
 * - string, number, boolean types
 * - enum (for dropdowns)
 * - array of strings (for multi-select)
 */

import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { AlertCircle } from 'lucide-react'

/**
 * One definition of the "we cannot accept this" wording, shared by this form's
 * own alert and by the pipeline panel's notice, so the two cannot drift apart.
 */
export const UNSUPPORTED_HITL_TITLE = 'This request type is not supported by this build'
export const UNSUPPORTED_HITL_DETAIL =
  'Nothing entered here can be submitted \u2014 update rsync to a version that understands it, or stop the run and start it again.'
export const UNSUPPORTED_HITL_MESSAGE = `${UNSUPPORTED_HITL_TITLE}. ${UNSUPPORTED_HITL_DETAIL}`

export interface JsonSchema {
  type: string
  required?: string[]
  properties?: Record<string, JsonSchemaProperty>
  description?: string
}

interface JsonSchemaProperty {
  type: string
  description?: string
  enum?: string[]
  items?: JsonSchemaProperty
  minItems?: number
  default?: unknown
}

interface GenericHitlFormProps {
  schema: JsonSchema
  /**
   * Omitting `onSubmit` means this build has no endpoint for the request type.
   * The form then renders read-only -- every control disabled, Submit disabled,
   * and a visible notice -- rather than accepting a click it would discard.
   */
  onSubmit?: (data: Record<string, unknown>) => void
  onCancel?: () => void
  /** Overrides the default read-only notice text. */
  unsupportedReason?: string
}

export function GenericHitlForm({ schema, onSubmit, onCancel, unsupportedReason }: GenericHitlFormProps) {
  const [formData, setFormData] = useState<Record<string, unknown>>({})
  const [errors, setErrors] = useState<Record<string, string>>({})
  const submitUnsupported = typeof onSubmit !== 'function'

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    // A disabled button is not enough on its own: this also blocks implicit
    // submission (Enter in a text field) and any programmatic requestSubmit().
    if (!onSubmit) return
    
    // Validate required fields
    const newErrors: Record<string, string> = {}
    if (schema.required) {
      for (const field of schema.required) {
        if (!formData[field]) {
          newErrors[field] = `${field} is required`
        }
      }
    }

    if (Object.keys(newErrors).length > 0) {
      setErrors(newErrors)
      return
    }

    onSubmit(formData)
  }

  const renderField = (name: string, prop: JsonSchemaProperty) => {
    const value = formData[name]
    const error = errors[name]

    // Handle enum (dropdown)
    if (prop.enum && prop.enum.length > 0) {
      return (
        <div key={name} className="space-y-2">
          <Label htmlFor={name}>
            {name.replace(/_/g, ' ')}
            {schema.required?.includes(name) && <span className="text-red-500">*</span>}
          </Label>
          {prop.description && (
            <p className="text-xs text-muted-foreground">{prop.description}</p>
          )}
          <Select
            value={value as string}
            onValueChange={(val) => setFormData({ ...formData, [name]: val })}
            disabled={submitUnsupported}
          >
            <SelectTrigger id={name}>
              <SelectValue placeholder="Select an option" />
            </SelectTrigger>
            <SelectContent>
              {prop.enum.map((option) => (
                <SelectItem key={option} value={option}>
                  {option}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {error && (
            <p className="text-xs text-red-500">{error}</p>
          )}
        </div>
      )
    }

    // Handle array of strings (multi-input)
    if (prop.type === 'array' && prop.items?.type === 'string') {
      return (
        <div key={name} className="space-y-2">
          <Label htmlFor={name}>
            {name.replace(/_/g, ' ')}
            {schema.required?.includes(name) && <span className="text-red-500">*</span>}
          </Label>
          {prop.description && (
            <p className="text-xs text-muted-foreground">{prop.description}</p>
          )}
          <Input
            id={name}
            type="text"
            disabled={submitUnsupported}
            placeholder="Enter comma-separated values"
            value={Array.isArray(value) ? value.join(', ') : ''}
            onChange={(e) => {
              const values = e.target.value.split(',').map((v) => v.trim()).filter(Boolean)
              setFormData({ ...formData, [name]: values })
            }}
          />
          {error && (
            <p className="text-xs text-red-500">{error}</p>
          )}
        </div>
      )
    }

    // Handle boolean (checkbox)
    if (prop.type === 'boolean') {
      return (
        <div key={name} className="flex items-center space-x-2">
          <Checkbox
            id={name}
            disabled={submitUnsupported}
            checked={value as boolean}
            onCheckedChange={(checked) => setFormData({ ...formData, [name]: checked })}
          />
          <Label htmlFor={name} className="text-sm font-normal cursor-pointer">
            {name.replace(/_/g, ' ')}
            {prop.description && ` - ${prop.description}`}
          </Label>
        </div>
      )
    }

    // Handle number
    if (prop.type === 'number' || prop.type === 'integer') {
      return (
        <div key={name} className="space-y-2">
          <Label htmlFor={name}>
            {name.replace(/_/g, ' ')}
            {schema.required?.includes(name) && <span className="text-red-500">*</span>}
          </Label>
          {prop.description && (
            <p className="text-xs text-muted-foreground">{prop.description}</p>
          )}
          <Input
            id={name}
            type="number"
            disabled={submitUnsupported}
            value={value as number}
            onChange={(e) => {
              const num = prop.type === 'integer' 
                ? parseInt(e.target.value, 10) 
                : parseFloat(e.target.value)
              setFormData({ ...formData, [name]: num })
            }}
          />
          {error && (
            <p className="text-xs text-red-500">{error}</p>
          )}
        </div>
      )
    }

    // Default: string input
    return (
      <div key={name} className="space-y-2">
        <Label htmlFor={name}>
          {name.replace(/_/g, ' ')}
          {schema.required?.includes(name) && <span className="text-red-500">*</span>}
        </Label>
        {prop.description && (
          <p className="text-xs text-muted-foreground">{prop.description}</p>
        )}
        <Input
          id={name}
          type="text"
          disabled={submitUnsupported}
          value={value as string || ''}
          onChange={(e) => setFormData({ ...formData, [name]: e.target.value })}
        />
        {error && (
          <p className="text-xs text-red-500">{error}</p>
        )}
      </div>
    )
  }

  if (!schema || !schema.properties) {
    return (
      <Alert variant="destructive">
        <AlertCircle className="h-4 w-4" />
        <AlertDescription>Invalid schema provided</AlertDescription>
      </Alert>
    )
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      {submitUnsupported ? (
        <Alert variant="destructive" role="alert">
          <AlertCircle className="h-4 w-4" />
          <AlertDescription>{unsupportedReason || UNSUPPORTED_HITL_MESSAGE}</AlertDescription>
        </Alert>
      ) : null}

      {schema.description && (
        <p className="text-sm text-muted-foreground">{schema.description}</p>
      )}

      {Object.entries(schema.properties).map(([name, prop]) => renderField(name, prop))}

      <div className="flex gap-2 pt-2">
        <Button type="submit" disabled={submitUnsupported}>Submit</Button>
        {onCancel && (
          <Button type="button" variant="outline" onClick={onCancel}>
            Cancel
          </Button>
        )}
      </div>
    </form>
  )
}

