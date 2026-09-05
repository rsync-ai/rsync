import { create } from 'zustand'

interface ConnectorConfigState {
  isOpen: boolean
  connectorType: string | null
  direction: "source" | "destination" | null
  pipelineId: string | null
  onSaveCallback: ((connectionId: string, config: Record<string, unknown>) => void) | null
  
  openModal: (data: {
    connectorType: string
    direction: "source" | "destination"
    pipelineId?: string
    onSave: (connectionId: string, config: Record<string, unknown>) => void
  }) => void
  
  closeModal: () => void
}

export const useConnectorConfigModal = create<ConnectorConfigState>((set) => ({
  isOpen: false,
  connectorType: null,
  direction: null,
  pipelineId: null,
  onSaveCallback: null,
  
  openModal: (data) => set({
    isOpen: true,
    connectorType: data.connectorType,
    direction: data.direction,
    pipelineId: data.pipelineId,
    onSaveCallback: data.onSave,
  }),
  
  closeModal: () => set({
    isOpen: false,
    connectorType: null,
    direction: null,
    pipelineId: null,
    onSaveCallback: null,
  }),
}))

