## Frontend — HLD

### Purpose
The Frontend is the **Next.js UI** for rsync-ai:
- chat-driven pipeline creation,
- connector generation + configuration,
- pipeline monitoring and troubleshooting.

### Runtime Interface
- **Container**: `frontend` (compose service name)
- **Host port**: `3000`
- **Key pages**
  - `/chat` (NL-driven UX)
  - `/connectors` (connector catalog + tier/warnings)
  - `/connectors/generate` (generation flow + category hinting)

### Responsibilities
- UI state machine for NL pipeline creation and HITL steps.
- Connector lifecycle UX:
  - generate connector,
  - configure connection,
  - test connection,
  - run pipeline.
- Displays tier/QA warnings and export attempts (transparency for bronze connectors).

### Dependencies
- **API Gateway** (public): `NEXT_PUBLIC_API_URL` (default `http://localhost:5001`)
- **WebSocket**: `NEXT_PUBLIC_WS_URL` for streaming progress updates
- Internal server-side URLs (within docker network) for SSR calls:
  - `API_GATEWAY_INTERNAL_URL`, `ORCHESTRATOR_INTERNAL_URL`

### Observability
- Browser-side OTEL export endpoint (optional):
  - `NEXT_PUBLIC_OTEL_ENDPOINT`


