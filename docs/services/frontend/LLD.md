## Frontend — LLD

### Repo Location
- `frontend/`

### Framework / Runtime
- Next.js (standalone build) running on Node.js
- Built via `npm run build` and served via `server.js` (standalone output)

### Key Areas (High Signal)
- `frontend/src/app/(dashboard)/chat/*`
  - chat UI flow and step timeline integration
- `frontend/src/app/(dashboard)/connectors/*`
  - connector list and generate pages
- `frontend/src/components/connectors/*`
  - connection modal and connector UI components (tier badges + warnings)
- `frontend/src/lib/types/*`
  - shared TS types for connector metadata (tier/QA fields)

### Backend Integration
- REST base URL: `NEXT_PUBLIC_API_URL`
- WebSocket: `NEXT_PUBLIC_WS_URL`
- The connector generation flow calls:
  - `POST /api/v1/connectors/detect-category`
  - `POST /api/v1/connectors/generate`

### Configuration (Env Vars)
- `NEXT_PUBLIC_API_URL`
- `NEXT_PUBLIC_WS_URL`
- `API_GATEWAY_INTERNAL_URL` (SSR)
- `ORCHESTRATOR_INTERNAL_URL` (SSR)
- `NEXTAUTH_URL`
- `NEXT_PUBLIC_OTEL_ENDPOINT` (optional)


