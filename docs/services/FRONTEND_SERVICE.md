# Frontend Service Documentation

**Technology**: Next.js 14, TypeScript, React, TailwindCSS, Radix UI
**Port**: 3000
**Directory**: `/frontend`

---

## Overview

The Frontend Service is a modern React application built with Next.js that provides the user interface for rsync-ai. It enables users to create data pipelines through natural language conversation, manage connections, monitor executions, and explore data using AI-powered SQL generation.

---

## Architecture

```
Frontend (Next.js)
├── Pages (App Router)
│   ├── Dashboard
│   ├── Pipelines
│   ├── Connections
│   ├── Connectors
│   ├── Explorer
│   └── Chat
├── Components
│   ├── UI (Radix-based)
│   ├── Pipeline Builder
│   ├── Execution Timeline
│   └── Chat Interface
├── Hooks
│   ├── useWebSocket
│   ├── usePipeline
│   └── useExplorer
└── Services
    ├── API Client
    └── WebSocket Manager
```

---

## Key Features

### 1. Natural Language Pipeline Creation

Users can describe pipelines in plain English through the chat interface:

```
User: "Sync all orders from MySQL to S3 daily"
```

The UI shows:
- Real-time AI processing status
- Intent parsing visualization
- Schema discovery progress
- HITL approval dialogs

### 2. Pipeline Management Dashboard

- **List View**: All pipelines with status, schedule, last run
- **Detail View**: Configuration, execution history, logs
- **Actions**: Run, pause, edit, delete
- **Filtering**: By status, connector type, schedule

### 3. Connection Management

- **Secure Credential Input**: Masked fields, validation
- **Connection Testing**: Real-time connectivity check
- **OAuth Flows**: GitHub, Google, Salesforce, HubSpot, Slack

### 4. Data Explorer (NL2SQL)

- **Natural Language Input**: Ask questions in English
- **Table Resolution**: AI identifies relevant tables
- **Column Mapping**: Smart column selection
- **Query Execution**: Run and view results
- **Export**: CSV, Metabase integration

### 5. Real-Time Updates

- **WebSocket Connection**: Live pipeline status
- **Event Streaming**: Stage-by-stage progress
- **Notifications**: Completion, errors, approvals needed

### 6. CDC Monitoring

- **Event Counts**: Real-time CDC statistics
- **Latency Metrics**: Source to destination delay
- **Topic Management**: View Kafka topics

---

## Pages & Routes

| Route | Description |
|-------|-------------|
| `/` | Dashboard with pipeline overview |
| `/pipelines` | Pipeline list and management |
| `/pipelines/[id]` | Pipeline detail view |
| `/pipelines/create` | Pipeline creation wizard |
| `/connections` | Connection management |
| `/connectors` | Connector catalog (17 pre-built + generated) |
| `/connectors/generate` | AI connector generation |
| `/explorer` | Data exploration with NL2SQL |
| `/cdc/new` | CDC pipeline creation |
| `/chat` | Direct agent conversation |
| `/admin` | System administration |
| `/pii` | PII scanning tool |
| `/transforms` | Data transformation rules |
| `/settings` | User preferences |
| `/executions` | Execution history |

---

## Key Components

### PipelineBuilder

Interactive component for creating pipelines step-by-step:
- Source selection
- Destination selection
- Table mapping
- Transformation rules
- Schedule configuration

### ExecutionTimeline

Visualizes pipeline execution progress:
- Stage indicators (intent → discovery → planning → execution)
- Time tracking per stage
- Error highlighting
- Row count display

### ChatInterface

Conversational UI for natural language interaction:
- Message history
- AI response streaming
- HITL action buttons
- Context preservation

### SchemaSelector

HITL component for approving discovered schemas:
- Table checkboxes
- Column selection
- Preview sample data
- Bulk select/deselect

### ConnectionForm

Secure credential input with validation:
- Dynamic fields based on connector type
- OAuth redirect handling
- Connection test integration
- Encrypted storage indicator

---

## State Management

### Local State (React useState/useReducer)
- Form inputs
- UI toggles
- Temporary selections

### Server State (React Query / SWR)
- Pipeline data
- Connection list
- Connector catalog
- Execution history

### Real-Time State (WebSocket)
- Pipeline status updates
- Stage progress
- CDC event counts

---

## API Integration

### REST API Client

```typescript
// Example API calls
api.pipelines.list()
api.pipelines.create(data)
api.pipelines.run(id)
api.connections.test(config)
api.explorer.query(sql)
```

### WebSocket Client

```typescript
// Real-time updates
ws.subscribe('pipeline.status', (event) => {
  updatePipelineStatus(event.pipelineId, event.status)
})

ws.subscribe('execution.progress', (event) => {
  updateStageProgress(event.executionId, event.stage, event.progress)
})
```

---

## Environment Variables

```env
NEXT_PUBLIC_API_URL=http://localhost:5001
NEXT_PUBLIC_WS_URL=ws://localhost:5001/ws
NEXT_PUBLIC_OAUTH_GITHUB_CLIENT_ID=xxx
NEXT_PUBLIC_OAUTH_GOOGLE_CLIENT_ID=xxx
```

---

## Demo Highlights

1. **Clean, Modern UI** - Professional appearance, responsive design
2. **Real-Time Updates** - Watch pipelines progress live
3. **HITL Dialogs** - Show approval workflows
4. **Dark/Light Mode** - User preference support
5. **Mobile Responsive** - Works on tablets

---

## Troubleshooting

### Frontend not loading
```bash
docker-compose logs frontend
docker-compose restart frontend
```

### WebSocket disconnected
- Check API Gateway is running
- Verify WS_URL environment variable
- Check browser console for errors

### Slow performance
- Check network tab for slow API calls
- Verify Redis cache is working
- Check for excessive re-renders (React DevTools)

---

## Development

```bash
# Start development server
cd frontend
npm install
npm run dev

# Build for production
npm run build

# Run tests
npm run test

# Run E2E tests
npm run test:e2e
```

---

*For more details, see the codebase at `/frontend`*
