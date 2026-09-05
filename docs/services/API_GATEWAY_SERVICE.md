# API Gateway Service Documentation

**Technology**: Go 1.24, Gin Framework, PostgreSQL
**Port**: 5001
**Directory**: `/api-gateway`

---

## Overview

The API Gateway is the central entry point for all client requests. It handles authentication, request routing, credential encryption, and coordination between frontend and backend services. Built in Go for high performance and low latency.

---

## Architecture

```
Client Request
      │
      ▼
┌─────────────────┐
│  API Gateway    │
│    (Go/Gin)     │
├─────────────────┤
│ - JWT Auth      │
│ - OAuth2 Flows  │
│ - Request Route │
│ - WebSocket Hub │
│ - Encryption    │
└────────┬────────┘
         │
    ┌────┴────┬────────────┐
    ▼         ▼            ▼
┌───────┐ ┌───────┐ ┌─────────────┐
│Temporal│ │ Kafka │ │ PostgreSQL  │
└───────┘ └───────┘ └─────────────┘
```

---

## Key Features

### 1. Authentication & Authorization

**JWT Token Authentication**
- Token generation on login
- Token validation middleware
- Refresh token support
- Session management

**OAuth2 Providers**
- GitHub - Repository access
- Google - Sheets, BigQuery
- Salesforce - CRM data
- HubSpot - Marketing data
- Slack - Team communication
- Pipedrive - Sales data

### 2. Connection Management

**Secure Credential Storage**
- AES-256 encryption at rest
- Encryption key shared with Temporal Adapter
- Never logged or exposed in responses

**Connection Testing**
- Validate before save
- Detailed error messages
- Timeout handling

### 3. Pipeline Operations

- Create, read, update, delete pipelines
- Trigger execution via Temporal
- Schedule management
- Execution history

### 4. Real-Time Updates

**WebSocket Hub**
- Client connection management
- Event broadcasting
- Heartbeat/ping-pong
- Automatic reconnection support

### 5. Data Exploration

- SQL query execution
- Schema caching
- Result pagination
- CSV export

---

## API Endpoints (80+)

### Authentication

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/auth/register` | User registration |
| POST | `/api/v1/auth/login` | User login |
| POST | `/api/v1/auth/logout` | User logout |
| GET | `/api/v1/auth/me` | Current user info |
| POST | `/api/v1/auth/refresh` | Refresh token |

### OAuth

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/oauth/:provider/authorize` | Get OAuth URL |
| POST | `/api/v1/oauth/:provider/callback` | Handle callback |
| DELETE | `/api/v1/oauth/:provider/revoke` | Revoke access |

### Connectors

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/connectors` | List all connectors |
| GET | `/api/v1/connectors/:name` | Get connector details |
| GET | `/api/v1/connectors/:name/logo` | Get connector logo |
| POST | `/api/v1/connectors/generate` | Generate new connector |
| POST | `/api/v1/connectors/validate` | Validate connector name |

### Connections

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/connections` | List user connections |
| POST | `/api/v1/connections` | Create connection |
| GET | `/api/v1/connections/:id` | Get connection |
| PUT | `/api/v1/connections/:id` | Update connection |
| DELETE | `/api/v1/connections/:id` | Delete connection |
| POST | `/api/v1/connections/:id/test` | Test connection |
| POST | `/api/v1/connections/test` | Test before save |

### Pipelines

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/pipelines` | List pipelines |
| POST | `/api/v1/pipelines` | Create pipeline |
| GET | `/api/v1/pipelines/:id` | Get pipeline |
| PUT | `/api/v1/pipelines/:id` | Update pipeline |
| DELETE | `/api/v1/pipelines/:id` | Delete pipeline |
| POST | `/api/v1/pipelines/:id/run` | Execute pipeline |
| POST | `/api/v1/pipelines/:id/stop` | Stop execution |
| GET | `/api/v1/pipelines/:id/executions` | Execution history |

### Pipeline Drafts

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/pipeline-drafts` | List drafts |
| POST | `/api/v1/pipeline-drafts` | Create draft |
| GET | `/api/v1/pipeline-drafts/:id` | Get draft |
| PUT | `/api/v1/pipeline-drafts/:id` | Update draft |
| DELETE | `/api/v1/pipeline-drafts/:id` | Delete draft |
| POST | `/api/v1/pipeline-drafts/:id/chat` | Chat with agent |
| POST | `/api/v1/pipeline-drafts/:id/promote` | Deploy to Temporal |

### Executions

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/executions` | List executions |
| GET | `/api/v1/executions/:id` | Get execution |
| POST | `/api/v1/executions/:id/cancel` | Cancel execution |
| GET | `/api/v1/executions/:id/logs` | Get logs |
| GET | `/api/v1/executions/:id/stages` | Get stage details |

### CDC (Change Data Capture)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/cdc/connectors` | List CDC connectors |
| POST | `/api/v1/cdc/connectors` | Create CDC connector |
| GET | `/api/v1/cdc/connectors/:name` | Get CDC status |
| DELETE | `/api/v1/cdc/connectors/:name` | Delete CDC connector |
| PUT | `/api/v1/cdc/connectors/:name/pause` | Pause CDC |
| PUT | `/api/v1/cdc/connectors/:name/resume` | Resume CDC |
| PUT | `/api/v1/cdc/connectors/:name/restart` | Restart CDC |
| GET | `/api/v1/cdc/connectors/:name/offsets` | Get offsets |
| POST | `/api/v1/cdc/connectors/:name/snapshot` | Trigger snapshot |

### Data Exploration

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/chat/message` | Send chat message |
| POST | `/api/v1/explorer/schema-index/:id/refresh` | Refresh schema |
| GET | `/api/v1/explorer/tables/retrieve` | Get tables |
| POST | `/api/v1/explorer/nl/resolve-tables` | NL to tables |
| POST | `/api/v1/explorer/nl/resolve-columns` | NL to columns |
| POST | `/api/v1/explorer/nl/next-steps` | Get suggestions |
| POST | `/api/v1/explorer/query` | Execute SQL |
| GET | `/api/v1/explorer/export` | Export results |
| POST | `/api/v1/explorer/export.csv` | Export as CSV |

### Monitoring

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/ws` | WebSocket connection |
| GET | `/metrics` | Prometheus metrics |

---

## Request/Response Examples

### Create Pipeline

**Request**:
```http
POST /api/v1/pipelines
Authorization: Bearer <token>
Content-Type: application/json

{
  "name": "MySQL to S3 Daily",
  "source_connection_id": "conn_123",
  "destination_connection_id": "conn_456",
  "tables": ["users", "orders"],
  "schedule": "0 0 * * *"
}
```

**Response**:
```json
{
  "id": "pipe_789",
  "name": "MySQL to S3 Daily",
  "status": "draft",
  "created_at": "2026-01-26T10:00:00Z"
}
```

### Test Connection

**Request**:
```http
POST /api/v1/connections/test
Authorization: Bearer <token>
Content-Type: application/json

{
  "connector_type": "mysql",
  "config": {
    "host": "mysql.example.com",
    "port": 3306,
    "database": "production",
    "username": "reader",
    "password": "***"
  }
}
```

**Response**:
```json
{
  "success": true,
  "message": "Connection successful",
  "latency_ms": 45,
  "server_version": "8.0.32"
}
```

---

## Middleware Chain

1. **CORS** - Cross-origin request handling
2. **Recovery** - Panic recovery
3. **Logger** - Request logging
4. **Auth** - JWT validation
5. **RateLimit** - Request throttling
6. **Trace** - OpenTelemetry context

---

## Configuration

### Environment Variables

```env
# Server
PORT=5001
GIN_MODE=release

# Database
DATABASE_URL=postgres://user:pass@localhost:5432/rsync_db

# Authentication
JWT_SECRET=your-secret-key
JWT_EXPIRY=24h

# Encryption
ENCRYPTION_KEY=***REMOVED***

# OAuth
GITHUB_CLIENT_ID=xxx
GITHUB_CLIENT_SECRET=xxx
GOOGLE_CLIENT_ID=xxx
GOOGLE_CLIENT_SECRET=xxx

# Services
TEMPORAL_ADDRESS=localhost:7233
KAFKA_BROKERS=localhost:9092
LLM_SERVICE_URL=http://localhost:5010

# Observability
OTEL_EXPORTER_ENDPOINT=localhost:14317
```

---

## Error Handling

### Standard Error Response

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid connection configuration",
    "details": {
      "field": "port",
      "reason": "must be between 1 and 65535"
    }
  }
}
```

### Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `UNAUTHORIZED` | 401 | Invalid or missing token |
| `FORBIDDEN` | 403 | Insufficient permissions |
| `NOT_FOUND` | 404 | Resource not found |
| `VALIDATION_ERROR` | 400 | Invalid input |
| `CONFLICT` | 409 | Resource already exists |
| `INTERNAL_ERROR` | 500 | Server error |

---

## WebSocket Events

### Client → Server

```json
{
  "type": "subscribe",
  "channel": "pipeline.status",
  "pipeline_id": "pipe_123"
}
```

### Server → Client

```json
{
  "type": "pipeline.status",
  "pipeline_id": "pipe_123",
  "status": "running",
  "stage": "discovery",
  "progress": 45
}
```

---

## Handlers Reference

| File | Responsibility |
|------|----------------|
| `auth.go` | User authentication |
| `connections.go` | Connection CRUD |
| `tools.go` | Connector management |
| `pipeline_*.go` | Pipeline operations |
| `domain_events.go` | Event streaming |
| `executions.go` | Execution tracking |
| `chat_nl_pipeline.go` | NL pipeline creation |
| `suggestions.go` | Smart suggestions |
| `transforms.go` | Data transformations |
| `checkpoints.go` | Execution checkpoints |
| `reconciler.go` | Data reconciliation |
| `monitoring.go` | Health & metrics |

---

## Demo Highlights

1. **API Explorer** - Use Swagger/OpenAPI UI
2. **Connection Test** - Show real-time validation
3. **OAuth Flow** - Demonstrate provider authentication
4. **WebSocket** - Show live pipeline updates
5. **Error Handling** - Show detailed error messages

---

## Troubleshooting

### API Gateway not starting
```bash
docker-compose logs api-gateway
# Check for database connection issues
# Verify environment variables
```

### Authentication failures
```bash
# Check JWT_SECRET matches across services
# Verify token expiry
# Check user exists in database
```

### WebSocket disconnections
```bash
# Check Kafka connectivity
# Verify Redis is running
# Check firewall rules
```

---

*For more details, see the codebase at `/api-gateway`*
