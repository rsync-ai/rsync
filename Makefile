.PHONY: help dev up down restart logs clean test-all check-ports stop-all \
        infra services backend frontend health watch rebuild migrate \
        backend-build sqlc-generate api-build context7-build init setup env-check \
        e2e-start e2e-stop e2e-clean e2e-test e2e-gate e2e-guard e2e-mysql e2e-postgres e2e-logs e2e-status \
        staging-up staging-check staging-guard connector-reference

# ============================================================================
# RSYNC AI - Makefile
# ============================================================================
# Single entry point for all development operations
# ============================================================================

# Service groups for selective startup
INFRA_SERVICES := postgres kafka schema-registry redis
BACKEND_SERVICES := orchestrator api-gateway llm-service tool-generator planner sentinel telemetry-agent
MCP_SERVICES :=
SUPPORT_SERVICES := fluent-bit otel-collector
ALL_SERVICES := $(INFRA_SERVICES) $(BACKEND_SERVICES) $(MCP_SERVICES) $(SUPPORT_SERVICES) frontend

# Colors for output
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
CYAN := \033[0;36m
NC := \033[0m # No Color

# ============================================================================
# HELP
# ============================================================================
help:
	@echo ""
	@echo "$(CYAN)╔══════════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(CYAN)║           RSYNC AI - Development Commands                        ║$(NC)"
	@echo "$(CYAN)╚══════════════════════════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(GREEN)🚀 QUICK START$(NC)"
	@echo "  $(YELLOW)make init$(NC)            - Initialize env files (first time setup)"
	@echo "  $(YELLOW)make dev$(NC)             - Start everything (recommended for development)"
	@echo "  $(YELLOW)make up$(NC)              - Start all Docker services in background"
	@echo "  $(YELLOW)make down$(NC)            - Stop all Docker services"
	@echo ""
	@echo "$(GREEN)🔧 SELECTIVE STARTUP$(NC)"
	@echo "  $(YELLOW)make infra$(NC)           - Start infrastructure only (postgres, kafka, redis, minio)"
	@echo "  $(YELLOW)make services$(NC)        - Start backend services (requires infra running)"
	@echo "  $(YELLOW)make backend$(NC)         - Start infra + backend services (no frontend)"
	@echo "  $(YELLOW)make frontend$(NC)        - Start frontend only (requires services running)"
	@echo ""
	@echo "$(GREEN)📊 MONITORING$(NC)"
	@echo "  $(YELLOW)make health$(NC)          - Check health status of all services"
	@echo "  $(YELLOW)make logs$(NC)            - View all logs (tail mode)"
	@echo "  $(YELLOW)make logs s=<service>$(NC) - View logs for specific service"
	@echo "  $(YELLOW)make watch$(NC)           - Watch logs with filtering"
	@echo "  $(YELLOW)make ps$(NC)              - Show running containers"
	@echo ""
	@echo "$(GREEN)🔄 OPERATIONS$(NC)"
	@echo "  $(YELLOW)make restart$(NC)         - Restart all services"
	@echo "  $(YELLOW)make restart s=<svc>$(NC) - Restart specific service"
	@echo "  $(YELLOW)make rebuild$(NC)         - Rebuild and restart all services"
	@echo "  $(YELLOW)make rebuild s=<svc>$(NC) - Rebuild specific service"
	@echo "  $(YELLOW)make stop-all$(NC)        - Force stop everything (Docker + processes)"
	@echo "  $(YELLOW)make clean$(NC)           - Remove containers, volumes, and temp files"
	@echo ""
	@echo "$(GREEN)🗄️  DATABASE$(NC)"
	@echo "  $(YELLOW)make migrate$(NC)         - Run database migrations"
	@echo "  $(YELLOW)make sqlc-generate$(NC)   - Generate Go code from SQL"
	@echo ""
	@echo "$(GREEN)🏗️  BUILD$(NC)"
	@echo "  $(YELLOW)make backend-build$(NC)   - Build backend orchestrator binary"
	@echo "  $(YELLOW)make api-build$(NC)       - Build API gateway binary"
	@echo "  $(YELLOW)make context7-build$(NC)  - Build Context7 MCP connector"
	@echo ""
	@echo "$(GREEN)🧪 TESTING$(NC)"
	@echo "  $(YELLOW)make test-all$(NC)        - Run all tests"
	@echo ""
	@echo "$(BOLD)E2E Testing:$(NC)"
	@echo "  $(YELLOW)make e2e-start$(NC)       - Start e2e test databases"
	@echo "  $(YELLOW)make e2e-stop$(NC)        - Stop e2e test databases"
	@echo "  $(YELLOW)make e2e-clean$(NC)       - Remove e2e databases + volumes"
	@echo "  $(YELLOW)make e2e-test$(NC)        - Run automated e2e tests"
	@echo "  $(YELLOW)make e2e-gate$(NC)        - Run the full data-pipeline gate (locked + self-cleaning)"
	@echo "  $(YELLOW)make e2e-guard$(NC)       - Reconcile shared-service wiring for local e2e (--fix)"
	@echo "  $(YELLOW)make e2e-mysql$(NC)       - Connect to e2e MySQL"
	@echo "  $(YELLOW)make e2e-postgres$(NC)    - Connect to e2e Postgres"
	@echo "  $(YELLOW)make e2e-logs$(NC)        - View e2e logs"
	@echo "  $(YELLOW)make e2e-status$(NC)      - Show e2e container status"
	@echo "  $(YELLOW)make check-ports$(NC)     - Check if required ports are available"
	@echo ""
	@echo "$(GREEN)📍 SERVICE URLS (when running)$(NC)"
	@echo "  Frontend:        http://localhost:3000"
	@echo "  API Gateway:     http://localhost:5001"
	@echo "  Orchestrator:    http://localhost:8081"
	@echo "  LLM Service:     http://localhost:5011"
	@echo "  Tool Generator:  http://localhost:5010"
	@echo "  Context7 MCP:    http://localhost:8087"
	@echo "  Kafka:           localhost:9092"
	@echo "  PostgreSQL:      localhost:5432"
	@echo "  Redis:           localhost:6379"
	@echo "  MinIO:           http://localhost:9000 (console: 9001)"
	@echo ""

# ============================================================================
# SETUP & INITIALIZATION
# ============================================================================

# Initialize environment files if they don't exist
init: env-check
	@echo "$(GREEN)✅ Environment initialized$(NC)"

setup: init
	@echo "$(CYAN)📦 Setting up development environment...$(NC)"
	@docker compose pull
	@echo "$(GREEN)✅ Setup complete. Run 'make dev' to start.$(NC)"

# Check and create missing env files
env-check:
	@echo "$(CYAN)🔍 Checking environment files...$(NC)"
	@if [ ! -f .env ]; then \
		echo "$(YELLOW)Creating .env...$(NC)"; \
		echo "# RSYNC AI Root Environment Variables" > .env; \
		echo "GITHUB_CLIENT_ID=" >> .env; \
		echo "GITHUB_CLIENT_SECRET=" >> .env; \
		echo "GOOGLE_CLIENT_ID=" >> .env; \
		echo "GOOGLE_CLIENT_SECRET=" >> .env; \
		echo "HUBSPOT_CLIENT_ID=" >> .env; \
		echo "HUBSPOT_CLIENT_SECRET=" >> .env; \
		echo "SALESFORCE_CLIENT_ID=" >> .env; \
		echo "SALESFORCE_CLIENT_SECRET=" >> .env; \
		echo "CONTEXT7_API_KEY=" >> .env; \
	fi
	@if [ ! -f llm-service/.env ]; then \
		echo "$(YELLOW)Creating llm-service/.env...$(NC)"; \
		echo "# LLM Service Environment Variables" > llm-service/.env; \
		echo "OPENAI_API_KEY=" >> llm-service/.env; \
		echo "ANTHROPIC_API_KEY=" >> llm-service/.env; \
		echo "LLM_PROVIDER=openai" >> llm-service/.env; \
		echo "LLM_MODEL=gpt-4o-mini" >> llm-service/.env; \
		echo "CONTEXT7_MCP_URL=http://context7-mcp:8080" >> llm-service/.env; \
		echo "OLLAMA_URL=http://host.docker.internal:11434" >> llm-service/.env; \
	fi
	@if [ ! -f frontend/.env ]; then \
		echo "$(YELLOW)Creating frontend/.env...$(NC)"; \
		echo "# Frontend Environment Variables" > frontend/.env; \
		echo "NEXT_PUBLIC_API_URL=http://localhost:5001" >> frontend/.env; \
		echo "NEXT_PUBLIC_APP_NAME=Rsync AI" >> frontend/.env; \
		echo "NEXT_PUBLIC_ENVIRONMENT=development" >> frontend/.env; \
	fi
	@echo "$(GREEN)✓$(NC) All environment files present"

# ============================================================================
# MAIN COMMANDS
# ============================================================================

# Full development environment - starts everything
dev: env-check
	@echo "$(CYAN)🚀 Starting RSYNC AI Development Environment$(NC)"
	@echo "$(CYAN)==============================================$(NC)"
	@echo ""
	@echo "$(YELLOW)Step 1/4:$(NC) Starting infrastructure..."
	@docker compose up -d $(INFRA_SERVICES)
	@echo "$(GREEN)✓$(NC) Infrastructure started"
	@echo ""
	@echo "$(YELLOW)Step 2/4:$(NC) Waiting for infrastructure to be healthy..."
	@sleep 8
	@echo "$(GREEN)✓$(NC) Infrastructure ready"
	@echo ""
	@echo "$(YELLOW)Step 3/4:$(NC) Starting backend services..."
	@docker compose up -d $(BACKEND_SERVICES) $(MCP_SERVICES) $(SUPPORT_SERVICES)
	@echo "$(GREEN)✓$(NC) Backend services started"
	@echo ""
	@echo "$(YELLOW)Step 4/4:$(NC) Starting frontend..."
	@docker compose up -d frontend
	@echo "$(GREEN)✓$(NC) Frontend started"
	@echo ""
	@echo "$(CYAN)==============================================$(NC)"
	@echo "$(GREEN)✅ All services started successfully!$(NC)"
	@echo ""
	@$(MAKE) --no-print-directory urls
	@echo ""
	@echo "$(YELLOW)💡 Tips:$(NC)"
	@echo "  • Check status:  make health"
	@echo "  • View logs:     make logs"
	@echo "  • Stop all:      make down"
	@echo ""

# Start all services (simple docker compose up)
up:
	@echo "$(CYAN)🚀 Starting all RSYNC AI services...$(NC)"
	@docker compose up -d
	@echo "$(GREEN)✅ All services started$(NC)"
	@$(MAKE) --no-print-directory urls

# Stop all services
down:
	@echo "$(YELLOW)🛑 Stopping all services...$(NC)"
	@docker compose down
	@echo "$(GREEN)✅ All services stopped$(NC)"

# ============================================================================
# SELECTIVE STARTUP
# ============================================================================

# Infrastructure only (databases, message queues, storage)
infra:
	@echo "$(CYAN)🏗️  Starting infrastructure services...$(NC)"
	@docker compose up -d $(INFRA_SERVICES)
	@echo "$(GREEN)✅ Infrastructure started$(NC)"
	@echo ""
	@echo "Services: postgres, kafka, schema-registry, redis, minio"

# Backend services only (requires infra)
services:
	@echo "$(CYAN)⚙️  Starting backend services...$(NC)"
	@docker compose up -d $(BACKEND_SERVICES) $(MCP_SERVICES) $(SUPPORT_SERVICES)
	@echo "$(GREEN)✅ Backend services started$(NC)"

# Backend = infra + services (no frontend)
backend: infra
	@sleep 5
	@$(MAKE) --no-print-directory services

# Frontend only
frontend:
	@echo "$(CYAN)🖥️  Starting frontend...$(NC)"
	@docker compose up -d frontend
	@echo "$(GREEN)✅ Frontend started at http://localhost:3000$(NC)"

# ============================================================================
# MONITORING
# ============================================================================

# Health check for all services
health:
	@echo "$(CYAN)🏥 Service Health Check$(NC)"
	@echo "$(CYAN)========================$(NC)"
	@echo ""
	@echo "$(YELLOW)Docker Containers:$(NC)"
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || docker compose ps
	@echo ""
	@echo "$(YELLOW)Service Endpoints:$(NC)"
	@echo -n "  API Gateway (5001):    "; curl -s -o /dev/null -w "%{http_code}" http://localhost:5001/health 2>/dev/null || echo "DOWN"; echo ""
	@echo -n "  Orchestrator (8081):   "; curl -s -o /dev/null -w "%{http_code}" http://localhost:8081/health 2>/dev/null || echo "DOWN"; echo ""
	@echo -n "  LLM Service (5011):    "; curl -s -o /dev/null -w "%{http_code}" http://localhost:5011/health 2>/dev/null || echo "DOWN"; echo ""
	@echo -n "  Tool Generator (5010): "; curl -s -o /dev/null -w "%{http_code}" http://localhost:5010/health 2>/dev/null || echo "DOWN"; echo ""
	@echo -n "  Context7 MCP (8087):   "; curl -s -o /dev/null -w "%{http_code}" http://localhost:8087/health 2>/dev/null || echo "DOWN"; echo ""
	@echo -n "  Frontend (3000):       "; curl -s -o /dev/null -w "%{http_code}" http://localhost:3000 2>/dev/null || echo "DOWN"; echo ""
	@echo ""

# View logs
logs:
ifdef s
	@docker compose logs -f --tail=100 $(s)
else
	@docker compose logs -f --tail=100
endif

# Watch logs with service filter
watch:
	@echo "$(CYAN)📊 Watching logs (Ctrl+C to stop)$(NC)"
	@docker compose logs -f --tail=50 api-gateway orchestrator llm-service tool-generator

# Show running containers
ps:
	@docker compose ps

# Print service URLs
urls:
	@echo "$(GREEN)📍 Service URLs:$(NC)"
	@echo "  Frontend:        http://localhost:3000"
	@echo "  API Gateway:     http://localhost:5001"
	@echo "  Orchestrator:    http://localhost:8081"
	@echo "  LLM Service:     http://localhost:5011"
	@echo "  Tool Generator:  http://localhost:5010"
	@echo "  Context7 MCP:    http://localhost:8087"

# ============================================================================
# OPERATIONS
# ============================================================================

# Restart services
restart:
ifdef s
	@echo "$(YELLOW)🔄 Restarting $(s)...$(NC)"
	@docker compose restart $(s)
else
	@echo "$(YELLOW)🔄 Restarting all services...$(NC)"
	@docker compose restart
endif
	@echo "$(GREEN)✅ Restart complete$(NC)"

# Rebuild services
rebuild:
ifdef s
	@echo "$(YELLOW)🏗️  Rebuilding $(s)...$(NC)"
	@docker compose build --no-cache $(s)
	@docker compose up -d $(s)
else
	@echo "$(YELLOW)🏗️  Rebuilding all services...$(NC)"
	@docker compose build --no-cache
	@docker compose up -d
endif
	@echo "$(GREEN)✅ Rebuild complete$(NC)"

# Force stop everything
stop-all:
	@echo "$(YELLOW)🛑 Force stopping all services...$(NC)"
	@docker compose down --remove-orphans 2>/dev/null || true
	@docker ps -q --filter "name=rsync" | xargs -r docker stop 2>/dev/null || true
	@echo "$(GREEN)✅ All services stopped$(NC)"

# Clean up everything
clean:
	@echo "$(YELLOW)🧹 Cleaning up...$(NC)"
	@docker compose down -v --remove-orphans
	@rm -rf backend-orchestrator/tmp
	@rm -rf api-gateway/tmp
	@echo "$(GREEN)✅ Cleanup complete$(NC)"

# Check ports
check-ports:
	@echo "$(CYAN)🔍 Checking port availability...$(NC)"
	@for port in 3000 5001 5010 5011 8081 8087 9092 5432 6379 9000; do \
		if lsof -i :$$port > /dev/null 2>&1; then \
			echo "  Port $$port: $(YELLOW)IN USE$(NC)"; \
		else \
			echo "  Port $$port: $(GREEN)AVAILABLE$(NC)"; \
		fi \
	done

# ============================================================================
# DATABASE
# ============================================================================

# Run database migrations
migrate:
	@echo "$(CYAN)🗄️  Running database migrations...$(NC)"
	@chmod +x scripts/migrate.sh
	@./scripts/migrate.sh
	@echo "$(GREEN)✅ Migrations complete$(NC)"

# Generate SQLC code
sqlc-generate:
	@echo "$(CYAN)📝 Generating SQLC code...$(NC)"
	@cd backend-orchestrator && sqlc generate
	@echo "$(GREEN)✅ SQLC generation complete$(NC)"

# ============================================================================
# BUILD
# ============================================================================

# Build backend orchestrator
backend-build:
	@echo "$(CYAN)🔨 Building backend orchestrator...$(NC)"
	@cd backend-orchestrator && go build -o bin/orchestrator ./cmd/orchestrator
	@echo "$(GREEN)✅ Backend orchestrator built$(NC)"

# Build API gateway
api-build:
	@echo "$(CYAN)🔨 Building API gateway...$(NC)"
	@cd api-gateway && go build -o bin/server ./cmd/server
	@echo "$(GREEN)✅ API gateway built$(NC)"

# Build Context7 MCP connector
context7-build:
	@echo "$(CYAN)🔨 Building Context7 MCP connector...$(NC)"
	@docker compose -p rsync-ai -f docker-compose.yml build context7-mcp
	@echo "$(GREEN)✅ Context7 MCP connector built$(NC)"

# ============================================================================
# DOCS
# ============================================================================

# Regenerate the connector catalogue in docs/connectors/reference.md from the
# connector tree. CI runs the same script with --check, so a connector added or
# retyped without running this turns the build red rather than rotting quietly.
connector-reference:
	@python3 scripts/generate_connector_reference.py

# ============================================================================
# TESTING
# ============================================================================

# Run all tests
test-all:
	@echo "$(CYAN)🧪 Running all tests...$(NC)"
	@echo ""
	@if [ -f .venv/bin/activate ]; then \
		source .venv/bin/activate && python3 e2e/test_pipeline_full.py; \
	else \
		python3 e2e/test_pipeline_full.py; \
	fi
	@echo ""
	@echo "$(GREEN)✅ All tests completed!$(NC)"

# ============================================================================
# E2E TESTING
# ============================================================================

e2e-start:
	@echo "$(CYAN)🚀 Starting E2E test databases...$(NC)"
	@docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml up -d
	@# The standalone minio is profile-gated (standalone-minio) because it shares
	@# the `minio` DNS alias with the main stack's rsync-ai-minio. Starting both
	@# round-robins claim-check reads/writes across two stores → NoSuchKey → DLQ →
	@# silent data drop. Only bring it up when the main stack is absent.
	@if docker ps --format '{{.Names}}' | grep -qx rsync-ai-minio; then \
		echo "$(YELLOW)⏭️  rsync-ai-minio (main stack) is running — NOT starting the standalone e2e minio.$(NC)"; \
		echo "   (it would hijack the 'minio' alias and split claim-check I/O across two stores)"; \
		echo "   Need standalone minio? Stop the main stack, or run it explicitly:"; \
		echo "     docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml --profile standalone-minio up -d minio minio-init"; \
	else \
		docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml --profile standalone-minio up -d minio minio-init; \
		echo "   MinIO:    localhost:9000/9001"; \
	fi
	@echo "$(GREEN)✅ E2E databases started$(NC)"
	@echo "   MySQL:    localhost:3307"
	@echo "   Postgres: localhost:5433"

e2e-stop:
	@echo "$(CYAN)🛑 Stopping E2E test databases...$(NC)"
	@docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml down
	@echo "$(GREEN)✅ E2E databases stopped$(NC)"

e2e-clean:
	@echo "$(CYAN)🧹 Removing E2E databases + volumes...$(NC)"
	@docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml down -v
	@echo "$(GREEN)✅ E2E databases removed$(NC)"

e2e-test:
	@echo "$(CYAN)🧪 Running E2E pipeline tests...$(NC)"
	@cd e2e && python3 test_pipeline_full.py

e2e-gate:
	@echo "$(CYAN)🚦 Running data-pipeline gate (locked, self-cleaning, wiring-guarded)...$(NC)"
	@E2E_BUILD=1 bash e2e/run_gate.sh

e2e-guard:
	@echo "$(CYAN)🔎 Reconciling shared-service wiring for local e2e...$(NC)"
	@scripts/preflight-e2e-runtime.sh --fix

smoke:
	@echo "$(CYAN)🚦 Running pipeline smoke test (mysql -> pg, row-count invariant)...$(NC)"
	@./scripts/smoke_pipeline_test.sh

e2e-mysql:
	@echo "$(CYAN)🔌 Connecting to E2E MySQL...$(NC)"
	@mysql -h 127.0.0.1 -P 3307 -u e2e_user -pe2e_password e2e_db

e2e-postgres:
	@echo "$(CYAN)🔌 Connecting to E2E Postgres...$(NC)"
	@psql -h 127.0.0.1 -p 5433 -U e2e_user -d e2e_db

e2e-logs:
	@docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml logs -f

e2e-status:
	@docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml ps

# ============================================================================
# STAGING (Azure-backed shared stack)
# ============================================================================
# The data-pipeline gate (e2e/run_gate.sh) and manual staging SHARE one Docker
# project (rsync-ai); a post-merge/nightly gate run leaves the shared containers
# on the base/e2e overlay (local pipeline_db). `make staging-up` restores them to
# the Azure `staging` overlay in one command (resolves the MAIN checkout, takes
# the shared-stack lock, brings up + reconciles wiring via the staging preflight).

staging-up:
	@scripts/staging-up.sh

# Report-only: is the running stack wired for Azure staging? (exit 1 on drift)
staging-check:
	@echo "$(CYAN)🔎 Checking running containers are wired for Azure staging...$(NC)"
	@scripts/preflight-staging-runtime.sh

# Reconcile in place: recreate only the services drifted onto base/e2e defaults.
staging-guard:
	@echo "$(CYAN)🔧 Reconciling staging wiring for Azure (recreate drifted services)...$(NC)"
	@scripts/preflight-staging-runtime.sh --fix

# ============================================================================
# ALIASES (for convenience)
# ============================================================================

start: dev
run: dev
status: health
log: logs
