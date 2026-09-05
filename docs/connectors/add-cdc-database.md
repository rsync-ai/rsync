# Adding a New Database Type to Debezium CDC

**Purpose**: Step-by-step checklist for adding support for a new database type to the rsync-ai CDC system.

**Example**: Adding PostgreSQL CDC support (though it's already implemented, this serves as a template)

---

## Prerequisites

- [ ] Debezium connector for the database exists (check https://debezium.io/documentation/reference/stable/connectors/)
- [ ] Debezium connector plugin is available on Maven Central
- [ ] Database has CDC capability (write-ahead log, binlog, oplog, etc.)
- [ ] Docker image for the database exists for local testing

---

## Implementation Checklist

### 1. Update Debezium MCP Connector Metadata

**File**: `shared/mcp-connectors/internal/debezium/versions/v1.0.0/metadata.json`

> Paths under `versions/` resolve through `latest.json`: the canonical directory is
> `versions/<latest.json .current_version>/`, **not** the highest-numbered one, and there are
> no root copies. `v1.0.0` above is debezium's current version at the time of writing.

- [ ] Add database to `supported_databases` section:
  ```json
  {
    "supported_databases": {
      "postgresql": {
        "connector_class": "io.debezium.connector.postgresql.PostgresConnector",
        "cdc_position_method": "lsn"
      }
    }
  }
  ```
- [ ] Specify the correct `connector_class` (full Java class name from Debezium docs)
- [ ] Specify the `cdc_position_method` (e.g., `lsn`, `gtid`, `scn`, `oplog`)

---

### 2. Update Debezium MCP Connector Logic

**File**: `shared/mcp-connectors/internal/debezium/versions/v1.0.0/connector.py`

- [ ] Add database-specific configuration logic to `start_sync` method:
  ```python
  elif database_type == "postgresql":
      connector_config.update({
          "database.hostname": db_host,
          "database.port": db_port or 5432,
          "database.user": db_user,
          "database.password": db_password,
          "database.dbname": db_name,
          "table.include.list": table_include_list,
          "plugin.name": "pgoutput",  # PostgreSQL-specific
          "publication.name": f"dbz_{pipeline_id}_pub",  # PostgreSQL-specific
          "slot.name": f"dbz_{pipeline_id}_slot",  # PostgreSQL-specific
      })
  ```
- [ ] Research database-specific required properties from Debezium docs
- [ ] Handle database-specific CDC configuration (e.g., PostgreSQL publications/slots, MySQL binlog)
- [ ] Add database-specific connection parameter mapping

---

### 3. Update Kafka Connect Dockerfile

**File**: `shared/internal/infra/kafka-connect/Dockerfile`

- [ ] Add `RUN curl` command to download Debezium connector plugin:
  ```dockerfile
  RUN curl -o /tmp/debezium-postgresql.tar.gz \
      https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgresql/${DEBEZIUM_VERSION}/debezium-connector-postgresql-${DEBEZIUM_VERSION}-plugin.tar.gz \
      && tar -xzf /tmp/debezium-postgresql.tar.gz -C /usr/share/java/ \
      && rm /tmp/debezium-postgresql.tar.gz
  ```
- [ ] Verify the Maven Central URL exists (check https://repo1.maven.org/maven2/io/debezium/)
- [ ] Use `${DEBEZIUM_VERSION}` variable for consistency

---

### 4. Add E2E Database Service

**File**: `docker-compose.e2e.yml`

- [ ] Add database service for E2E testing:
  ```yaml
  postgresql-e2e:
    image: postgres:17
    container_name: rsync-ai-postgres-e2e
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres_password
      POSTGRES_DB: e2e_db
    command:
      - "postgres"
      - "-c"
      - "wal_level=logical"  # PostgreSQL-specific
      - "-c"
      - "max_replication_slots=10"  # PostgreSQL-specific
    ports:
      - "5433:5432"
    networks:
      - rsync-ai-network
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 10
  ```
- [ ] Configure database for CDC (e.g., `wal_level=logical` for PostgreSQL, `binlog` for MySQL)
- [ ] Use a non-conflicting port for E2E testing
- [ ] Add appropriate healthcheck command

---

### 5. Create E2E Test Script

**File**: `e2e/test_postgresql_cdc_debezium.sh`

- [ ] Copy `e2e/test_mysql_cdc_debezium.sh` as a template
- [ ] Update database connection parameters (host, port, user, password, database)
- [ ] Update database-specific CDC setup (e.g., create publication/slot for PostgreSQL)
- [ ] Update connector configuration with database-specific properties
- [ ] Update SQL commands for the database dialect
- [ ] Test INSERT, UPDATE, DELETE operations
- [ ] Verify CDC events in Kafka topic
- [ ] Add cleanup logic (drop publication/slot for PostgreSQL)

**Key sections to update**:
```bash
# Database connection
DB_HOST="postgresql-e2e"
DB_PORT="5432"
DB_USER="postgres"
DB_PASSWORD="postgres_password"
DB_NAME="e2e_db"

# PostgreSQL-specific: Create publication
psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" <<EOF
CREATE PUBLICATION dbz_pub FOR ALL TABLES;
EOF

# Debezium connector config
CREATE_PAYLOAD=$(cat <<EOF
{
  "name": "postgresql-cdc-test",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "$DB_HOST",
    "database.port": "$DB_PORT",
    "database.user": "$DB_USER",
    "database.password": "$DB_PASSWORD",
    "database.dbname": "$DB_NAME",
    "table.include.list": "$DB_NAME.$TABLE",
    "plugin.name": "pgoutput",
    "publication.name": "dbz_pub",
    "slot.name": "dbz_slot",
    ...
  }
}
EOF
)
```

---

### 6. Update Planner CDC Config Generator (Optional)

**File**: `llm-service/src/agents/planner/cdc_config_generator.py`

- [ ] Add database-specific template to `_get_database_specific_config` method (if not already generic)
- [ ] Add validation rules for the database type
- [ ] Update LLM prompt if using LLM generation fallback

---

### 7. Connection form fields (no frontend change)

The connection form is **generic**: `frontend/src/components/connectors/GenericConnectorForm.tsx`
renders its fields from `connector.configuration_schema.properties` (`GenericConnectorForm.tsx:266`, `:373`) and contains no hardcoded database list. The api-gateway
maps the connector's own `config_schema` onto that wire field (`api-gateway/internal/handlers/tools.go:1448`).

So there is no frontend file to edit — add the fields to the connector instead:

- [ ] Add any database-specific connection fields to `config_schema` in the connector's `versions/<current_version>/metadata.json`
- [ ] Add database-specific CDC configuration fields there too (e.g. publication name for PostgreSQL)
- [ ] Mark which of them are `required`, and set `ui_tier: "advanced"` on the ones that should stay collapsed by default

---

## Testing Checklist

### Unit Tests

- [ ] Test Debezium MCP connector `start_sync` with new database type
- [ ] Test `CDCProviderRegistry.get_cdc_provider("new_db")` returns "debezium"
- [ ] Test `CDCProviderRegistry.get_provider_config("new_db")` returns correct connector class

### Integration Tests

- [ ] Rebuild Kafka Connect image: `docker compose build kafka-connect`
- [ ] Restart services: `docker compose restart kafka-connect planner`
- [ ] Verify connector plugin installed: `docker compose exec kafka-connect ls /usr/share/java/ | grep debezium-new-db`
- [ ] Verify planner discovery: `curl http://localhost:5011/intelligence/cdc-providers | jq '.cdc_capable_sources'`

### E2E Tests

- [ ] Run E2E test script: `bash e2e/test_postgresql_cdc_debezium.sh`
- [ ] Verify database container starts and is healthy
- [ ] Verify Kafka Connect creates the Debezium connector successfully
- [ ] Verify connector reaches `RUNNING` state
- [ ] Verify INSERT event appears in Kafka topic
- [ ] Verify UPDATE event appears in Kafka topic (if applicable)
- [ ] Verify DELETE event appears in Kafka topic (if applicable)
- [ ] Verify cleanup completes without errors

### End-to-End Pipeline Test

- [ ] Create connection via UI or API
- [ ] Create CDC pipeline from new database to S3/Snowflake
- [ ] Verify planner generates correct plan with `tool: "debezium"`, `method: "start_sync"`
- [ ] Verify orchestrator calls Debezium MCP connector with correct `database_type`
- [ ] Verify Debezium connector is created in Kafka Connect
- [ ] Verify CDC events flow to destination
- [ ] Verify pipeline can be stopped and restarted

---

## Troubleshooting Checklist

### Kafka Connect Issues

- [ ] Check Kafka Connect logs: `docker compose logs kafka-connect --tail 100`
- [ ] Verify plugin installed: `docker compose exec kafka-connect ls /usr/share/java/`
- [ ] Check connector status: `curl http://localhost:8083/connectors/connector-name/status | jq .`
- [ ] Check connector config: `curl http://localhost:8083/connectors/connector-name/config | jq .`

### Database CDC Issues

- [ ] Verify database CDC is enabled (e.g., `SHOW VARIABLES LIKE 'log_bin'` for MySQL)
- [ ] Verify database user has CDC permissions (e.g., `REPLICATION SLAVE` for MySQL)
- [ ] Check database logs for CDC-related errors
- [ ] Verify database is accessible from Kafka Connect container

### Planner Discovery Issues

- [ ] Check planner logs: `docker compose logs planner --tail 100 | grep CDC`
- [ ] Verify `metadata.json` is valid JSON: `jq . shared/mcp-connectors/internal/debezium/versions/v1.0.0/metadata.json`
- [ ] Restart planner: `docker compose restart planner`
- [ ] Query discovery endpoint: `curl http://localhost:5011/intelligence/cdc-providers | jq .`

---

## Rollback Plan

If the new database type causes issues:

1. **Revert `metadata.json`**:
   ```bash
   git checkout shared/mcp-connectors/internal/debezium/versions/v1.0.0/metadata.json
   ```

2. **Revert `connector.py`**:
   ```bash
   git checkout shared/mcp-connectors/internal/debezium/versions/v1.0.0/connector.py
   ```

3. **Rebuild Kafka Connect** (without new plugin):
   ```bash
   docker compose build kafka-connect
   docker compose restart kafka-connect planner
   ```

4. **Verify system still works**:
   ```bash
   bash e2e/test_mysql_cdc_debezium.sh
   ```

---

## Documentation

After successful implementation:

- [ ] Add the database to the prerequisites table in [`docs/connectors/cdc-source-prerequisites.md`](cdc-source-prerequisites.md) (what the operator must enable on the source before a pipeline can run)
- [ ] Record the offset/position semantics in [`docs/connectors/cdc-exactly-once-offsets.md`](cdc-exactly-once-offsets.md) if the new database's position type is not already covered
- [ ] Update README.md with supported database list

---

## Summary

**Time Estimate**: 2-4 hours per database type  
**Difficulty**: Medium (assuming Debezium connector exists and is well-documented)  
**Risk**: Low (changes are isolated to MCP connector and Kafka Connect plugin)

**Key Success Criteria**:
1. ✅ Planner discovery endpoint lists new database as CDC-capable
2. ✅ E2E test script passes for the new database
3. ✅ Full CDC pipeline (DB → Kafka → Destination) works end-to-end
4. ✅ System remains stable with existing databases (no regressions)


