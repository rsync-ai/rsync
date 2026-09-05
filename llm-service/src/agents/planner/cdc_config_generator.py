#!/usr/bin/env python3
"""
CDC Configuration Generator
LLM-driven Debezium configuration generation for any database type.

This module replaces hardcoded switch statements in Go handlers with a 
metadata-driven approach that can generate CDC configs for any database.

Design Goals:
- ZERO code changes needed to add new CDC sources
- Metadata-driven configuration from connector metadata.json
- LLM fallback for completely unknown database types
- Consistent with the Generic Agent architecture

Usage:
    generator = CDCConfigGenerator()
    config = generator.generate_config(
        source_type="mysql",
        connection_config={"host": "...", "port": 3306, ...},
        tables=["users", "orders"],
        cdc_mode="initial"
    )
"""

import os

from src.utils.kafka_security import (
    brokers_from_env,
    debezium_schema_history_security,
)
import json
import time
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote_plus
from src.utils.kafka_topics import topic
from src.utils.connector_paths import iter_connector_dirs, resolve_current_dir

logger = logging.getLogger("cdc-config-generator")


@dataclass
class CDCConfigResult:
    """Result of CDC configuration generation"""
    success: bool
    config: Optional[Dict[str, Any]] = None
    connector_name: str = ""
    kafka_topic: str = ""
    error: Optional[str] = None
    method_used: str = "unknown"  # "metadata", "template", "llm"
    warnings: List[str] = field(default_factory=list)


class CDCConfigGenerator:
    """
    LLM-driven CDC configuration generator.
    Generates Debezium configs dynamically based on connector metadata.
    
    Priority Order:
    1. Connector metadata.json (preferred - fully declarative)
    2. Built-in templates (common databases)
    3. LLM generation (unknown databases)
    """
    
    # Debezium connector class mappings
    # This is the ONLY place database-specific connector classes are defined
    CONNECTOR_CLASSES = {
        "mysql": "io.debezium.connector.mysql.MySqlConnector",
        "postgresql": "io.debezium.connector.postgresql.PostgresConnector",
        "postgres": "io.debezium.connector.postgresql.PostgresConnector",
        "mongodb": "io.debezium.connector.mongodb.MongoDbConnector",
        "sqlserver": "io.debezium.connector.sqlserver.SqlServerConnector",
        "mssql": "io.debezium.connector.sqlserver.SqlServerConnector",
        "oracle": "io.debezium.connector.oracle.OracleConnector",
        "db2": "io.debezium.connector.db2.Db2Connector",
        "cassandra": "io.debezium.connector.cassandra.CassandraConnector",
        "vitess": "io.debezium.connector.vitess.VitessConnector",
        "spanner": "io.debezium.connector.spanner.SpannerConnector",
    }
    
    # Debezium's PostgreSQL connector class. A metadata template is recognised as
    # PostgreSQL-family by the class it actually loads, not only by the source
    # name: a new derivative (Neon, Supabase, AlloyDB, CockroachDB) ships a
    # metadata.json naming this class long before anyone remembers to add its
    # name to a list, and the publication rule below must hold for it on day one.
    POSTGRES_CONNECTOR_CLASS = "io.debezium.connector.postgresql.PostgresConnector"
    
    # Mirrors isPostgresFamily() in backend-orchestrator/internal/agents/executor/
    # executor.go (names normalised to lower_snake there and here). Adding a new
    # PostgreSQL derivative means adding it to BOTH.
    POSTGRES_FAMILY = {
        "postgresql",
        "postgres",
        "cockroachdb",
        "cockroach_db",
        "aurora_postgresql",
        "alloydb",
        "neon",
        "supabase",
    }
    
    # Default ports for common databases
    DEFAULT_PORTS = {
        "mysql": 3306,
        "postgresql": 5432,
        "postgres": 5432,
        "mongodb": 27017,
        "sqlserver": 1433,
        "mssql": 1433,
        "oracle": 1521,
        "db2": 50000,
        "cassandra": 9042,
    }
    
    def __init__(self, llm_client=None, connectors_dir: str = None):
        """
        Initialize the CDC Config Generator.
        
        Args:
            llm_client: Optional LLM client for fallback generation
            connectors_dir: Path to MCP connectors directory
        """
        self.llm_client = llm_client
        self.connectors_dir = connectors_dir or os.getenv(
            "CONNECTORS_DIR", 
            "/app/shared/mcp-connectors"
        )
        # KAFKA_BROKERS first, matching every other Kafka client in the product
        # (the Go services, the sink worker, the agents). Reading only
        # KAFKA_BOOTSTRAP_SERVERS meant a deployment that set KAFKA_BROKERS
        # pointed Debezium's schema history at the wrong cluster.
        self.kafka_bootstrap_servers = ",".join(
            brokers_from_env("kafka:29092")
        )
        self.connector_metadata: Dict[str, Dict] = {}
        self._load_connector_metadata()
    
    def _load_connector_metadata(self):
        """Load each connector's metadata.json from its current versioned dir.

        Both halves of the obvious walk are wrong for this layout, and the
        result was a map that was always empty:

          - connectors are not direct children of the root. They live under
            ``public/<id>``, ``public/<category>/<id>``, ``internal/<id>`` and
            ``oauth/<id>``, so ``connectors_path.iterdir()`` yields the category
            directories, not connectors.
          - there is no ``metadata.json`` at a connector root. The canonical copy
            is ``versions/<current_version>/metadata.json``; the root holds only
            ``latest.json``, the version pointer. (The old root-copy mechanism was
            deleted deliberately -- see the connector rules in CLAUDE.md.)

        A count of zero is not an error here, which is why this survived: an empty
        map makes the metadata-driven branch of generate_config simply never fire,
        and every source falls through to the built-in template. Resolution goes
        through src.utils.connector_paths so there is one place that knows the
        layout.
        """
        connectors_path = Path(self.connectors_dir)
        
        if not connectors_path.exists():
            logger.warning(f"Connectors directory not found: {self.connectors_dir}")
            return
        
        for connector_dir in iter_connector_dirs(connectors_path):
            metadata_file = resolve_current_dir(connector_dir) / "metadata.json"
            if not metadata_file.exists():
                continue
            try:
                with open(metadata_file) as f:
                    metadata = json.load(f)
                self.connector_metadata[connector_dir.name] = metadata
                
                # Check if connector has CDC-specific config template
                if metadata.get("supports_cdc"):
                    logger.debug(f"Loaded CDC-capable connector: {connector_dir.name}")
            except Exception as e:
                logger.warning(f"Failed to load metadata for {connector_dir.name}: {e}")
        
        logger.info(f"Loaded metadata for {len(self.connector_metadata)} connectors")
    
    def generate_config(
        self,
        source_type: str,
        connection_config: Dict[str, Any],
        tables: List[str],
        cdc_mode: str = "initial",
        connector_name: str = None,
        use_smt: bool = True,
        connection_name: str = None,
        pk_fields: List[str] = None,
        **overrides
    ) -> CDCConfigResult:
        """
        Generate Debezium connector configuration.
        
        Args:
            source_type: Database type (mysql, postgresql, mongodb, etc.)
            connection_config: Connection parameters (host, port, user, password, database)
            tables: List of tables to capture (can be ["db.table"] or ["table"])
            cdc_mode: CDC mode - "initial" (snapshot + stream) or "streaming_only" (no snapshot)
            connector_name: Optional connector name (auto-generated if not provided)
            use_smt: Whether to use SMT for connection-level topic routing (default: True)
            connection_name: Connection name for SMT topic naming
            pk_fields: Primary key fields for partition key generation
            **overrides: Additional Debezium config overrides
            
        Returns:
            CDCConfigResult with generated configuration
        """
        # Normalize source type
        source_type_lower = source_type.lower().replace("-", "_")
        
        # Extract database name
        db_name = self._extract_database_name(connection_config, tables)
        
        # Generate connector name if not provided
        if not connector_name:
            table_suffix = tables[0].replace(".", "_") if tables else "all"
            connector_name = f"cdc-{source_type_lower}-{db_name}-{table_suffix}-{int(time.time())}"
        
        # Map cdc_mode to Debezium snapshot.mode
        snapshot_mode = self._map_cdc_mode_to_snapshot(cdc_mode)
        
        logger.info(f"Generating CDC config for {source_type} (cdc_mode={cdc_mode}, snapshot_mode={snapshot_mode})")
        
        # Try generation methods in priority order
        result = None
        
        # 1. Try metadata-driven generation
        if source_type_lower in self.connector_metadata:
            metadata = self.connector_metadata[source_type_lower]
            if metadata.get("supports_cdc") and metadata.get("cdc_config_template"):
                result = self._generate_from_metadata(
                    source_type_lower, connection_config, tables, 
                    snapshot_mode, connector_name, metadata, overrides
                )
        
        # 2. Try built-in template
        if not result or not result.success:
            if source_type_lower in self.CONNECTOR_CLASSES:
                result = self._generate_from_template(
                    source_type_lower, connection_config, tables,
                    snapshot_mode, connector_name, overrides
                )
        
        # 3. Try LLM generation as fallback
        if not result or not result.success:
            if self.llm_client:
                result = self._generate_via_llm(
                    source_type, connection_config, tables,
                    snapshot_mode, connector_name, overrides
                )
        
        # If successful and SMT is enabled, add SMT configuration
        if result and result.success and use_smt:
            result = self._add_smt_configuration(
                result,
                source_type_lower,
                connection_name or db_name,
                # MongoDB's primary key is always _id, never id; defaulting to
                # ["id"] would point the partition-key SMT at a non-existent field.
                pk_fields or (["_id"] if source_type_lower == "mongodb" else ["id"]),
            )
            logger.info(f"SMT configuration added: topic={result.kafka_topic}")
        
        if result and result.success:
            return result
        
        # All methods failed
        return CDCConfigResult(
            success=False,
            error=f"Unknown CDC source type: {source_type}. "
                  f"Supported types: {list(self.CONNECTOR_CLASSES.keys())}. "
                  f"Add metadata.json with cdc_config_template or enable LLM fallback."
        )
    
    def _add_smt_configuration(
        self,
        result: CDCConfigResult,
        source_type: str,
        connection_name: str,
        pk_fields: List[str],
    ) -> CDCConfigResult:
        """
        Add SMT (Single Message Transform) configuration for connection-level topic routing.
        
        This configures:
        - TopicRouter: Routes all tables to a single cdc.{connection} topic
        - PartitionKeyHeader: Adds partition key headers for ordering
        
        Args:
            result: The base CDCConfigResult to enhance
            source_type: Database type
            connection_name: Name for topic routing
            pk_fields: Primary key fields for partition key
            
        Returns:
            Enhanced CDCConfigResult with SMT configuration
        """
        if not result.config:
            return result
        
        # Sanitize connection name for topic
        sanitized_connection = connection_name.lower().replace(" ", "-").replace("_", "-")
        
        # SMT configuration
        smt_config = {
            # Transform chain
            "transforms": "route,addPartitionKey,insertTimestamp",
            
            # TopicRouter - routes all tables to single topic
            "transforms.route.type": "com.rsync.kafka.smt.TopicRouter",
            "transforms.route.topic.format": topic(f"cdc.{sanitized_connection}"),
            "transforms.route.connection.name": connection_name,
            "transforms.route.preserve.original.topic": "true",
            
            # PartitionKeyHeader - adds partition key headers
            "transforms.addPartitionKey.type": "com.rsync.kafka.smt.PartitionKeyHeader",
            "transforms.addPartitionKey.pk.fields": ",".join(pk_fields),
            "transforms.addPartitionKey.pk.separator": "_",
            "transforms.addPartitionKey.key.format": "${connection}.${schema}.${table}",
            
            # Insert timestamp
            "transforms.insertTimestamp.type": "org.apache.kafka.connect.transforms.InsertField$Value",
            "transforms.insertTimestamp.timestamp.field": "rsync_processed_at",
            
            # JSON without a Schema Registry (JsonConverter embeds the schema inline per
            # message). schemas.enable MUST stay true: the kafka-mcp-sink derives the
            # destination column types from this per-field type envelope. With it false,
            # relational sinks fall back to all-TEXT columns and lose type fidelity (CDC-01).
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "true",
            "value.converter.schemas.enable": "true",
        }
        
        # Merge SMT config into result
        result.config.update(smt_config)
        
        # Update topic name to connection-level topic
        result.kafka_topic = topic(f"cdc.{sanitized_connection}")
        
        # Add SMT info to warnings (for visibility)
        result.warnings.append(f"SMT enabled: All tables route to topic '{result.kafka_topic}'")
        result.warnings.append(f"Partition key fields: {pk_fields}")
        
        return result
    
    @staticmethod
    def _pin_topic_prefix(config: dict, db_name: str) -> str:
        """Qualify ``topic.prefix`` in place and return it.

        Every non-SMT path predicts its ``kafka_topic`` as
        ``<topic.prefix>.<db>.<table>`` -- that is how Debezium derives the topic
        it writes. The prediction is only true if it is computed from the value
        that survived the override merge, so both happen here, off one variable.

        Qualification has to land on the prefix rather than on the predicted
        name: the topic is Debezium's to create, and namespacing only the
        prediction would make the sink subscribe to ``rsync.cdc-x.db.t`` while
        the data went to ``cdc-x.db.t``. ``topic()`` is idempotent, so a caller
        that already passed a qualified prefix is left alone.
        """
        prefix = topic(str(config.get("topic.prefix") or f"cdc-{db_name}"))
        config["topic.prefix"] = prefix
        return prefix

    @staticmethod
    def _pin_schema_history_topic(config: dict, connector_name: str) -> None:
        """Pin Debezium's schema-history topic to the platform-canonical name.

        Rewritten here rather than trusted to the per-family branches or to the
        model, for the same reason ``topic.prefix`` is: this name is not the
        connector's to choose. It has to match what the rest of the platform
        already assumes about it -- the confinement allowlist keys on the
        ``schemahistory.`` prefix (handlers/topology.go:135) and the real
        deploy path mints ``_qualify_topic(f"schemahistory.{connector}")``
        (debezium connector.py:528) -- so a second spelling invented here is
        a topic nothing else on the platform recognises.

        The two failures this closes are both delayed and both read as data
        loss rather than as a deploy error. Unqualified, the history topic sits
        outside the ``rsync.`` grant: on a cluster with a PREFIXED-only ACL the
        history client's PRODUCER is denied, and because its CONSUMER half runs
        only on task restart the connector snapshots and streams cleanly for
        weeks before failing to resume. Off-prefix it also escapes the topology
        confinement allowlist, so it is left behind on the customer's cluster
        when the pipeline is deleted.

        Only rewrites a key that is already present: the four history-using
        families (mysql, sqlserver, oracle, db2) set it and PostgreSQL has no
        schema history at all, so presence is the family test. ``topic()`` is
        idempotent, so an already-qualified value is left alone.
        """
        key = "schema.history.internal.kafka.topic"
        if not str(config.get(key) or "").strip():
            return
        config[key] = topic(f"schemahistory.{connector_name}")

    def _map_cdc_mode_to_snapshot(self, cdc_mode: str) -> str:
        """Map user-friendly cdc_mode to Debezium snapshot.mode"""
        mode_mapping = {
            "initial": "initial",           # Full snapshot first, then CDC
            "streaming_only": "never",      # No snapshot, stream from current position
            "schema_only": "schema_only",   # Schema snapshot only, then CDC
            "never": "never",               # Alias for streaming_only
        }
        return mode_mapping.get(cdc_mode.lower(), "initial")
    
    def _extract_database_name(self, config: Dict, tables: List[str]) -> str:
        """Extract database name from config or table names"""
        # Try config keys in order of preference
        for key in ["database", "db_name", "dbname", "database_name"]:
            if config.get(key):
                return config[key]
        
        # Try to extract from table name (format: db.table)
        if tables and "." in tables[0]:
            return tables[0].split(".")[0]
        
        return "default_db"
    
    def _generate_from_metadata(
        self,
        source_type: str,
        config: Dict,
        tables: List[str],
        snapshot_mode: str,
        connector_name: str,
        metadata: Dict,
        overrides: Dict
    ) -> CDCConfigResult:
        """Generate config from connector metadata.json template"""
        try:
            template = metadata.get("cdc_config_template", {})
            
            # Get connector class from metadata or fallback
            connector_class = template.get(
                "connector_class", 
                self.CONNECTOR_CLASSES.get(source_type)
            )
            
            if not connector_class:
                return CDCConfigResult(
                    success=False,
                    error=f"No connector class found for {source_type}"
                )
            
            # Build base config from template
            base_config = {
                "connector.class": connector_class,
                "tasks.max": template.get("tasks_max", "1"),
            }
            
            # Apply field mappings from template
            field_mappings = template.get("field_mappings", {})
            for debezium_key, config_keys in field_mappings.items():
                if isinstance(config_keys, str):
                    config_keys = [config_keys]
                for key in config_keys:
                    if config.get(key):
                        base_config[debezium_key] = str(config[key])
                        break
            
            # Add common config
            db_name = self._extract_database_name(config, tables)
            table_list = self._format_table_list(tables, source_type, db_name)
            
            base_config.update({
                "snapshot.mode": snapshot_mode,
                "topic.prefix": f"cdc-{db_name}",
                "table.include.list": table_list,
            })
            
            # Apply template defaults
            for key, value in template.get("defaults", {}).items():
                if key not in base_config:
                    base_config[key] = value
            
            # Debezium's schema-history client is a separate Kafka client inside
            # the connector task and does not inherit the Connect worker's
            # credentials. Its PRODUCER half runs during snapshot, its CONSUMER
            # half only on task restart — so an unconfigured history client on a
            # SASL cluster snapshots and streams cleanly for weeks and then
            # cannot resume, which reads as data loss months after this deploy.
            # Same call _generate_from_template makes; the two paths must never
            # drift. Empty dict for PLAINTEXT.
            # Placed before overrides so an explicit caller override still wins.
            base_config.update(debezium_schema_history_security())
            
            # A PostgreSQL-family template must NEVER let Debezium create the
            # publication: the orchestrator creates the publication BEFORE the
            # replication slot, and Debezium's default ("all") reverses that
            # order and drops rows silently (CDC-02). Enforced here rather than
            # trusted to every connector's metadata.json defaults.
            if (
                source_type in self.POSTGRES_FAMILY
                or base_config.get("connector.class") == self.POSTGRES_CONNECTOR_CLASS
            ):
                base_config["publication.autocreate.mode"] = "disabled"
            
            # Apply user overrides
            base_config.update(overrides)
            
            prefix = self._pin_topic_prefix(base_config, db_name)
            self._pin_schema_history_topic(base_config, connector_name)
            kafka_topic = f"{prefix}.{db_name}.{tables[0].split('.')[-1]}" if tables else prefix
            
            return CDCConfigResult(
                success=True,
                config=base_config,
                connector_name=connector_name,
                kafka_topic=kafka_topic,
                method_used="metadata"
            )
            
        except Exception as e:
            logger.error(f"Metadata-based generation failed: {e}")
            return CDCConfigResult(success=False, error=str(e))
    
    def _generate_from_template(
        self,
        source_type: str,
        config: Dict,
        tables: List[str],
        snapshot_mode: str,
        connector_name: str,
        overrides: Dict
    ) -> CDCConfigResult:
        """Generate config using built-in database-specific templates"""
        try:
            connector_class = self.CONNECTOR_CLASSES[source_type]
            
            # Extract connection parameters with fallbacks
            host = config.get("host") or config.get("db_host") or "localhost"
            port = config.get("port") or config.get("db_port") or self.DEFAULT_PORTS.get(source_type, 3306)
            user = config.get("username") or config.get("user") or config.get("db_user")
            password = config.get("password") or config.get("db_password")
            db_name = self._extract_database_name(config, tables)
            
            # Format table list for this database type
            table_list = self._format_table_list(tables, source_type, db_name)
            
            # Base config common to all Debezium connectors
            base_config = {
                "connector.class": connector_class,
                "tasks.max": "1",
                "snapshot.mode": snapshot_mode,
                "topic.prefix": f"cdc-{db_name}",
                "include.schema.changes": "true",
            }
            
            # Get database-specific config
            db_specific = self._get_database_specific_config(
                source_type, host, port, user, password, db_name, table_list
            )
            
            # Debezium's schema-history client is a separate Kafka client inside
            # the connector task and does not inherit the Connect worker's
            # credentials; on a SASL cluster it must be configured explicitly or
            # the connector fails after snapshot. Empty dict for PLAINTEXT.
            # Placed before overrides so an explicit caller override still wins.
            history_security = debezium_schema_history_security()

            # Merge configs
            final_config = {**base_config, **db_specific, **history_security, **overrides}
            
            # Calculate Kafka topic
            table_name = tables[0].split(".")[-1] if tables else "all"
            kafka_topic = f"{self._pin_topic_prefix(final_config, db_name)}.{db_name}.{table_name}"
            self._pin_schema_history_topic(final_config, connector_name)
            
            return CDCConfigResult(
                success=True,
                config=final_config,
                connector_name=connector_name,
                kafka_topic=kafka_topic,
                method_used="template"
            )
            
        except Exception as e:
            logger.error(f"Template-based generation failed: {e}")
            return CDCConfigResult(success=False, error=str(e))
    
    def _get_database_specific_config(
        self,
        source_type: str,
        host: str,
        port: int,
        user: str,
        password: str,
        db_name: str,
        table_list: str
    ) -> Dict[str, str]:
        """
        Get database-specific Debezium configuration.
        
        This is the ONLY method that contains database-specific logic.
        New databases can be added here or via metadata.json templates.
        """
        # Deterministic, collision-resistant server id derived from db_name —
        # NOT int(time.time()) (two connectors created in the same second would
        # collide, stalling one binlog stream). Mirrors debezium _stable_server_id.
        import hashlib as _hashlib
        server_id = str((int(_hashlib.sha1(f"dbserver-{db_name}".encode("utf-8")).hexdigest()[:8], 16) % 4_294_967_000) + 1)
        server_name = f"dbserver-{db_name}"
        
        configs = {
            "mysql": {
                "database.hostname": host,
                "database.port": str(port),
                "database.user": user,
                "database.password": password,
                "database.server.id": server_id,
                "database.server.name": server_name,
                "database.include.list": db_name,
                "table.include.list": table_list,
                "schema.history.internal.kafka.bootstrap.servers": self.kafka_bootstrap_servers,
                "schema.history.internal.kafka.topic": f"schema-changes.{db_name}",
            },
            
            "postgresql": {
                "database.hostname": host,
                "database.port": str(port),
                "database.user": user,
                "database.password": password,
                "database.dbname": db_name,
                "database.server.name": server_name,
                "table.include.list": table_list,
                "plugin.name": "pgoutput",
                "slot.name": f"debezium_{db_name.replace('-', '_').replace('.', '_')}",
                "publication.name": f"dbz_publication_{db_name.replace('-', '_').replace('.', '_')}",
                # MUST be disabled: the orchestrator creates the publication BEFORE the
                # replication slot. Debezium's default ("all") would make it create the
                # publication itself after decoding starts, reversing that order and
                # causing silent data loss (CDC-02). Never set to "filtered"/"all".
                "publication.autocreate.mode": "disabled",
            },
            
            "mongodb": {
                # Debezium 2.x+/3.x MongoDB uses a single connection URI; the legacy
                # mongodb.hosts / mongodb.name keys were removed and fail validation
                # on a modern image. topic.prefix (added by the SMT/base config)
                # serves as the logical name. capture.mode=change_streams_update_full
                # yields complete post-images so the sink's packed (_id + document)
                # upsert stays correct. (This generator is advisory; the live config
                # is built by the debezium MCP connector — keep them in lockstep.)
                "mongodb.connection.string": (
                    f"mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/?authSource=admin"
                    if user else f"mongodb://{host}:{port}/"
                ),
                "capture.mode": "change_streams_update_full",
                "database.include.list": db_name,
                "collection.include.list": table_list,
            },
            
            "sqlserver": {
                "database.hostname": host,
                "database.port": str(port),
                "database.user": user,
                "database.password": password,
                "database.dbname": db_name,
                "database.server.name": server_name,
                "table.include.list": table_list,
                "database.encrypt": "false",
                "schema.history.internal.kafka.bootstrap.servers": self.kafka_bootstrap_servers,
                "schema.history.internal.kafka.topic": f"schema-changes.{db_name}",
            },
            
            "oracle": {
                "database.hostname": host,
                "database.port": str(port),
                "database.user": user,
                "database.password": password,
                "database.dbname": db_name,
                "database.server.name": server_name,
                "table.include.list": table_list,
                "log.mining.strategy": "online_catalog",
                "schema.history.internal.kafka.bootstrap.servers": self.kafka_bootstrap_servers,
                "schema.history.internal.kafka.topic": f"schema-changes.{db_name}",
            },
            
            "db2": {
                "database.hostname": host,
                "database.port": str(port),
                "database.user": user,
                "database.password": password,
                "database.dbname": db_name,
                "database.server.name": server_name,
                "table.include.list": table_list,
                "schema.history.internal.kafka.bootstrap.servers": self.kafka_bootstrap_servers,
                "schema.history.internal.kafka.topic": f"schema-changes.{db_name}",
            },
            
            "cassandra": {
                "cassandra.hosts": host,
                "cassandra.port": str(port),
                "cassandra.username": user,
                "cassandra.password": password,
                "cassandra.keyspace": db_name,
                "table.include.list": table_list,
            },
        }
        
        # Handle aliases
        if source_type == "postgres":
            source_type = "postgresql"
        if source_type == "mssql":
            source_type = "sqlserver"
        
        return configs.get(source_type, {
            # Generic fallback for unknown types
            "database.hostname": host,
            "database.port": str(port),
            "database.user": user,
            "database.password": password,
            "database.dbname": db_name,
            "table.include.list": table_list,
        })
    
    def _format_table_list(self, tables: List[str], source_type: str, db_name: str) -> str:
        """Format table list for Debezium based on database type.

        An empty list is rejected. A whole-source ("*") selection is resolved to an
        explicit, namespace-qualified list upstream (the api-gateway select-all
        resolver), so the CDC config always receives real table names. The old
        empty -> f"{db_name}.*" fallback was dead (the live debezium connector
        rejects empty tables) AND regex-wrong for schema-qualified engines: it
        matched db.table, but the Postgres/SQLServer/Oracle include-lists match
        schema.table / OWNER.TABLE, so f"{db_name}.*" matched nothing.
        """
        if not tables:
            raise ValueError(
                "table.include.list requires an explicit table list; a whole-source "
                "selection must be resolved to explicit tables upstream"
            )
        
        formatted_tables = []
        for table in tables:
            if "." in table:
                # Already qualified (db.table or schema.table)
                formatted_tables.append(table)
            else:
                # Add qualifier based on database type
                if source_type in ("postgresql", "postgres"):
                    # PostgreSQL uses schema.table, default schema is "public"
                    formatted_tables.append(f"public.{table}")
                elif source_type == "mongodb":
                    # MongoDB uses database.collection
                    formatted_tables.append(f"{db_name}.{table}")
                else:
                    # Most databases use database.table
                    formatted_tables.append(f"{db_name}.{table}")
        
        return ",".join(formatted_tables)
    
    def _generate_via_llm(
        self,
        source_type: str,
        config: Dict,
        tables: List[str],
        snapshot_mode: str,
        connector_name: str,
        overrides: Dict
    ) -> CDCConfigResult:
        """Use LLM to generate Debezium config for unknown database types"""
        try:
            db_name = self._extract_database_name(config, tables)
            
            # Build prompt for LLM
            prompt = f"""Generate a Debezium Kafka Connect connector configuration for {source_type} database.

Connection details:
- Host: {config.get('host', 'localhost')}
- Port: {config.get('port', 'default')}
- Database: {db_name}
- Tables to capture: {tables}
- Snapshot mode: {snapshot_mode}

Requirements:
1. Return ONLY valid JSON (no markdown, no explanation)
2. Include the correct "connector.class" for {source_type}
3. Include all necessary connection and CDC properties
4. Use "topic.prefix": "{topic(f"cdc-{db_name}")}"
5. Include schema history configuration if needed

Example format:
{{
  "connector.class": "io.debezium.connector.xxx.XxxConnector",
  "database.hostname": "...",
  ...
}}
"""
            
            response = self.llm_client.complete("planner/cdc_config", {"prompt": prompt})
            content = response.get("content", "{}")

            from src.utils.json_extract import loads as _safe_json_loads
            llm_config = _safe_json_loads(content)
            
            # Ensure required fields
            if "connector.class" not in llm_config:
                return CDCConfigResult(
                    success=False,
                    error="LLM response missing connector.class"
                )
            
            # Apply overrides
            llm_config.update(overrides)
            
            table_name = tables[0].split(".")[-1] if tables else "all"
            # The LLM is asked for the qualified prefix but is not trusted with it:
            # a model that answers "cdc-mydb" would otherwise make kafka_topic a
            # prediction of a topic nothing writes.
            kafka_topic = f"{self._pin_topic_prefix(llm_config, db_name)}.{db_name}.{table_name}"
            self._pin_schema_history_topic(llm_config, connector_name)
            
            return CDCConfigResult(
                success=True,
                config=llm_config,
                connector_name=connector_name,
                kafka_topic=kafka_topic,
                method_used="llm",
                warnings=["Config generated by LLM - please verify before use"]
            )
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            return CDCConfigResult(
                success=False,
                error=f"LLM response was not valid JSON: {e}"
            )
        except Exception as e:
            logger.error(f"LLM-based generation failed: {e}")
            return CDCConfigResult(success=False, error=str(e))
    
    def get_supported_databases(self) -> Dict[str, Any]:
        """Get list of supported CDC databases with their capabilities"""
        supported = {}
        
        # Built-in templates
        for db_type, connector_class in self.CONNECTOR_CLASSES.items():
            supported[db_type] = {
                "connector_class": connector_class,
                "source": "template",
                "default_port": self.DEFAULT_PORTS.get(db_type),
            }
        
        # Add from metadata.
        #
        # The two branches gate differently on purpose. Naming a NEW database here
        # requires a cdc_config_template, because that template is where the
        # connector_class comes from -- gating on supports_cdc alone let three
        # connectors that are not databases at all (databricks, debezium,
        # kafka-mcp-sink) advertise themselves over GET /cdc/supported-databases
        # with "connector_class": null. This is the same pair of conditions
        # generate_config already requires before it will use a metadata template.
        #
        # Annotating a database that ALREADY has a built-in template needs no
        # such gate: its connector_class comes from CONNECTOR_CLASSES either way,
        # and the label only records that its metadata declares CDC support too.
        for name, metadata in self.connector_metadata.items():
            if not metadata.get("supports_cdc"):
                continue
            if name in supported:
                supported[name]["source"] = "template+metadata"
            elif metadata.get("cdc_config_template"):
                supported[name] = {
                    "connector_class": metadata["cdc_config_template"].get("connector_class"),
                    "source": "metadata",
                    "display_name": metadata.get("display_name", name),
                }
        
        return supported
    
    def validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a Debezium configuration"""
        errors = []
        warnings = []
        
        # Required fields
        required = ["connector.class"]
        for field in required:
            if field not in config:
                errors.append(f"Missing required field: {field}")
        
        # Check connector class is valid
        connector_class = config.get("connector.class", "")
        if connector_class and "debezium" not in connector_class.lower():
            warnings.append(f"Connector class '{connector_class}' may not be a Debezium connector")
        
        # Database connection fields
        db_fields = ["database.hostname", "database.port", "database.user"]
        missing_db = [f for f in db_fields if f not in config]
        if missing_db and "mongodb.connection.string" not in config and "cassandra.hosts" not in config:
            warnings.append(f"Missing database connection fields: {missing_db}")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
        }


# Singleton instance for module-level access
_generator_instance: Optional[CDCConfigGenerator] = None


def get_cdc_config_generator(llm_client=None, connectors_dir: str = None) -> CDCConfigGenerator:
    """Get or create the global CDC config generator"""
    global _generator_instance
    if _generator_instance is None:
        _generator_instance = CDCConfigGenerator(llm_client, connectors_dir)
    return _generator_instance
