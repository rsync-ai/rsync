#!/usr/bin/env python3
"""
Planning Strategies Module
Provides pluggable planning strategies for pipeline generation.

Implements the Strategy Pattern to allow swapping between:
- LLM-based planning (primary)
- Heuristic-based planning (fallback)
- Hybrid approaches

Design Goals:
- Decoupled from main service code
- Easily extensible for new planning approaches
- Dynamic tool registry integration
"""

import os
import re
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import requests
import asyncio
from concurrent.futures import ThreadPoolExecutor
from src.utils.connector_paths import iter_connector_dirs, resolve_current_dir
from src.utils.masking import scrub_error_for_llm
from src.utils.kafka_topics import topic

logger = logging.getLogger("planner.strategies")

# =============================================================================
# CDC PROVIDER REGISTRY (PHASE 1: Metadata-Driven CDC Provider Selection)
# =============================================================================

class CDCProviderRegistry:
    """
    Metadata-driven CDC provider registry.
    Determines which CDC engine to use for each source type by reading connector metadata.
    
    This replaces hardcoded "debezium" references with dynamic provider selection.
    
    Example:
        registry = CDCProviderRegistry()
        provider = registry.get_cdc_provider("mysql")  # Returns "debezium"
        provider = registry.get_cdc_provider("salesforce")  # Returns "salesforce-streaming" (future)
    """
    
    def __init__(self, connectors_dir: str = None):
        self.connectors_dir = connectors_dir or os.getenv(
            "CONNECTORS_DIR", 
            "/app/shared/mcp-connectors"
        )
        self._provider_cache: Dict[str, str] = {}
        self._metadata_cache: Dict[str, Dict] = {}
        self._load_cdc_providers()
    
    def _load_cdc_providers(self):
        """
        Load CDC provider mappings from connector metadata.

        IMPORTANT: Connectors live in a nested structure (e.g. public/database/<id>, internal/<id>).
        We therefore discover each connector root (latest.json) and read its metadata
        from the canonical current version dir (versions/<current_version>/metadata.json).
        """
        connectors_path = Path(self.connectors_dir)

        if not connectors_path.exists():
            logger.warning(f"Connectors directory not found: {self.connectors_dir}")
            return

        # (connector_root_dir, metadata.json under versions/<current_version>/)
        metadata_files = []
        try:
            for connector_dir in iter_connector_dirs(connectors_path):
                meta = resolve_current_dir(connector_dir) / "metadata.json"
                if meta.exists():
                    metadata_files.append((connector_dir, meta))
        except Exception as e:
            logger.warning(f"Failed to scan connectors directory {self.connectors_dir}: {e}")
            return

        # Load all connector metadata we can find. connector_id is the connector
        # ROOT dir name (e.g. "mysql"), NOT the version dir name.
        for connector_dir, metadata_file in metadata_files:
            connector_id = connector_dir.name
            try:
                with open(metadata_file) as f:
                    metadata = json.load(f)
                self._metadata_cache[connector_id] = metadata

                # Check if this connector is a CDC engine (like debezium)
                if metadata.get("supports_cdc") and metadata.get("supported_databases"):
                    supported_dbs = metadata.get("supported_databases", {}) or {}
                    if isinstance(supported_dbs, dict):
                        for db_type in supported_dbs.keys():
                            db_key = str(db_type).strip().lower().replace("-", "_")
                            if not db_key:
                                continue
                            self._provider_cache[db_key] = connector_id
                            logger.debug(f"Registered CDC provider: {db_key} -> {connector_id}")
            except Exception as e:
                logger.warning(f"Failed to load metadata for {connector_id} ({metadata_file}): {e}")

        logger.info(
            f"✅ CDC Provider Registry: {len(self._provider_cache)} database types mapped to CDC providers "
            f"(from {len(metadata_files)} metadata files)"
        )
    
    def get_cdc_provider(self, source_type: str) -> Optional[str]:
        """
        Get the CDC provider for a given source type.
        
        Args:
            source_type: Database/source type (e.g., "mysql", "postgresql")
            
        Returns:
            CDC provider name (e.g., "debezium") or None if not CDC-capable
        """
        normalized = source_type.lower().replace("-", "_")
        
        # Direct lookup
        if normalized in self._provider_cache:
            return self._provider_cache[normalized]
        
        # Handle aliases
        aliases = {
            "postgres": "postgresql",
            "mssql": "sqlserver",
            "mariadb": "mysql",  # MariaDB uses MySQL connector
        }
        if normalized in aliases:
            aliased = aliases[normalized]
            if aliased in self._provider_cache:
                return self._provider_cache[aliased]
        
        return None
    
    def get_provider_config(self, source_type: str) -> Optional[Dict]:
        """Get CDC provider configuration for a source type"""
        provider = self.get_cdc_provider(source_type)
        if not provider:
            return None
        
        metadata = self._metadata_cache.get(provider, {})
        supported_dbs = metadata.get("supported_databases", {})
        
        normalized = source_type.lower().replace("-", "_")
        if normalized == "postgres":
            normalized = "postgresql"
        
        return supported_dbs.get(normalized)
    
    def is_cdc_capable(self, source_type: str) -> bool:
        """Check if a source type supports CDC"""
        return self.get_cdc_provider(source_type) is not None
    
    def get_all_cdc_sources(self) -> List[str]:
        """Get all source types that support CDC"""
        return list(self._provider_cache.keys())


# Global CDC provider registry instance
_cdc_provider_registry: Optional[CDCProviderRegistry] = None


def get_cdc_provider_registry(connectors_dir: str = None) -> CDCProviderRegistry:
    """Get or create the global CDC provider registry"""
    global _cdc_provider_registry
    if _cdc_provider_registry is None:
        _cdc_provider_registry = CDCProviderRegistry(connectors_dir)
    return _cdc_provider_registry

# =============================================================================
# DYNAMIC TOOL REGISTRY (replaces hardcoded lists)
# =============================================================================

class ToolRegistry:
    """
    Dynamic tool registry that fetches available tools from the Tool Generator.
    Replaces hardcoded KNOWN_DATABASES, KNOWN_DESTINATIONS, etc.
    """
    
    # Defaults used when registry is unavailable
    DEFAULT_DATABASES = [
        "mysql", "postgresql", "postgres", "mongodb", "redis", "oracle", 
        "sqlserver", "sqlite", "mariadb", "cassandra", "dynamodb", "cockroachdb",
        "clickhouse", "timescaledb", "influxdb", "neo4j", "elasticsearch"
    ]
    
    # NOTE: Prefer concrete connector names that exist in this repo/tool-generator.
    # "s3" is intentionally excluded (we use aws-s3).
    DEFAULT_DESTINATIONS = [
        "aws-s3",
        "snowflake", "bigquery", "gcs", "azure_blob", "redshift",
        "databricks", "kafka", "parquet", "csv", "json", "minio"
    ]
    
    DEFAULT_APIS = [
        "stripe", "hubspot", "salesforce", "slack", "twilio", "sendgrid",
        "shopify", "zendesk", "jira", "github", "gitlab", "notion"
    ]
    
    def __init__(self, tool_generator_url: str = "http://tool-generator:5010"):
        self.tool_generator_url = tool_generator_url
        self._cache: Dict[str, List[str]] = {}
        self._cache_ttl = 300  # 5 minutes
        self._last_fetch = 0
    
    def get_databases(self) -> List[str]:
        """Get list of known database connectors"""
        return self._get_category("databases", self.DEFAULT_DATABASES)
    
    def get_destinations(self) -> List[str]:
        """Get list of known destination connectors"""
        return self._get_category("destinations", self.DEFAULT_DESTINATIONS)
    
    def get_apis(self) -> List[str]:
        """Get list of known API connectors"""
        return self._get_category("apis", self.DEFAULT_APIS)
    
    def get_all_tools(self) -> List[str]:
        """Get all available tools"""
        return list(set(
            self.get_databases() + 
            self.get_destinations() + 
            self.get_apis()
        ))
    
    def _get_category(self, category: str, defaults: List[str]) -> List[str]:
        """Get tools for a category, with fallback to defaults"""
        import time
        
        # Check cache freshness
        if time.time() - self._last_fetch > self._cache_ttl:
            self._refresh_cache()
        
        return self._cache.get(category, defaults)
    
    def _refresh_cache(self):
        """Refresh the tool cache from Tool Generator"""
        import time
        
        try:
            # Tool-generator serves the catalog at /v1/connectors; the
            # unversioned alias is a 404 (legacy). See planner/service.py
            # for the matching fix.
            # 6s (was 2s): a cold tool-generator /v1/connectors call takes ~2-5s.
            # With the timestamp guard below this runs at most once per TTL window,
            # so the larger timeout lets the registry actually populate instead of
            # always falling back to defaults.
            resp = requests.get(f"{self.tool_generator_url}/v1/connectors", timeout=6)
            if resp.status_code == 200:
                data = resp.json()
                raw = data.get("connectors", [])
                # /v1/connectors emits objects ({name, path, …}); older
                # /connectors emitted plain names. Project to names and
                # canonicalize to kebab-case so the downstream
                # category-classifier sees consistent ids regardless of
                # tool-generator response shape.
                names: List[str] = []
                for c in raw:
                    n: Optional[str] = None
                    if isinstance(c, str):
                        n = c
                    elif isinstance(c, dict):
                        v = c.get("name")
                        if isinstance(v, str):
                            n = v
                    if n and n.strip():
                        names.append(n.strip().lower().replace("_", "-"))
                connectors = names
                
                # Categorize connectors (this could be enhanced with metadata from Tool Generator)
                self._cache = {
                    "databases": [c for c in connectors if self._is_database(c)],
                    "destinations": [c for c in connectors if self._is_destination(c)],
                    "apis": [c for c in connectors if self._is_api(c)]
                }
                self._last_fetch = time.time()
                logger.info(f"✅ Refreshed tool registry: {len(connectors)} connectors")
        except Exception as e:
            # Record the attempt even on failure. Otherwise a slow/unreachable
            # tool-generator triggers a fresh refresh on EVERY category lookup
            # (databases/destinations/apis × every get_all_tools call) — a
            # retry-storm that pushed total planning past the orchestrator's 30s
            # deadline and wedged the pipeline in the planning stage. One failed
            # try then falls back to defaults for the TTL window; the plan still
            # works via text-detection of connector names.
            self._last_fetch = time.time()
            logger.warning(f"⚠️  Failed to refresh tool registry: {e}")
    
    def _is_database(self, name: str) -> bool:
        """Check if connector name looks like a database"""
        db_patterns = ["sql", "db", "mongo", "redis", "cassandra", "dynamo", "neo4j", "elastic"]
        return any(p in name.lower() for p in db_patterns) or name.lower() in self.DEFAULT_DATABASES
    
    def _is_destination(self, name: str) -> bool:
        """Check if connector name looks like a destination"""
        dest_patterns = ["s3", "snowflake", "bigquery", "gcs", "blob", "redshift", "kafka", "minio"]
        return any(p in name.lower() for p in dest_patterns) or name.lower() in self.DEFAULT_DESTINATIONS
    
    def _is_api(self, name: str) -> bool:
        """Check if connector name looks like an API"""
        return name.lower() in self.DEFAULT_APIS


# Global registry instance
_tool_registry: Optional[ToolRegistry] = None

def get_tool_registry(tool_generator_url: str = None) -> ToolRegistry:
    """Get or create the global tool registry"""
    global _tool_registry
    if _tool_registry is None:
        _tool_registry = ToolRegistry(tool_generator_url or "http://tool-generator:5010")
    return _tool_registry

# =============================================================================
# DATA PATH DECISION LOGIC
# =============================================================================

@dataclass
class DataPathDecision:
    """Result of data path decision"""
    sync_mode: str  # "batch" or "cdc"
    reason: str
    cdc_mode: Optional[str] = None  # "initial" or "streaming_only" (only for cdc)
    staging: Optional[str] = None  # None or "minio"
    transport: str = "kafka"  # "kafka" or "kafka_url"
    sink: str = "kafka-mcp-sink"  # Always "kafka-mcp-sink"
    estimated_data_size_mb: Optional[float] = None


class DataPathDecider:
    """
    Decides the optimal data path based on sync mode and data size.
    
    Decision Logic (from AGENTIC_AGENTS_FLOW_V2.md):
    
    1. CDC/Streaming: Always Kafka directly (small events)
    2. Batch < 10MB: Direct Kafka
    3. Batch >= 10MB: MinIO staging (Kafka carries URLs only)
    4. Sink is always kafka-mcp-sink (generic MCP destination)
    """
    
    # Threshold in MB for using MinIO staging
    MINIO_THRESHOLD_MB = 10.0
    
    def __init__(self):
        logger.info("DataPathDecider initialized")
    
    def decide(
        self,
        sync_mode: str,
        cdc_mode: Optional[str] = None,
        estimated_data_size_mb: Optional[float] = None,
        source_connector: str = None,
        destination_connector: str = None
    ) -> DataPathDecision:
        """
        Decide the optimal data path for a pipeline.
        
        Args:
            sync_mode: "batch" or "cdc"
            estimated_data_size_mb: Estimated data size in megabytes
            source_connector: Source connector type (e.g., "mysql")
            destination_connector: Destination connector type (e.g., "aws-s3")
            
        Returns:
            DataPathDecision with staging, transport, and sink configuration
        """
        
        # FLOW 1: CDC/Streaming - Always Kafka directly
        if sync_mode in ["cdc", "streaming", "real-time", "realtime"]:
            # cdc_mode determines if we do full snapshot first ("initial") or stream only ("streaming_only")
            resolved_cdc_mode = cdc_mode or "initial"  # Default to initial (full snapshot first)
            snapshot_note = "with initial full snapshot" if resolved_cdc_mode == "initial" else "streaming only (no snapshot)"
            return DataPathDecision(
                sync_mode="cdc",
                cdc_mode=resolved_cdc_mode,
                staging=None,
                transport="kafka",
                sink="kafka-mcp-sink",
                reason=f"CDC/streaming {snapshot_note} - uses Kafka directly for real-time event delivery"
            )
        
        # FLOW 2 & 3: Batch - Check data size
        if sync_mode == "batch":
            # If we have data size estimate, use it
            if estimated_data_size_mb is not None:
                if estimated_data_size_mb >= self.MINIO_THRESHOLD_MB:
                    # FLOW 3: Large batch - MinIO staging
                    return DataPathDecision(
                        sync_mode="batch",
                        staging="minio",
                        transport="kafka_url",
                        sink="kafka-mcp-sink",
                        reason=f"Large batch data ({estimated_data_size_mb:.1f} MB >= {self.MINIO_THRESHOLD_MB} MB threshold) uses MinIO staging for efficiency",
                        estimated_data_size_mb=estimated_data_size_mb
                    )
                else:
                    # FLOW 2: Small batch - Direct Kafka
                    return DataPathDecision(
                        sync_mode="batch",
                        staging=None,
                        transport="kafka",
                        sink="kafka-mcp-sink",
                        reason=f"Small batch data ({estimated_data_size_mb:.1f} MB < {self.MINIO_THRESHOLD_MB} MB threshold) uses Kafka directly",
                        estimated_data_size_mb=estimated_data_size_mb
                    )
            else:
                # Unknown data size - default to direct Kafka.
                # MinIO staging should only be used when we have a size estimate that crosses the threshold.
                return DataPathDecision(
                    sync_mode="batch",
                    staging=None,
                    transport="kafka",
                    sink="kafka-mcp-sink",
                    reason="Batch mode with unknown data size - defaulting to direct Kafka (MinIO only for large payloads)"
                )
        
        # Default fallback
        return DataPathDecision(
            sync_mode=sync_mode or "batch",
            staging=None,
            transport="kafka",
            sink="kafka-mcp-sink",
            reason="Default path: direct Kafka transport"
        )
    
    def estimate_data_size(
        self, 
        table_name: str, 
        row_count: Optional[int] = None,
        schema_info: Optional[Dict] = None
    ) -> Optional[float]:
        """
        Estimate data size in MB based on table metadata.
        
        This is a heuristic estimation. More accurate estimation
        would require querying the source database.
        """
        if row_count is None:
            return None
        
        # Average bytes per row (rough estimate)
        # Can be refined based on schema_info
        avg_bytes_per_row = 500  # Conservative estimate
        
        if schema_info:
            # More accurate estimate based on column types
            column_sizes = {
                "integer": 8,
                "bigint": 8,
                "string": 100,  # Average
                "text": 500,    # Variable
                "json": 1000,   # Variable
                "timestamp": 8,
                "boolean": 1,
                "float": 8,
                "double": 8,
            }
            
            columns = schema_info.get("columns", [])
            if columns:
                total_bytes = sum(
                    column_sizes.get(col.get("type", "string"), 100)
                    for col in columns
                )
                avg_bytes_per_row = total_bytes * 1.2  # 20% overhead for formatting
        
        estimated_bytes = row_count * avg_bytes_per_row
        estimated_mb = estimated_bytes / (1024 * 1024)
        
        logger.info(f"Estimated data size for {table_name}: {estimated_mb:.2f} MB "
                   f"({row_count} rows × ~{avg_bytes_per_row} bytes/row)")
        
        return estimated_mb
    
    def get_pipeline_config(self, decision: DataPathDecision) -> Dict[str, Any]:
        """
        Generate pipeline configuration based on data path decision.
        """
        config = {
            "sync_mode": decision.sync_mode,
            "transport": {
                "type": decision.transport,
                "kafka_enabled": True,
            },
            "sink": {
                "type": decision.sink,
                "generic_mcp": True,
            }
        }
        
        if decision.staging == "minio":
            config["staging"] = {
                "enabled": True,
                "type": "minio",
                "threshold_mb": self.MINIO_THRESHOLD_MB,
                # bucket is owned by the orchestrator/executor (pipeline-data);
                # the old "rsync-staging" literal here was stale and never read.
                "retention_hours": 24,
            }
        else:
            config["staging"] = {
                "enabled": False
            }
        
        if decision.sync_mode == "cdc":
            # Map cdc_mode to Debezium snapshot.mode
            # "initial" -> snapshot.mode=initial (full snapshot first, then CDC)
            # "streaming_only" -> snapshot.mode=never (no snapshot, stream from current position)
            debezium_snapshot_mode = "initial" if decision.cdc_mode != "streaming_only" else "never"
            config["cdc"] = {
                "engine": "debezium",
                "auto_configure": True,
                "cdc_mode": decision.cdc_mode or "initial",
                "snapshot_mode": debezium_snapshot_mode,  # Maps to Debezium's snapshot.mode
            }
        
        return config


# Global data path decider instance
_data_path_decider: Optional[DataPathDecider] = None


def get_data_path_decider() -> DataPathDecider:
    """Get or create the global data path decider"""
    global _data_path_decider
    if _data_path_decider is None:
        _data_path_decider = DataPathDecider()
    return _data_path_decider

# =============================================================================
# PHASE 2: PLAN-TIME TOPIC PROVISIONING
# =============================================================================

@dataclass
class TopicProvisioningResult:
    """Result of topic provisioning"""
    success: bool
    topic_name: str
    partitions: int
    replication_factor: int
    reason: str
    estimated_throughput_mb_s: Optional[float] = None
    error: Optional[str] = None


class TopicProvisioner:
    """
    Intelligent Kafka topic provisioner.
    
    Creates topics with optimal partitions BEFORE execution starts.
    This ensures CDC connectors don't auto-create topics with default (1) partition.
    
    Partition Calculation:
    - 1 partition per 2GB of estimated data (for parallelism)
    - Minimum 3 partitions (for fault tolerance)
    - Maximum 50 partitions (to avoid overhead)
    - CDC topics: partition by table count for ordering
    
    Usage:
        provisioner = TopicProvisioner()
        result = provisioner.provision_topic(
            pipeline_id="pipe-123",
            sync_mode="cdc",
            tables=["users", "orders"],
            estimated_size_gb=100
        )
    """
    
    # Constants for partition calculation
    GB_PER_PARTITION = 2.0  # 1 partition per 2GB
    MIN_PARTITIONS = 3
    MAX_PARTITIONS = 50
    DEFAULT_REPLICATION = 3
    
    def __init__(self, kafka_admin_url: str = None):
        # Whether the PLANNER pre-creates the topic at plan time, over HTTP, via the
        # orchestrator's topology API. Off by default, and set in no compose or env file.
        #
        # This is not the flag that decides whether topics get created — the executor now
        # creates every topic it produces to explicitly, through the same TopologyManager,
        # immediately before it produces. What this flag buys is creating the topic
        # EARLIER, at plan time, with a partition count derived from the estimated data
        # size, which only matters for a large batch load that wants parallelism.
        #
        # It stays off by default because turning it on has costs that have nothing to do
        # with auto-creation: it fires a synchronous HTTP call on every plan, and for CDC
        # it would mint "cdc.<connection-name>" — a topic no producer writes to, because
        # the real CDC streams are Debezium's "<topic.prefix>.<db>.<table>". When
        # disabled, we still return a deterministic topic name + recommended partitions
        # for observability.
        self.enabled = os.getenv("ENABLE_TOPIC_PROVISIONING", "false").strip().lower() == "true"
        self.kafka_admin_url = kafka_admin_url or os.getenv(
            "KAFKA_ADMIN_URL",
            "http://kafka:29092"
        )
        # Topology Agent URL for topic creation (Go Orchestrator)
        self.topology_agent_url = os.getenv(
            "TOPOLOGY_AGENT_URL",
            # docker-compose service name is "orchestrator"
            "http://orchestrator:8080/api/v1/topology"
        )
        logger.info(f"📊 TopicProvisioner initialized (enabled={self.enabled}, url={self.topology_agent_url})")
    
    def calculate_optimal_partitions(
        self,
        sync_mode: str,
        estimated_size_gb: float,
        table_count: int = 1,
        estimated_rows: int = 0
    ) -> int:
        """
        Calculate optimal partition count based on data characteristics.
        
        Args:
            sync_mode: "cdc" or "batch"
            estimated_size_gb: Estimated data size in GB
            table_count: Number of tables being synced
            estimated_rows: Estimated total row count
            
        Returns:
            Optimal partition count
        """
        if sync_mode == "cdc":
            # CDC: Partition by table count to maintain ordering per table
            # Each table gets its own partition for guaranteed ordering
            partitions = max(self.MIN_PARTITIONS, table_count)
        else:
            # Batch: Partition by data size for parallelism
            partitions = max(
                self.MIN_PARTITIONS,
                int(estimated_size_gb / self.GB_PER_PARTITION)
            )
        
        # Apply bounds
        partitions = min(partitions, self.MAX_PARTITIONS)
        partitions = max(partitions, self.MIN_PARTITIONS)
        
        logger.info(f"📐 Calculated partitions: {partitions} (mode={sync_mode}, size={estimated_size_gb:.1f}GB, tables={table_count})")
        return partitions
    
    def generate_topic_name(
        self,
        pipeline_id: str,
        connection_name: str = None,
        sync_mode: str = "batch"
    ) -> str:
        """
        Generate a standardized topic name.
        
        Format: cdc.{connection} or batch.{pipeline_id}
        """
        if sync_mode == "cdc" and connection_name:
            # CDC topics are per-connection (all tables route to one topic)
            sanitized = connection_name.lower().replace(" ", "-").replace("_", "-")
            return topic(f"cdc.{sanitized}")
        else:
            # Batch topics are per-pipeline
            short_id = pipeline_id[:8] if len(pipeline_id) > 8 else pipeline_id
            return topic(f"pipeline.{short_id}.data")
    
    def provision_topic(
        self,
        pipeline_id: str,
        sync_mode: str,
        tables: List[str],
        estimated_size_gb: float = 0,
        estimated_rows: int = 0,
        connection_name: str = None,
        replication_factor: int = None
    ) -> TopicProvisioningResult:
        """
        Provision a Kafka topic with optimal configuration.
        
        This is called DURING PLANNING, before execution starts.
        
        Args:
            pipeline_id: Unique pipeline identifier
            sync_mode: "cdc" or "batch"
            tables: List of tables being synced
            estimated_size_gb: Estimated data size
            estimated_rows: Estimated row count
            connection_name: Connection name (for CDC topic naming)
            replication_factor: Override replication factor
            
        Returns:
            TopicProvisioningResult with topic details
        """
        topic_name = self.generate_topic_name(pipeline_id, connection_name, sync_mode)
        partitions = self.calculate_optimal_partitions(
            sync_mode=sync_mode,
            estimated_size_gb=estimated_size_gb,
            table_count=len(tables),
            estimated_rows=estimated_rows
        )
        repl_factor = replication_factor or self.DEFAULT_REPLICATION
        
        logger.info(f"📦 Provisioning topic: {topic_name} (partitions={partitions}, replication={repl_factor})")

        if not self.enabled:
            return TopicProvisioningResult(
                success=False,
                topic_name=topic_name,
                partitions=partitions,
                replication_factor=repl_factor,
                # Deliberately does NOT say the topic will be auto-created by Kafka.
                # It said that for as long as auto-creation was in fact load-bearing, and
                # that sentence is why nobody noticed the platform could not run against a
                # broker with auto.create.topics.enable=false. The executor pre-creates
                # this topic through the orchestrator's topology manager before producing.
                reason=(
                    "Plan-time topic pre-creation is off (ENABLE_TOPIC_PROVISIONING=false). "
                    "The executor creates this topic explicitly before producing to it."
                ),
                estimated_throughput_mb_s=partitions * 10,
            )
        
        # Try to create topic via Topology Agent
        try:
            result = self._create_topic_via_topology_agent(
                topic_name=topic_name,
                partitions=partitions,
                replication_factor=repl_factor,
                sync_mode=sync_mode
            )
            
            if result.get("success"):
                logger.info(f"✅ Topic provisioned: {topic_name}")
                return TopicProvisioningResult(
                    success=True,
                    topic_name=topic_name,
                    partitions=partitions,
                    replication_factor=repl_factor,
                    reason=f"Pre-created with {partitions} partitions for optimal parallelism",
                    estimated_throughput_mb_s=partitions * 10  # ~10MB/s per partition
                )
            else:
                # Topic might already exist - that's OK
                if "already exists" in result.get("error", "").lower():
                    logger.info(f"ℹ️ Topic already exists: {topic_name}")
                    return TopicProvisioningResult(
                        success=True,
                        topic_name=topic_name,
                        partitions=partitions,
                        replication_factor=repl_factor,
                        reason="Topic already exists (using existing configuration)"
                    )
                else:
                    logger.warning(f"⚠️ Topic creation returned error: {result.get('error')}")
                    return TopicProvisioningResult(
                        success=False,
                        topic_name=topic_name,
                        partitions=partitions,
                        replication_factor=repl_factor,
                        reason="Topic creation failed",
                        error=result.get("error")
                    )
                    
        except Exception as e:
            logger.warning(f"⚠️ Could not pre-create topic at plan time (the executor creates it before producing): {e}")
            return TopicProvisioningResult(
                success=False,
                topic_name=topic_name,
                partitions=partitions,
                replication_factor=repl_factor,
                reason=(
                    f"Plan-time pre-creation failed: {e}. The executor still creates this "
                    "topic explicitly before producing to it."
                ),
                error=str(e)
            )
    
    def _create_topic_via_topology_agent(
        self,
        topic_name: str,
        partitions: int,
        replication_factor: int,
        sync_mode: str
    ) -> Dict:
        """Create topic via the Topology Agent API"""
        if not self.enabled:
            return {"success": False, "error": "Topic provisioning disabled"}
        # The orchestrator's /api/v1/topology group is gated by requirePrincipal
        # (backend-orchestrator/cmd/orchestrator/auth_middleware.go), which accepts
        # either a user session or the shared internal secret. This is a
        # service-to-service call with no user session, so it must send the secret
        # or it gets 401 in any production-like ENVIRONMENT. Same header contract
        # as tool_generator/deployment/deployer_client.py.
        headers = {"Content-Type": "application/json"}
        internal_secret = os.getenv("INTERNAL_SERVICE_SECRET", "").strip()
        if internal_secret:
            headers["X-Internal-Secret"] = internal_secret

        try:
            response = requests.post(
                f"{self.topology_agent_url}/topics",
                json={
                    "topic_name": topic_name,
                    "partitions": partitions,
                    "replication_factor": replication_factor,
                    "config": {
                        "cleanup.policy": "delete" if sync_mode == "batch" else "compact",
                        "retention.ms": "604800000" if sync_mode == "batch" else "-1",  # 7 days for batch, forever for CDC
                        "min.insync.replicas": min(2, replication_factor),
                    }
                },
                headers=headers,
                timeout=10
            )
            
            if response.status_code in [200, 201]:
                return {"success": True, "result": response.json()}
            else:
                return {"success": False, "error": response.text}
                
        except requests.exceptions.ConnectionError:
            return {"success": False, "error": "Topology Agent not available"}
        except Exception as e:
            return {"success": False, "error": str(e)}


# Global topic provisioner instance
_topic_provisioner: Optional[TopicProvisioner] = None


def get_topic_provisioner() -> TopicProvisioner:
    """Get or create the global topic provisioner"""
    global _topic_provisioner
    if _topic_provisioner is None:
        _topic_provisioner = TopicProvisioner()
    return _topic_provisioner

# =============================================================================
# PLANNING STRATEGY INTERFACE
# =============================================================================

@dataclass
class PlanningContext:
    """Context for planning operations"""
    natural_language: str
    pipeline_id: Optional[str] = None
    detected_tools: List[str] = field(default_factory=list)
    available_connections: List[Dict] = field(default_factory=list)
    available_tools: List[str] = field(default_factory=list)
    generated_connectors: List[str] = field(default_factory=list)
    trace_id: Optional[str] = None
    # New: Sync mode and data path info
    sync_mode: Optional[str] = None  # "batch" or "cdc"
    cdc_mode: Optional[str] = None  # "initial" or "streaming_only"
    estimated_data_size_mb: Optional[float] = None
    source_connection: Optional[Dict] = None
    destination_connection: Optional[Dict] = None
    # Auto-replan (beta): sanitized snapshot of a prior execution failure. Present only when a
    # V2 workflow re-invokes the planner to correct a failed plan; None on the normal first pass.
    error_context: Optional[Dict[str, Any]] = None


def build_replan_user_request(
    natural_language: str, error_context: Optional[Dict[str, Any]]
) -> str:
    """Augment the user request with a corrective instruction when replanning.

    Auto-replan (beta): when a V2 workflow re-invokes the planner after a deterministic
    executor failure, ``error_context`` carries the sanitized failure. We append a corrective
    instruction so the LLM avoids regenerating the failing plan. Template-agnostic (folded into
    ``user_request`` rather than a placeholder). Returns ``natural_language`` unchanged when
    ``error_context`` is falsy, keeping the normal first-pass prompt byte-identical.
    """
    if not error_context:
        return natural_language
    reason = (
        error_context.get("reason")
        or error_context.get("policy_code")
        or "unknown execution error"
    )
    stage = error_context.get("stage", "execution")
    # Upstream promises a sanitized reason, but the privacy contract (row values
    # never reach the LLM) must not depend on every producer honoring that —
    # scrub at the last hop before the prompt.
    reason = scrub_error_for_llm(str(reason), max_len=500)
    return (
        f"{natural_language}\n\n"
        f"IMPORTANT — the previous execution plan FAILED during {stage}: {reason}. "
        f"Produce a CORRECTED plan that avoids this failure; do not regenerate the same failing steps."
    )


@dataclass
class PlanResult:
    """Result of a planning operation"""
    success: bool
    pipeline_id: str
    plan: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    generated_connectors: Optional[List[str]] = None
    strategy_used: str = "unknown"
    data_path: Optional[Dict[str, Any]] = None  # DataPathDecision as dict


class PlanningStrategy(ABC):
    """Abstract base class for planning strategies"""
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Name of this strategy"""
        pass
    
    @abstractmethod
    def create_plan(self, context: PlanningContext) -> PlanResult:
        """Create a pipeline plan from the given context"""
        pass
    
    def can_handle(self, context: PlanningContext) -> bool:
        """Check if this strategy can handle the given context"""
        return True


# =============================================================================
# HEURISTIC PLANNING STRATEGY
# =============================================================================

class HeuristicPlanningStrategy(PlanningStrategy):
    """
    Rule-based planning strategy that uses pattern matching and heuristics.
    Used as fallback when LLM-based planning fails.
    
    PHASE 1 ENHANCEMENT: Uses CDCProviderRegistry for metadata-driven CDC provider selection
    instead of hardcoding "debezium".
    """
    
    def __init__(self, tool_registry: ToolRegistry = None, connectors_dir: str = None):
        self.registry = tool_registry or get_tool_registry()
        self.data_path_decider = get_data_path_decider()
        self.cdc_registry = get_cdc_provider_registry(connectors_dir)
        self.topic_provisioner = get_topic_provisioner()
        logger.info(f"🧠 HeuristicPlanningStrategy initialized with {len(self.cdc_registry.get_all_cdc_sources())} CDC-capable sources")
    
    @property
    def name(self) -> str:
        return "heuristic"
    
    def create_plan(self, context: PlanningContext) -> PlanResult:
        """Create a plan using heuristic rules"""
        logger.info(f"🔧 Using heuristic planning for: {context.natural_language[:50]}...")
        
        nl = context.natural_language.lower()
        detected_tools = context.detected_tools
        
        # Get dynamic tool lists from registry
        source_types = self.registry.get_databases()
        dest_types = self.registry.get_destinations()
        
        # Categorize detected tools
        sources = [t for t in detected_tools if t in source_types]
        destinations = [t for t in detected_tools if t in dest_types]
        others = [t for t in detected_tools if t not in sources and t not in destinations]
        
        # If we know the explicit source/destination connections (from orchestrator context),
        # prefer those roles even for connectors that can be both source and destination
        # (e.g., postgresql/mysql). This prevents the heuristic plan from treating the
        # destination connector as an additional source for CDC.
        if context.source_connection and context.destination_connection:
            src_tool = (context.source_connection.get("connector_type") or "").strip().lower().replace("_", "-")
            dst_tool = (context.destination_connection.get("connector_type") or "").strip().lower().replace("_", "-")
            if src_tool:
                sources = [src_tool]
            if dst_tool:
                destinations = [dst_tool]
        
        # Order: sources first, then destinations, then others
        # NOTE: duplicates are allowed (e.g., postgresql->postgresql) to represent source+destination steps.
        ordered_tools = sources + destinations + others
        logger.info(f"📋 Ordered tools for plan: {ordered_tools}")
        
        # Determine sync mode from explicit context if provided, else infer from NL.
        sync_mode = (context.sync_mode or "").strip().lower()
        if not sync_mode:
            inferred_streaming = self._detect_streaming_request(nl)
            sync_mode = "cdc" if inferred_streaming else "batch"
        elif sync_mode in ["streaming", "realtime", "real-time", "real time"]:
            sync_mode = "cdc"
        elif sync_mode in ["one-time", "one_time", "once"]:
            sync_mode = "batch"

        # Treat streaming flag as a function of sync_mode (avoid false positives like table name "cdc_test")
        is_streaming = sync_mode == "cdc"

        # Get cdc_mode from context (only relevant for CDC sync mode)
        cdc_mode = context.cdc_mode if sync_mode == "cdc" else None

        # Make data path decision
        data_path_decision = self.data_path_decider.decide(
            sync_mode=sync_mode,
            cdc_mode=cdc_mode,
            estimated_data_size_mb=context.estimated_data_size_mb,
            source_connector=sources[0] if sources else None,
            destination_connector=destinations[0] if destinations else None
        )
        logger.info(f"📊 Data path decision: {data_path_decision.reason}")
        
        # Build steps
        steps = []
        for i, tool in enumerate(ordered_tools):
            # Determine role by position (sources first, then destinations, then other tools).
            # This disambiguates connectors that can be both source and destination (e.g., mysql/postgresql).
            role = None
            if i < len(sources):
                role = "source"
            elif i < len(sources) + len(destinations):
                role = "destination"

            step = self._create_step_for_tool(
                tool=tool,
                index=i,
                nl=nl,
                source_types=source_types,
                dest_types=dest_types,
                is_streaming=is_streaming,
                connections=context.available_connections,
                generated_connectors=context.generated_connectors,
                data_path=data_path_decision,
                role=role,
            )
            steps.append(step)
        
        # If no tools detected, return helpful error
        if not steps:
            return PlanResult(
                success=False,
                pipeline_id="",
                error="Could not detect any data sources or destinations from your request. "
                      "Try something like: 'Read from SQLite and write to S3'",
                strategy_used=self.name
            )
        
        pipeline_id = context.pipeline_id or f"pipeline-{int(__import__('time').time())}"
        
        # Get pipeline config from data path decision
        pipeline_config = self.data_path_decider.get_pipeline_config(data_path_decision)
        
        # =================================================================
        # PHASE 2: PLAN-TIME TOPIC PROVISIONING
        # Pre-create Kafka topics with optimal partitions BEFORE execution
        # =================================================================
        topic_provisioning = None
        try:
            # Extract table names for partition calculation
            table_names = []
            for step in steps:
                if "table" in step.get("params", {}):
                    table_names.append(step["params"]["table"])
            
            # Get connection name for CDC topic naming
            connection_name = None
            if context.source_connection:
                connection_name = context.source_connection.get("name")
            
            # Provision topic with optimal partitions
            provisioning_result = self.topic_provisioner.provision_topic(
                pipeline_id=pipeline_id,
                sync_mode=sync_mode,
                tables=table_names or ["default"],
                estimated_size_gb=context.estimated_data_size_mb / 1024 if context.estimated_data_size_mb else 0,
                connection_name=connection_name
            )
            
            topic_provisioning = {
                "topic_name": provisioning_result.topic_name,
                "partitions": provisioning_result.partitions,
                "replication_factor": provisioning_result.replication_factor,
                "provisioned": provisioning_result.success,
                "reason": provisioning_result.reason,
            }
            
            if provisioning_result.success:
                logger.info(f"📦 Topic pre-provisioned: {provisioning_result.topic_name} with {provisioning_result.partitions} partitions")
            else:
                logger.warning(f"⚠️ Topic provisioning deferred: {provisioning_result.reason}")
                
        except Exception as e:
            logger.warning(f"⚠️ Topic provisioning failed (non-fatal): {e}")
            topic_provisioning = {"provisioned": False, "error": str(e)}
        
        # =======================================================================
        # PHASE 2: Get enhanced CDC metadata for the plan
        # =======================================================================
        cdc_provider = self._get_plan_cdc_provider(steps, is_streaming)
        cdc_metadata = self._get_cdc_metadata(sources, cdc_provider) if is_streaming else {}
        
        plan = {
            "pipeline_id": pipeline_id,
            "steps": steps,
            "is_streaming": is_streaming,
            "sync_mode": sync_mode,
            "cdc_mode": cdc_mode or "initial",  # Default to "initial" for full snapshot first
            "data_path": {
                "staging": data_path_decision.staging,
                "transport": data_path_decision.transport,
                "sink": data_path_decision.sink,
                "cdc_mode": data_path_decision.cdc_mode,
                "reason": data_path_decision.reason,
            },
            "topic_provisioning": topic_provisioning,  # PHASE 2: Include provisioning info
            "config": pipeline_config,
            "generated_connectors": context.generated_connectors,
            # PHASE 1 & 4: Include CDC provider info for Executor
            "cdc_provider": cdc_provider,
            # PHASE 2: Enhanced CDC metadata
            "cdc_metadata": cdc_metadata,
            # PHASE 2: Partition key guidance for Kafka
            "partition_key_strategy": self._get_partition_key_strategy(sync_mode, sources),
        }
        
        # Log if any step needs connection configuration
        needs_config = [s["tool"] for s in steps if s.get("requires_connection")]
        if needs_config:
            logger.info(f"📝 Pipeline needs connection config for: {needs_config}")
        
        logger.info(f"✅ Heuristic plan created: {len(steps)} steps, transport={data_path_decision.transport}")
        
        return PlanResult(
            success=True,
            pipeline_id=pipeline_id,
            plan=plan,
            generated_connectors=context.generated_connectors if context.generated_connectors else None,
            strategy_used=self.name,
            data_path={
                "sync_mode": data_path_decision.sync_mode,
                "staging": data_path_decision.staging,
                "transport": data_path_decision.transport,
                "sink": data_path_decision.sink,
                "reason": data_path_decision.reason,
            }
        )
    
    def _detect_streaming_request(self, nl: str) -> bool:
        """Detect if the request is for streaming/CDC"""
        # Use word-boundary regexes to avoid accidental matches inside identifiers
        # (e.g., table name "cdc_test" should not trigger CDC mode).
        patterns = [
            r"\bcdc\b",
            r"\breal[- ]?time\b",
            r"\bstreaming\b",
            r"\bchange data capture\b",
            r"\bdebezium\b",
            r"\blive\b",
            r"\bcontinuous\b",
        ]
        return any(re.search(p, nl) for p in patterns)
    
    def _create_step_for_tool(
        self, 
        tool: str, 
        index: int, 
        nl: str,
        source_types: List[str],
        dest_types: List[str],
        is_streaming: bool,
        connections: List[Dict],
        generated_connectors: List[str],
        data_path: Optional[DataPathDecision] = None,
        role: Optional[str] = None,  # "source" | "destination" | None
    ) -> Dict[str, Any]:
        """
        Create a step for a specific tool with data path awareness.
        
        PHASE 1 ENHANCEMENT: Uses CDCProviderRegistry for metadata-driven CDC provider selection.
        Instead of hardcoding "debezium", we look up the appropriate CDC provider from metadata.
        """
        step_id = f"step_{index + 1}"
        
        # PHASE 1: Metadata-driven CDC provider selection
        # Check if this tool is a CDC provider itself OR if it's a source that needs CDC.
        #
        # IMPORTANT: When we route a source (e.g. "mysql") to a CDC provider tool (e.g. "debezium"),
        # we must still attach the SOURCE connection_id (mysql) to the step. Otherwise the UI/Executor
        # will treat the CDC step as missing a connection.
        cdc_provider = self.cdc_registry.get_cdc_provider(tool)
        is_cdc_provider_tool = tool in ["debezium"]  # Tool is itself a CDC provider
        is_source_role = role == "source" if role else (tool in source_types)
        is_destination_role = role == "destination" if role else (tool in dest_types)
        needs_cdc_routing = (is_source_role and is_streaming and cdc_provider)
        match_tool_for_connection = tool
        
        if is_cdc_provider_tool or needs_cdc_routing:
            method = "start_sync"
            params = {"table": "unknown", "snapshot_mode": "initial"}
            
            if needs_cdc_routing:
                # SMART: Route to the appropriate CDC provider based on metadata
                original_source = tool
                match_tool_for_connection = original_source
                tool = cdc_provider  # e.g., "mysql" -> "debezium"
                params["source_type"] = original_source
                params["source_connector"] = original_source
                # Keep a canonical DB type for downstream (Executor/Debezium MCP accept this key)
                params["database_type"] = original_source
                
                # CRITICAL: Pass cdc_provider to Executor (Step 4)
                params["cdc_provider"] = cdc_provider
                
                # Get provider-specific config from metadata
                provider_config = self.cdc_registry.get_provider_config(original_source)
                if provider_config:
                    params["connector_class"] = provider_config.get("connector_class")
                    params["cdc_position_method"] = provider_config.get("cdc_position_method")
                    logger.info(f"🔄 CDC Routing: {original_source} -> {tool} (class: {params.get('connector_class', 'auto')})")
                else:
                    logger.info(f"🔄 CDC Routing: {original_source} -> {tool} (using defaults)")
            else:
                # Tool is already a CDC provider (e.g., "debezium" was explicitly mentioned)
                params["cdc_provider"] = tool
        elif is_source_role:
            method = "query" if "query" in nl or "select" in nl else "export"
            params = {"table": "users"}  # Default table
            # If using MinIO staging, add staging config
            if data_path and data_path.staging == "minio":
                params["staging"] = {
                    "enabled": True,
                    "type": "minio",
                    # bucket omitted: executor owns it (pipeline-data); the old
                    # "rsync-staging" literal here was stale and never read.
                }
        elif is_destination_role:
            # For destinations, use standard import_data
            # The Executor/Sink will handle the actual data transfer via Kafka
            method = "import_data"
            params = {
                "source": f"$step_{index}.output" if index > 0 else None,
                # Include sink info in params for Executor to use
                "sink_type": "kafka-mcp-sink" if data_path and data_path.sink == "kafka-mcp-sink" else "direct",
                "destination_connector": tool
            }
        elif tool in self.registry.get_apis():
            method = "fetch_data"
            params = {"endpoint": "default"}
        else:
            method = "execute"
            params = {}
        
        # Try to match connection.
        # For CDC-routed steps, match against the ORIGINAL SOURCE tool (mysql/postgresql/etc),
        # not the CDC provider tool ("debezium").
        # Prefer matching connections by role (source vs destination) to avoid selecting a
        # destination connection for a source step (or vice versa).
        desired_conn_type = None
        if method == "start_sync":
            desired_conn_type = "source"
        elif role == "destination":
            desired_conn_type = "destination"
        elif role == "source":
            desired_conn_type = "source"
        elif tool in dest_types:
            desired_conn_type = "destination"
        elif tool in source_types:
            desired_conn_type = "source"

        conn_id = self._match_tool_to_connection(
            match_tool_for_connection,
            connections,
            user_request=nl,
            connection_type=desired_conn_type,
        )
        
        step = {
            "id": step_id,
            "tool": tool,
            "method": method,
            "params": params,
        }
        
        # Add data path info to step
        if data_path:
            step["data_path"] = {
                "transport": data_path.transport,
                "staging": data_path.staging,
            }
        
        if conn_id:
            step["connection_id"] = conn_id
        else:
            step["connection_id"] = None
            step["requires_connection"] = True
            logger.info(f"⚠️  Tool '{match_tool_for_connection}' needs connection configuration")
        
        return step
    
    def _get_plan_cdc_provider(self, steps: List[Dict], is_streaming: bool) -> Optional[str]:
        """
        Extract CDC provider from steps for plan-level metadata.
        
        This makes it easy for the Executor to find the CDC provider without
        having to parse individual steps.
        """
        if not is_streaming:
            return None
        
        # Look for cdc_provider in step params
        for step in steps:
            params = step.get("params", {})
            if "cdc_provider" in params:
                return params["cdc_provider"]
        
        # Check if any step tool is a CDC provider
        for step in steps:
            tool = step.get("tool", "")
            if tool in ["debezium"]:  # Known CDC providers
                return tool
        
        return None
    
    def _get_cdc_metadata(self, sources: List[str], cdc_provider: Optional[str]) -> Dict[str, Any]:
        """
        PHASE 2: Get enhanced CDC metadata from the provider registry.
        
        Returns metadata including:
        - connector_class: The Java class for Debezium/CDC connector
        - cdc_position_method: How CDC tracks position (binlog, wal_lsn, oplog, etc.)
        - prerequisites: Required settings for the source database
        - snapshot_mode_default: Default snapshot mode
        """
        metadata = {}
        
        if not sources or not cdc_provider:
            return metadata
        
        source_type = sources[0] if sources else None
        if not source_type:
            return metadata
        
        # Get provider config from registry
        provider_config = self.cdc_registry.get_provider_config(source_type)
        if provider_config:
            metadata["connector_class"] = provider_config.get("connector_class")
            metadata["cdc_position_method"] = provider_config.get("cdc_position_method")
            metadata["prerequisites"] = provider_config.get("prerequisites", [])
            metadata["requires_schema_history"] = provider_config.get("requires_schema_history", False)
            metadata["requires_replication_slot"] = provider_config.get("requires_replication_slot", False)
            metadata["mcp_connector"] = provider_config.get("mcp_connector", source_type)
            
            logger.info(f"📋 CDC metadata: position_method={metadata.get('cdc_position_method')}, "
                       f"connector_class={metadata.get('connector_class', 'auto')[:40]}...")
        
        # Default snapshot mode based on cdc_mode
        metadata["snapshot_mode_default"] = "initial"  # Full snapshot first, then CDC
        
        return metadata
    
    def _get_partition_key_strategy(self, sync_mode: str, sources: List[str]) -> Dict[str, Any]:
        """
        PHASE 2: Get partition key strategy guidance for Kafka.
        
        Based on KAFKA_TOPOLOGY_STRATEGY.md:
        - CDC: Partition by {connection}.{schema}.{table}.{record_id}
        - Batch: Partition by {pipeline_id}.{table}.{batch_id}
        """
        if sync_mode in ["cdc", "streaming"]:
            return {
                "type": "cdc",
                "format": "{connection}.{schema}.{table}.{record_id}",
                "description": "CDC events partitioned by table + record for ordering guarantee",
                "ordering_guarantee": "per_record",
                "example": "conn-001.public.users.12345",
            }
        else:
            return {
                "type": "batch",
                "format": "{pipeline_id}.{table}.{batch_id}",
                "description": "Batch data partitioned by table for parallelism",
                "ordering_guarantee": "per_batch",
                "example": "pipe-abc.users.batch-001",
            }
    
    def _match_tool_to_connection(
        self,
        tool_name: str,
        connections: List[Dict],
        user_request: Optional[str] = None,
        connection_type: Optional[str] = None,
    ) -> Optional[str]:
        """Match a tool name to a connection ID with basic normalization and stable selection."""
        tool = (tool_name or "").strip()
        if not tool:
            return None

        # Connections use connector_type (often kebab-case), while some older code paths may still
        # emit underscore variants.
        # Example: tool="aws_s3" vs connection.connector_type="aws-s3"
        candidates = []
        candidates.append(tool)
        candidates.append(tool.replace("_", "-"))
        candidates.append(tool.replace("-", "_"))
        # Deduplicate while preserving order
        seen = set()
        cand_unique = []
        for c in candidates:
            if c and c not in seen:
                seen.add(c)
                cand_unique.append(c)

        matches: List[Dict] = []
        for c in connections:
            if connection_type:
                # API gateway uses `type`: "source" | "destination"
                if str(c.get("type") or "").strip().lower() != connection_type:
                    continue
            ctype = c.get("connector_type")
            if ctype in cand_unique:
                matches.append(c)

        if not matches:
            return None

        # If multiple connections, prefer one explicitly named in the user request
        req = (user_request or "").lower()
        if req:
            for c in matches:
                name = (c.get("name") or "").lower()
                if name and name in req:
                    logger.info(f"🔗 Matched tool '{tool_name}' to named connection '{c.get('name')}'")
                    return c.get("id")

        # Stable default: smallest id (string sort)
        matches_sorted = sorted(matches, key=lambda x: str(x.get("id", "")))
        chosen = matches_sorted[0]
        logger.info(f"🔗 Matched tool '{tool_name}' to connection '{chosen.get('name')}'")
        return chosen.get("id")


# =============================================================================
# LLM PLANNING STRATEGY
# =============================================================================

class LLMPlanningStrategy(PlanningStrategy):
    """
    LLM-based planning strategy that uses AI to generate plans.
    Primary strategy for complex planning requests.
    """
    
    def __init__(self, llm_client, tool_registry: ToolRegistry = None):
        self.llm_client = llm_client
        self.registry = tool_registry or get_tool_registry()
    
    @property
    def name(self) -> str:
        return "llm"
    
    def create_plan(self, context: PlanningContext) -> PlanResult:
        """Create a plan using LLM"""
        logger.info(f"🤖 Using LLM planning for: {context.natural_language[:50]}...")
        
        try:
            # Format context for prompt
            avail_tools_list = list(context.available_tools)
            avail_conns_list = [{
                "type": c.get("connector_type"), 
                "id": c.get("id"), 
                "name": c.get("name")
            } for c in context.available_connections]
            
            # Build prompt variables
            prompt_vars = {
                "available_tools": json.dumps(avail_tools_list),
                "available_connections": json.dumps(avail_conns_list),
                "user_request": context.natural_language
            }

            # Auto-replan (beta): when re-invoked after an execution failure, append a corrective
            # instruction to the request so the LLM avoids regenerating the failing plan. Appended
            # to user_request (template-agnostic) rather than relying on a template placeholder.
            # Skipped entirely when error_context is None, keeping the normal prompt unchanged.
            if context.error_context:
                prompt_vars["user_request"] = build_replan_user_request(
                    context.natural_language, context.error_context
                )
                _stage = context.error_context.get("stage", "execution")
                _reason = (
                    context.error_context.get("reason")
                    or context.error_context.get("policy_code")
                    or "unknown execution error"
                )
                logger.info(f"🔁 Replanning with error_context (stage={_stage}): {str(_reason)[:160]}")

            # Add sync mode information if available
            if context.sync_mode:
                prompt_vars["sync_mode"] = context.sync_mode
                logger.info(f"📊 Sync mode: {context.sync_mode}")
                
                # If CDC mode, add CDC-specific context
                if context.sync_mode == "cdc" and context.cdc_mode:
                    prompt_vars["cdc_mode"] = context.cdc_mode
                    logger.info(f"   CDC mode: {context.cdc_mode}")
            
            # Call LLM
            llm_response = self.llm_client.complete(
                "planner/dynamic_planning",
                prompt_vars
            )
            
            content = llm_response.get("content", "")

            from src.utils.json_extract import loads as _safe_json_loads
            llm_plan = _safe_json_loads(content)
            
            # Validate and process the plan
            steps = llm_plan.get("steps", [])
            pipeline_id = context.pipeline_id or llm_plan.get("pipeline_id") or f"pipeline-{int(__import__('time').time())}"
            
            is_streaming = any(s.get("method") == "start_sync" for s in steps)
            
            logger.info(f"✅ LLM generated plan with {len(steps)} steps")
            
            # Enrich steps with metadata
            for step in steps:
                if not step.get("connection_id") and step.get("tool") in context.generated_connectors:
                    step["requires_connection"] = True
            
            plan = {
                "pipeline_id": pipeline_id,
                "steps": steps,
                "is_streaming": is_streaming,
                "generated_connectors": context.generated_connectors
            }
            
            return PlanResult(
                success=True,
                pipeline_id=pipeline_id,
                plan=plan,
                generated_connectors=context.generated_connectors if context.generated_connectors else None,
                strategy_used=self.name
            )
            
        except Exception as e:
            logger.error(f"❌ LLM Planning failed: {e}")
            return PlanResult(
                success=False,
                pipeline_id=context.pipeline_id or "",
                error=str(e),
                strategy_used=self.name
            )


# =============================================================================
# COMPOSITE PLANNING STRATEGY (Primary + Fallback)
# =============================================================================

class CompositePlanningStrategy(PlanningStrategy):
    """
    Composite strategy that tries primary strategy first, then falls back.
    """
    
    def __init__(self, primary: PlanningStrategy, fallback: PlanningStrategy):
        self.primary = primary
        self.fallback = fallback
    
    @property
    def name(self) -> str:
        return f"composite({self.primary.name}+{self.fallback.name})"
    
    def create_plan(self, context: PlanningContext) -> PlanResult:
        """Try primary strategy, fall back if it fails.

        NOTE: For CDC/streaming, we deliberately bypass the LLM to keep plans
        deterministic and compatible with the executor's CDC execution path.
        """
        # Deterministic CDC routing: skip LLM entirely
        sync_mode = (context.sync_mode or "").strip().lower()
        if sync_mode in ["cdc", "streaming", "real-time", "realtime"]:
            logger.info("🧭 Routing to heuristic planner for CDC/streaming (deterministic)")
            result = self.fallback.create_plan(context)
            return self._validate_or_replan(context, result, allow_replan=False)

        # Optional routing hook: if primary provides can_handle(), allow it to decline
        if hasattr(self.primary, "can_handle"):
            try:
                can_handle = getattr(self.primary, "can_handle")(context)
                if not can_handle:
                    logger.info(f"🧭 Routing to fallback planner (primary '{self.primary.name}' not applicable)")
                    fallback_result = self.fallback.create_plan(context)
                    return self._validate_or_replan(context, fallback_result, allow_replan=False)
            except Exception as e:
                logger.warning(f"Primary strategy can_handle() failed; continuing with primary: {e}")

        # Try primary strategy
        result = self.primary.create_plan(context)
        
        if result.success:
            return self._validate_or_replan(context, result, allow_replan=True)
        
        # Fall back to secondary strategy
        logger.info(f"⚠️  Primary strategy '{self.primary.name}' failed, falling back to '{self.fallback.name}'")
        fallback_result = self.fallback.create_plan(context)
        return self._validate_or_replan(context, fallback_result, allow_replan=False)

    def _validate_or_replan(self, context: PlanningContext, result: PlanResult, allow_replan: bool) -> PlanResult:
        """
        Hard guardrails to keep plans compatible with the executor/runtime.

        Key invariants:
        - Never emit explicit kafka-mcp-sink steps (executor starts sink implicitly).
        - For batch: do not use start_sync.
        - For CDC: do not use export/import_data steps to represent streaming.
        - Avoid non-existent tool aliases (e.g. tool 's3').
        """
        if not result or not result.success or not result.plan:
            return result

        plan = result.plan or {}
        steps = plan.get("steps") or []
        if not isinstance(steps, list):
            return result

        nl = (context.natural_language or "").lower()
        sync_mode = (context.sync_mode or plan.get("sync_mode") or "").strip().lower()

        # Collect violations
        violations: List[str] = []

        def step_str(s: Dict[str, Any]) -> str:
            return f"{s.get('tool')}:{s.get('method')}"

        for s in steps:
            if not isinstance(s, dict):
                continue
            tool = (s.get("tool") or "").strip().lower()
            method = (s.get("method") or "").strip()
            method_lower = method.lower()

            # Disallow explicit sink steps (always executor-managed)
            if tool in ["kafka-mcp-sink"] or method_lower in ["start_sink", "consume_kafka", "consume"]:
                violations.append(f"explicit_sink_step:{step_str(s)}")

            # Disallow tool aliases that we don't ship as connectors
            if tool == "s3":
                violations.append(f"invalid_tool_alias:{step_str(s)}")

            # Disallow MinIO as a user-facing pipeline tool unless explicitly requested.
            if tool == "minio" and "minio" not in nl:
                violations.append(f"unexpected_minio_tool:{step_str(s)}")

            # Disallow legacy start_* batch operation names (not normalized by MCP client)
            if method_lower in ["start_export", "start_import", "start_load"]:
                violations.append(f"invalid_method_name:{step_str(s)}")

        # Mode-specific guardrails
        if sync_mode in ["batch"]:
            for s in steps:
                if isinstance(s, dict) and str(s.get("method", "")).lower() in ["start_sync", "start_connector"]:
                    violations.append(f"batch_contains_cdc:{step_str(s)}")
        if sync_mode in ["cdc", "streaming", "real-time", "realtime"]:
            # For CDC plans, we primarily care that the plan includes a start_sync step and does not
            # include explicit sink plumbing. Destination steps may still appear (e.g., heuristic plans)
            # but the executor will not execute plan steps for CDC; it uses executeDataTransfer().
            #
            # We only enforce "no export/import_data" for CDC when the plan was produced by the LLM,
            # because that indicates the LLM tried to model streaming as batch.
            if (result.strategy_used or "").lower() == "llm":
                for s in steps:
                    if not isinstance(s, dict):
                        continue
                    m = str(s.get("method", "")).lower()
                    if m in ["export", "import", "import_data"]:
                        violations.append(f"cdc_contains_batch_steps:{step_str(s)}")
            has_start_sync = any(
                isinstance(s, dict) and str(s.get("method", "")).lower() in ["start_sync", "start_connector"]
                for s in steps
            )
            if not has_start_sync:
                violations.append("cdc_missing_start_sync")

        if not violations:
            return result

        logger.warning(f"🛡️ Plan guardrail triggered ({result.strategy_used}): {violations}")
        if allow_replan:
            logger.info("🛠️ Replanning via heuristic due to guardrail violations")
            replanned = self.fallback.create_plan(context)
            # Do not recurse indefinitely; validate but do not replan again.
            return self._validate_or_replan(context, replanned, allow_replan=False)

        # If we can't replan again, surface a structured error
        return PlanResult(
            success=False,
            pipeline_id=result.pipeline_id,
            error=f"Planner produced an invalid plan for sync_mode={sync_mode}: {violations}",
            strategy_used=result.strategy_used,
        )


# =============================================================================
# ASYNC CONNECTOR CHECKING
# =============================================================================

class AsyncConnectorChecker:
    """
    Async wrapper for connector existence checks and JIT generation.
    Improves latency by not blocking the main request thread.
    """
    
    def __init__(self, connectors_dir: str, tool_generator_url: str):
        self.connectors_dir = connectors_dir
        self.tool_generator_url = tool_generator_url
        self._executor = ThreadPoolExecutor(max_workers=4)
    
    async def ensure_connectors_exist_async(
        self, 
        tools: List[str], 
        context: str = None
    ) -> Tuple[List[str], List[str]]:
        """
        Async version of ensure_connectors_exist.
        Uses thread pool to avoid blocking the event loop.
        """
        loop = asyncio.get_event_loop()
        
        # Check existence in parallel
        existence_futures = [
            loop.run_in_executor(self._executor, self._connector_exists, tool)
            for tool in tools
        ]
        existence_results = await asyncio.gather(*existence_futures)
        
        available = []
        to_generate = []
        
        for tool, exists in zip(tools, existence_results):
            if exists:
                available.append(tool)
            else:
                to_generate.append(tool)
        
        # Generate missing connectors in parallel
        if to_generate:
            gen_futures = [
                loop.run_in_executor(
                    self._executor, 
                    self._generate_connector, 
                    tool, 
                    context
                )
                for tool in to_generate
            ]
            gen_results = await asyncio.gather(*gen_futures)
            
            generated = []
            for tool, (success, _) in zip(to_generate, gen_results):
                if success:
                    available.append(tool)
                    generated.append(tool)
        else:
            generated = []
        
        return available, generated
    
    def _connector_exists(self, connector_name: str) -> bool:
        """Check if a connector exists"""
        import os
        def _exists_in_tree(base_root: str, cand: str) -> bool:
            if not base_root or not os.path.isdir(base_root):
                return False
            # Direct child: <base_root>/<cand> is a connector root. resolve_current_dir
            # returns versions/<current_version>/ (or the dir itself for flat layout);
            # for a non-existent path it falls back harmlessly to a missing file.
            if (resolve_current_dir(os.path.join(base_root, cand)) / "connector.py").exists():
                return True
            # Nested (e.g. public/database/<cand>): find a connector root named cand.
            for connector_dir in iter_connector_dirs(base_root):
                if connector_dir.name == cand and (resolve_current_dir(connector_dir) / "connector.py").exists():
                    return True
            return False

        # Search roots (legacy + new public/internal)
        roots = [
            self.connectors_dir,
            os.path.join(self.connectors_dir, "public"),
            os.path.join(self.connectors_dir, "internal"),
        ]

        candidates = [connector_name]
        if "-" in connector_name:
            candidates.append(connector_name.replace("-", "_"))
        if "_" in connector_name:
            candidates.append(connector_name.replace("_", "-"))

        for cand in dict.fromkeys(candidates):
            for root in roots:
                if _exists_in_tree(root, cand):
                    return True
        return False
    
    def _generate_connector(self, connector_name: str, description: str = None) -> Tuple[bool, str]:
        """Generate a connector via Tool Generator"""
        try:
            gen_request = {
                "connector_name": connector_name,
                "description": description or f"MCP connector for {connector_name}",
                "force_regenerate": False
            }
            
            resp = requests.post(
                # Tool-generator v1 API: POST /v1/generate (legacy /generate was removed)
                f"{self.tool_generator_url}/v1/generate",
                json=gen_request,
                timeout=120
            )
            
            if resp.status_code == 200:
                result = resp.json()
                if result.get("success"):
                    logger.info(f"✅ JIT: Connector '{connector_name}' generated")
                    return True, f"Connector '{connector_name}' generated"
                else:
                    return False, result.get("error", "Unknown error")
            else:
                return False, f"HTTP {resp.status_code}"
                
        except Exception as e:
            logger.error(f"❌ JIT generation failed for '{connector_name}': {e}")
            return False, str(e)


# =============================================================================
# SMART PIPELINE STRATEGY DECIDER (Phase 2)
# =============================================================================

@dataclass
class TableStats:
    """Statistics for a single table"""
    name: str
    row_count: int = 0
    estimated_size_mb: float = 0.0
    has_primary_key: bool = True
    column_count: int = 0


@dataclass
class DataStatistics:
    """Aggregate statistics for pipeline data"""
    total_rows: int = 0
    estimated_size_mb: float = 0.0
    estimated_size_gb: float = 0.0
    table_count: int = 0
    largest_table: Optional[str] = None
    largest_table_rows: int = 0
    tables: List[TableStats] = field(default_factory=list)
    
    def add_table(self, table: TableStats):
        """Add a table to statistics"""
        self.tables.append(table)
        self.total_rows += table.row_count
        self.estimated_size_mb += table.estimated_size_mb
        self.estimated_size_gb = self.estimated_size_mb / 1024
        self.table_count += 1
        if table.row_count > self.largest_table_rows:
            self.largest_table_rows = table.row_count
            self.largest_table = table.name


@dataclass
class PipelinePhase:
    """A single phase in a multi-phase pipeline"""
    phase_number: int
    phase_type: str  # "batch_export", "wait_completion", "cdc_catchup", "cdc_snapshot"
    description: str
    config: Dict[str, Any] = field(default_factory=dict)
    estimated_duration: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "phase": self.phase_number,
            "type": self.phase_type,
            "description": self.description,
            "config": self.config,
            "estimated_duration": self.estimated_duration,
        }


@dataclass
class SmartStrategyDecision:
    """Result of intelligent pipeline strategy selection"""
    strategy: str  # "cdc_snapshot", "batch", "hybrid", "cdc_streaming_only"
    phases: List[PipelinePhase] = field(default_factory=list)
    recommendation: str = ""
    requires_user_confirmation: bool = False
    data_stats: Optional[DataStatistics] = None
    estimated_total_time: Optional[str] = None
    benefits: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy": self.strategy,
            "phases": [p.to_dict() for p in self.phases],
            "recommendation": self.recommendation,
            "requires_user_confirmation": self.requires_user_confirmation,
            "estimated_total_time": self.estimated_total_time,
            "benefits": self.benefits,
            "warnings": self.warnings,
            "data_stats": {
                "total_rows": self.data_stats.total_rows if self.data_stats else 0,
                "estimated_size_gb": self.data_stats.estimated_size_gb if self.data_stats else 0,
                "table_count": self.data_stats.table_count if self.data_stats else 0,
            } if self.data_stats else None,
        }


class SmartPipelineDecider:
    """
    Intelligent pipeline strategy selector.
    
    Decides between batch, CDC, or hybrid strategies based on:
    - Data volume (row count and estimated size)
    - Source database capabilities
    - User's sync mode preference
    
    Thresholds:
    - Small table (< 100K rows): Standard CDC snapshot is efficient
    - Medium table (100K - 10M rows): CDC snapshot with optimizations
    - Large table (> 10M rows OR > 100GB): Recommend hybrid approach
    """
    
    # Threshold constants
    SMALL_TABLE_ROWS = 100_000           # < 100K rows: CDC snapshot is fine
    MEDIUM_TABLE_ROWS = 10_000_000       # < 10M rows: CDC snapshot with tips
    LARGE_TABLE_GB = 100                 # > 100GB: definitely recommend hybrid
    HYBRID_RECOMMENDED_ROWS = 10_000_000 # > 10M rows: suggest hybrid
    
    # Performance estimates
    CDC_SNAPSHOT_ROWS_PER_MINUTE = 100_000  # Conservative estimate
    BATCH_EXPORT_MB_PER_MINUTE = 500        # With parallelism
    
    def __init__(self):
        logger.info("SmartPipelineDecider initialized")

    def estimate_data_size_from_discovery(self, tables: List[Dict[str, Any]]) -> DataStatistics:
        """
        Build `DataStatistics` from a connection discovery-style payload.

        Expected input shape (subset):
        - [{"name": "...", "row_count": 123, "columns": [{"name": "...", "type": "..."}]}]
        """
        stats = DataStatistics()
        tables = tables or []

        # Rough per-column byte estimates (heuristics)
        type_sizes = {
            "int": 8,
            "integer": 8,
            "bigint": 8,
            "smallint": 4,
            "serial": 8,
            "bigserial": 8,
            "float": 8,
            "double": 8,
            "real": 8,
            "numeric": 16,
            "decimal": 16,
            "bool": 1,
            "boolean": 1,
            "date": 8,
            "time": 8,
            "timestamp": 8,
            "timestamptz": 8,
            "uuid": 16,
            "json": 1000,
            "jsonb": 1000,
            "text": 500,
            "varchar": 100,
            "character varying": 100,
            "string": 100,
        }

        for t in tables:
            name = str(t.get("name") or t.get("table") or "").strip() or "unknown"
            try:
                row_count = int(t.get("row_count") or 0)
            except Exception:
                row_count = 0

            cols = t.get("columns") or []
            col_count = len(cols) if isinstance(cols, list) else 0

            if isinstance(cols, list) and cols:
                bytes_per_row = 0.0
                for c in cols:
                    ct = str((c or {}).get("type") or "string").lower()
                    # normalize common type prefixes (e.g., varchar(255))
                    ct = ct.split("(", 1)[0].strip()
                    bytes_per_row += type_sizes.get(ct, 100)
                bytes_per_row *= 1.2  # overhead
            else:
                bytes_per_row = 500.0

            estimated_mb = (row_count * bytes_per_row) / (1024 * 1024)

            stats.add_table(
                TableStats(
                    name=name,
                    row_count=row_count,
                    estimated_size_mb=float(estimated_mb),
                    column_count=col_count,
                )
            )

        return stats
    
    def decide_strategy(
        self,
        sync_mode: str,
        data_stats: DataStatistics,
        source_type: str,
        cdc_mode: Optional[str] = None,
        user_preference: Optional[str] = None,
    ) -> SmartStrategyDecision:
        """
        Decide the optimal sync strategy based on data characteristics.
        
        Args:
            sync_mode: User's preferred sync mode ("batch" or "cdc")
            data_stats: Statistics about the data (row counts, sizes)
            source_type: Source database type (mysql, postgresql, etc.)
            cdc_mode: CDC mode if applicable ("initial" or "streaming_only")
            user_preference: Optional explicit strategy preference
            
        Returns:
            SmartStrategyDecision with strategy, phases, and recommendations
        """
        
        # If user explicitly requested batch mode, respect it
        if sync_mode == "batch":
            return self._create_batch_decision(data_stats, source_type)
        
        # If user explicitly wants streaming only (no historical data)
        if cdc_mode == "streaming_only":
            return self._create_streaming_only_decision(data_stats, source_type)
        
        # CDC mode with initial sync - this is where smart decisions matter
        total_rows = data_stats.total_rows
        estimated_gb = data_stats.estimated_size_gb
        
        # SCENARIO A: Small table - standard Debezium snapshot
        if total_rows < self.SMALL_TABLE_ROWS:
            return self._create_small_table_decision(data_stats, source_type)
        
        # SCENARIO B: Huge table - strongly recommend hybrid
        if estimated_gb > self.LARGE_TABLE_GB or total_rows > self.HYBRID_RECOMMENDED_ROWS:
            return self._create_hybrid_decision(data_stats, source_type)
        
        # SCENARIO C: Medium table - CDC snapshot with optimization tips
        return self._create_medium_table_decision(data_stats, source_type)
    
    def _create_small_table_decision(
        self, 
        data_stats: DataStatistics, 
        source_type: str
    ) -> SmartStrategyDecision:
        """Create decision for small tables - use standard CDC snapshot"""
        
        estimated_minutes = data_stats.total_rows / self.CDC_SNAPSHOT_ROWS_PER_MINUTE
        
        return SmartStrategyDecision(
            strategy="cdc_snapshot",
            phases=[
                PipelinePhase(
                    phase_number=1,
                    phase_type="cdc_snapshot",
                    description="Debezium will snapshot existing data, then stream changes",
                    config={
                        "snapshot_mode": "initial",
                        "snapshot_fetch_size": 10000,
                    },
                    estimated_duration=f"{max(1, int(estimated_minutes))} minutes"
                )
            ],
            recommendation=f"""
✅ **Standard CDC Sync** is optimal for your data.

📊 **Your Data**: {data_stats.total_rows:,} rows ({data_stats.estimated_size_mb:.1f} MB)

Debezium will:
1. Take a consistent snapshot of all {data_stats.table_count} table(s)
2. Automatically switch to streaming mode
3. Capture all future changes in real-time

⏱️ Estimated initial sync time: ~{max(1, int(estimated_minutes))} minutes
""".strip(),
            requires_user_confirmation=False,
            data_stats=data_stats,
            estimated_total_time=f"{max(1, int(estimated_minutes))} minutes",
            benefits=[
                "Simple setup - single phase",
                "Consistent snapshot guaranteed",
                "Automatic transition to streaming",
                "Low source database impact",
            ]
        )
    
    def _create_medium_table_decision(
        self, 
        data_stats: DataStatistics, 
        source_type: str
    ) -> SmartStrategyDecision:
        """Create decision for medium tables - CDC snapshot with optimizations"""
        
        estimated_minutes = data_stats.total_rows / self.CDC_SNAPSHOT_ROWS_PER_MINUTE
        
        # Optimize snapshot settings based on size
        fetch_size = 20000 if data_stats.total_rows > 1_000_000 else 10000
        max_threads = min(4, max(1, int(data_stats.estimated_size_gb / 5)))
        
        return SmartStrategyDecision(
            strategy="cdc_snapshot",
            phases=[
                PipelinePhase(
                    phase_number=1,
                    phase_type="cdc_snapshot",
                    description="Optimized Debezium snapshot with parallel processing",
                    config={
                        "snapshot_mode": "initial",
                        "snapshot_fetch_size": fetch_size,
                        "snapshot_max_threads": max_threads,
                        "incremental_snapshot": True,
                    },
                    estimated_duration=f"{int(estimated_minutes)} minutes"
                )
            ],
            recommendation=f"""
✅ **CDC Sync with Optimizations** recommended.

📊 **Your Data**: {data_stats.total_rows:,} rows ({data_stats.estimated_size_gb:.1f} GB)
📋 **Tables**: {data_stats.table_count} (largest: {data_stats.largest_table} with {data_stats.largest_table_rows:,} rows)

I'll configure Debezium with optimizations:
- Increased fetch size: {fetch_size:,} rows/batch
- Parallel snapshot threads: {max_threads}
- Incremental snapshot enabled

⏱️ Estimated initial sync time: ~{int(estimated_minutes)} minutes

💡 **Tip**: The initial snapshot may take a while, but it's a one-time operation.
After that, changes stream in real-time (typically < 1 second latency).
""".strip(),
            requires_user_confirmation=False,
            data_stats=data_stats,
            estimated_total_time=f"{int(estimated_minutes)} minutes",
            benefits=[
                "Optimized for medium-sized data",
                "Parallel snapshot processing",
                "Consistent data guaranteed",
                "Automatic streaming after snapshot",
            ],
            warnings=[
                f"Initial sync may take ~{int(estimated_minutes)} minutes",
                "Source database will see increased read load during snapshot",
            ]
        )
    
    def _create_hybrid_decision(
        self, 
        data_stats: DataStatistics, 
        source_type: str
    ) -> SmartStrategyDecision:
        """Create decision for large tables - recommend hybrid batch + CDC"""
        
        # Calculate estimates for hybrid approach
        batch_parallelism = min(16, max(4, int(data_stats.estimated_size_gb / 10)))
        batch_minutes = (data_stats.estimated_size_mb / self.BATCH_EXPORT_MB_PER_MINUTE) / batch_parallelism
        
        # Compare with CDC-only approach
        cdc_only_minutes = data_stats.total_rows / self.CDC_SNAPSHOT_ROWS_PER_MINUTE
        speedup = cdc_only_minutes / batch_minutes if batch_minutes > 0 else 1
        
        return SmartStrategyDecision(
            strategy="hybrid",
            phases=[
                PipelinePhase(
                    phase_number=1,
                    phase_type="capture_position",
                    description="Capture current binlog/WAL position before export",
                    config={
                        "source_type": source_type,
                        "capture_method": "binlog_position" if source_type == "mysql" else "wal_lsn",
                    },
                    estimated_duration="< 1 second"
                ),
                PipelinePhase(
                    phase_number=2,
                    phase_type="batch_export",
                    description=f"Parallel batch export with {batch_parallelism}x parallelism",
                    config={
                        "parallelism": batch_parallelism,
                        "use_minio_staging": True,
                        "partition_by": "primary_key",
                        "batch_size": 50000,
                    },
                    estimated_duration=f"{int(batch_minutes)} minutes"
                ),
                PipelinePhase(
                    phase_number=3,
                    phase_type="wait_verification",
                    description="Verify batch completion and row counts",
                    config={
                        "verify_row_counts": True,
                        "verify_checksums": False,  # Optional
                    },
                    estimated_duration="1-2 minutes"
                ),
                PipelinePhase(
                    phase_number=4,
                    phase_type="cdc_catchup",
                    description="Start CDC from captured position to catch up changes",
                    config={
                        "snapshot_mode": "never",
                        "start_from_position": "captured_in_phase_1",
                    },
                    estimated_duration="Varies based on change rate"
                ),
            ],
            recommendation=f"""
⚠️ **Large Dataset Detected** - Hybrid approach recommended!

📊 **Your Data**: 
- **{data_stats.total_rows:,} rows** ({data_stats.estimated_size_gb:.1f} GB)
- **{data_stats.table_count} tables** (largest: {data_stats.largest_table})

🚀 **Recommended: Hybrid Batch + CDC**

Instead of a single slow Debezium snapshot, I recommend:

**Phase 1**: Capture binlog/WAL position (instant)
**Phase 2**: Parallel batch export ({batch_parallelism}x speed) → ~{int(batch_minutes)} min
**Phase 3**: Verify data completeness
**Phase 4**: CDC catchup from captured position

📈 **Why Hybrid?**
- ⚡ **{speedup:.0f}x faster** than Debezium-only snapshot
- 🔄 **Zero data loss** - CDC catches changes during export
- 📉 **Lower source impact** - batch reads are more efficient
- ✅ **Resumable** - can restart from MinIO if interrupted

⏱️ **Estimated total time**: ~{int(batch_minutes + 5)} minutes (vs ~{int(cdc_only_minutes)} min with CDC-only)

Would you like to proceed with the hybrid approach?
""".strip(),
            requires_user_confirmation=True,  # Ask user for large operations
            data_stats=data_stats,
            estimated_total_time=f"{int(batch_minutes + 5)} minutes",
            benefits=[
                f"{speedup:.0f}x faster than Debezium-only snapshot",
                "Zero data loss during migration",
                f"Parallel export with {batch_parallelism}x parallelism",
                "Resumable from checkpoint if interrupted",
                "Lower impact on source database",
            ],
            warnings=[
                "Requires MinIO staging storage",
                "More complex pipeline with multiple phases",
                "CDC catchup time depends on change rate during export",
            ]
        )
    
    def _create_batch_decision(
        self, 
        data_stats: DataStatistics, 
        source_type: str
    ) -> SmartStrategyDecision:
        """Create decision for batch-only mode (no CDC)"""
        
        use_minio = data_stats.estimated_size_mb >= 10
        parallelism = min(8, max(1, int(data_stats.estimated_size_gb / 5))) if use_minio else 1
        batch_minutes = data_stats.estimated_size_mb / (self.BATCH_EXPORT_MB_PER_MINUTE * parallelism)
        
        return SmartStrategyDecision(
            strategy="batch",
            phases=[
                PipelinePhase(
                    phase_number=1,
                    phase_type="batch_export",
                    description="One-time batch export of data",
                    config={
                        "parallelism": parallelism,
                        "use_minio_staging": use_minio,
                        "batch_size": 50000 if data_stats.total_rows > 100000 else 10000,
                    },
                    estimated_duration=f"{max(1, int(batch_minutes))} minutes"
                )
            ],
            recommendation=f"""
📦 **Batch Export** configured.

📊 **Your Data**: {data_stats.total_rows:,} rows ({data_stats.estimated_size_mb:.1f} MB)

This is a one-time export - no ongoing sync.
{"Using MinIO staging for optimal performance." if use_minio else "Direct Kafka transfer."}

⏱️ Estimated time: ~{max(1, int(batch_minutes))} minutes
""".strip(),
            requires_user_confirmation=False,
            data_stats=data_stats,
            estimated_total_time=f"{max(1, int(batch_minutes))} minutes",
            benefits=[
                "Simple one-time transfer",
                "No ongoing CDC overhead",
                f"{'MinIO staging for large data' if use_minio else 'Direct transfer'}",
            ]
        )
    
    def _create_streaming_only_decision(
        self, 
        data_stats: DataStatistics, 
        source_type: str
    ) -> SmartStrategyDecision:
        """Create decision for streaming-only mode (no snapshot)"""
        
        return SmartStrategyDecision(
            strategy="cdc_streaming_only",
            phases=[
                PipelinePhase(
                    phase_number=1,
                    phase_type="cdc_streaming",
                    description="Stream changes from current position (no historical data)",
                    config={
                        "snapshot_mode": "never",
                    },
                    estimated_duration="Continuous"
                )
            ],
            recommendation=f"""
⚡ **Streaming Only** configured.

This will capture changes from NOW onwards - no historical data will be synced.

📊 Existing data ({data_stats.total_rows:,} rows) will NOT be transferred.
🔄 New inserts, updates, and deletes will stream in real-time.

Use this when:
- Destination already has historical data
- You only need new changes going forward
- Resuming an existing pipeline
""".strip(),
            requires_user_confirmation=False,
            data_stats=data_stats,
            estimated_total_time="Continuous",
            benefits=[
                "Instant start - no snapshot phase",
                "Minimal source database impact",
                "Real-time change streaming",
            ],
            warnings=[
                "Historical data will NOT be synced",
                f"Existing {data_stats.total_rows:,} rows ignored",
            ]
        )


# Global smart decider instance
_smart_decider: Optional[SmartPipelineDecider] = None


def get_smart_pipeline_decider() -> SmartPipelineDecider:
    """Get or create the global smart pipeline decider"""
    global _smart_decider
    if _smart_decider is None:
        _smart_decider = SmartPipelineDecider()
    return _smart_decider


# =============================================================================
# TOOL DETECTION UTILITIES
# =============================================================================

def detect_tools_from_text(text: str, registry: ToolRegistry = None) -> List[str]:
    """
    Detect database/tool names from natural language text.
    Uses dynamic tool registry instead of hardcoded lists.
    """
    registry = registry or get_tool_registry()
    text_lower = text.lower()
    detected = []
    
    # Check all known tools from registry
    all_known = registry.get_all_tools()
    
    for tool in all_known:
        # Match whole words to avoid false positives
        pattern = r'\b' + re.escape(tool) + r'\b'
        if re.search(pattern, text_lower):
            detected.append(tool)
    
    # Also detect explicitly mentioned tools (case-insensitive, supports hyphens/underscores).
    # IMPORTANT: Users often type "postgresql" / "snowflake" in lowercase.
    explicit_patterns = [
        r'\bfrom\s+([a-z0-9][a-z0-9_-]+)\b',
        r'\bto\s+([a-z0-9][a-z0-9_-]+)\b',
        r'\busing\s+([a-z0-9][a-z0-9_-]+)\b',
        r'\bread\s+from\s+([a-z0-9][a-z0-9_-]+)\b',
        r'\bread\s+([a-z0-9][a-z0-9_-]+)\b',
        r'\bwrite\s+to\s+([a-z0-9][a-z0-9_-]+)\b',
        r'\bwrite\s+([a-z0-9][a-z0-9_-]+)\b',
        r'\bconnect\s+to\s+([a-z0-9][a-z0-9_-]+)\b',
        r'\bsync\s+([a-z0-9][a-z0-9_-]+)\s+to\s+([a-z0-9][a-z0-9_-]+)\b',
    ]

    stopwords = {
        "data", "table", "tables", "database", "db", "schema", "warehouse",
        "source", "destination", "dest", "prod", "staging",
        # Avoid treating cloud vendor prefixes as tools (e.g. "to aws s3" -> "aws")
        "aws",
    }

    aliases = {
        "postgres": "postgresql",
        "postgre": "postgresql",
        "pg": "postgresql",
        "sf": "snowflake",
        "s3": "aws-s3",
        "aws_s3": "aws-s3",
        "aws-s3": "aws-s3",
        "aws s3": "aws-s3",
        "gcs": "gcs",
        "azure": "azure_blob",
        "azureblob": "azure_blob",
    }

    for pattern in explicit_patterns:
        matches = re.findall(pattern, text_lower)
        for match in matches:
            parts = match if isinstance(match, tuple) else (match,)
            for part in parts:
                tool_name = part.strip().lower()
                if not tool_name or tool_name in stopwords:
                    continue
                tool_name = aliases.get(tool_name, tool_name)
                if tool_name not in detected and len(tool_name) > 2:
                    detected.append(tool_name)

    # Normalize any tools detected from the registry loop as well (e.g., "s3" -> "aws-s3")
    normalized: List[str] = []
    seen = set()
    for t in detected:
        t_raw = str(t).strip().lower().replace("_", "-")
        t_norm = aliases.get(t_raw, t_raw)
        if not t_norm or t_norm in seen:
            continue
        seen.add(t_norm)
        normalized.append(t_norm)
    
    logger.info(f"🔍 Detected tools from text: {normalized}")
    return normalized