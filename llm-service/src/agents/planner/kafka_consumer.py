#!/usr/bin/env python3
"""
Generic Planner Kafka Consumer
Builds execution plans for ANY MCP connector combination.

Flow:
1. Receives pipeline request from Kafka (with source/dest connection IDs)
2. Extracts trace context from Kafka headers for distributed tracing
3. Builds a generic execution plan based on connector types
4. Returns AgentResponse format for Orchestrator routing
"""

import os
import sys
import logging
import json
import threading
import time
import uuid
from typing import Dict, Any, List, Optional
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from src.utils.kafka_security import kafka_security_kwargs

# Import Avro utilities
try:
    from src.utils.avro_kafka import AvroKafkaProducer, AvroKafkaConsumer, DecodedMessage
    from src.utils.avro_serializer import get_serializer
    AVRO_AVAILABLE = True
except ImportError:
    AVRO_AVAILABLE = False

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

# Import telemetry utilities for trace propagation
from src.utils.telemetry import (
    init_tracer, start_span, get_current_trace_id,
    extract_trace_context, inject_trace_context,
    add_span_attributes, set_span_status
)
from opentelemetry.trace import SpanKind
from src.utils.kafka_topics import group, topic

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger("planner-kafka-consumer")

# Initialize tracer for this consumer
tracer = init_tracer("planner-kafka-consumer", "1.0.0")

# Configuration  
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKERS", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")).split(",")
INPUT_TOPIC = topic("agent.planner.requests")
OUTPUT_TOPIC = topic("agent.planner.responses")
# Qualified for the same reason the topics above are: on a customer-managed
# cluster the operator grants ACLs, and a bare group id is not covered by the
# PREFIXED "rsync." grant that covers the topics. The consumer would then join,
# be refused, and receive nothing -- with no crash to notice.
#
# Resolved at import, like INPUT_TOPIC/OUTPUT_TOPIC, and deliberately so: the
# group and the topics it reads MUST be computed under one environment. Nothing
# in this service mutates KAFKA_TOPIC_PREFIX after start-up, so import time and
# use time see the same value; splitting them would reintroduce exactly the
# topic/group drift this contract exists to prevent.
CONSUMER_GROUP = group("planner-service")


# =============================================================================
# GENERIC MCP CONNECTOR OPERATION MAPPING
# =============================================================================
# This defines what operations each connector category supports.
# New connectors automatically get the right operations based on their category.

CONNECTOR_CATEGORIES = {
    # Databases - support query/export for reading
    "databases": [
        "mysql", "postgresql", "postgres", "mongodb", "redis", "oracle",
        "sqlserver", "sqlite", "mariadb", "cassandra", "dynamodb", "cockroachdb",
        "clickhouse", "timescaledb", "influxdb", "neo4j", "elasticsearch"
    ],
    # Cloud storage - support read/write operations
    "cloud_storage": [
        "aws-s3", "s3", "gcs", "azure-blob", "azure_blob", "minio"
    ],
    # Data warehouses - support both read and write
    "data_warehouses": [
        "snowflake", "bigquery", "redshift", "databricks"
    ],
    # Streaming - special handling
    "streaming": [
        "kafka", "kinesis", "pubsub", "eventhub"
    ],
    # APIs - typically source only
    "apis": [
        "stripe", "hubspot", "salesforce", "slack", "shopify", "zendesk",
        "jira", "github", "gitlab", "notion"
    ]
}


def get_connector_category(connector_type: str) -> str:
    """Get the category of a connector type"""
    # Canonical connector ids are kebab-case; accept underscore legacy forms.
    connector_type = connector_type.lower().replace("_", "-")
    
    for category, connectors in CONNECTOR_CATEGORIES.items():
        if connector_type in connectors or connector_type.replace("_", "-") in connectors:
            return category
    
    # Default to databases for unknown types (most common case)
    return "databases"


def get_source_operation(connector_type: str) -> str:
    """Get the appropriate read operation for a source connector"""
    category = get_connector_category(connector_type)
    
    operations = {
        "databases": "export",      # Export data from DB
        "cloud_storage": "read",    # Read files from storage
        "data_warehouses": "query", # Query data warehouse
        "streaming": "consume",     # Consume from stream
        "apis": "fetch"             # Fetch from API
    }
    
    return operations.get(category, "export")


def get_destination_operation(connector_type: str) -> str:
    """Get the appropriate write operation for a destination connector"""
    category = get_connector_category(connector_type)
    
    operations = {
        "databases": "import",          # Import data to DB
        "cloud_storage": "import_data", # Write files to storage
        "data_warehouses": "load",      # Load into warehouse
        "streaming": "produce",         # Produce to stream
        "apis": "push"                  # Push to API
    }
    
    return operations.get(category, "import_data")


def build_generic_plan(
    pipeline_id: str,
    source_connection_id: Optional[str],
    source_type: Optional[str],
    destination_connection_id: Optional[str],
    destination_type: Optional[str],
    tables: Optional[List[str]] = None,
    options: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Build a generic execution plan for ANY source → destination combination.
    
    This is the core of connector-agnostic pipeline execution.
    
    Args:
        pipeline_id: Pipeline ID
        source_connection_id: Source connection UUID
        source_type: Source connector type (mysql, postgresql, etc.)
        destination_connection_id: Destination connection UUID
        destination_type: Destination connector type (aws-s3, snowflake, etc.)
        tables: Optional list of tables to transfer
        options: Additional options (format, compression, etc.)
        
    Returns:
        Generic execution plan
    """
    steps = []
    options = options or {}
    
    # Step 1: Export/Read from source
    if source_connection_id and source_type:
        source_op = get_source_operation(source_type)
        source_category = get_connector_category(source_type)
        
        source_params = {}
        
        # Add table parameter for databases
        if source_category == "databases":
            if tables:
                source_params["tables"] = tables
            else:
                source_params["all_tables"] = True
            source_params["format"] = options.get("format", "json")
            source_params["batch_size"] = options.get("batch_size", 10000)
        
        # Add path for cloud storage
        elif source_category == "cloud_storage":
            source_params["prefix"] = options.get("source_prefix", "")
            source_params["format"] = options.get("format", "json")
        
        steps.append({
            "id": "step_1_extract",
            "tool": source_type,
            "method": source_op,
            "connection_id": source_connection_id,
            "params": source_params,
            "description": f"Extract data from {source_type}"
        })
    
    # Step 2: Load/Write to destination
    if destination_connection_id and destination_type:
        dest_op = get_destination_operation(destination_type)
        dest_category = get_connector_category(destination_type)
        
        dest_params = {
            "source_data": "$step_1_extract.output"  # Reference previous step output
        }
        
        # Add path/bucket for cloud storage
        if dest_category == "cloud_storage":
            dest_params["prefix"] = options.get("dest_prefix", f"pipelines/{pipeline_id}/")
            dest_params["format"] = options.get("format", "json")
            dest_params["compression"] = options.get("compression", "none")
        
        # Add table for databases/warehouses
        elif dest_category in ["databases", "data_warehouses"]:
            dest_params["table"] = options.get("dest_table", "pipeline_output")
            dest_params["mode"] = options.get("mode", "append")  # append, overwrite, merge
        
        steps.append({
            "id": "step_2_load",
            "tool": destination_type,
            "method": dest_op,
            "connection_id": destination_connection_id,
            "params": dest_params,
            "description": f"Load data to {destination_type}"
        })
    
    plan = {
        "pipeline_id": pipeline_id,
        "steps": steps,
        "is_streaming": False,
        "source_type": source_type,
        "destination_type": destination_type,
        "metadata": {
            "source_category": get_connector_category(source_type) if source_type else None,
            "destination_category": get_connector_category(destination_type) if destination_type else None,
            "total_steps": len(steps)
        }
    }
    
    return plan


class PlannerKafkaConsumer:
    """Generic Kafka consumer for planner agent - works with any MCP connector"""
    
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.running = False
        
    def init_kafka(self):
        """Initialize Kafka consumer and producer with Avro support"""
        max_retries = 10
        
        # Check if Avro is enabled
        use_avro = os.getenv("KAFKA_USE_AVRO", "true").lower() == "true" and AVRO_AVAILABLE
        self.use_avro = use_avro
        
        for attempt in range(max_retries):
            try:
                logger.info(f"🔌 Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS} (attempt {attempt + 1}/{max_retries})")
                
                if use_avro:
                    # Use Avro-enabled consumer/producer
                    logger.info("📦 Initializing with Avro serialization support")
                    
                    brokers = ",".join(KAFKA_BOOTSTRAP_SERVERS) if isinstance(KAFKA_BOOTSTRAP_SERVERS, list) else KAFKA_BOOTSTRAP_SERVERS
                    
                    self.avro_consumer = AvroKafkaConsumer(
                        topics=[INPUT_TOPIC],
                        group_id=CONSUMER_GROUP,
                        bootstrap_servers=brokers,
                        auto_offset_reset='earliest',
                        enable_auto_commit=True,
                        session_timeout_ms=30000,
                    )
                    
                    self.avro_producer = AvroKafkaProducer(
                        bootstrap_servers=brokers,
                        use_avro=True,
                    )
                    
                    # Legacy references for compatibility
                    self.consumer = None
                    self.producer = None
                    
                    logger.info(f"✅ Avro Kafka consumer initialized for topic: {INPUT_TOPIC}")
                    logger.info(f"✅ Avro Kafka producer initialized for topic: {OUTPUT_TOPIC}")
                else:
                    # Use legacy JSON consumer/producer
                    logger.info("📄 Initializing with JSON serialization")
                    
                    security = kafka_security_kwargs()
                    self.consumer = KafkaConsumer(
                        INPUT_TOPIC,
                        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                        **security,
                        group_id=CONSUMER_GROUP,
                        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                        auto_offset_reset='earliest',  # Process all pending messages
                        enable_auto_commit=True,
                        session_timeout_ms=30000,
                        heartbeat_interval_ms=10000
                    )
                    
                    self.producer = KafkaProducer(
                        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                        **security,
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                        acks='all',
                        retries=3
                    )
                    
                    self.avro_consumer = None
                    self.avro_producer = None
                    
                    logger.info(f"✅ JSON Kafka consumer initialized for topic: {INPUT_TOPIC}")
                    logger.info(f"✅ JSON Kafka producer initialized for topic: {OUTPUT_TOPIC}")
                
                return True
                
            except Exception as e:
                logger.warning(f"⚠️  Kafka connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                else:
                    logger.error(f"❌ Failed to initialize Kafka after {max_retries} attempts")
                    return False
    
    def process_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a planning request and build a GENERIC execution plan.
        
        This is connector-agnostic - works for MySQL→S3, PostgreSQL→Snowflake, etc.
        
        Args:
            message: Planning request from Kafka containing:
                - pipeline_id: Pipeline UUID
                - source_connection_id: Source connection UUID
                - source_type: Source connector type
                - destination_connection_id: Destination connection UUID
                - destination_type: Destination connector type
                - (optional) tables: List of tables to transfer
                - (optional) options: Additional options
                
        Returns:
            AgentResponse format for Orchestrator
        """
        pipeline_id = message.get("pipeline_id", str(uuid.uuid4()))
        execution_id = message.get("execution_id", str(uuid.uuid4()))
        trace_id = message.get("trace_id", pipeline_id)
        task_id = f"plan-{pipeline_id[:8]}-{int(time.time())}"
        
        try:
            # Extract connection information
            source_connection_id = message.get("source_connection_id")
            source_type = message.get("source_type", "").lower()
            destination_connection_id = message.get("destination_connection_id")
            destination_type = message.get("destination_type", "").lower()
            
            logger.info("=" * 60)
            logger.info(f"🎯 Building GENERIC plan for pipeline: {pipeline_id}")
            logger.info(f"   Source: {source_type} ({source_connection_id[:8] if source_connection_id else 'N/A'}...)")
            logger.info(f"   Destination: {destination_type} ({destination_connection_id[:8] if destination_connection_id else 'N/A'}...)")
            logger.info("=" * 60)
            
            # Validate we have at least source and destination
            if not source_connection_id or not source_type:
                raise ValueError("Missing source connection ID or type")
            if not destination_connection_id or not destination_type:
                raise ValueError("Missing destination connection ID or type")
            
            # Build generic plan
            plan = build_generic_plan(
                pipeline_id=pipeline_id,
                source_connection_id=source_connection_id,
                source_type=source_type,
                destination_connection_id=destination_connection_id,
                destination_type=destination_type,
                tables=message.get("tables"),
                options=message.get("options", {})
            )
            
            logger.info(f"✅ Plan built with {len(plan['steps'])} steps")
            for step in plan["steps"]:
                logger.info(f"   → {step['id']}: {step['tool']}.{step['method']}")
            
            # Return in AgentResponse format (for Orchestrator compatibility)
            return {
                "task_id": task_id,
                "pipeline_id": pipeline_id,
                "correlation_id": trace_id,
                "agent": "planner",
                "status": "success",
                "result": {
                    "plan": plan,
                    "execution_id": execution_id,
                    "source_type": source_type,
                    "destination_type": destination_type
                },
                "error": None,
                "next_agent": "validator"  # Hint for orchestrator
            }
            
        except ValueError as e:
            logger.error(f"❌ Validation error: {e}")
            return {
                "task_id": task_id,
                "pipeline_id": pipeline_id,
                "correlation_id": trace_id,
                "agent": "planner",
                "status": "failed",
                "result": None,
                "error": str(e),
                "next_agent": None
            }
        except Exception as e:
            logger.exception(f"❌ Error building plan: {e}")
            return {
                "task_id": task_id,
                "pipeline_id": pipeline_id,
                "correlation_id": trace_id,
                "agent": "planner",
                "status": "failed",
                "result": None,
                "error": str(e),
                "next_agent": None
            }
    
    def _extract_trace_headers(self, message) -> Dict[str, str]:
        """Extract trace context headers from Kafka message headers.

        Handles both raw KafkaConsumer messages (headers = list of (key, bytes))
        and AvroKafkaConsumer DecodedMessage objects (headers = Dict[str, str]).
        """
        headers = {}
        if not message.headers:
            return headers
        if isinstance(message.headers, dict):
            # DecodedMessage from AvroKafkaConsumer — already decoded
            return dict(message.headers)
        for key, value in message.headers:
            if isinstance(value, bytes):
                headers[key] = value.decode('utf-8')
            else:
                headers[key] = str(value)
        return headers
    
    def send_response(self, response: Dict[str, Any], trace_headers: Dict[str, str] = None):
        """Send response to output topic with trace context"""
        try:
            trace_id = response.get('correlation_id', '')

            if getattr(self, 'use_avro', False) and self.avro_producer is not None:
                # Avro path: AvroKafkaProducer.send_agent_message handles serialization
                extra_headers = dict(trace_headers) if trace_headers else {}
                self.avro_producer.send_agent_message(OUTPUT_TOPIC, trace_id or 'planner', response)
                logger.info(f"✅ Sent AgentResponse (Avro) to {OUTPUT_TOPIC} (status: {response.get('status')}, trace_id: {trace_id or 'N/A'})")
                return

            # JSON path (legacy)
            kafka_headers = []
            if trace_headers:
                for key, value in trace_headers.items():
                    kafka_headers.append((key, value.encode('utf-8') if isinstance(value, str) else value))

            if trace_id:
                kafka_headers.append(('trace_id', trace_id.encode('utf-8')))

            future = self.producer.send(
                OUTPUT_TOPIC,
                value=response,
                headers=kafka_headers if kafka_headers else None
            )
            future.get(timeout=10)
            logger.info(f"✅ Sent AgentResponse to {OUTPUT_TOPIC} (status: {response.get('status')}, trace_id: {trace_id or 'N/A'})")
        except KafkaError as e:
            logger.error(f"❌ Failed to send response to Kafka: {e}")
    
    def consume_loop(self):
        """Main consumption loop with distributed tracing"""
        logger.info(f"🔄 Starting consumption loop for {INPUT_TOPIC}")
        self.running = True

        # Choose the correct message iterator based on the active serialization mode.
        # When KAFKA_USE_AVRO=true, self.consumer is None and self.avro_consumer is set;
        # iterating self.consumer would silently consume zero messages.
        if getattr(self, 'use_avro', False) and self.avro_consumer is not None:
            message_iter = self.avro_consumer.consume()
        else:
            message_iter = self.consumer

        try:
            for message in message_iter:
                if not self.running:
                    break

                try:
                    # Extract trace context from Kafka headers
                    trace_headers = self._extract_trace_headers(message)
                    trace_id = trace_headers.get('trace_id', trace_headers.get('X-Trace-ID', ''))

                    # Start a span for this consume operation
                    with start_span(
                        f"kafka.consume.{INPUT_TOPIC}",
                        kind=SpanKind.CONSUMER,
                        attributes={
                            "messaging.system": "kafka",
                            "messaging.destination": INPUT_TOPIC,
                            "messaging.kafka.partition": message.partition,
                            "messaging.kafka.offset": message.offset,
                            "trace_id": trace_id
                        }
                    ) as span:
                        request_data = message.value
                        pipeline_id = request_data.get('pipeline_id', 'unknown')

                        # Inject trace_id from headers into request if not present
                        if trace_id and not request_data.get('trace_id'):
                            request_data['trace_id'] = trace_id

                        logger.info(f"📨 Received planning request for pipeline: {pipeline_id} (trace_id: {trace_id or request_data.get('trace_id', 'N/A')})")

                        # Process the request (builds generic plan)
                        response = self.process_message(request_data)

                        # Add trace attributes to span
                        add_span_attributes(
                            pipeline_id=pipeline_id,
                            status=response.get('status', 'unknown')
                        )

                        # Inject current trace context for response
                        response_headers = {}
                        inject_trace_context(response_headers)

                        # Send response using the active producer
                        self.send_response(response, response_headers)

                        set_span_status(response.get('status') == 'success')

                except Exception as e:
                    logger.exception(f"❌ Error processing Kafka message: {e}")

        except KeyboardInterrupt:
            logger.info("⏹️  Stopping consumer...")
        finally:
            self.stop()
    
    def start(self):
        """Start the Kafka consumer in a separate thread"""
        if not self.init_kafka():
            logger.error("❌ Failed to start Kafka consumer - initialization failed")
            return False
        
        consumer_thread = threading.Thread(target=self.consume_loop, daemon=True)
        consumer_thread.start()
        
        logger.info("✅ Planner Kafka consumer started (GENERIC MODE)")
        return True
    
    def stop(self):
        """Stop the Kafka consumer"""
        self.running = False
        # Close Avro consumers/producers
        if hasattr(self, 'avro_consumer') and self.avro_consumer:
            self.avro_consumer.close()
        if hasattr(self, 'avro_producer') and self.avro_producer:
            self.avro_producer.close()
        # Close legacy consumers/producers
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.close()
        logger.info("✅ Kafka consumer stopped")


def run_consumer():
    """Run the Kafka consumer"""
    logger.info("=" * 60)
    logger.info("🚀 Generic Planner Kafka Consumer Starting")
    logger.info("=" * 60)
    logger.info(f"Kafka Brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Input Topic: {INPUT_TOPIC}")
    logger.info(f"Output Topic: {OUTPUT_TOPIC}")
    logger.info(f"Consumer Group: {CONSUMER_GROUP}")
    logger.info("")
    logger.info("Supported connector categories:")
    for category, connectors in CONNECTOR_CATEGORIES.items():
        logger.info(f"  {category}: {', '.join(connectors[:5])}...")
    logger.info("=" * 60)
    
    consumer = PlannerKafkaConsumer()
    
    if consumer.start():
        logger.info("✅ Consumer running. Press Ctrl+C to stop...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("⏹️  Shutting down...")
            consumer.stop()
    else:
        logger.error("❌ Failed to start consumer")
        sys.exit(1)


if __name__ == "__main__":
    run_consumer()
