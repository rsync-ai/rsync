"""
Avro-enabled Kafka Producer and Consumer for RSync Platform.

This module provides Kafka integration with Avro serialization support,
compatible with all MCP connectors and the Confluent Schema Registry.

Usage:
    from src.utils.avro_kafka import AvroKafkaProducer, AvroKafkaConsumer
    
    # Producer
    producer = AvroKafkaProducer()
    producer.send_pipeline_request(topic, trace_id, data)
    
    # Consumer
    consumer = AvroKafkaConsumer(topics=["agent.planner.requests"])
    for message in consumer.consume():
        print(message.value)
"""

import os
import json
import logging
import time
from typing import Dict, Any, List, Optional, Iterator, Callable
from dataclasses import dataclass
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

from .kafka_security import kafka_security_kwargs, parse_brokers
from .kafka_topics import (
    topic as qualify_topic,
    topics as qualify_topics,
    group as qualify_group,
)

from .avro_serializer import (
    AvroSerializer, get_serializer,
    serialize_pipeline_request, serialize_agent_message,
    serialize_mcp_message, serialize_cdc_event,
    deserialize_message
)

logger = logging.getLogger(__name__)


@dataclass
class DecodedMessage:
    """Decoded Kafka message with Avro support."""
    topic: str
    partition: int
    offset: int
    key: str
    value: Dict[str, Any]
    headers: Dict[str, str]
    timestamp: int
    schema_name: Optional[str] = None
    trace_id: Optional[str] = None
    is_avro: bool = False


class AvroKafkaProducer:
    """
    Kafka producer with Avro serialization support.
    
    Automatically detects whether to use Avro or JSON based on configuration.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = None,
        use_avro: bool = None,
        schema_registry_url: str = None,
        acks: str = 'all',
        retries: int = 3
    ):
        """
        Initialize Avro Kafka producer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            use_avro: Whether to use Avro (default: from env KAFKA_USE_AVRO)
            schema_registry_url: Schema Registry URL
            acks: Producer acknowledgment mode
            retries: Number of retries
        """
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BROKERS", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        )
        
        # Determine if Avro is enabled
        if use_avro is None:
            use_avro = os.getenv("KAFKA_USE_AVRO", "true").lower() == "true"
        self.use_avro = use_avro
        
        self.serializer = get_serializer() if use_avro else None
        
        # Create base Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=parse_brokers(self.bootstrap_servers),
            **kafka_security_kwargs(),
            acks=acks,
            retries=retries,
            # Use byte serializers - we handle serialization ourselves
            key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else k,
            value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode('utf-8'),
        )
        
        mode = "Avro" if self.use_avro else "JSON"
        logger.info(f"✓ AvroKafkaProducer initialized ({mode} mode) - {self.bootstrap_servers}")
    
    def send(
        self,
        topic: str,
        key: str,
        value: Dict[str, Any],
        schema_name: str = None,
        headers: Dict[str, str] = None
    ) -> bool:
        """
        Send a message to Kafka.
        
        Args:
            topic: Kafka topic
            key: Message key
            value: Message value (dict)
            schema_name: Avro schema name (required if use_avro=True)
            headers: Optional headers
            
        Returns:
            True if successful
        """
        # Qualify here rather than at each call site: every send in this
        # service funnels through this method, so a topic named anywhere --
        # including in code added later -- lands in the platform namespace.
        topic = qualify_topic(topic)
        try:
            # Build headers
            kafka_headers = []
            if headers:
                for k, v in headers.items():
                    kafka_headers.append((k, v.encode('utf-8') if isinstance(v, str) else v))
            
            # Add trace_id header
            trace_id = value.get('trace_id', key)
            kafka_headers.extend([
                ('trace_id', trace_id.encode('utf-8')),
                ('X-Trace-ID', trace_id.encode('utf-8')),
            ])
            
            # Serialize value
            if self.use_avro and schema_name:
                encoded_value = self.serializer.serialize(topic, schema_name, value)
                kafka_headers.extend([
                    ('content-type', b'application/avro'),
                    ('avro.schema.name', schema_name.encode('utf-8')),
                ])
            else:
                encoded_value = json.dumps(value).encode('utf-8')
                kafka_headers.append(('content-type', b'application/json'))
            
            # Send
            future = self.producer.send(
                topic,
                key=key.encode('utf-8'),
                value=encoded_value,
                headers=kafka_headers
            )
            future.get(timeout=10)  # Wait for acknowledgment
            
            logger.debug(f"✓ Sent message to {topic} (key={key}, avro={self.use_avro})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send message to {topic}: {e}")
            return False
    
    def send_pipeline_request(
        self,
        topic: str,
        trace_id: str,
        data: Dict[str, Any]
    ) -> bool:
        """Send a pipeline request message."""
        if 'trace_id' not in data:
            data['trace_id'] = trace_id
        if 'created_at' not in data:
            data['created_at'] = int(time.time() * 1000)
            
        return self.send(
            topic, trace_id, data,
            schema_name="com.rsync.pipeline.PipelineRequest"
        )
    
    def send_agent_message(
        self,
        topic: str,
        trace_id: str,
        data: Dict[str, Any]
    ) -> bool:
        """Send an agent-to-agent message."""
        if 'trace_id' not in data:
            data['trace_id'] = trace_id
        if 'timestamp' not in data:
            data['timestamp'] = int(time.time() * 1000)
            
        return self.send(
            topic, trace_id, data,
            schema_name="com.rsync.agent.AgentMessage"
        )
    
    def send_mcp_message(
        self,
        topic: str,
        trace_id: str,
        data: Dict[str, Any]
    ) -> bool:
        """Send an MCP connector message."""
        if 'trace_id' not in data:
            data['trace_id'] = trace_id
        if 'timestamp' not in data:
            data['timestamp'] = int(time.time() * 1000)
            
        return self.send(
            topic, trace_id, data,
            schema_name="com.rsync.mcp.MCPMessage"
        )
    
    def send_cdc_event(
        self,
        topic: str,
        trace_id: str,
        data: Dict[str, Any]
    ) -> bool:
        """Send a CDC change event."""
        if 'trace_id' not in data:
            data['trace_id'] = trace_id
        if 'processed_ts' not in data:
            data['processed_ts'] = int(time.time() * 1000)
            
        return self.send(
            topic, trace_id, data,
            schema_name="com.rsync.cdc.ChangeEvent"
        )
    
    def flush(self, timeout: float = 10):
        """Flush pending messages."""
        self.producer.flush(timeout=timeout)
    
    def close(self):
        """Close the producer."""
        self.producer.close()


def _consumer_group_id(qualified_topics: List[str], group_id: Optional[str]) -> str:
    """Resolve this consumer's group id, namespaced like every other Kafka name.

    Both arms go through ``kafka_topics.group`` -- the Python half of
    ``shared/go/kafkaclient/groups.go`` -- so one PREFIXED ACL on a customer's
    cluster covers this consumer's topics and its group together. That includes
    a ``group_id`` the caller passed in: an unqualified escape hatch would leave
    exactly one group outside the operator's single grant, and the resulting
    authorization failure surfaces as a consumer that receives nothing rather
    than as an error. Qualification is idempotent, so a caller who already
    spelled the namespace gets its string back verbatim, and
    ``KAFKA_TOPIC_PREFIX=""`` still disables the whole thing.

    The derived fallback is built from the ALREADY-QUALIFIED topic names, which
    is why it can read ``rsync.avro-consumer-rsync.agent.planner.requests``. The
    doubled namespace is deliberate: it makes the group id a function of the
    subscription as actually resolved, so ``AvroKafkaConsumer(["x"])`` and
    ``AvroKafkaConsumer([topic("x")])`` -- the same subscription spelled two
    ways -- land in the SAME group. Deriving from the logical names instead
    reads better and puts those two in different groups, which is not a
    cosmetic difference: both would then receive every record instead of
    sharing the partitions between them.
    """
    return qualify_group(group_id or f"avro-consumer-{'-'.join(qualified_topics)}")


class AvroKafkaConsumer:
    """
    Kafka consumer with Avro deserialization support.
    
    Automatically detects Avro or JSON format and deserializes accordingly.
    """
    
    def __init__(
        self,
        topics: List[str],
        group_id: str = None,
        bootstrap_servers: str = None,
        auto_offset_reset: str = 'earliest',
        enable_auto_commit: bool = True,
        session_timeout_ms: int = 30000,
        max_poll_interval_ms: int = 300000
    ):
        """
        Initialize Avro Kafka consumer.
        
        Args:
            topics: List of topics to subscribe to
            group_id: Consumer group ID. Namespaced under
                KAFKA_TOPIC_PREFIX like every other Kafka name here;
                derived from the topic list when omitted.
            bootstrap_servers: Kafka bootstrap servers
            auto_offset_reset: Where to start reading
            enable_auto_commit: Enable auto commit
            session_timeout_ms: Session timeout
            max_poll_interval_ms: Max poll interval
        """
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BROKERS", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        )
        # Qualified before group_id is derived from it, so the subscription
        # and the group name cannot disagree about which topics these are.
        topics = qualify_topics(*topics)
        self.topics = topics
        self.group_id = _consumer_group_id(topics, group_id)
        
        self.serializer = get_serializer()
        
        # Create Kafka consumer
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=parse_brokers(self.bootstrap_servers),
            **kafka_security_kwargs(),
            group_id=self.group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            session_timeout_ms=session_timeout_ms,
            max_poll_interval_ms=max_poll_interval_ms,
            # Don't deserialize - we handle it
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            value_deserializer=None,  # Raw bytes
        )
        
        logger.info(f"✓ AvroKafkaConsumer initialized for topics: {topics}")
    
    def _decode_message(self, msg) -> DecodedMessage:
        """Decode a Kafka message (Avro or JSON)."""
        # Parse headers
        headers = {}
        schema_name = None
        trace_id = None
        is_avro = False
        
        if msg.headers:
            for key, value in msg.headers:
                if isinstance(value, bytes):
                    value = value.decode('utf-8')
                headers[key] = value
                
                if key == 'avro.schema.name':
                    schema_name = value
                    is_avro = True
                elif key == 'content-type' and value == 'application/avro':
                    is_avro = True
                elif key in ('trace_id', 'X-Trace-ID'):
                    trace_id = value
        
        # Deserialize value
        if is_avro and self.serializer.is_avro_format(msg.value):
            try:
                value = self.serializer.deserialize(msg.value, schema_name)
            except Exception as e:
                logger.warning(f"Avro deserialization failed, trying JSON: {e}")
                try:
                    # Try stripping wire format header
                    value = json.loads(msg.value[5:].decode('utf-8'))
                except:
                    value = json.loads(msg.value.decode('utf-8'))
                    is_avro = False
        else:
            try:
                value = json.loads(msg.value.decode('utf-8'))
            except Exception as e:
                logger.error(f"Failed to decode message: {e}")
                value = {"_raw": msg.value.decode('utf-8', errors='replace')}
        
        # Extract trace_id from value if not in headers
        if not trace_id and isinstance(value, dict):
            trace_id = value.get('trace_id')
        
        return DecodedMessage(
            topic=msg.topic,
            partition=msg.partition,
            offset=msg.offset,
            key=msg.key,
            value=value,
            headers=headers,
            timestamp=msg.timestamp,
            schema_name=schema_name,
            trace_id=trace_id,
            is_avro=is_avro
        )
    
    def consume(
        self,
        timeout_ms: int = 1000,
        max_records: int = None
    ) -> Iterator[DecodedMessage]:
        """
        Consume messages from Kafka.
        
        Args:
            timeout_ms: Poll timeout in milliseconds
            max_records: Max records per poll
            
        Yields:
            DecodedMessage objects
        """
        while True:
            try:
                records = self.consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
                for topic_partition, messages in records.items():
                    for msg in messages:
                        yield self._decode_message(msg)
            except Exception as e:
                logger.error(f"Error consuming messages: {e}")
                time.sleep(1)
    
    def consume_once(self, timeout_ms: int = 5000) -> List[DecodedMessage]:
        """
        Consume available messages once (non-blocking).
        
        Args:
            timeout_ms: Poll timeout
            
        Returns:
            List of decoded messages
        """
        messages = []
        try:
            records = self.consumer.poll(timeout_ms=timeout_ms)
            for topic_partition, msgs in records.items():
                for msg in msgs:
                    messages.append(self._decode_message(msg))
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
        return messages
    
    def commit(self):
        """Commit current offsets."""
        self.consumer.commit()
    
    def close(self):
        """Close the consumer."""
        self.consumer.close()


def create_avro_producer() -> AvroKafkaProducer:
    """Factory function to create an Avro Kafka producer."""
    return AvroKafkaProducer()


def create_avro_consumer(topics: List[str], group_id: str = None) -> AvroKafkaConsumer:
    """Factory function to create an Avro Kafka consumer."""
    return AvroKafkaConsumer(topics=topics, group_id=group_id)
