"""
Unit tests for Avro Serialization utilities.

Tests the AvroSerializer and related Kafka integration.
"""

import pytest
import json
import struct
import time
from unittest.mock import Mock, patch, MagicMock

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from src.utils.avro_serializer import (
    AvroSerializer,
    SchemaRegistryClient,
    get_serializer,
    serialize_pipeline_request,
    serialize_agent_message,
    serialize_mcp_message,
    serialize_cdc_event,
    deserialize_message,
)


class TestSchemaRegistryClient:
    """Tests for Schema Registry client."""
    
    def test_init(self):
        """Test client initialization."""
        client = SchemaRegistryClient("http://localhost:8081")
        assert client.url == "http://localhost:8081"
        assert client.schema_cache == {}
        assert client.subject_cache == {}
    
    def test_init_with_auth(self):
        """Test client with authentication."""
        client = SchemaRegistryClient("http://localhost:8081", auth=("user", "pass"))
        assert client.auth == ("user", "pass")
    
    @patch('requests.Session.get')
    def test_get_schema_by_id_cached(self, mock_get):
        """Test schema retrieval from cache."""
        client = SchemaRegistryClient("http://localhost:8081")
        
        # Pre-populate cache
        cached_schema = {"type": "record", "name": "Test"}
        client.schema_cache[123] = cached_schema
        
        # Should return from cache without HTTP call
        result = client.get_schema_by_id(123)
        assert result == cached_schema
        mock_get.assert_not_called()
    
    @patch('requests.Session.get')
    def test_get_schema_by_id_from_registry(self, mock_get):
        """Test schema retrieval from registry."""
        client = SchemaRegistryClient("http://localhost:8081")
        
        schema = {"type": "record", "name": "Test"}
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"schema": json.dumps(schema)}
        mock_get.return_value = mock_response
        
        result = client.get_schema_by_id(456)
        assert result == schema
        assert 456 in client.schema_cache


class TestAvroSerializer:
    """Tests for Avro serializer."""
    
    def test_init(self):
        """Test serializer initialization."""
        serializer = AvroSerializer(
            schema_registry_url="http://localhost:8081",
            auto_register=False
        )
        assert serializer.schema_registry_url == "http://localhost:8081"
        assert serializer.auto_register is False
    
    def test_builtin_schemas(self):
        """Test built-in schemas are loaded."""
        serializer = AvroSerializer()
        
        assert "com.rsync.pipeline.PipelineRequest" in serializer.SCHEMAS
        assert "com.rsync.agent.AgentMessage" in serializer.SCHEMAS
        assert "com.rsync.mcp.MCPMessage" in serializer.SCHEMAS
        assert "com.rsync.cdc.ChangeEvent" in serializer.SCHEMAS
    
    def test_register_custom_schema(self):
        """Test registering custom schema."""
        serializer = AvroSerializer()
        
        custom_schema = {
            "namespace": "com.test",
            "type": "record",
            "name": "CustomRecord",
            "fields": [{"name": "field1", "type": "string"}]
        }
        
        serializer.register_schema("com.test.CustomRecord", custom_schema)
        assert "com.test.CustomRecord" in serializer.SCHEMAS
    
    def test_get_subject_name_topic_record_strategy(self):
        """Test TopicRecordNameStrategy subject naming."""
        serializer = AvroSerializer(subject_name_strategy="TopicRecordNameStrategy")
        
        subject = serializer.get_subject_name(
            "my-topic",
            "com.rsync.pipeline.PipelineRequest",
            is_key=False
        )
        assert subject == "my-topic-com.rsync.pipeline.PipelineRequest"
    
    def test_get_subject_name_topic_strategy(self):
        """Test TopicNameStrategy subject naming."""
        serializer = AvroSerializer(subject_name_strategy="TopicNameStrategy")
        
        subject = serializer.get_subject_name("my-topic", "AnySchema", is_key=False)
        assert subject == "my-topic-value"
        
        subject_key = serializer.get_subject_name("my-topic", "AnySchema", is_key=True)
        assert subject_key == "my-topic-key"
    
    @patch.object(SchemaRegistryClient, 'register_schema')
    def test_serialize_wire_format(self, mock_register):
        """Test Avro wire format serialization."""
        mock_register.return_value = 123  # Schema ID
        
        serializer = AvroSerializer(auto_register=True)
        
        data = {
            "trace_id": "test-trace",
            "pipeline_id": "pipeline-123",
            "pipeline_name": None,
            "source": {
                "connection_id": "conn-1",
                "connector_type": "mysql",
                "config": None
            },
            "destination": {
                "connection_id": "conn-2",
                "connector_type": "postgresql",
                "config": None
            },
            "sync_mode": "FULL",
            "entities": None,
            "pii_handling": None,
            "schedule": None,
            "metadata": None,
            "created_at": int(time.time() * 1000),
            "user_id": None
        }
        
        result = serializer.serialize(
            "agent.planner.requests",
            "com.rsync.pipeline.PipelineRequest",
            data
        )
        
        # Check wire format
        assert result[0] == 0  # Magic byte
        schema_id = struct.unpack('>I', result[1:5])[0]
        assert schema_id == 123
        
        # Payload should be JSON or Avro binary
        payload = result[5:]
        assert len(payload) > 0
    
    def test_serialize_unknown_schema(self):
        """Test serialization with unknown schema raises error."""
        serializer = AvroSerializer()
        
        with pytest.raises(ValueError, match="Unknown schema"):
            serializer.serialize("topic", "com.unknown.Schema", {"data": "test"})
    
    def _json_wire_data(self):
        """Confluent wire framing (magic 0, schema id 0) around a JSON payload."""
        payload = json.dumps({"trace_id": "test", "field": "value"}).encode('utf-8')
        return struct.pack('>bI', 0, 0) + payload

    def test_deserialize_wire_format_json_payload_is_fail_closed(self, monkeypatch):
        """Schema id 0 with a JSON body must NOT be silently accepted by default.

        The legacy behaviour was to fall back to json.loads() on any payload the
        binary reader could not handle. That turned an unresolvable schema into a
        successful parse of attacker-shaped bytes, so it now requires an explicit
        opt-in and otherwise raises.
        """
        monkeypatch.delenv("AVRO_ALLOW_JSON_PAYLOAD", raising=False)
        serializer = AvroSerializer()

        with pytest.raises(ValueError):
            serializer.deserialize(self._json_wire_data())

    def test_deserialize_wire_format_json_payload_opt_in(self, monkeypatch):
        """AVRO_ALLOW_JSON_PAYLOAD re-enables the legacy path for migration."""
        monkeypatch.setenv("AVRO_ALLOW_JSON_PAYLOAD", "1")
        serializer = AvroSerializer()

        result = serializer.deserialize(self._json_wire_data())
        assert result["trace_id"] == "test"
        assert result["field"] == "value"
    
    def test_deserialize_non_avro_json(self):
        """Test deserialization of plain JSON (non-Avro)."""
        serializer = AvroSerializer()
        
        # Plain JSON (no magic byte)
        json_data = json.dumps({"key": "value"}).encode('utf-8')
        
        # Should fail with non-zero magic byte
        with pytest.raises(ValueError):
            serializer.deserialize(json_data)
    
    def test_is_avro_format(self):
        """Test Avro format detection."""
        serializer = AvroSerializer()
        
        # Valid Avro wire format
        avro_data = struct.pack('>bI', 0, 123) + b'payload'
        assert serializer.is_avro_format(avro_data) is True
        
        # Plain JSON
        json_data = b'{"key": "value"}'
        assert serializer.is_avro_format(json_data) is False
        
        # Too short
        assert serializer.is_avro_format(b'abc') is False


class TestConvenienceFunctions:
    """Tests for convenience serialization functions."""
    
    @patch('src.utils.avro_serializer.get_serializer')
    def test_serialize_pipeline_request(self, mock_get_serializer):
        """Test serialize_pipeline_request convenience function."""
        mock_serializer = Mock()
        mock_serializer.serialize.return_value = b'serialized'
        mock_get_serializer.return_value = mock_serializer
        
        result = serialize_pipeline_request("topic", {"trace_id": "test"})
        
        mock_serializer.serialize.assert_called_once_with(
            "topic",
            "com.rsync.pipeline.PipelineRequest",
            {"trace_id": "test"}
        )
        assert result == b'serialized'
    
    @patch('src.utils.avro_serializer.get_serializer')
    def test_serialize_agent_message(self, mock_get_serializer):
        """Test serialize_agent_message convenience function."""
        mock_serializer = Mock()
        mock_serializer.serialize.return_value = b'serialized'
        mock_get_serializer.return_value = mock_serializer
        
        result = serialize_agent_message("topic", {"trace_id": "test"})
        
        mock_serializer.serialize.assert_called_once_with(
            "topic",
            "com.rsync.agent.AgentMessage",
            {"trace_id": "test"}
        )
    
    @patch('src.utils.avro_serializer.get_serializer')
    def test_serialize_mcp_message(self, mock_get_serializer):
        """Test serialize_mcp_message convenience function."""
        mock_serializer = Mock()
        mock_serializer.serialize.return_value = b'serialized'
        mock_get_serializer.return_value = mock_serializer
        
        result = serialize_mcp_message("topic", {"trace_id": "test"})
        
        mock_serializer.serialize.assert_called_once_with(
            "topic",
            "com.rsync.mcp.MCPMessage",
            {"trace_id": "test"}
        )
    
    @patch('src.utils.avro_serializer.get_serializer')
    def test_serialize_cdc_event(self, mock_get_serializer):
        """Test serialize_cdc_event convenience function."""
        mock_serializer = Mock()
        mock_serializer.serialize.return_value = b'serialized'
        mock_get_serializer.return_value = mock_serializer
        
        result = serialize_cdc_event("topic", {"trace_id": "test"})
        
        mock_serializer.serialize.assert_called_once_with(
            "topic",
            "com.rsync.cdc.ChangeEvent",
            {"trace_id": "test"}
        )
    
    @patch('src.utils.avro_serializer.get_serializer')
    def test_deserialize_message(self, mock_get_serializer):
        """Test deserialize_message convenience function."""
        mock_serializer = Mock()
        mock_serializer.deserialize.return_value = {"data": "test"}
        mock_get_serializer.return_value = mock_serializer
        
        result = deserialize_message(b'data', schema_hint="com.rsync.test.Schema")
        
        mock_serializer.deserialize.assert_called_once_with(b'data', "com.rsync.test.Schema")
        assert result == {"data": "test"}


class TestSingletonSerializer:
    """Tests for singleton serializer instance."""
    
    def test_get_serializer_singleton(self):
        """Test get_serializer returns singleton."""
        # Reset singleton
        import src.utils.avro_serializer as module
        module._serializer = None
        
        s1 = get_serializer()
        s2 = get_serializer()
        
        assert s1 is s2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
