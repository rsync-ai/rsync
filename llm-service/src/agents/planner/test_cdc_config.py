#!/usr/bin/env python3
"""
Test suite for CDC Config Generator

Run with: python -m pytest test_cdc_config.py -v
Or directly: python test_cdc_config.py
"""

import unittest
import json
import os
import sys
from unittest import mock

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from src.agents.planner.cdc_config_generator import (
    CDCConfigGenerator,
    CDCConfigResult,
    get_cdc_config_generator
)


class TestCDCConfigGenerator(unittest.TestCase):
    """Test cases for CDCConfigGenerator"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Use a mock connectors dir (doesn't need to exist for template-based tests)
        self.generator = CDCConfigGenerator(
            llm_client=None,
            connectors_dir="/tmp/mock-connectors"
        )
    
    def test_mysql_config_generation(self):
        """Test MySQL CDC config generation"""
        result = self.generator.generate_config(
            source_type="mysql",
            connection_config={
                "host": "mysql.example.com",
                "port": 3306,
                "user": "cdc_user",
                "password": "secret123",
                "database": "production"
            },
            tables=["users", "orders"],
            cdc_mode="initial"
        )
        
        self.assertTrue(result.success)
        self.assertEqual(result.method_used, "template")
        self.assertIn("connector.class", result.config)
        self.assertEqual(result.config["connector.class"], "io.debezium.connector.mysql.MySqlConnector")
        self.assertEqual(result.config["database.hostname"], "mysql.example.com")
        self.assertEqual(result.config["database.port"], "3306")
        self.assertEqual(result.config["snapshot.mode"], "initial")
        self.assertIn("production.users", result.config["table.include.list"])
    
    def test_postgresql_config_generation(self):
        """Test PostgreSQL CDC config generation"""
        result = self.generator.generate_config(
            source_type="postgresql",
            connection_config={
                "host": "pg.example.com",
                "port": 5432,
                "user": "replication_user",
                "password": "secret",
                "database": "analytics"
            },
            tables=["events"],
            cdc_mode="streaming_only"
        )
        
        self.assertTrue(result.success)
        self.assertEqual(result.config["connector.class"], "io.debezium.connector.postgresql.PostgresConnector")
        self.assertEqual(result.config["snapshot.mode"], "never")  # streaming_only maps to never
        self.assertIn("plugin.name", result.config)
        self.assertEqual(result.config["plugin.name"], "pgoutput")
    
    def test_mongodb_config_generation(self):
        """Test MongoDB CDC config generation"""
        result = self.generator.generate_config(
            source_type="mongodb",
            connection_config={
                "host": "mongo.example.com",
                "port": 27017,
                "user": "admin",
                "password": "password",
                "database": "app_db"
            },
            tables=["users", "sessions"],
            cdc_mode="initial"
        )
        
        self.assertTrue(result.success)
        self.assertEqual(result.config["connector.class"], "io.debezium.connector.mongodb.MongoDbConnector")
        # Debezium 2.x+/3.x removed mongodb.hosts / mongodb.name; the single
        # mongodb.connection.string (URI) is the required key (see cdc_config_generator).
        self.assertIn("mongodb.connection.string", result.config)
    
    def test_sqlserver_config_generation(self):
        """Test SQL Server CDC config generation"""
        result = self.generator.generate_config(
            source_type="sqlserver",
            connection_config={
                "host": "mssql.example.com",
                "port": 1433,
                "user": "sa",
                "password": "StrongPassword!",
                "database": "enterprise"
            },
            tables=["customers"],
            cdc_mode="initial"
        )
        
        self.assertTrue(result.success)
        self.assertEqual(result.config["connector.class"], "io.debezium.connector.sqlserver.SqlServerConnector")
    
    def test_oracle_config_generation(self):
        """Test Oracle CDC config generation"""
        result = self.generator.generate_config(
            source_type="oracle",
            connection_config={
                "host": "oracle.example.com",
                "port": 1521,
                "user": "cdc_user",
                "password": "oracle_secret",
                "database": "ORCL"
            },
            tables=["EMPLOYEES"],
            cdc_mode="initial"
        )
        
        self.assertTrue(result.success)
        self.assertEqual(result.config["connector.class"], "io.debezium.connector.oracle.OracleConnector")
        self.assertIn("log.mining.strategy", result.config)
    
    def test_cdc_mode_mapping(self):
        """Test CDC mode to snapshot mode mapping"""
        # Test initial mode
        result1 = self.generator.generate_config(
            source_type="mysql",
            connection_config={"host": "localhost", "database": "test"},
            tables=["t1"],
            cdc_mode="initial"
        )
        self.assertEqual(result1.config["snapshot.mode"], "initial")
        
        # Test streaming_only mode
        result2 = self.generator.generate_config(
            source_type="mysql",
            connection_config={"host": "localhost", "database": "test"},
            tables=["t1"],
            cdc_mode="streaming_only"
        )
        self.assertEqual(result2.config["snapshot.mode"], "never")
        
        # Test schema_only mode
        result3 = self.generator.generate_config(
            source_type="mysql",
            connection_config={"host": "localhost", "database": "test"},
            tables=["t1"],
            cdc_mode="schema_only"
        )
        self.assertEqual(result3.config["snapshot.mode"], "schema_only")
    
    def test_table_list_formatting(self):
        """Test table list formatting for different databases"""
        # MySQL: db.table format
        result1 = self.generator.generate_config(
            source_type="mysql",
            connection_config={"host": "localhost", "database": "mydb"},
            tables=["table1", "table2"],
            cdc_mode="initial"
        )
        self.assertIn("mydb.table1", result1.config["table.include.list"])
        self.assertIn("mydb.table2", result1.config["table.include.list"])
        
        # PostgreSQL: public.table format
        result2 = self.generator.generate_config(
            source_type="postgresql",
            connection_config={"host": "localhost", "database": "mydb"},
            tables=["table1"],
            cdc_mode="initial"
        )
        self.assertIn("public.table1", result2.config["table.include.list"])
    
    def test_overrides(self):
        """Test config overrides"""
        result = self.generator.generate_config(
            source_type="mysql",
            connection_config={"host": "localhost", "database": "test"},
            tables=["t1"],
            cdc_mode="initial",
            **{"tasks.max": "2", "custom.property": "value"}
        )
        
        self.assertTrue(result.success)
        self.assertEqual(result.config["tasks.max"], "2")
        self.assertEqual(result.config["custom.property"], "value")
    
    def test_custom_connector_name(self):
        """Test custom connector name"""
        result = self.generator.generate_config(
            source_type="mysql",
            connection_config={"host": "localhost", "database": "test"},
            tables=["t1"],
            cdc_mode="initial",
            connector_name="my-custom-connector"
        )
        
        self.assertEqual(result.connector_name, "my-custom-connector")
    
    def test_unsupported_database_without_llm(self):
        """Test unsupported database type without LLM fallback"""
        result = self.generator.generate_config(
            source_type="unknown_db",
            connection_config={"host": "localhost", "database": "test"},
            tables=["t1"],
            cdc_mode="initial"
        )
        
        self.assertFalse(result.success)
        self.assertIn("Unknown CDC source type", result.error)
    
    def test_get_supported_databases(self):
        """Test listing supported databases"""
        supported = self.generator.get_supported_databases()
        
        self.assertIn("mysql", supported)
        self.assertIn("postgresql", supported)
        self.assertIn("mongodb", supported)
        self.assertIn("sqlserver", supported)
        self.assertIn("oracle", supported)
    
    def test_validate_config(self):
        """Test config validation"""
        # Valid config
        valid_config = {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": "localhost",
            "database.port": "3306",
            "database.user": "root"
        }
        result = self.generator.validate_config(valid_config)
        self.assertTrue(result["valid"])
        
        # Invalid config (missing connector.class)
        invalid_config = {
            "database.hostname": "localhost"
        }
        result = self.generator.validate_config(invalid_config)
        self.assertFalse(result["valid"])
        self.assertIn("connector.class", str(result["errors"]))
    
    def test_kafka_topic_generation(self):
        """Test Kafka topic name generation"""
        result = self.generator.generate_config(
            source_type="mysql",
            connection_config={"host": "localhost", "database": "mydb"},
            tables=["users"],
            cdc_mode="initial"
        )
        
        self.assertTrue(result.success)
        self.assertIn("mydb", result.kafka_topic)
        # Default SMT routing is connection-level (not per-table), and the topic
        # lives in the deployment's namespace so one PREFIXED ACL covers it on a
        # customer-managed cluster. Spelled out rather than recomputed from
        # kafka_topics.topic(): deriving the expectation from the code under test
        # would keep this green if qualification were dropped on one side only.
        self.assertEqual(result.kafka_topic, "rsync.cdc.mydb")

    def test_kafka_topic_follows_the_prefix_lever(self):
        """KAFKA_TOPIC_PREFIX="" is the migration lever, and it has to reach the
        CDC topic too -- an existing deployment has live topics under the bare
        names and the sink is pointed at whatever this returns."""
        with mock.patch.dict(os.environ, {"KAFKA_TOPIC_PREFIX": ""}):
            result = self.generator.generate_config(
                source_type="mysql",
                connection_config={"host": "localhost", "database": "mydb"},
                tables=["users"],
                cdc_mode="initial"
            )

        self.assertTrue(result.success)
        self.assertEqual(result.kafka_topic, "cdc.mydb")
        self.assertEqual(result.config["transforms.route.topic.format"], "cdc.mydb")

    def test_non_smt_topic_is_predicted_from_the_prefix_that_survived_overrides(self):
        """Without the SMT, the topic is Debezium's to create: it derives it from
        topic.prefix as ``<prefix>.<db>.<table>``. So kafka_topic is a PREDICTION,
        and it is only true if it is computed from the prefix actually shipped in
        the config -- including an operator override. Assert the relationship,
        not a literal, because that is the invariant that breaks silently: a
        wrong prediction subscribes the sink to a topic nobody writes."""
        result = self.generator.generate_config(
            source_type="mysql",
            connection_config={"host": "localhost", "database": "mydb"},
            tables=["users"],
            cdc_mode="initial",
            use_smt=False,
        )

        self.assertTrue(result.success)
        prefix = result.config["topic.prefix"]
        self.assertEqual(result.kafka_topic, f"{prefix}.mydb.users")
        self.assertTrue(
            prefix.startswith("rsync."),
            f"topic.prefix={prefix!r} is outside the namespace, so a PREFIXED "
            "rsync. ACL would not cover the CDC data topics",
        )

    def test_non_smt_prefix_override_is_qualified_and_still_wins(self):
        """An explicit topic.prefix override must survive -- and must still be
        qualified, or the override becomes a way to escape the namespace by
        accident."""
        result = self.generator.generate_config(
            source_type="mysql",
            connection_config={"host": "localhost", "database": "mydb"},
            tables=["users"],
            cdc_mode="initial",
            use_smt=False,
            **{"topic.prefix": "custom-prefix"},
        )

        self.assertTrue(result.success)
        self.assertEqual(result.config["topic.prefix"], "rsync.custom-prefix")
        self.assertEqual(result.kafka_topic, "rsync.custom-prefix.mydb.users")

    def test_non_smt_prefix_is_not_double_qualified(self):
        """topic.prefix round-trips through persisted connector configs, so an
        already-qualified value arriving a second time must not become
        rsync.rsync.cdc-mydb."""
        result = self.generator.generate_config(
            source_type="mysql",
            connection_config={"host": "localhost", "database": "mydb"},
            tables=["users"],
            cdc_mode="initial",
            use_smt=False,
            **{"topic.prefix": "rsync.cdc-mydb"},
        )

        self.assertEqual(result.config["topic.prefix"], "rsync.cdc-mydb")

    def test_smt_route_target_and_reported_topic_are_the_same_string(self):
        """The SMT writes to transforms.route.topic.format; kafka_topic is what
        the caller subscribes to. If the two ever diverge the pipeline reports
        healthy and delivers nothing, so pin them to each other."""
        result = self.generator.generate_config(
            source_type="mysql",
            connection_config={"host": "localhost", "database": "mydb"},
            tables=["users"],
            cdc_mode="initial"
        )

        self.assertEqual(result.config["transforms.route.topic.format"], result.kafka_topic)


class TestCDCConfigGeneratorSingleton(unittest.TestCase):
    """Test the singleton pattern for CDCConfigGenerator"""
    
    def test_get_cdc_config_generator_singleton(self):
        """Test that get_cdc_config_generator returns the same instance"""
        gen1 = get_cdc_config_generator()
        gen2 = get_cdc_config_generator()
        self.assertIs(gen1, gen2)


if __name__ == "__main__":
    # Run tests
    print("=" * 60)
    print("CDC Config Generator Test Suite")
    print("=" * 60)
    
    unittest.main(verbosity=2)
