/*
 * TopicRouter SMT - Routes all Debezium CDC events to a single connection-level topic.
 *
 * Instead of creating one topic per table (default Debezium behavior),
 * this SMT routes ALL tables from a connection to a single topic:
 *   cdc.{connection_name}
 *
 * The original table information is preserved in message headers for downstream processing.
 */
package com.rsync.kafka.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Single Message Transform that routes all CDC events to a connection-level topic.
 *
 * <p>Configuration:
 * <ul>
 *   <li>topic.format - Format for the target topic. Default: "cdc.${connection}"</li>
 *   <li>connection.field - Field name containing connection identifier. Default: from topic name</li>
 *   <li>preserve.original.topic - Whether to add original topic as header. Default: true</li>
 * </ul>
 *
 * <p>Example configuration:
 * <pre>
 * "transforms": "route",
 * "transforms.route.type": "com.rsync.kafka.smt.TopicRouter",
 * "transforms.route.topic.format": "cdc.${connection}",
 * "transforms.route.preserve.original.topic": "true"
 * </pre>
 */
public class TopicRouter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(TopicRouter.class);

    // Configuration keys
    public static final String TOPIC_FORMAT_CONFIG = "topic.format";
    public static final String TOPIC_FORMAT_DEFAULT = "cdc.${connection}";
    public static final String TOPIC_FORMAT_DOC = "Format for target topic. Use ${connection} as placeholder.";

    public static final String PRESERVE_ORIGINAL_CONFIG = "preserve.original.topic";
    public static final boolean PRESERVE_ORIGINAL_DEFAULT = true;
    public static final String PRESERVE_ORIGINAL_DOC = "Add original topic name as header.";

    public static final String CONNECTION_NAME_CONFIG = "connection.name";
    public static final String CONNECTION_NAME_DEFAULT = "";
    public static final String CONNECTION_NAME_DOC = "Explicit connection name. If empty, extracted from topic.";

    // Header names for metadata
    public static final String HEADER_ORIGINAL_TOPIC = "rsync.original.topic";
    public static final String HEADER_DATABASE = "rsync.database";
    public static final String HEADER_SCHEMA = "rsync.schema";
    public static final String HEADER_TABLE = "rsync.table";
    public static final String HEADER_CONNECTION = "rsync.connection";

    // Pattern to extract database.schema.table from Debezium topic
    // Format: {server_name}.{database}.{table} or {server_name}.{database}.{schema}.{table}
    private static final Pattern DEBEZIUM_TOPIC_PATTERN =
            Pattern.compile("^([^.]+)\\.([^.]+)\\.([^.]+)(?:\\.([^.]+))?$");

    private String topicFormat;
    private boolean preserveOriginal;
    private String connectionName;

    @Override
    public void configure(Map<String, ?> configs) {
        // configs is Map<String, ?>, so avoid getOrDefault with String defaults (generic capture issues).
        final Object topicFmtObj = configs.get(TOPIC_FORMAT_CONFIG);
        topicFormat = (topicFmtObj == null ? TOPIC_FORMAT_DEFAULT : topicFmtObj.toString());

        final Object preserveObj = configs.get(PRESERVE_ORIGINAL_CONFIG);
        preserveOriginal = Boolean.parseBoolean(
                preserveObj == null ? String.valueOf(PRESERVE_ORIGINAL_DEFAULT) : preserveObj.toString()
        );

        final Object connNameObj = configs.get(CONNECTION_NAME_CONFIG);
        connectionName = (connNameObj == null ? CONNECTION_NAME_DEFAULT : connNameObj.toString());

        log.info("TopicRouter configured: format={}, preserveOriginal={}, connectionName={}",
                topicFormat, preserveOriginal, connectionName);
    }

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        String originalTopic = record.topic();

        // Parse the original topic to extract components
        TopicComponents components = parseDebeziumTopic(originalTopic);

        // Determine connection name
        String connection = connectionName.isEmpty() ? components.serverName : connectionName;

        // Build the new topic name
        String newTopic = topicFormat
                .replace("${connection}", sanitizeForTopic(connection))
                .replace("${database}", sanitizeForTopic(components.database))
                .replace("${schema}", sanitizeForTopic(components.schema))
                .replace("${table}", sanitizeForTopic(components.table));

        // Add headers with original topic info
        Headers headers = record.headers();
        if (preserveOriginal) {
            headers.addString(HEADER_ORIGINAL_TOPIC, originalTopic);
        }
        headers.addString(HEADER_CONNECTION, connection);
        headers.addString(HEADER_DATABASE, components.database);
        headers.addString(HEADER_SCHEMA, components.schema);
        headers.addString(HEADER_TABLE, components.table);

        log.debug("Routing {} -> {} (connection={}, table={})",
                originalTopic, newTopic, connection, components.table);

        // Create new record with updated topic
        return record.newRecord(
                newTopic,
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                headers
        );
    }

    /**
     * Parse a Debezium topic name into its components.
     *
     * Debezium topics follow the pattern:
     * - MySQL: {server}.{database}.{table}
     * - PostgreSQL: {server}.{schema}.{table}
     * - SQL Server: {server}.{database}.{schema}.{table}
     */
    private TopicComponents parseDebeziumTopic(String topic) {
        TopicComponents components = new TopicComponents();

        Matcher matcher = DEBEZIUM_TOPIC_PATTERN.matcher(topic);
        if (matcher.matches()) {
            components.serverName = matcher.group(1);

            if (matcher.group(4) != null) {
                // 4 parts: server.database.schema.table
                components.database = matcher.group(2);
                components.schema = matcher.group(3);
                components.table = matcher.group(4);
            } else {
                // 3 parts: server.database.table or server.schema.table
                components.database = matcher.group(2);
                components.schema = matcher.group(2); // Schema same as database for MySQL
                components.table = matcher.group(3);
            }
        } else {
            // Fallback: use full topic as table name
            log.warn("Could not parse Debezium topic format: {}", topic);
            components.serverName = "unknown";
            components.database = "unknown";
            components.schema = "default";
            components.table = topic;
        }

        return components;
    }

    /**
     * Sanitize a string for use in a Kafka topic name.
     */
    private String sanitizeForTopic(String value) {
        if (value == null) {
            return "unknown";
        }
        // Replace invalid characters with hyphens, lowercase
        return value.toLowerCase()
                .replaceAll("[^a-z0-9._-]", "-")
                .replaceAll("-+", "-")
                .replaceAll("^-|-$", "");
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(TOPIC_FORMAT_CONFIG,
                        ConfigDef.Type.STRING,
                        TOPIC_FORMAT_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        TOPIC_FORMAT_DOC)
                .define(PRESERVE_ORIGINAL_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        PRESERVE_ORIGINAL_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        PRESERVE_ORIGINAL_DOC)
                .define(CONNECTION_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        CONNECTION_NAME_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        CONNECTION_NAME_DOC);
    }

    @Override
    public void close() {
        // No resources to close
    }

    /**
     * Helper class to hold parsed topic components.
     */
    private static class TopicComponents {
        String serverName = "";
        String database = "";
        String schema = "";
        String table = "";
    }
}
