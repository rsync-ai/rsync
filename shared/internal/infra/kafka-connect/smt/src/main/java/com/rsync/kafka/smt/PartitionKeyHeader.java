/*
 * PartitionKeyHeader SMT - Adds a partition key header to Debezium CDC events.
 *
 * This SMT extracts the primary key from CDC events and adds it as a header
 * in the format: {connection}.{schema}.{table}.{record_id}
 *
 * This allows downstream consumers to use consistent partition keys
 * for ordering guarantees without modifying the message key itself.
 */
package com.rsync.kafka.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Single Message Transform that adds partition key information as headers.
 *
 * <p>This SMT extracts:
 * <ul>
 *   <li>Connection name (from config or topic)</li>
 *   <li>Schema/database name</li>
 *   <li>Table name</li>
 *   <li>Primary key value(s)</li>
 * </ul>
 *
 * <p>And produces headers:
 * <ul>
 *   <li>rsync.partition.key.base - {connection}.{schema}.{table}</li>
 *   <li>rsync.partition.key.full - {connection}.{schema}.{table}.{record_id}</li>
 *   <li>rsync.record.id - The primary key value</li>
 * </ul>
 *
 * <p>Example configuration:
 * <pre>
 * "transforms": "addPartitionKey",
 * "transforms.addPartitionKey.type": "com.rsync.kafka.smt.PartitionKeyHeader",
 * "transforms.addPartitionKey.pk.fields": "id",
 * "transforms.addPartitionKey.pk.separator": "_"
 * </pre>
 */
public class PartitionKeyHeader<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(PartitionKeyHeader.class);

    // Configuration keys
    public static final String PK_FIELDS_CONFIG = "pk.fields";
    public static final String PK_FIELDS_DEFAULT = "id";
    public static final String PK_FIELDS_DOC = "Comma-separated list of primary key field names.";

    public static final String PK_SEPARATOR_CONFIG = "pk.separator";
    public static final String PK_SEPARATOR_DEFAULT = "_";
    public static final String PK_SEPARATOR_DOC = "Separator for composite primary keys.";

    public static final String KEY_FORMAT_CONFIG = "key.format";
    public static final String KEY_FORMAT_DEFAULT = "${connection}.${schema}.${table}";
    public static final String KEY_FORMAT_DOC = "Format for base partition key.";

    // Header names
    public static final String HEADER_PARTITION_KEY_BASE = "rsync.partition.key.base";
    public static final String HEADER_PARTITION_KEY_FULL = "rsync.partition.key.full";
    public static final String HEADER_RECORD_ID = "rsync.record.id";
    public static final String HEADER_IS_HOT_ENTITY = "rsync.is.hot.entity";
    public static final String HEADER_OPERATION = "rsync.operation";

    // Headers set by TopicRouter (we read from these)
    private static final String HEADER_CONNECTION = "rsync.connection";
    private static final String HEADER_SCHEMA = "rsync.schema";
    private static final String HEADER_TABLE = "rsync.table";

    private String[] pkFields;
    private String pkSeparator;
    private String keyFormat;

    @Override
    public void configure(Map<String, ?> configs) {
        // configs is Map<String, ?>, so avoid getOrDefault with String defaults (generic capture issues).
        final Object pkFieldsObj = configs.get(PK_FIELDS_CONFIG);
        final String pkFieldsStr = (pkFieldsObj == null ? PK_FIELDS_DEFAULT : pkFieldsObj.toString());
        pkFields = pkFieldsStr.split("\\s*,\\s*");

        final Object pkSepObj = configs.get(PK_SEPARATOR_CONFIG);
        pkSeparator = (pkSepObj == null ? PK_SEPARATOR_DEFAULT : pkSepObj.toString());

        final Object keyFmtObj = configs.get(KEY_FORMAT_CONFIG);
        keyFormat = (keyFmtObj == null ? KEY_FORMAT_DEFAULT : keyFmtObj.toString());

        log.info("PartitionKeyHeader configured: pkFields={}, separator={}, format={}",
                pkFieldsStr, pkSeparator, keyFormat);
    }

    @Override
    public R apply(R record) {
        if (record == null || record.value() == null) {
            return record;
        }

        Headers headers = record.headers();

        // Get connection, schema, table from headers (set by TopicRouter)
        String connection = getHeaderValue(headers, HEADER_CONNECTION, "unknown");
        String schema = getHeaderValue(headers, HEADER_SCHEMA, "default");
        String table = getHeaderValue(headers, HEADER_TABLE, "unknown");

        // Extract record ID from the message
        String recordId = extractRecordId(record);

        // Build partition keys
        String baseKey = keyFormat
                .replace("${connection}", connection)
                .replace("${schema}", schema)
                .replace("${table}", table);

        String fullKey = baseKey + "." + recordId;

        // Extract operation type
        String operation = extractOperation(record);

        // Add headers
        headers.addString(HEADER_PARTITION_KEY_BASE, baseKey);
        headers.addString(HEADER_PARTITION_KEY_FULL, fullKey);
        headers.addString(HEADER_RECORD_ID, recordId);
        headers.addString(HEADER_OPERATION, operation);
        // Note: IS_HOT_ENTITY will be determined by the consumer based on EntityStats

        log.debug("Added partition key headers: base={}, full={}, op={}",
                baseKey, fullKey, operation);

        return record.newRecord(
                record.topic(),
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
     * Extract the record ID from the message key or value.
     *
     * For Debezium, the key contains the primary key value(s).
     * We also check the 'after' field in the value for inserts/updates.
     */
    private String extractRecordId(R record) {
        List<String> keyParts = new ArrayList<>();

        // Try to extract from key first (Debezium keys contain PK)
        if (record.key() != null && record.key() instanceof Struct) {
            Struct keyStruct = (Struct) record.key();
            for (String pkField : pkFields) {
                Object value = getFieldValue(keyStruct, pkField.trim());
                if (value != null) {
                    keyParts.add(String.valueOf(value));
                }
            }
        }

        // If no key found, try value's 'after' field
        if (keyParts.isEmpty() && record.value() instanceof Struct) {
            Struct valueStruct = (Struct) record.value();

            // Check 'after' field (Debezium envelope)
            Object afterObj = getFieldValue(valueStruct, "after");
            if (afterObj instanceof Struct) {
                Struct afterStruct = (Struct) afterObj;
                for (String pkField : pkFields) {
                    Object value = getFieldValue(afterStruct, pkField.trim());
                    if (value != null) {
                        keyParts.add(String.valueOf(value));
                    }
                }
            }

            // Fallback: try 'before' for deletes
            if (keyParts.isEmpty()) {
                Object beforeObj = getFieldValue(valueStruct, "before");
                if (beforeObj instanceof Struct) {
                    Struct beforeStruct = (Struct) beforeObj;
                    for (String pkField : pkFields) {
                        Object value = getFieldValue(beforeStruct, pkField.trim());
                        if (value != null) {
                            keyParts.add(String.valueOf(value));
                        }
                    }
                }
            }
        }

        if (keyParts.isEmpty()) {
            // Generate a fallback ID
            return "unknown_" + System.currentTimeMillis();
        }

        return String.join(pkSeparator, keyParts);
    }

    /**
     * Extract the operation type from a Debezium message.
     */
    private String extractOperation(R record) {
        if (record.value() instanceof Struct) {
            Struct valueStruct = (Struct) record.value();
            Object opValue = getFieldValue(valueStruct, "op");
            if (opValue != null) {
                String op = opValue.toString();
                switch (op) {
                    case "c": return "INSERT";
                    case "u": return "UPDATE";
                    case "d": return "DELETE";
                    case "r": return "READ";   // Snapshot read
                    case "t": return "TRUNCATE";
                    default: return op.toUpperCase();
                }
            }
        }
        return "UNKNOWN";
    }

    /**
     * Safely get a field value from a Struct.
     */
    private Object getFieldValue(Struct struct, String fieldName) {
        try {
            Schema schema = struct.schema();
            Field field = schema.field(fieldName);
            if (field != null) {
                return struct.get(field);
            }
        } catch (Exception e) {
            log.trace("Could not get field {}: {}", fieldName, e.getMessage());
        }
        return null;
    }

    /**
     * Get a string value from headers.
     */
    private String getHeaderValue(Headers headers, String key, String defaultValue) {
        Header header = headers.lastWithName(key);
        if (header != null && header.value() != null) {
            return header.value().toString();
        }
        return defaultValue;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(PK_FIELDS_CONFIG,
                        ConfigDef.Type.STRING,
                        PK_FIELDS_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        PK_FIELDS_DOC)
                .define(PK_SEPARATOR_CONFIG,
                        ConfigDef.Type.STRING,
                        PK_SEPARATOR_DEFAULT,
                        ConfigDef.Importance.LOW,
                        PK_SEPARATOR_DOC)
                .define(KEY_FORMAT_CONFIG,
                        ConfigDef.Type.STRING,
                        KEY_FORMAT_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        KEY_FORMAT_DOC);
    }

    @Override
    public void close() {
        // No resources to close
    }
}
