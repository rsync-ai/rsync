package handlers

import "strings"

// cdcDataTopic is the Kafka topic Debezium writes one include-list entry to, read
// from the live connector config: "<topic.prefix>.<entry>" for PostgreSQL, MySQL,
// Oracle and MongoDB, and "<topic.prefix>.<database>.<schema>.<table>" for SQL
// Server, whose include-list entries are "<schema>.<table>" but whose topics carry
// the database named in database.names as an extra segment (connector.py builds
// the same name; the executor's buildCDCSinkTopics gates the same prepend on the
// source type).
//
// Deriving "<prefix>.<entry>" for a SQL Server connector named a topic that never
// exists: the removed-table reaper then kept the real topic for good ("the topic
// does not exist"), and the sink respawn after a table edit subscribed to the
// phantom name, which a broker with auto.create.topics.enable creates empty.
//
// ok is false when the name cannot be known for certain — no topic.prefix, or a
// SQL Server connector with other than exactly one database — and a caller that
// deletes must then skip the entry rather than guess.
func cdcDataTopic(connCfg map[string]interface{}, prefix, entry string) (string, bool) {
	prefix = strings.TrimSpace(prefix)
	entry = strings.TrimSpace(strings.ReplaceAll(entry, `\`, ""))
	if prefix == "" || entry == "" {
		return "", false
	}
	if strings.HasPrefix(entry, prefix+".") {
		return entry, true
	}
	if inferDebeziumDatabaseType(connCfg) != "sqlserver" {
		return prefix + "." + entry, true
	}
	dbs := splitCommaList(connCfgString(connCfg, "database.names"))
	if len(dbs) != 1 {
		return "", false
	}
	if strings.Count(entry, ".") >= 2 && strings.HasPrefix(entry, dbs[0]+".") {
		// Already "<database>.<schema>.<table>".
		return prefix + "." + entry, true
	}
	return prefix + "." + dbs[0] + "." + entry, true
}

// cdcDataTopics maps include-list entries to their data topics, de-duplicated in
// order, dropping any entry whose topic cannot be named for certain.
func cdcDataTopics(connCfg map[string]interface{}, prefix string, entries []string) []string {
	out := make([]string, 0, len(entries))
	seen := map[string]struct{}{}
	for _, e := range entries {
		topic, ok := cdcDataTopic(connCfg, prefix, e)
		if !ok {
			continue
		}
		if _, dup := seen[topic]; dup {
			continue
		}
		seen[topic] = struct{}{}
		out = append(out, topic)
	}
	return out
}
