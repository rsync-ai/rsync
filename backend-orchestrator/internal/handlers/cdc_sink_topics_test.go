package handlers

import (
	"reflect"
	"testing"
)

func TestDeriveCDCSinkTopics_MongoCollectionIncludeList(t *testing.T) {
	cfg := map[string]interface{}{
		"topic.prefix":            "rsync.cdc-aaa0ded3",
		"database.include.list":   "datingapp",
		"collection.include.list": "datingapp.matches, datingapp.users,datingapp.matches",
	}
	prefix := connectorConfigString(cfg, "topic.prefix")
	got := deriveCDCSinkTopics(prefix, connectorIncludeList(cfg))
	want := []string{"rsync.cdc-aaa0ded3.datingapp.matches", "rsync.cdc-aaa0ded3.datingapp.users"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("topics = %v, want %v", got, want)
	}
}

func TestDeriveCDCSinkTopics_TableIncludeList(t *testing.T) {
	cfg := map[string]interface{}{
		"topic.prefix":       "rsync.cdc-12345678",
		"table.include.list": "public.orders,rsync.cdc-12345678.public.users",
	}
	got := deriveCDCSinkTopics(connectorConfigString(cfg, "topic.prefix"), connectorIncludeList(cfg))
	want := []string{"rsync.cdc-12345678.public.orders", "rsync.cdc-12345678.public.users"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("topics = %v, want %v", got, want)
	}
}

func TestConnectorConfig_MissingKeysAreEmptyNotNilString(t *testing.T) {
	cfg := map[string]interface{}{"table.include.list": nil}
	if v := connectorConfigString(cfg, "topic.prefix"); v != "" {
		t.Fatalf("missing topic.prefix = %q, want empty", v)
	}
	if got := connectorIncludeList(cfg); len(got) != 0 {
		t.Fatalf("include list = %v, want empty", got)
	}
	if got := parseTableIncludeList(cfg); len(got) != 0 {
		t.Fatalf("parseTableIncludeList = %v, want empty", got)
	}
}
