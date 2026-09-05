package cdc

import (
	"context"
	"testing"
)

// MongoDB must self-register via init() and dispatch like the relational
// families, even though it provisions nothing.
func TestMongoDBProviderRegistered(t *testing.T) {
	if !IsSupportedDBType("mongodb") {
		t.Fatalf("expected mongodb to have a registered CDC provider")
	}
	mgr, ok := NewProvider("mongodb", nil)
	if !ok {
		t.Fatalf("NewProvider(mongodb) not ok")
	}
	if mgr.Family() != "mongodb" {
		t.Errorf("mongodb Family()=%q, want mongodb", mgr.Family())
	}
}

// Collections are qualified by the MongoDB database name, never a schema.
func TestMongoDBPrimaryKeyNamespace(t *testing.T) {
	mgr, _ := NewProvider("mongodb", nil)
	if got := mgr.PrimaryKeyNamespace("mydb", "ignored"); got != "mydb" {
		t.Errorf("mongodb PrimaryKeyNamespace=%q, want mydb", got)
	}
}

// Provision + cleanup are no-ops (change streams create no server-side objects)
// and must never error just because there is nothing to do. Prereq + PK gates
// must not block: _id is always the packed table's primary key.
func TestMongoDBNoOpProvisionAndGates(t *testing.T) {
	mgr, _ := NewProvider("mongodb", nil)
	ctx := context.Background()

	res, err := mgr.ProvisionResources(ctx, CDCResourceConfig{
		PipelineID:   "p1",
		ConnectionID: "c1",
		DatabaseType: "mongodb",
		Database:     "appdb",
	}, []string{"appdb.orders", "appdb.customers"})
	if err != nil {
		t.Fatalf("ProvisionResources errored: %v", err)
	}
	if len(res) != 0 {
		t.Errorf("ProvisionResources returned %d resources, want 0 (no server-side objects)", len(res))
	}

	if err := mgr.CleanupResources(ctx, "p1"); err != nil {
		t.Errorf("CleanupResources errored: %v", err)
	}

	prereq, err := mgr.ValidatePrerequisites(ctx, "c1")
	if err != nil {
		t.Errorf("ValidatePrerequisites errored: %v", err)
	}
	if len(prereq) != 0 {
		t.Errorf("ValidatePrerequisites returned %d errors, want 0 (deferred to connector start)", len(prereq))
	}

	// Even a collection with no relational PK must not be reported missing: the
	// packed destination table keys on _id, which every document carries.
	missing, err := mgr.ValidateTablesHavePrimaryKeys(ctx, "c1", "appdb", []string{"appdb.orders"})
	if err != nil {
		t.Errorf("ValidateTablesHavePrimaryKeys errored: %v", err)
	}
	if len(missing) != 0 {
		t.Errorf("ValidateTablesHavePrimaryKeys reported %v missing, want none (_id is the PK)", missing)
	}
}
