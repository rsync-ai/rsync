package cdc

import "testing"

// A customer with many databases on one server installs one connection (and
// pipeline) per database, often several pipelines on the same database too.
// Every source-side CDC resource must then be distinct per pipeline: a shared
// replication slot is consumed by two readers, and a shared MySQL server id
// makes the server drop one binlog stream while the connector shows RUNNING.
func TestResourceNames_PipelinesOnOneServerDoNotCollide(t *testing.T) {
	sameDB := []CDCResourceConfig{
		{PipelineID: "0a1b2c3d-0000-4000-8000-000000000001", ConnectionID: "conn-a", Database: "appdb"},
		{PipelineID: "9f8e7d6c-0000-4000-8000-000000000002", ConnectionID: "conn-a", Database: "appdb"},
	}
	// Two connections to the same server, one per database.
	perDB := []CDCResourceConfig{
		{PipelineID: "11111111-0000-4000-8000-000000000003", ConnectionID: "conn-sales", Database: "sales"},
		{PipelineID: "22222222-0000-4000-8000-000000000004", ConnectionID: "conn-hr", Database: "hr"},
	}
	for _, kind := range []string{"replication_slot", "publication", "server_id"} {
		for _, pair := range [][]CDCResourceConfig{sameDB, perDB} {
			a, b := GenerateResourceName(pair[0], kind), GenerateResourceName(pair[1], kind)
			if a == "" || a == b {
				t.Errorf("%s: pipelines %s and %s on %s/%s both get %q", kind,
					pair[0].PipelineID, pair[1].PipelineID, pair[0].Database, pair[1].Database, a)
			}
		}
		// Control: the same pipeline asks again and gets the same name, so a
		// redeploy reuses its slot instead of leaking a second one.
		if a, again := GenerateResourceName(sameDB[0], kind), GenerateResourceName(sameDB[0], kind); a != again {
			t.Errorf("%s not deterministic: %q vs %q", kind, a, again)
		}
	}
}
