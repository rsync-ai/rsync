package handlers

import (
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestTopologyRoutesDoNotMintPerPipelineCDCTopics pins the removal of
// POST /topics/pipeline. That route built "rsync.cdc.<id8>" — a topic nothing
// produced to (Debezium writes rsync.cdc-<id8>.<db>.<table>) and that the CDC
// teardown's name matcher does not recognise, so every call leaked a topic. Its
// only would-be caller, the planner's TopicProvisioner, posts to POST /topics.
func TestTopologyRoutesDoNotMintPerPipelineCDCTopics(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	NewTopologyHandler(nil, nil).RegisterRoutes(r.Group("/api/v1/topology"))

	var sawCreate bool
	for _, ri := range r.Routes() {
		if ri.Method == http.MethodPost && ri.Path == "/api/v1/topology/topics/pipeline" {
			t.Fatalf("POST %s is registered again; it mints rsync.cdc.<id8>, which nothing produces to", ri.Path)
		}
		if ri.Method == http.MethodPost && ri.Path == "/api/v1/topology/topics" {
			sawCreate = true
		}
	}
	// Control: the route table was actually populated, so the absence above
	// is not an empty-router artefact.
	if !sawCreate {
		t.Fatal("test is vacuous: POST /api/v1/topology/topics is not registered either")
	}
}
