package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rsync-ai/backend-orchestrator/pkg/namespacemodel"
)

// GetConnectorNamespaceModels returns the namespace_model of every connector
// that declares one, keyed by namespacemodel.Key of its id and of each alias,
// plus the defaults for any other name, and the keys of the connectors that
// list their namespaces (lists_namespaces). The frontend labels the table
// picker, seeds the destination field and decides whether a connection gets a
// Scope step from it (frontend/src/lib/pipeline/namespaceModel.ts) instead of
// carrying its own per-type switch.
func GetConnectorNamespaceModels(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"models":           namespacemodel.All(),
		"defaults":         namespacemodel.Defaults(),
		"lists_namespaces": namespacemodel.Listing(),
	})
}
