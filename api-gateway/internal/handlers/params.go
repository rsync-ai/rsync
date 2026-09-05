package handlers

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func requireUUIDParam(c *gin.Context, paramName string, errCode string, errMessage string) (string, bool) {
	raw := strings.TrimSpace(c.Param(paramName))
	if raw == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   errCode,
			"message": errMessage,
		})
		return "", false
	}
	// Return the CANONICAL form, never raw. uuid.Parse is deliberately lenient:
	// besides the hyphenated form it also accepts "urn:uuid:<uuid>", "{<uuid>}"
	// and the 32-char undashed form. Postgres's uuid input accepts the braces
	// and undashed forms but NOT the urn: prefix, so returning raw let
	// "urn:uuid:<valid-uuid>" past this gate and into `WHERE id = $1`, where it
	// raised
	//
	//	invalid input syntax for type uuid: "urn:uuid:..."
	//
	// — the exact 500 this helper exists to prevent, in the one input shape the
	// /workspaces/current fix missed. u.String() is always hyphenated lowercase,
	// which Postgres accepts for every input uuid.Parse accepts, so this closes
	// the gap at all nine call sites at once rather than one handler at a time.
	u, err := uuid.Parse(raw)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   errCode,
			"message": errMessage,
		})
		return "", false
	}
	return u.String(), true
}
