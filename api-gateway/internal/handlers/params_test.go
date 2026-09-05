package handlers

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

func TestRequireUUIDParam_InvalidUUID_Returns400(t *testing.T) {
	mockDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	prev := db.DB
	db.DB = mockDB
	defer func() {
		db.DB = prev
		_ = mockDB.Close()
	}()

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/pipelines/:id", GetPipeline)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/pipelines/not-a-uuid", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, w.Code)
	}
}

// The one spelling Postgres accepts unconditionally. It is tolerant of some
// others — verified against prod's own database, it takes "{<uuid>}", the
// undashed 32-char form and uppercase hex — but it rejects "urn:uuid:<uuid>",
// which uuid.Parse accepts. Rather than encode which spellings happen to
// overlap, require the canonical one: it is the only shape guaranteed to reach
// `WHERE id = $1` without raising "invalid input syntax for type uuid".
var pgCanonicalUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func requireUUIDParamDirect(t *testing.T, raw string) (string, bool, int) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Params = gin.Params{{Key: "id", Value: raw}}
	got, ok := requireUUIDParam(c, "id", "invalid_id", "Invalid ID format")
	return got, ok, w.Code
}

// THE INVARIANT: whatever this helper accepts, it returns in a form Postgres
// parses. Enumerating uuid.Parse's accepted spellings would rot the moment the
// library adds one; asserting the shape of the OUTPUT does not.
//
// The urn: case is the regression under test. uuid.Parse accepts
// "urn:uuid:<uuid>" and Postgres rejects it, so while this helper returned the
// raw string that spelling produced the same 500 as /workspaces/current did —
// at every call site, not just workspaces.
func TestRequireUUIDParam_AcceptedInputIsAlwaysPostgresParseable(t *testing.T) {
	const canonical = "11111111-2222-3333-4444-555555555555"

	cases := []struct {
		name string
		in   string
	}{
		{"canonical", canonical},
		{"uppercase hex", "11111111-2222-3333-4444-AAAAAAAAAAAA"},
		{"braces", "{" + canonical + "}"},
		{"undashed", "11111111222233334444555555555555"},
		{"urn prefix", "urn:uuid:" + canonical},
		{"urn prefix uppercase", "URN:UUID:" + canonical},
		{"surrounding whitespace", "  " + canonical + "  "},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok, _ := requireUUIDParamDirect(t, tc.in)
			if !ok {
				t.Fatalf("uuid.Parse accepts %q, so the helper must too", tc.in)
			}
			if !pgCanonicalUUID.MatchString(got) {
				t.Fatalf("%q returned %q, which is not the canonical form. Postgres "+
					"tolerates some non-canonical spellings but rejects urn:uuid:, so a "+
					"non-canonical return can still reach SQL and 500", tc.in, got)
			}
		})
	}
}

// The other half of the boundary: malformed ids are still refused before SQL,
// and still with the caller-supplied error code.
func TestRequireUUIDParam_MalformedStillRejected(t *testing.T) {
	for _, raw := range []string{
		"current", "me", "null", "undefined", "not-a-uuid", "12345", "",
		"   ",
		"11111111-2222-3333-4444-55555555555",   // one short
		"11111111-2222-3333-4444-5555555555555", // one long
		"1111111g-2222-3333-4444-555555555555",  // non-hex
		"urn:uuid:not-a-uuid",
	} {
		t.Run(raw, func(t *testing.T) {
			got, ok, code := requireUUIDParamDirect(t, raw)
			if ok {
				t.Fatalf("%q must be refused, got %q", raw, got)
			}
			if code != http.StatusBadRequest {
				t.Fatalf("%q: want 400, got %d", raw, code)
			}
		})
	}
}
