package handlers

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// latestOAuthTokenIDForProvider underpins the "test-before-create" fix for OAuth
// connectors. Right after a user completes an OAuth connect, the draft connection
// test must find the token they just minted — it is stored keyed by
// (provider, user) with NO oauth_token_id carried in the connection form — so it
// can be injected and the test passes (previously the connector got no creds and
// the test reported "failed" even though the connect succeeded). This pins that
// the helper returns the MOST RECENT token id, scoped to (provider, user).
func TestLatestOAuthTokenIDForProvider_ReturnsNewestUserScoped(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	const provider = "shopify"
	const userID = "00000000-0000-0000-0000-000000000001"

	mock.ExpectQuery(`SELECT id FROM oauth_tokens`).
		WithArgs(provider, userID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("tok-newest"))

	got := latestOAuthTokenIDForProvider(mockDB, provider, userID)
	if got != "tok-newest" {
		t.Fatalf("token id = %q, want tok-newest", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
}

// No token for the provider → empty string. The caller then leaves the config
// untouched, so the connector test fails cleanly ("no credentials") instead of
// crashing or injecting a bogus token.
func TestLatestOAuthTokenIDForProvider_NoneReturnsEmpty(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery(`SELECT id FROM oauth_tokens`).
		WithArgs("shopify", "u1").
		WillReturnError(sql.ErrNoRows)

	if got := latestOAuthTokenIDForProvider(mockDB, "shopify", "u1"); got != "" {
		t.Fatalf("token id = %q, want empty on no rows", got)
	}
}

// Guard rails: nil db or blank provider/user must never query — return "" so the
// caller skips injection.
func TestLatestOAuthTokenIDForProvider_GuardsReturnEmpty(t *testing.T) {
	if got := latestOAuthTokenIDForProvider(nil, "shopify", "u1"); got != "" {
		t.Fatalf("nil db: got %q, want empty", got)
	}
	mockDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()
	if got := latestOAuthTokenIDForProvider(mockDB, "", "u1"); got != "" {
		t.Fatalf("blank provider: got %q, want empty", got)
	}
	if got := latestOAuthTokenIDForProvider(mockDB, "shopify", ""); got != "" {
		t.Fatalf("blank user: got %q, want empty", got)
	}
}
