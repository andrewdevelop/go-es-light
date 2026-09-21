//go:build integration

// Package pgstore_test contains integration tests for es/pgstore against a
// real Postgres instance. They are excluded from normal `go test ./...` by
// the `integration` build tag, since they need a live database.
//
// Run with:
//
//	docker run --rm -d --name go-es-light-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16
//	PGSTORE_TEST_DSN="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" \
//		go test -tags=integration ./test/pgstore/... -race -v
//
// No manual schema setup needed — newStore calls Store.Migrate itself
// against the brand-new database, which creates everything from scratch.
//
// If running alongside test/user (which points at the same
// PGSTORE_TEST_DSN and also runs Migrate against the shared events table),
// pass go test -p 1 — see README.md's integration test section for why.
package pgstore_test

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"testing"

	_ "github.com/lib/pq"

	"go-es-light/es/pgstore"
)

// testDSN returns PGSTORE_TEST_DSN, skipping the test if it isn't set.
func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("PGSTORE_TEST_DSN")
	if dsn == "" {
		t.Skip("PGSTORE_TEST_DSN not set; skipping pgstore integration test (see file header for how to run)")
	}
	return dsn
}

// roleDSN returns dsn with its user/password replaced, so a test can open a
// connection authenticated as a specific role (e.g. one it just created)
// against the same host/database.
func roleDSN(t *testing.T, dsn, user, password string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	u.User = url.UserPassword(user, password)
	return u.String()
}

func newStore(t *testing.T, opts ...pgstore.Option) (*pgstore.Store, *sql.DB) {
	t.Helper()

	dsn := testDSN(t)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Bounded so this package's pools plus test/user's (run concurrently by
	// `go test` when both packages are given on one command line) don't
	// blow past Postgres's default max_connections=100 between them.
	db.SetMaxOpenConns(10)

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping database at PGSTORE_TEST_DSN: %v", err)
	}

	store := pgstore.New(db, dsn, opts...)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	return store, db
}
