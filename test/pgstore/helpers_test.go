//go:build integration

// Package pgstore_test contains integration tests for es/pgstore against a
// real Postgres instance. They are excluded from normal `go test ./...` by
// the `integration` build tag, since they need a live database.
//
// Run with:
//
//	docker run --rm -d --name go-es-light-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16
//	psql "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" -f ../../es/pgstore/schema.sql
//	PGSTORE_TEST_DSN="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" \
//		go test -tags=integration ./test/pgstore/... -race -v
package pgstore_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/lib/pq"

	"go-es-light/es/pgstore"
)

func newStore(t *testing.T) (*pgstore.Store, *sql.DB) {
	t.Helper()

	dsn := os.Getenv("PGSTORE_TEST_DSN")
	if dsn == "" {
		t.Skip("PGSTORE_TEST_DSN not set; skipping pgstore integration test (see file header for how to run)")
	}

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

	return pgstore.New(db, dsn), db
}
