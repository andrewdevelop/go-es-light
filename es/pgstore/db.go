package pgstore

import (
	"context"
	"database/sql"
)

// DB is the subset of *sql.DB that Store and AdvisoryLock need. *sql.DB
// satisfies it directly; so does *sqlx.DB from github.com/jmoiron/sqlx,
// structurally — sqlx.DB embeds *sql.DB without overriding any of these
// signatures. A project that already has a *sqlx.DB can hand it to
// pgstore.New / pgstore.NewAdvisoryLock as-is, no adapter required.
type DB interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Conn(ctx context.Context) (*sql.Conn, error)
}
