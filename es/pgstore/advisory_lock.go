package pgstore

import (
	"context"
	"database/sql"
	"fmt"
)

// AdvisoryLock elects a single leader among several replicas of the same
// worker (e.g. one projector instance per read-model) using a Postgres
// session-scoped advisory lock keyed by name. See TASK.md: projections need
// strict ordering, so exactly one replica may run at a time — if the leader
// dies, the lock is released automatically when its connection closes and
// another replica can take over.
type AdvisoryLock struct {
	db   DB
	name string
	conn *sql.Conn
}

// NewAdvisoryLock creates a lock keyed by name. Two AdvisoryLocks with the
// same name (even across processes) never both succeed in TryAcquire.
func NewAdvisoryLock(db DB, name string) *AdvisoryLock {
	return &AdvisoryLock{db: db, name: name}
}

// TryAcquire attempts to become leader without blocking. false, nil means
// another replica already holds the lock — the caller should retry later.
// On success the lock pins a single connection (via database/sql.Conn)
// until Release is called; do not use a single AdvisoryLock from multiple
// goroutines concurrently.
func (l *AdvisoryLock) TryAcquire(ctx context.Context) (bool, error) {
	conn, err := l.db.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("pgstore: acquire conn: %w", err)
	}

	var acquired bool
	err = conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, l.name).Scan(&acquired)
	if err != nil {
		conn.Close()
		return false, fmt.Errorf("pgstore: try advisory lock: %w", err)
	}
	if !acquired {
		conn.Close()
		return false, nil
	}

	l.conn = conn
	return true, nil
}

// Release gives up leadership and returns the connection to the pool. Safe
// to call even if TryAcquire was never called or already failed.
func (l *AdvisoryLock) Release(ctx context.Context) error {
	if l.conn == nil {
		return nil
	}
	conn := l.conn
	l.conn = nil
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, l.name); err != nil {
		return fmt.Errorf("pgstore: advisory unlock: %w", err)
	}
	return nil
}
