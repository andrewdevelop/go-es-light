// Package pgstore is a Postgres-backed es.EventStore + es.Checkpointer,
// built directly on database/sql and lib/pq — no extra SQL helper library:
// row scanning is a handful of fixed-order positional Scan calls, so
// reflection-based tools like sqlx buy nothing here. Store and
// AdvisoryLock accept anything satisfying the small DB interface, so a
// *sqlx.DB works too if that's what your project already has.
//
// It follows the design in TASK.md: events are the single source of truth
// (no outbox/dual-write problem, since there is no separate broker to keep
// in sync), per-aggregate optimistic concurrency is enforced with a
// transaction-scoped advisory lock plus a MAX(aggregate_version) check, and
// StreamAll wakes up promptly via LISTEN/NOTIFY (through a dedicated
// pq.Listener) while still falling back to polling (NOTIFY delivery is
// best-effort, not persistent). Run schema.sql against your database before
// using this package.
package pgstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"

	"go-es-light/es"
)

// PollInterval is the fallback polling cadence used by StreamAll when no
// LISTEN/NOTIFY wakeup arrives in that window.
var PollInterval = 2 * time.Second

// Store implements es.EventStore and es.Checkpointer on top of a DB (a
// *sql.DB, or a *sqlx.DB if your project already uses one — see DB).
type Store struct {
	db  DB
	dsn string
}

// New wraps an existing DB (e.g. *sql.DB opened via
// sql.Open("postgres", dsn) after importing github.com/lib/pq). dsn is
// required in addition to db because StreamAll opens its own dedicated
// pq.Listener connection for LISTEN/NOTIFY, independent of db's connection
// pool. The db's lifecycle (Close) remains the caller's responsibility.
func New(db DB, dsn string) *Store {
	return &Store{db: db, dsn: dsn}
}

// Commit implements es.EventStore. All events must belong to the same
// aggregate. A transaction-scoped advisory lock keyed by aggregate id
// serializes concurrent commits for that aggregate so the version check
// below can't race.
func (s *Store) Commit(ctx context.Context, events []*es.DomainEvent, expectedVersion uint64) error {
	if len(events) == 0 {
		return nil
	}
	aggregateID := events[0].AggregateID

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("pgstore: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op once Commit succeeds

	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, aggregateID.String()); err != nil {
		return fmt.Errorf("pgstore: advisory lock: %w", err)
	}

	var currentVersion uint64
	err = tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(aggregate_version), 0) FROM events WHERE aggregate_id = $1`,
		aggregateID,
	).Scan(&currentVersion)
	if err != nil {
		return fmt.Errorf("pgstore: read current version: %w", err)
	}

	if currentVersion != expectedVersion {
		return es.ErrConcurrencyConflict
	}

	for _, e := range events {
		err := tx.QueryRowContext(ctx, `
			INSERT INTO events (id, aggregate_id, aggregate_version, version, name, payload, occurred_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			RETURNING global_id
		`, e.ID, e.AggregateID, e.AggregateVersion, e.Version, e.Name, e.Payload, e.OccurredAt,
		).Scan(&e.GlobalID)
		if err != nil {
			return fmt.Errorf("pgstore: insert event %q: %w", e.Name, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("pgstore: commit tx: %w", err)
	}
	return nil
}

// Load implements es.EventStore.
func (s *Store) Load(ctx context.Context, id uuid.UUID) ([]*es.DomainEvent, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT global_id, id, aggregate_id, aggregate_version, version, name, payload, occurred_at
		FROM events WHERE aggregate_id = $1 ORDER BY aggregate_version ASC
	`, id)
	if err != nil {
		return nil, fmt.Errorf("pgstore: load: %w", err)
	}

	events, err := scanEvents(rows)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, es.ErrAggregateNotFound
	}
	return events, nil
}

// FetchAfter implements es.EventStore.
func (s *Store) FetchAfter(ctx context.Context, lastID uint64, limit int) ([]*es.DomainEvent, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT global_id, id, aggregate_id, aggregate_version, version, name, payload, occurred_at
		FROM events WHERE global_id > $1 ORDER BY global_id ASC LIMIT $2
	`, lastID, limit)
	if err != nil {
		return nil, fmt.Errorf("pgstore: fetch after: %w", err)
	}

	return scanEvents(rows)
}

// StreamAll implements es.EventStore. It backfills every event from
// global_id 0, then LISTENs on events_channel (via a dedicated pq.Listener
// connection) to wake up promptly as new events are committed, re-polling
// on a fixed interval regardless (NOTIFY is best-effort: a listener that
// wasn't connected at the moment of NOTIFY gets nothing, unlike the
// durably-stored events themselves).
func (s *Store) StreamAll(ctx context.Context) (<-chan *es.DomainEvent, <-chan error) {
	out := make(chan *es.DomainEvent)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		if ctx.Err() != nil {
			return
		}

		listener := pq.NewListener(s.dsn, 10*time.Second, time.Minute, nil)
		if err := listener.Listen("events_channel"); err != nil {
			errc <- fmt.Errorf("pgstore: listen: %w", err)
			return
		}
		defer listener.Close() //nolint:errcheck

		var lastID uint64
		drain := func() (bool, error) {
			for {
				events, err := s.FetchAfter(ctx, lastID, 500)
				if err != nil {
					return false, err
				}
				if len(events) == 0 {
					return true, nil
				}
				for _, e := range events {
					select {
					case out <- e:
						lastID = e.GlobalID
					case <-ctx.Done():
						return false, nil
					}
				}
			}
		}

		if ok, err := drain(); err != nil {
			errc <- err
			return
		} else if !ok {
			return
		}

		for {
			select {
			case <-listener.Notify:
			case <-time.After(PollInterval):
			case <-ctx.Done():
				return
			}

			// Whether this woke up from a real notification or the poll
			// timeout, just re-check for new rows.
			if ok, err := drain(); err != nil {
				errc <- err
				return
			} else if !ok {
				return
			}
		}
	}()

	return out, errc
}

// Get implements es.Checkpointer.
func (s *Store) Get(ctx context.Context, name string) (uint64, error) {
	var last uint64
	err := s.db.QueryRowContext(ctx, `SELECT last_global_id FROM checkpoints WHERE name = $1`, name).Scan(&last)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("pgstore: get checkpoint: %w", err)
	}
	return last, nil
}

// Save implements es.Checkpointer.
func (s *Store) Save(ctx context.Context, name string, lastID uint64) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO checkpoints (name, last_global_id) VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET last_global_id = EXCLUDED.last_global_id
	`, name, lastID)
	if err != nil {
		return fmt.Errorf("pgstore: save checkpoint: %w", err)
	}
	return nil
}

// scanEvents reads every row into a DomainEvent by positional Scan, in the
// same fixed column order used by every query above — no reflection.
func scanEvents(rows *sql.Rows) ([]*es.DomainEvent, error) {
	defer rows.Close()

	var out []*es.DomainEvent
	for rows.Next() {
		e := &es.DomainEvent{}
		if err := rows.Scan(
			&e.GlobalID, &e.ID, &e.AggregateID, &e.AggregateVersion,
			&e.Version, &e.Name, &e.Payload, &e.OccurredAt,
		); err != nil {
			return nil, fmt.Errorf("pgstore: scan event: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
