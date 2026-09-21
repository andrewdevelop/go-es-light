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
// best-effort, not persistent). There is no separate schema.sql to apply by
// hand: call Store.Migrate once (idempotent, safe to call on every startup,
// including against a brand-new empty database) and it creates the base
// tables/trigger plus the tenant_id/metadata columns Commit/Load/FetchAfter
// always reference. actor_id/actor_type and pii_id are further optional —
// see WithActorTracking and WithPiiTracking — since not every deployment
// needs actor attribution or PII/erasure support at all; a Store built
// without them never creates, queries, or writes those columns.
package pgstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
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

	actorTracking bool
	piiTracking   bool
}

// Option customizes a Store at construction time. See WithActorTracking,
// WithPiiTracking.
type Option func(*Store)

// WithActorTracking opts the Store into persisting actor_id/actor_type
// (see es.WithActor): Migrate creates the columns, and Commit/Load/
// FetchAfter include them in every query. Without it, the Store never
// creates or references those columns at all — Commit returns an error if
// an event carries an actor but the Store wasn't built with this option,
// rather than silently dropping it.
func WithActorTracking() Option {
	return func(s *Store) { s.actorTracking = true }
}

// WithPiiTracking opts the Store into persisting pii_id and pii_fields (see
// es.WithPiiID, es.PiiAnonymizer): Migrate creates the columns, and
// Commit/Load/FetchAfter include them in every query. Without it, the Store
// never creates or references those columns at all — Commit returns an
// error if an event carries a PiiID but the Store wasn't built with this
// option, rather than silently dropping it.
func WithPiiTracking() Option {
	return func(s *Store) { s.piiTracking = true }
}

// New wraps an existing DB (e.g. *sql.DB opened via
// sql.Open("postgres", dsn) after importing github.com/lib/pq). dsn is
// required in addition to db because StreamAll opens its own dedicated
// pq.Listener connection for LISTEN/NOTIFY, independent of db's connection
// pool. The db's lifecycle (Close) remains the caller's responsibility.
// opts is optional — see WithActorTracking, WithPiiTracking.
func New(db DB, dsn string, opts ...Option) *Store {
	s := &Store{db: db, dsn: dsn}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// migrateConfig holds MigrateOption settings. Zero value is the default: no
// row-level security.
type migrateConfig struct {
	tenantIsolation bool
}

// MigrateOption customizes a single Migrate call. See WithTenantIsolation.
type MigrateOption func(*migrateConfig)

// WithTenantIsolation additionally enables Postgres row-level security on
// events as defense-in-depth for multitenancy, on top of the WHERE
// tenant_id filtering Commit/Load/FetchAfter already do in Go. A stronger
// commitment than the rest of Migrate: the policy itself is permissive by
// default (a session that never calls Commit/Load with es.WithTenant sees
// every row, unaffected), but FORCE ROW LEVEL SECURITY does mean this
// changes behavior for every role including the table owner, which the
// rest of Migrate's plain column/table adds don't. That's why it's an
// explicit opt-in option rather than something Migrate always does.
//
// Verified against a real Postgres 16 instance: a non-superuser role
// without BYPASSRLS is correctly blocked from another tenant's rows (and
// still sees its own, and still sees everything when no app.tenant_id is
// set). One important caveat, also Postgres behavior rather than anything
// this policy controls: superuser roles — the default "postgres" role most
// local/Docker setups connect as — always bypass row security entirely,
// regardless of FORCE ROW LEVEL SECURITY. To actually observe this policy
// enforcing anything, connect as (or test with) a non-superuser role
// without the BYPASSRLS attribute.
func WithTenantIsolation() MigrateOption {
	return func(c *migrateConfig) { c.tenantIsolation = true }
}

// Migrate idempotently brings the database up to the shape this Store
// needs — including against a brand-new, empty database, so there's no
// separate schema file to apply by hand first. Safe to call on every
// startup: every statement is additive (CREATE ... IF NOT EXISTS / ADD
// COLUMN IF NOT EXISTS / a guarded constraint add), so it's a no-op once
// already applied, and there's no corresponding "down" migration — these
// are nullable/defaulted columns with no data to roll back if a feature
// you'd started using stops being used. opts is optional — see
// WithTenantIsolation.
//
// Required before using Store at all: Commit/Load/FetchAfter reference the
// base tenant_id/metadata columns unconditionally, and additionally
// actor_id/actor_type or pii_id if this Store was built with
// WithActorTracking/WithPiiTracking — Migrate creates exactly the columns
// this Store's queries will reference, no more, no less.
func (s *Store) Migrate(ctx context.Context, opts ...MigrateOption) error {
	cfg := migrateConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	stmts := []string{
		// Base event log + checkpoints tables, and the LISTEN/NOTIFY trigger
		// StreamAll uses. UNIQUE (aggregate_id, aggregate_version) doubles as
		// the single-tenant optimistic-concurrency guard and gives Load a
		// matching index for free — no separate CREATE INDEX needed.
		`CREATE TABLE IF NOT EXISTS events (
			global_id         BIGSERIAL PRIMARY KEY,
			id                UUID NOT NULL,
			aggregate_id      UUID NOT NULL,
			aggregate_version BIGINT NOT NULL,
			version           INT NOT NULL DEFAULT 1,
			name              TEXT NOT NULL,
			payload           JSONB NOT NULL,
			occurred_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

			UNIQUE (aggregate_id, aggregate_version)
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			name           TEXT PRIMARY KEY,
			last_global_id BIGINT NOT NULL DEFAULT 0
		)`,
		`CREATE OR REPLACE FUNCTION notify_events() RETURNS trigger AS $$
		BEGIN
			PERFORM pg_notify('events_channel', NEW.global_id::text);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`,
		`DROP TRIGGER IF EXISTS events_notify ON events`,
		`CREATE TRIGGER events_notify
			AFTER INSERT ON events
			FOR EACH ROW EXECUTE FUNCTION notify_events()`,

		// Base columns Commit/Load/FetchAfter always reference, added to a
		// table that may already exist from an older version of Migrate (the
		// CREATE TABLE IF NOT EXISTS above is a no-op in that case).
		`ALTER TABLE events ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000'`,
		`ALTER TABLE events ADD COLUMN IF NOT EXISTS metadata JSONB`,
		// A named constraint so this is idempotent — ALTER TABLE ... ADD
		// CONSTRAINT has no IF NOT EXISTS form in Postgres. Its index also
		// covers tenant-scoped aggregate_id/version lookups, so — like the
		// base UNIQUE above — no separate CREATE INDEX is needed for it.
		`DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'events_tenant_version_unique') THEN
				ALTER TABLE events ADD CONSTRAINT events_tenant_version_unique UNIQUE (tenant_id, aggregate_id, aggregate_version);
			END IF;
		END $$`,
		// Speeds up tenant-scoped FetchAfter/StreamAll (es.WithTenant): a
		// projector catching up on one tenant's events needs global_id
		// ordering *within* that tenant, which the UNIQUE constraint's index
		// above (ordered by aggregate_id, not global_id) doesn't serve.
		`CREATE INDEX IF NOT EXISTS events_tenant_global_idx ON events (tenant_id, global_id)`,

		// Cleanup for anyone who ran an earlier version of Migrate: these
		// indexes duplicated the two UNIQUE constraints' own indexes exactly
		// (same columns, same order) — pure write overhead with no read
		// benefit the constraints' indexes didn't already provide.
		`DROP INDEX IF EXISTS events_aggregate_id_idx`,
		`DROP INDEX IF EXISTS events_tenant_aggregate_idx`,
	}

	if s.actorTracking {
		stmts = append(stmts,
			`ALTER TABLE events ADD COLUMN IF NOT EXISTS actor_id UUID`,
			`ALTER TABLE events ADD COLUMN IF NOT EXISTS actor_type TEXT`,
		)
	}
	if s.piiTracking {
		stmts = append(stmts,
			`ALTER TABLE events ADD COLUMN IF NOT EXISTS pii_id UUID`,
			`ALTER TABLE events ADD COLUMN IF NOT EXISTS pii_fields JSONB`,
			// Subject-centric lookups (es.PiiLookup.FetchByPiiID — a data
			// subject's access-request/erasure file): the pii_id column is
			// nullable and mostly sparse, so an index is what keeps a
			// per-subject scan from turning into a full-table scan as the
			// log grows.
			`CREATE INDEX IF NOT EXISTS events_pii_id_idx ON events (pii_id)`,
		)
	}

	if cfg.tenantIsolation {
		stmts = append(stmts,
			`ALTER TABLE events ENABLE ROW LEVEL SECURITY`,
			`ALTER TABLE events FORCE ROW LEVEL SECURITY`,
			`DROP POLICY IF EXISTS events_tenant_isolation ON events`,
			`CREATE POLICY events_tenant_isolation ON events
				USING (
					current_setting('app.tenant_id', true) IS NULL
					OR current_setting('app.tenant_id', true) = ''
					OR tenant_id = current_setting('app.tenant_id', true)::uuid
					OR tenant_id = '00000000-0000-0000-0000-000000000000'
				)`,
		)
	}

	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("pgstore: migrate: %w", err)
		}
	}
	return nil
}

// Commit implements es.EventStore. All events must belong to the same
// aggregate. A transaction-scoped advisory lock keyed by aggregate id
// serializes concurrent commits for that aggregate so the version check
// below can't race.
func (s *Store) Commit(ctx context.Context, events []*es.DomainEvent, expectedVersion uint64, opts ...es.StoreOption) (err error) {
	if len(events) == 0 {
		return nil
	}
	aggregateID := events[0].AggregateID

	cfg := es.ResolveStoreConfig(opts...)
	if cfg.TenantID != nil {
		for _, e := range events {
			if e.TenantID == uuid.Nil {
				e.TenantID = *cfg.TenantID
			}
		}
	}

	if !s.actorTracking {
		for _, e := range events {
			if e.ActorID != nil || e.ActorType != nil {
				return fmt.Errorf("pgstore: event %q carries an actor but this Store wasn't built with WithActorTracking", e.Name)
			}
		}
	}
	if !s.piiTracking {
		for _, e := range events {
			if e.PiiID != nil || len(e.PiiFields) > 0 {
				return fmt.Errorf("pgstore: event %q carries a PiiID but this Store wasn't built with WithPiiTracking", e.Name)
			}
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("pgstore: begin tx: %w", err)
	}
	defer func() {
		// tx.Rollback after a successful tx.Commit always returns
		// sql.ErrTxDone — expected, not swallowed silently, just not an
		// actual problem. Any other Rollback failure is surfaced by folding
		// it into the returned error, since it can mean the transaction was
		// left in an unexpected state.
		rbErr := tx.Rollback()
		if rbErr == nil || errors.Is(rbErr, sql.ErrTxDone) {
			return
		}
		if err != nil {
			err = fmt.Errorf("%w (additionally, rollback failed: %v)", err, rbErr)
		} else {
			err = fmt.Errorf("pgstore: rollback: %w", rbErr)
		}
	}()

	// Only set the RLS session variable when a tenant was actually given —
	// harmless even if WithTenantIsolation was never passed to Migrate (no
	// policy to apply it to), and leaving it unset when tenant isolation
	// *is* enabled keeps this call a no-op against its permissive default,
	// so single-tenant callers who never pass es.WithTenant see zero
	// behavior change either way.
	switch {
	case cfg.TenantID != nil:
		if _, err = tx.ExecContext(ctx, `SELECT set_config('app.tenant_id', $1, true)`, cfg.TenantID.String()); err != nil {
			return fmt.Errorf("pgstore: set tenant context: %w", err)
		}
	case cfg.AllTenants:
		// Explicitly clear it rather than just leaving it untouched: same
		// permissive effect against the RLS policy (empty string is one of
		// its allow-everything branches), but now observable in the DB
		// session/logs as "this write intentionally opted out of tenant
		// scoping," not silence indistinguishable from never having thought
		// about it. See es.WithAllTenants.
		if _, err = tx.ExecContext(ctx, `SELECT set_config('app.tenant_id', '', true)`); err != nil {
			return fmt.Errorf("pgstore: clear tenant context: %w", err)
		}
	}

	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, aggregateID.String()); err != nil {
		return fmt.Errorf("pgstore: advisory lock: %w", err)
	}

	conditions := []string{"aggregate_id = $1"}
	args := []any{aggregateID}
	if cfg.TenantID != nil {
		conditions = append(conditions, "tenant_id = "+placeholder(len(args)+1))
		args = append(args, *cfg.TenantID)
	}
	versionQuery := "SELECT COALESCE(MAX(aggregate_version), 0) FROM events WHERE " + strings.Join(conditions, " AND ")

	var currentVersion uint64
	if err = tx.QueryRowContext(ctx, versionQuery, args...).Scan(&currentVersion); err != nil {
		return fmt.Errorf("pgstore: read current version: %w", err)
	}

	if currentVersion != expectedVersion {
		return es.ErrConcurrencyConflict
	}

	insertColumns := []string{"id", "tenant_id", "aggregate_id", "aggregate_version", "version", "name", "payload", "metadata"}
	if s.actorTracking {
		insertColumns = append(insertColumns, "actor_id", "actor_type")
	}
	if s.piiTracking {
		insertColumns = append(insertColumns, "pii_id", "pii_fields")
	}
	insertColumns = append(insertColumns, "occurred_at")

	placeholders := make([]string, len(insertColumns))
	for i := range placeholders {
		placeholders[i] = placeholder(i + 1)
	}
	insertQuery := "INSERT INTO events (" + strings.Join(insertColumns, ", ") + ") VALUES (" + strings.Join(placeholders, ", ") + ") RETURNING global_id"

	for _, e := range events {
		values := []any{e.ID, e.TenantID, e.AggregateID, e.AggregateVersion, e.Version, e.Name, e.Payload, nullRawMessage(e.Metadata)}
		if s.actorTracking {
			values = append(values, nullUUID(e.ActorID), nullString(e.ActorType))
		}
		if s.piiTracking {
			values = append(values, nullUUID(e.PiiID), nullRawMessage(e.PiiFields))
		}
		values = append(values, e.OccurredAt)

		if err = tx.QueryRowContext(ctx, insertQuery, values...).Scan(&e.GlobalID); err != nil {
			return fmt.Errorf("pgstore: insert event %q: %w", e.Name, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("pgstore: commit tx: %w", err)
	}
	return nil
}

// Load implements es.EventStore. opts is optional — see es.WithTenant. When
// given, an aggregate id that exists only under a different tenant behaves
// as not found (ErrAggregateNotFound), same as if it never existed.
func (s *Store) Load(ctx context.Context, id uuid.UUID, opts ...es.StoreOption) ([]*es.DomainEvent, error) {
	cfg := es.ResolveStoreConfig(opts...)

	conditions := []string{"aggregate_id = $1"}
	args := []any{id}
	if cfg.TenantID != nil {
		conditions = append(conditions, "tenant_id IN ("+placeholder(len(args)+1)+", "+placeholder(len(args)+2)+")")
		args = append(args, *cfg.TenantID, es.GlobalTenantID)
	}
	query := "SELECT " + strings.Join(s.columns(), ", ") + " FROM events WHERE " + strings.Join(conditions, " AND ") + " ORDER BY aggregate_version ASC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("pgstore: load: %w", err)
	}

	events, err := s.scanEvents(rows)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, es.ErrAggregateNotFound
	}
	return events, nil
}

// FetchAfter implements es.EventStore. opts is optional — see es.WithTenant,
// which restricts the returned events to a single tenant (used by
// projectors that must only ever build one tenant's read model).
func (s *Store) FetchAfter(ctx context.Context, lastID uint64, limit int, opts ...es.StoreOption) ([]*es.DomainEvent, error) {
	cfg := es.ResolveStoreConfig(opts...)

	conditions := []string{"global_id > $1"}
	args := []any{lastID}
	if cfg.TenantID != nil {
		conditions = append(conditions, "tenant_id IN ("+placeholder(len(args)+1)+", "+placeholder(len(args)+2)+")")
		args = append(args, *cfg.TenantID, es.GlobalTenantID)
	}
	args = append(args, limit)
	query := "SELECT " + strings.Join(s.columns(), ", ") + " FROM events WHERE " + strings.Join(conditions, " AND ") +
		" ORDER BY global_id ASC LIMIT " + placeholder(len(args))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("pgstore: fetch after: %w", err)
	}

	return s.scanEvents(rows)
}

// StreamAll implements es.EventStore. It backfills every event from
// global_id 0, then LISTENs on events_channel (via a dedicated pq.Listener
// connection) to wake up promptly as new events are committed, re-polling
// on a fixed interval regardless (NOTIFY is best-effort: a listener that
// wasn't connected at the moment of NOTIFY gets nothing, unlike the
// durably-stored events themselves).
func (s *Store) StreamAll(ctx context.Context, opts ...es.StoreOption) (<-chan *es.DomainEvent, <-chan error) {
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
		defer func() {
			if err := listener.Close(); err != nil {
				// errc is buffered (1) and nothing else sends to it after
				// this deferred func runs (it's the last one queued), but
				// guard with a non-blocking send anyway rather than assume
				// that invariant never changes.
				select {
				case errc <- fmt.Errorf("pgstore: close listener: %w", err):
				default:
				}
			}
		}()

		var lastID uint64
		drain := func() (bool, error) {
			for {
				events, err := s.FetchAfter(ctx, lastID, 500, opts...)
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

// FetchByPiiID implements es.PiiLookup: every event carrying piiID, across
// all aggregates and tenants, ordered by GlobalID ascending, served by the
// events_pii_id_idx index Migrate creates when the Store is built with
// WithPiiTracking. opts is optional — see es.WithTenant, which restricts
// results to that tenant plus any es.GlobalTenantID events.
func (s *Store) FetchByPiiID(ctx context.Context, piiID uuid.UUID, opts ...es.StoreOption) ([]*es.DomainEvent, error) {
	if !s.piiTracking {
		return nil, fmt.Errorf("pgstore: FetchByPiiID needs the Store to be built with WithPiiTracking")
	}
	cfg := es.ResolveStoreConfig(opts...)

	conditions := []string{"pii_id = $1"}
	args := []any{piiID}
	if cfg.TenantID != nil {
		conditions = append(conditions, "tenant_id IN ("+placeholder(len(args)+1)+", "+placeholder(len(args)+2)+")")
		args = append(args, *cfg.TenantID, es.GlobalTenantID)
	}
	query := "SELECT " + strings.Join(s.columns(), ", ") + " FROM events WHERE " + strings.Join(conditions, " AND ") +
		" ORDER BY global_id ASC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("pgstore: fetch by pii id: %w", err)
	}

	events, err := s.scanEvents(rows)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, es.ErrSubjectNotFound
	}
	return events, nil
}

var _ es.PiiLookup = (*Store)(nil)

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

// columns returns this Store's SELECT column list, in the exact order
// scanEvents expects — base columns always present, actor_id/actor_type
// and pii_id only if this Store was built with WithActorTracking/
// WithPiiTracking respectively.
func (s *Store) columns() []string {
	cols := []string{"global_id", "id", "tenant_id", "aggregate_id", "aggregate_version", "version", "name", "payload", "metadata"}
	if s.actorTracking {
		cols = append(cols, "actor_id", "actor_type")
	}
	if s.piiTracking {
		cols = append(cols, "pii_id", "pii_fields")
	}
	return append(cols, "occurred_at")
}

// placeholder returns the "$N" positional parameter Postgres expects, for a
// query being built up as a []string of conditions alongside a parallel
// []any of args. Callers pass n = len(args)+1 (or +2, +3, ... for an
// argument that isn't immediately next) right before appending the
// corresponding value to args, so the two stay in lockstep without the
// query text ever being written out by hand for each opts combination.
func placeholder(n int) string {
	return "$" + strconv.Itoa(n)
}

// scanEvents reads every row into a DomainEvent by positional Scan, in the
// same column order Store.columns() built the SELECT with — no reflection.
func (s *Store) scanEvents(rows *sql.Rows) ([]*es.DomainEvent, error) {
	defer rows.Close()

	var out []*es.DomainEvent
	for rows.Next() {
		e := &es.DomainEvent{}
		var metadata sql.NullString
		dest := []any{
			&e.GlobalID, &e.ID, &e.TenantID, &e.AggregateID, &e.AggregateVersion,
			&e.Version, &e.Name, &e.Payload, &metadata,
		}

		var actorID uuid.NullUUID
		var actorType sql.NullString
		if s.actorTracking {
			dest = append(dest, &actorID, &actorType)
		}

		var piiID uuid.NullUUID
		var piiFields sql.NullString
		if s.piiTracking {
			dest = append(dest, &piiID, &piiFields)
		}

		dest = append(dest, &e.OccurredAt)

		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("pgstore: scan event: %w", err)
		}

		if metadata.Valid {
			e.Metadata = json.RawMessage(metadata.String)
		}
		if s.actorTracking {
			if actorID.Valid {
				e.ActorID = &actorID.UUID
			}
			if actorType.Valid {
				e.ActorType = &actorType.String
			}
		}
		if s.piiTracking {
			if piiID.Valid {
				e.PiiID = &piiID.UUID
			}
			if piiFields.Valid {
				e.PiiFields = json.RawMessage(piiFields.String)
			}
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// nullRawMessage adapts a possibly-nil/empty json.RawMessage for a nullable
// JSONB column.
func nullRawMessage(m json.RawMessage) any {
	if len(m) == 0 {
		return nil
	}
	return []byte(m)
}

// nullUUID adapts a possibly-nil *uuid.UUID for a nullable UUID column.
func nullUUID(id *uuid.UUID) any {
	if id == nil {
		return nil
	}
	return *id
}

// nullString adapts a possibly-nil *string for a nullable TEXT column.
func nullString(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}
