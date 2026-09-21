//go:build integration

package pgstore_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/es/pgstore"
)

func mkEvent(aggID uuid.UUID, version uint64, name string, payload string) *es.DomainEvent {
	return &es.DomainEvent{
		ID:               uuid.New(),
		AggregateID:      aggID,
		AggregateVersion: version,
		Version:          1,
		Name:             name,
		Payload:          json.RawMessage(payload),
		OccurredAt:       time.Now().UTC(),
	}
}

func TestPgstore_CommitLoad_RoundTrip(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	aggID := uuid.New()

	events := []*es.DomainEvent{
		mkEvent(aggID, 1, "Created", `{"n":1}`),
		mkEvent(aggID, 2, "Updated", `{"n":2}`),
	}
	if err := store.Commit(ctx, events, 0); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if events[0].GlobalID == 0 || events[1].GlobalID == 0 {
		t.Fatal("expected GlobalID to be populated by Commit")
	}
	if events[1].GlobalID <= events[0].GlobalID {
		t.Fatalf("expected increasing global ids, got %d then %d", events[0].GlobalID, events[1].GlobalID)
	}

	loaded, err := store.Load(ctx, aggID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("expected 2 events, got %d", len(loaded))
	}
	if loaded[0].Name != "Created" || loaded[1].Name != "Updated" {
		t.Fatalf("unexpected order/content: %+v", loaded)
	}
	var payload struct{ N int }
	if err := loaded[1].UnmarshalPayload(&payload); err != nil {
		t.Fatalf("UnmarshalPayload: %v", err)
	}
	if payload.N != 2 {
		t.Fatalf("expected payload.N=2, got %d", payload.N)
	}
}

func TestPgstore_Load_NotFound(t *testing.T) {
	store, _ := newStore(t)
	_, err := store.Load(context.Background(), uuid.New())
	if !errors.Is(err, es.ErrAggregateNotFound) {
		t.Fatalf("expected ErrAggregateNotFound, got %v", err)
	}
}

func TestPgstore_Commit_ConcurrencyConflict(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	aggID := uuid.New()

	if err := store.Commit(ctx, []*es.DomainEvent{mkEvent(aggID, 1, "Created", `{}`)}, 0); err != nil {
		t.Fatalf("seed Commit: %v", err)
	}

	// Stale expectedVersion (0, but store now holds version 1).
	err := store.Commit(ctx, []*es.DomainEvent{mkEvent(aggID, 2, "Stale", `{}`)}, 0)
	if !errors.Is(err, es.ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict, got %v", err)
	}

	// Nothing from the failed commit must have been persisted.
	events, err := store.Load(ctx, aggID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected the rejected commit to persist nothing, got %d events", len(events))
	}
}

func TestPgstore_Commit_PartialBatchIsAtomic(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	aggID := uuid.New()

	// Two events in one Commit call, but the second collides with a row
	// someone else already inserted concurrently (same aggregate_id +
	// aggregate_version): the whole batch must roll back, not just the
	// second event.
	if err := store.Commit(ctx, []*es.DomainEvent{mkEvent(aggID, 1, "First", `{}`)}, 0); err != nil {
		t.Fatalf("seed Commit: %v", err)
	}

	batch := []*es.DomainEvent{
		mkEvent(aggID, 2, "SecondA", `{}`),
		mkEvent(aggID, 2, "SecondB", `{}`), // duplicate version within the same batch -> unique violation
	}
	if err := store.Commit(ctx, batch, 1); err == nil {
		t.Fatal("expected the batch with a duplicate aggregate_version to fail")
	}

	events, err := store.Load(ctx, aggID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected the failed batch to persist nothing beyond the seed, got %d events", len(events))
	}
}

// TestPgstore_FetchAfter_Pagination pages through the *entire* events
// table with a small limit, not just this test's own aggregate: the table
// is shared with whatever else is running against the same database (e.g.
// test/user's integration tests, when both packages are given to `go test`
// on one command line and run as concurrent binaries), so other aggregates'
// events can legitimately land between this one's in global_id order. The
// test only asserts what must hold regardless of that interleaving: pages
// never exceed the requested limit, the cursor strictly increases, and —
// filtering every page down to this test's own aggregate — its 7 events
// come back in order, with no gaps or duplicates.
func TestPgstore_FetchAfter_Pagination(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	aggID := uuid.New()

	const total = 7
	var lastGlobalID uint64
	for i := uint64(1); i <= total; i++ {
		e := mkEvent(aggID, i, fmt.Sprintf("E%d", i), `{}`)
		if err := store.Commit(ctx, []*es.DomainEvent{e}, i-1); err != nil {
			t.Fatalf("Commit %d: %v", i, err)
		}
		if i == 1 {
			lastGlobalID = e.GlobalID - 1 // one before the first of this aggregate's events
		}
	}

	var mine []*es.DomainEvent
	cursor := lastGlobalID
	for page := 0; len(mine) < total; page++ {
		if page > 10_000 {
			t.Fatalf("paginated %d times without finding all %d of this test's own events — got %d, foreign traffic must be unbounded", page, total, len(mine))
		}

		events, err := store.FetchAfter(ctx, cursor, 3)
		if err != nil {
			t.Fatalf("FetchAfter: %v", err)
		}
		if len(events) == 0 {
			t.Fatal("FetchAfter returned no events before finding all of this test's own — cursor made no progress")
		}
		if len(events) > 3 {
			t.Fatalf("expected at most 3 events per page, got %d", len(events))
		}

		for i, e := range events {
			if i > 0 && e.GlobalID <= events[i-1].GlobalID {
				t.Fatal("events within a page are not strictly increasing by global_id")
			}
			if e.GlobalID <= cursor {
				t.Fatal("page did not strictly advance past the requested cursor")
			}
			if e.AggregateID == aggID {
				mine = append(mine, e)
			}
		}
		cursor = events[len(events)-1].GlobalID
	}

	if len(mine) != total {
		t.Fatalf("expected exactly %d of this test's own events, got %d", total, len(mine))
	}
	for i, e := range mine {
		wantVersion := uint64(i + 1)
		if e.AggregateVersion != wantVersion {
			t.Fatalf("event %d: expected aggregate_version %d, got %d (gap/duplicate/reorder)", i, wantVersion, e.AggregateVersion)
		}
	}
}

func TestPgstore_Checkpointer_GetSave(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	name := "projector:" + uuid.New().String()

	last, err := store.Get(ctx, name)
	if err != nil {
		t.Fatalf("Get (fresh): %v", err)
	}
	if last != 0 {
		t.Fatalf("expected fresh checkpoint 0, got %d", last)
	}

	if err := store.Save(ctx, name, 42); err != nil {
		t.Fatalf("Save: %v", err)
	}
	last, err = store.Get(ctx, name)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if last != 42 {
		t.Fatalf("expected checkpoint 42, got %d", last)
	}

	// Save must upsert, not fail on an existing row.
	if err := store.Save(ctx, name, 100); err != nil {
		t.Fatalf("Save (update): %v", err)
	}
	last, err = store.Get(ctx, name)
	if err != nil {
		t.Fatalf("Get (after update): %v", err)
	}
	if last != 100 {
		t.Fatalf("expected checkpoint 100, got %d", last)
	}
}

func TestPgstore_StreamAll_BackfillThenLive(t *testing.T) {
	store, _ := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aggID := uuid.New()
	seed := mkEvent(aggID, 1, "Seeded", `{}`)
	if err := store.Commit(context.Background(), []*es.DomainEvent{seed}, 0); err != nil {
		t.Fatalf("seed Commit: %v", err)
	}

	out, errc := store.StreamAll(ctx)

	// Backfill must eventually surface our seeded event (there may be
	// other events already in this database from earlier test runs).
	deadline := time.After(10 * time.Second)
	found := false
	for !found {
		select {
		case e, ok := <-out:
			if !ok {
				t.Fatal("out closed before finding the seeded event")
			}
			if e.ID == seed.ID {
				found = true
			}
		case err := <-errc:
			t.Fatalf("StreamAll error: %v", err)
		case <-deadline:
			t.Fatal("timed out waiting for backfilled seed event")
		}
	}

	live := mkEvent(aggID, 2, "Live", `{}`)
	if err := store.Commit(context.Background(), []*es.DomainEvent{live}, 1); err != nil {
		t.Fatalf("live Commit: %v", err)
	}

	deadline = time.After(10 * time.Second)
	for {
		select {
		case e, ok := <-out:
			if !ok {
				t.Fatal("out closed before observing the live event")
			}
			if e.ID == live.ID {
				return // success
			}
		case err := <-errc:
			t.Fatalf("StreamAll error: %v", err)
		case <-deadline:
			t.Fatal("timed out waiting for the live (NOTIFY-or-poll delivered) event")
		}
	}
}

func TestPgstore_StreamAll_ContextCancelStopsCleanly(t *testing.T) {
	store, _ := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())

	out, errc := store.StreamAll(ctx)
	cancel()

	deadline := time.After(10 * time.Second)
	for {
		select {
		case _, ok := <-out:
			if !ok {
				out = nil
			}
		case _, ok := <-errc:
			if !ok {
				errc = nil
			}
		case <-deadline:
			t.Fatal("timed out waiting for StreamAll to shut down after cancel")
		}
		if out == nil && errc == nil {
			return
		}
	}
}

// TestPgstore_Concurrency_NoLostUpdates is the real-database counterpart of
// the memstore concurrency test in /test/concurrency: many goroutines race
// Commit on the same aggregate through Postgres itself, each retrying on
// ErrConcurrencyConflict. It proves the transaction-scoped advisory lock +
// MAX(aggregate_version) check in pgstore.Store.Commit actually serializes
// writers under a real database, not just in mocked-out Go code.
func TestPgstore_Concurrency_NoLostUpdates(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	aggID := uuid.New()

	const writers = 50
	var successes int64
	var wg sync.WaitGroup
	wg.Add(writers)

	for i := 0; i < writers; i++ {
		go func(n int) {
			defer wg.Done()

			for {
				current, err := store.Load(ctx, aggID)
				var expectedVersion uint64
				if errors.Is(err, es.ErrAggregateNotFound) {
					expectedVersion = 0
				} else if err != nil {
					t.Errorf("Load: %v", err)
					return
				} else {
					expectedVersion = current[len(current)-1].AggregateVersion
				}

				event := mkEvent(aggID, expectedVersion+1, "Incremented", `{}`)
				err = store.Commit(ctx, []*es.DomainEvent{event}, expectedVersion)
				if err == nil {
					atomic.AddInt64(&successes, 1)
					return
				}
				if errors.Is(err, es.ErrConcurrencyConflict) {
					continue
				}
				t.Errorf("Commit: %v", err)
				return
			}
		}(i)
	}

	wg.Wait()

	if successes != writers {
		t.Fatalf("expected %d successful writers, got %d", writers, successes)
	}

	events, err := store.Load(ctx, aggID)
	if err != nil {
		t.Fatalf("final Load: %v", err)
	}
	if len(events) != writers {
		t.Fatalf("expected %d persisted events, got %d (lost update)", writers, len(events))
	}
	for i, e := range events {
		want := uint64(i + 1)
		if e.AggregateVersion != want {
			t.Fatalf("event %d: expected aggregate_version %d, got %d (gap/duplicate)", i, want, e.AggregateVersion)
		}
	}
}

func TestPgstore_AdvisoryLock_LeaderElection(t *testing.T) {
	_, pool := newStore(t)
	name := "projector:" + uuid.New().String()

	leaderLock := pgstore.NewAdvisoryLock(pool, name)
	ok, err := leaderLock.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("first TryAcquire: %v", err)
	}
	if !ok {
		t.Fatal("expected the first replica to become leader")
	}

	followerLock := pgstore.NewAdvisoryLock(pool, name)
	ok, err = followerLock.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("second TryAcquire: %v", err)
	}
	if ok {
		t.Fatal("expected the second replica to NOT become leader while the first holds the lock")
	}

	if err := leaderLock.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	ok, err = followerLock.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("TryAcquire after release: %v", err)
	}
	if !ok {
		t.Fatal("expected the follower to become leader after the original leader released")
	}
	if err := followerLock.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

func TestPgstore_ActorPiiTracking_OptionalColumns(t *testing.T) {
	ctx := context.Background()
	actorID := uuid.New()
	actorType := "employee"
	piiID := uuid.New()

	// Runs first, deliberately: it checks the shared events table has no
	// actor/pii columns yet. The tracking-enabled subtest below calls
	// Migrate with those options against the same PGSTORE_TEST_DSN database,
	// and Migrate never drops columns once added — so this check would give
	// a false pass if it ran after that subtest instead of before it.
	t.Run("store without tracking never creates actor_id/actor_type/pii_id/pii_fields columns", func(t *testing.T) {
		_, pool := newStore(t)
		rows, err := pool.QueryContext(ctx, `
			SELECT column_name FROM information_schema.columns
			WHERE table_name = 'events' AND column_name IN ('actor_id', 'actor_type', 'pii_id', 'pii_fields')
		`)
		if err != nil {
			t.Fatalf("query information_schema: %v", err)
		}
		defer rows.Close()
		var found []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("scan: %v", err)
			}
			found = append(found, name)
		}
		if len(found) != 0 {
			t.Fatalf("expected no actor/pii columns without WithActorTracking/WithPiiTracking, found %v", found)
		}
	})

	t.Run("store without tracking rejects events carrying actor/pii", func(t *testing.T) {
		store, _ := newStore(t)
		aggID := uuid.New()

		withActor := mkEvent(aggID, 1, "Created", `{}`)
		withActor.ActorID = &actorID
		withActor.ActorType = &actorType
		if err := store.Commit(ctx, []*es.DomainEvent{withActor}, 0); err == nil {
			t.Fatal("expected Commit to reject an event carrying an actor without WithActorTracking")
		}

		withPii := mkEvent(aggID, 1, "Created", `{}`)
		withPii.PiiID = &piiID
		if err := store.Commit(ctx, []*es.DomainEvent{withPii}, 0); err == nil {
			t.Fatal("expected Commit to reject an event carrying a PiiID without WithPiiTracking")
		}
	})

	// Runs last: enables tracking and calls Migrate, which permanently adds
	// actor_id/actor_type/pii_id/pii_fields to the shared events table for
	// the rest of this test binary's run.
	t.Run("store with tracking round-trips actor_id/actor_type/pii_id", func(t *testing.T) {
		store, _ := newStore(t, pgstore.WithActorTracking(), pgstore.WithPiiTracking())
		aggID := uuid.New()

		e := mkEvent(aggID, 1, "Created", `{}`)
		e.ActorID = &actorID
		e.ActorType = &actorType
		e.PiiID = &piiID
		if err := store.Commit(ctx, []*es.DomainEvent{e}, 0); err != nil {
			t.Fatalf("Commit: %v", err)
		}

		loaded, err := store.Load(ctx, aggID)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if len(loaded) != 1 {
			t.Fatalf("expected 1 event, got %d", len(loaded))
		}
		got := loaded[0]
		if got.ActorID == nil || *got.ActorID != actorID {
			t.Fatalf("expected ActorID %v, got %v", actorID, got.ActorID)
		}
		if got.ActorType == nil || *got.ActorType != actorType {
			t.Fatalf("expected ActorType %q, got %v", actorType, got.ActorType)
		}
		if got.PiiID == nil || *got.PiiID != piiID {
			t.Fatalf("expected PiiID %v, got %v", piiID, got.PiiID)
		}
	})
}

// TestPgstore_GrantTenantBypass_RoleSeesEveryTenant is the role-based
// counterpart to memstore's TestStore_WithAllTenants_SeesEveryTenant: it
// proves GrantTenantBypass actually enforces a bypass at the database
// level, for a non-superuser role, independent of whether that role's own
// queries ever pass es.WithTenant/es.WithAllTenants. Connecting as a
// superuser (the default local/Docker "postgres" role) would always bypass
// RLS regardless of this grant — see WithTenantIsolation's doc comment —
// so this test deliberately creates and connects as an ordinary,
// non-superuser role to actually exercise the grant.
func TestPgstore_GrantTenantBypass_RoleSeesEveryTenant(t *testing.T) {
	ctx := context.Background()
	dsn := testDSN(t)
	store, pool := newStore(t)
	if err := store.Migrate(ctx, pgstore.WithTenantIsolation()); err != nil {
		t.Fatalf("Migrate with WithTenantIsolation: %v", err)
	}

	tenantA := uuid.New()
	aggA := uuid.New()
	if err := store.Commit(ctx, []*es.DomainEvent{mkEvent(aggA, 1, "Created", `{}`)}, 0, es.WithTenant(tenantA)); err != nil {
		t.Fatalf("seed Commit: %v", err)
	}

	roleName := "bypass_test_" + sanitizeRoleName(uuid.New().String())
	if _, err := pool.ExecContext(ctx, `CREATE ROLE `+roleName+` LOGIN PASSWORD 'test' NOSUPERUSER NOBYPASSRLS`); err != nil {
		t.Fatalf("CREATE ROLE: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.ExecContext(context.Background(), `DROP ROLE IF EXISTS `+roleName)
	})
	if _, err := pool.ExecContext(ctx, `GRANT SELECT, INSERT ON events TO `+roleName); err != nil {
		t.Fatalf("GRANT on events: %v", err)
	}
	if _, err := pool.ExecContext(ctx, `GRANT USAGE, SELECT ON SEQUENCE events_global_id_seq TO `+roleName); err != nil {
		t.Fatalf("GRANT on sequence: %v", err)
	}

	openAs := func(t *testing.T) *sql.DB {
		t.Helper()
		db, err := sql.Open("postgres", roleDSN(t, dsn, roleName, "test"))
		if err != nil {
			t.Fatalf("sql.Open as %s: %v", roleName, err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}

	tenantB := uuid.New()

	// Before the grant: an ordinary role without BYPASSRLS must NOT see
	// tenant A's row from a session scoped to a different tenant — RLS
	// isolation holds for it, same as it did for pgstore.Store's own
	// WithTenantIsolation test.
	before := openAs(t)
	var countBefore int
	txBefore, err := before.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx (before grant): %v", err)
	}
	if _, err := txBefore.ExecContext(ctx, `SELECT set_config('app.tenant_id', $1, true)`, tenantB.String()); err != nil {
		t.Fatalf("set_config (before grant): %v", err)
	}
	if err := txBefore.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE aggregate_id = $1`, aggA).Scan(&countBefore); err != nil {
		t.Fatalf("count (before grant): %v", err)
	}
	_ = txBefore.Rollback()
	if countBefore != 0 {
		t.Fatalf("expected role without BYPASSRLS to see 0 rows for a foreign tenant, got %d", countBefore)
	}

	// Grant the bypass, then connect fresh — Postgres checks role
	// attributes at authentication, not per-query, so an already-open
	// session wouldn't pick this up.
	if err := store.GrantTenantBypass(ctx, roleName); err != nil {
		t.Fatalf("GrantTenantBypass: %v", err)
	}

	after := openAs(t)
	var countAfter int
	txAfter, err := after.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx (after grant): %v", err)
	}
	if _, err := txAfter.ExecContext(ctx, `SELECT set_config('app.tenant_id', $1, true)`, tenantB.String()); err != nil {
		t.Fatalf("set_config (after grant): %v", err)
	}
	if err := txAfter.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE aggregate_id = $1`, aggA).Scan(&countAfter); err != nil {
		t.Fatalf("count (after grant): %v", err)
	}
	_ = txAfter.Rollback()
	if countAfter != 1 {
		t.Fatalf("expected BYPASSRLS role to see tenant A's row regardless of its own session tenant, got %d", countAfter)
	}

	// Revoke, connect fresh again, confirm isolation resumes.
	if err := store.RevokeTenantBypass(ctx, roleName); err != nil {
		t.Fatalf("RevokeTenantBypass: %v", err)
	}

	revoked := openAs(t)
	var countRevoked int
	txRevoked, err := revoked.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx (after revoke): %v", err)
	}
	if _, err := txRevoked.ExecContext(ctx, `SELECT set_config('app.tenant_id', $1, true)`, tenantB.String()); err != nil {
		t.Fatalf("set_config (after revoke): %v", err)
	}
	if err := txRevoked.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE aggregate_id = $1`, aggA).Scan(&countRevoked); err != nil {
		t.Fatalf("count (after revoke): %v", err)
	}
	_ = txRevoked.Rollback()
	if countRevoked != 0 {
		t.Fatalf("expected isolation to resume after RevokeTenantBypass, got %d rows", countRevoked)
	}
}

// sanitizeRoleName turns a UUID's hyphens into underscores so it's
// safe to splice directly into a bare (unquoted) SQL identifier for a
// throwaway test role name.
func sanitizeRoleName(s string) string {
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '-' {
			out[i] = '_'
		} else {
			out[i] = s[i]
		}
	}
	return string(out)
}
