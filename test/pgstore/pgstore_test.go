//go:build integration

package pgstore_test

import (
	"context"
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

	page1, err := store.FetchAfter(ctx, lastGlobalID, 3)
	if err != nil {
		t.Fatalf("FetchAfter page1: %v", err)
	}
	if len(page1) != 3 {
		t.Fatalf("expected page of 3, got %d", len(page1))
	}
	page2, err := store.FetchAfter(ctx, page1[len(page1)-1].GlobalID, 3)
	if err != nil {
		t.Fatalf("FetchAfter page2: %v", err)
	}
	if len(page2) != 3 {
		t.Fatalf("expected page of 3, got %d", len(page2))
	}
	page3, err := store.FetchAfter(ctx, page2[len(page2)-1].GlobalID, 3)
	if err != nil {
		t.Fatalf("FetchAfter page3: %v", err)
	}
	if len(page3) != 1 {
		t.Fatalf("expected final page of 1, got %d", len(page3))
	}
	if page1[0].GlobalID >= page1[2].GlobalID || page2[0].GlobalID <= page1[2].GlobalID {
		t.Fatal("pages are not strictly increasing by global_id")
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
