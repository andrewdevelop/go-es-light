package memstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/es/memstore"
)

func TestStore_Commit_EmptyIsNoop(t *testing.T) {
	store := memstore.New()
	if err := store.Commit(context.Background(), nil, 0); err != nil {
		t.Fatalf("Commit with no events: %v", err)
	}
	events, err := store.FetchAfter(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("FetchAfter: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no events, got %d", len(events))
	}
}

func TestStore_Commit_ConcurrencyConflict(t *testing.T) {
	store := memstore.New()
	aggID := uuid.New()

	first := &es.DomainEvent{ID: uuid.New(), AggregateID: aggID, AggregateVersion: 1, Name: "A", Payload: []byte(`{}`)}
	if err := store.Commit(context.Background(), []*es.DomainEvent{first}, 0); err != nil {
		t.Fatalf("Commit first: %v", err)
	}

	stale := &es.DomainEvent{ID: uuid.New(), AggregateID: aggID, AggregateVersion: 2, Name: "B", Payload: []byte(`{}`)}
	err := store.Commit(context.Background(), []*es.DomainEvent{stale}, 0)
	if !errors.Is(err, es.ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict, got %v", err)
	}
}

func TestStore_FetchAfter_SkipsAndLimits(t *testing.T) {
	store := memstore.New()
	aggID := uuid.New()

	for i := uint64(1); i <= 5; i++ {
		e := &es.DomainEvent{ID: uuid.New(), AggregateID: aggID, AggregateVersion: i, Name: "E", Payload: []byte(`{}`)}
		if err := store.Commit(context.Background(), []*es.DomainEvent{e}, i-1); err != nil {
			t.Fatalf("Commit %d: %v", i, err)
		}
	}

	// lastID=2 must skip the first two events (exercises the `continue` branch).
	events, err := store.FetchAfter(context.Background(), 2, 2)
	if err != nil {
		t.Fatalf("FetchAfter: %v", err)
	}
	// limit=2 must stop early (exercises the `break` branch) even though 3 remain.
	if len(events) != 2 {
		t.Fatalf("expected 2 events (limited), got %d", len(events))
	}
	if events[0].GlobalID != 3 {
		t.Fatalf("expected first returned event to be global_id 3, got %d", events[0].GlobalID)
	}
}

func TestStore_StreamAll_CancelDuringBacklogSend(t *testing.T) {
	store := memstore.New()
	aggID := uuid.New()
	seed := &es.DomainEvent{ID: uuid.New(), AggregateID: aggID, AggregateVersion: 1, Name: "Seeded", Payload: []byte(`{}`)}
	if err := store.Commit(context.Background(), []*es.DomainEvent{seed}, 0); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Context is already cancelled and nobody ever reads `out`, so the
	// backlog-send select must take the ctx.Done() branch, not out<-e.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	out, errc := store.StreamAll(ctx)

	// ctx is already cancelled and nobody has read from `out` yet, so by
	// the time the goroutine reaches its first select, out<-e isn't a ready
	// case (no receiver waiting) and ctx.Done() is the only one that can
	// fire — give it a moment to run and exit before we read.
	time.Sleep(50 * time.Millisecond)

	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("expected out to close without emitting the backlog")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for out to close")
	}
	if err, ok := <-errc; ok && err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStore_StreamAll_CancelDuringLiveSend(t *testing.T) {
	store := memstore.New()
	ctx, cancel := context.WithCancel(context.Background())

	// Empty store: StreamAll goes straight past the (empty) backlog loop
	// into the live-forwarding loop.
	out, errc := store.StreamAll(ctx)

	aggID := uuid.New()
	live := &es.DomainEvent{ID: uuid.New(), AggregateID: aggID, AggregateVersion: 1, Name: "Live", Payload: []byte(`{}`)}
	if err := store.Commit(context.Background(), []*es.DomainEvent{live}, 0); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Give the goroutine time to pull the event off `live` and block trying
	// to send it on `out` (nobody reads out), then cancel: the inner select
	// must take the ctx.Done() branch, not out<-e.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("expected out to close without emitting the live event")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for out to close")
	}
	if err, ok := <-errc; ok && err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStore_StreamAll(t *testing.T) {
	store := memstore.New()

	aggID := uuid.New()
	seed := &es.DomainEvent{
		ID:               uuid.New(),
		AggregateID:      aggID,
		AggregateVersion: 1,
		Version:          1,
		Name:             "Seeded",
		Payload:          []byte(`{}`),
		OccurredAt:       time.Now(),
	}
	if err := store.Commit(context.Background(), []*es.DomainEvent{seed}, 0); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out, errc := store.StreamAll(ctx)

	first := <-out
	if first.Name != "Seeded" {
		t.Fatalf("expected backfilled event, got %+v", first)
	}

	live := &es.DomainEvent{
		ID:               uuid.New(),
		AggregateID:      aggID,
		AggregateVersion: 2,
		Version:          1,
		Name:             "Live",
		Payload:          []byte(`{}`),
		OccurredAt:       time.Now(),
	}
	go func() {
		if err := store.Commit(context.Background(), []*es.DomainEvent{live}, 1); err != nil {
			t.Errorf("Commit live: %v", err)
		}
	}()

	select {
	case e := <-out:
		if e.Name != "Live" {
			t.Fatalf("expected live event, got %+v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for live event")
	}

	cancel()
	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("expected out channel to close after cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for out to close")
	}
	if err, ok := <-errc; ok && err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
