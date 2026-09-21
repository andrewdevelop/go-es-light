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

// TestAdvisoryLock_NoOpContract pins down memstore.AdvisoryLock's documented
// no-op behavior: within a single process there's nothing to coordinate
// with, so TryAcquire must always succeed (including a second, concurrent
// "holder" — unlike pgstore's real advisory lock, which would refuse) and
// Release must always be a no-op. This exists so code written against
// es.AdvisoryLock (leader-election in a projector, say) can run unchanged in
// tests against memstore instead of a real Postgres advisory lock.
func TestAdvisoryLock_NoOpContract(t *testing.T) {
	ctx := context.Background()

	lockA := memstore.NewAdvisoryLock("projector-leader")
	ok, err := lockA.TryAcquire(ctx)
	if err != nil || !ok {
		t.Fatalf("expected TryAcquire to succeed, got ok=%v err=%v", ok, err)
	}

	// A second "holder" of the same named lock must also succeed — memstore
	// has no cross-holder coordination at all, by design.
	lockB := memstore.NewAdvisoryLock("projector-leader")
	ok, err = lockB.TryAcquire(ctx)
	if err != nil || !ok {
		t.Fatalf("expected a second TryAcquire to also succeed (no-op lock), got ok=%v err=%v", ok, err)
	}

	if err := lockA.Release(ctx); err != nil {
		t.Fatalf("expected Release to be a no-op, got err=%v", err)
	}
	// Release must be safe to call more than once.
	if err := lockA.Release(ctx); err != nil {
		t.Fatalf("expected a second Release to also be a no-op, got err=%v", err)
	}
}

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

func TestStore_WithTenant_Isolation(t *testing.T) {
	store := memstore.New()
	aggID := uuid.New()
	tenantA := uuid.New()
	tenantB := uuid.New()

	eventA := &es.DomainEvent{ID: uuid.New(), AggregateID: aggID, AggregateVersion: 1, Name: "A", Payload: []byte(`{}`)}
	if err := store.Commit(context.Background(), []*es.DomainEvent{eventA}, 0, es.WithTenant(tenantA)); err != nil {
		t.Fatalf("Commit tenant A: %v", err)
	}

	// Same aggregate id, different tenant: must not see tenant A's stream,
	// so this looks like a brand-new aggregate at version 0.
	eventB := &es.DomainEvent{ID: uuid.New(), AggregateID: aggID, AggregateVersion: 1, Name: "B", Payload: []byte(`{}`)}
	if err := store.Commit(context.Background(), []*es.DomainEvent{eventB}, 0, es.WithTenant(tenantB)); err != nil {
		t.Fatalf("Commit tenant B: %v", err)
	}

	if _, err := store.Load(context.Background(), aggID, es.WithTenant(tenantB)); err != nil {
		t.Fatalf("Load tenant B: %v", err)
	}

	// A tenant-scoped Load for a tenant that never committed this aggregate
	// id must behave as not-found, even though the id exists under another
	// tenant.
	tenantC := uuid.New()
	if _, err := store.Load(context.Background(), aggID, es.WithTenant(tenantC)); !errors.Is(err, es.ErrAggregateNotFound) {
		t.Fatalf("expected ErrAggregateNotFound for foreign tenant, got %v", err)
	}

	// Unscoped Commit/Load (no es.WithTenant at all) must behave exactly as
	// before this feature existed — single-tenant callers see zero change.
	untenantedAgg := uuid.New()
	untenanted := &es.DomainEvent{ID: uuid.New(), AggregateID: untenantedAgg, AggregateVersion: 1, Name: "U", Payload: []byte(`{}`)}
	if err := store.Commit(context.Background(), []*es.DomainEvent{untenanted}, 0); err != nil {
		t.Fatalf("Commit unscoped: %v", err)
	}
	if _, err := store.Load(context.Background(), untenantedAgg); err != nil {
		t.Fatalf("Load unscoped: %v", err)
	}
}

func TestStore_WithTenant_GlobalTenantVisibleToAll(t *testing.T) {
	store := memstore.New()
	aggID := uuid.New()

	// Committed without es.WithTenant, so it lands under es.GlobalTenantID
	// (the nil UUID) — a genuinely shared/tenant-less stream.
	global := &es.DomainEvent{ID: uuid.New(), AggregateID: aggID, AggregateVersion: 1, Name: "Global", Payload: []byte(`{}`)}
	if err := store.Commit(context.Background(), []*es.DomainEvent{global}, 0); err != nil {
		t.Fatalf("Commit global: %v", err)
	}

	tenant := uuid.New()
	events, err := store.Load(context.Background(), aggID, es.WithTenant(tenant))
	if err != nil {
		t.Fatalf("Load global aggregate as tenant-scoped caller: %v", err)
	}
	if len(events) != 1 || events[0].Name != "Global" {
		t.Fatalf("expected the global event to be visible, got %+v", events)
	}
}

func TestStore_WithAllTenants_SeesEveryTenant(t *testing.T) {
	store := memstore.New()
	tenantA := uuid.New()
	tenantB := uuid.New()

	aggA := uuid.New()
	if err := store.Commit(context.Background(), []*es.DomainEvent{{ID: uuid.New(), AggregateID: aggA, AggregateVersion: 1, Name: "A", Payload: []byte(`{}`)}}, 0, es.WithTenant(tenantA)); err != nil {
		t.Fatalf("Commit tenant A: %v", err)
	}
	aggB := uuid.New()
	if err := store.Commit(context.Background(), []*es.DomainEvent{{ID: uuid.New(), AggregateID: aggB, AggregateVersion: 1, Name: "B", Payload: []byte(`{}`)}}, 0, es.WithTenant(tenantB)); err != nil {
		t.Fatalf("Commit tenant B: %v", err)
	}

	// es.WithAllTenants is an explicit marker for "no scope" — it must
	// behave exactly like omitting es.WithTenant entirely (which is already
	// unscoped), not filter anything out.
	events, err := store.FetchAfter(context.Background(), 0, 0, es.WithAllTenants())
	if err != nil {
		t.Fatalf("FetchAfter with WithAllTenants: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected both tenants' events visible under WithAllTenants, got %d", len(events))
	}

	unscoped, err := store.FetchAfter(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("FetchAfter unscoped: %v", err)
	}
	if len(unscoped) != len(events) {
		t.Fatalf("expected WithAllTenants to match plain omission, got %d vs %d", len(events), len(unscoped))
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
