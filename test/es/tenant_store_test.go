package es_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/es/memstore"
)

func TestTenantScopedStore_ScopesEveryCall(t *testing.T) {
	ctx := context.Background()
	raw := memstore.New()

	tenantA := uuid.New()
	tenantB := uuid.New()

	// Seed one event under each tenant, same aggregate id, via the raw
	// (unscoped) store directly.
	aggID := uuid.New()
	if err := raw.Commit(ctx, []*es.DomainEvent{{ID: uuid.New(), AggregateID: aggID, AggregateVersion: 1, Name: "A", Payload: []byte(`{}`)}}, 0, es.WithTenant(tenantA)); err != nil {
		t.Fatalf("seed tenant A: %v", err)
	}
	if err := raw.Commit(ctx, []*es.DomainEvent{{ID: uuid.New(), AggregateID: aggID, AggregateVersion: 1, Name: "B", Payload: []byte(`{}`)}}, 0, es.WithTenant(tenantB)); err != nil {
		t.Fatalf("seed tenant B: %v", err)
	}

	scoped := es.NewTenantScopedStore(raw, tenantA)
	if scoped.TenantID() != tenantA {
		t.Fatalf("expected TenantID() to return %v, got %v", tenantA, scoped.TenantID())
	}

	// A bare, unscoped-looking call must still only see tenant A's stream —
	// the whole point of the wrapper is that the caller doesn't have to
	// pass es.WithTenant itself.
	events, err := scoped.Load(ctx, aggID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(events) != 1 || events[0].Name != "A" {
		t.Fatalf("expected only tenant A's event, got %+v", events)
	}
}

func TestTenantScopedStore_BoundTenantCannotBeOverridden(t *testing.T) {
	ctx := context.Background()
	raw := memstore.New()

	tenantA := uuid.New()
	tenantB := uuid.New()

	aggID := uuid.New()
	if err := raw.Commit(ctx, []*es.DomainEvent{{ID: uuid.New(), AggregateID: aggID, AggregateVersion: 1, Name: "A", Payload: []byte(`{}`)}}, 0, es.WithTenant(tenantA)); err != nil {
		t.Fatalf("seed tenant A: %v", err)
	}

	scoped := es.NewTenantScopedStore(raw, tenantA)

	// Even a caller that explicitly tries to escape the bound tenant — via
	// es.WithTenant(tenantB) or es.WithAllTenants — must not succeed:
	// TenantScopedStore appends its own WithTenant(tenantA) last, so it
	// always wins. aggID only exists under tenant A, so a genuine escape to
	// tenant B would surface as ErrAggregateNotFound; getting the real event
	// back instead proves the override was ignored.
	events, err := scoped.Load(ctx, aggID, es.WithTenant(tenantB))
	if err != nil {
		t.Fatalf("expected WithTenant(tenantB) to be overridden by the bound tenant (still find tenant A's event), got err=%v", err)
	}
	if len(events) != 1 || events[0].Name != "A" {
		t.Fatalf("expected tenant A's event despite the WithTenant(tenantB) override attempt, got %+v", events)
	}

	if _, err := scoped.Load(ctx, aggID, es.WithAllTenants()); err != nil {
		t.Fatalf("expected WithAllTenants to still be overridden by the bound tenant, got err=%v", err)
	}
}

// TestTenantScopedStore_FetchAfterAlwaysScoped covers FetchAfter, which
// TestTenantScopedStore_ScopesEveryCall/_BoundTenantCannotBeOverridden don't
// touch — every EventStore method TenantScopedStore implements must honor
// the same "bound tenant always wins" guarantee, not just Load/Commit.
func TestTenantScopedStore_FetchAfterAlwaysScoped(t *testing.T) {
	ctx := context.Background()
	raw := memstore.New()

	tenantA := uuid.New()
	tenantB := uuid.New()

	if err := raw.Commit(ctx, []*es.DomainEvent{{ID: uuid.New(), AggregateID: uuid.New(), AggregateVersion: 1, Name: "A", Payload: []byte(`{}`)}}, 0, es.WithTenant(tenantA)); err != nil {
		t.Fatalf("seed tenant A: %v", err)
	}
	if err := raw.Commit(ctx, []*es.DomainEvent{{ID: uuid.New(), AggregateID: uuid.New(), AggregateVersion: 1, Name: "B", Payload: []byte(`{}`)}}, 0, es.WithTenant(tenantB)); err != nil {
		t.Fatalf("seed tenant B: %v", err)
	}

	scoped := es.NewTenantScopedStore(raw, tenantA)

	// A bare call (no options) must only see tenant A's events.
	events, err := scoped.FetchAfter(ctx, 0, 10)
	if err != nil {
		t.Fatalf("FetchAfter: %v", err)
	}
	if len(events) != 1 || events[0].Name != "A" {
		t.Fatalf("expected only tenant A's event, got %+v", events)
	}

	// An attempt to escape the bound tenant via es.WithAllTenants must be
	// overridden — TenantScopedStore appends its own WithTenant(tenantA)
	// last, so it always wins, same as it does for Load/Commit.
	events, err = scoped.FetchAfter(ctx, 0, 10, es.WithAllTenants())
	if err != nil {
		t.Fatalf("FetchAfter with WithAllTenants override attempt: %v", err)
	}
	if len(events) != 1 || events[0].Name != "A" {
		t.Fatalf("expected WithAllTenants to be overridden, still only tenant A's event, got %+v", events)
	}
}

// TestTenantScopedStore_StreamAllAlwaysScoped is StreamAll's equivalent of
// TestTenantScopedStore_FetchAfterAlwaysScoped.
func TestTenantScopedStore_StreamAllAlwaysScoped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	raw := memstore.New()

	tenantA := uuid.New()
	tenantB := uuid.New()

	if err := raw.Commit(ctx, []*es.DomainEvent{{ID: uuid.New(), AggregateID: uuid.New(), AggregateVersion: 1, Name: "A", Payload: []byte(`{}`)}}, 0, es.WithTenant(tenantA)); err != nil {
		t.Fatalf("seed tenant A: %v", err)
	}
	if err := raw.Commit(ctx, []*es.DomainEvent{{ID: uuid.New(), AggregateID: uuid.New(), AggregateVersion: 1, Name: "B", Payload: []byte(`{}`)}}, 0, es.WithTenant(tenantB)); err != nil {
		t.Fatalf("seed tenant B: %v", err)
	}

	scoped := es.NewTenantScopedStore(raw, tenantA)

	// Even an explicit escape attempt (es.WithAllTenants) must be overridden
	// by the bound tenant.
	out, errc := scoped.StreamAll(ctx, es.WithAllTenants())

	select {
	case e := <-out:
		if e.Name != "A" {
			t.Fatalf("expected tenant A's event from the backfill, got %+v", e)
		}
	case err := <-errc:
		t.Fatalf("unexpected stream error: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for StreamAll's backfill")
	}

	// Tenant B's event must never arrive — confirm the stream stays quiet
	// (not merely "the first event happened to be A's").
	select {
	case e := <-out:
		t.Fatalf("expected no further events (tenant B's must be filtered out), got %+v", e)
	case err := <-errc:
		t.Fatalf("unexpected stream error: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestTenantScopedStore_ComposesWithPiiEventStore proves the two wrappers
// this package now has — one for PII, one for tenant scoping — stack
// cleanly: a worker can be handed a store that's simultaneously
// tenant-bound and PII-transparent, with neither wrapper aware the other
// exists.
func TestTenantScopedStore_ComposesWithPiiEventStore(t *testing.T) {
	ctx := context.Background()
	raw := memstore.New()
	registry := es.NewRegistry()
	registry.Register("PersonRegistered", func() any { return &personPayload{} })

	anonymizer := es.NewPiiAnonymizer(memstore.NewKeyRing())
	sealed := es.NewPiiEventStore(raw, registry, anonymizer)

	tenantA := uuid.New()
	scoped := es.NewTenantScopedStore(sealed, tenantA)

	piiID := uuid.New()
	aggID := uuid.New()
	originalPayload := `{"email":"ann@example.com"}`
	event := &es.DomainEvent{ID: uuid.New(), AggregateID: aggID, AggregateVersion: 1, Name: "PersonRegistered", PiiID: &piiID, Payload: []byte(originalPayload)}
	if err := scoped.Commit(ctx, []*es.DomainEvent{event}, 0); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// Commit seals event.Payload in place (PiiEventStore mutates the same
	// *DomainEvent it was given), so from here on event.Payload is already
	// ciphertext too — originalPayload, captured before Commit, is what the
	// raw-store comparison below needs.

	// Through the composed (tenant + PII) store: plaintext.
	loaded, err := scoped.Load(ctx, aggID)
	if err != nil {
		t.Fatalf("Load via composed store: %v", err)
	}
	var plain personPayload
	if err := loaded[0].UnmarshalPayload(&plain); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if plain.Email != "ann@example.com" {
		t.Fatalf("expected decrypted email via composed store, got %+v", plain)
	}

	// Straight off raw (bypassing both wrappers): ciphertext, and tagged
	// with the tenant TenantScopedStore stamped on Commit.
	rawEvents, err := raw.Load(ctx, aggID, es.WithTenant(tenantA))
	if err != nil {
		t.Fatalf("Load raw: %v", err)
	}
	if string(rawEvents[0].Payload) == originalPayload {
		t.Fatal("expected the raw stored event to be sealed (ciphertext), not the original plaintext")
	}
	if rawEvents[0].TenantID != tenantA {
		t.Fatalf("expected TenantScopedStore to have stamped tenant %v, got %v", tenantA, rawEvents[0].TenantID)
	}
}

type personPayload struct {
	Email string `json:"email"`
}

func (personPayload) EventName() string { return "PersonRegistered" }

func (personPayload) PiiFields() map[string][]string {
	return map[string][]string{"payload": {"email"}}
}
