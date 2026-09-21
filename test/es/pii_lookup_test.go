package es_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/es/memstore"
)

func ev(id uuid.UUID, version uint64, name string, pii *uuid.UUID, tenant uuid.UUID) *es.DomainEvent {
	return &es.DomainEvent{
		ID:               uuid.New(),
		TenantID:         tenant,
		AggregateID:      id,
		AggregateVersion: version,
		Version:          1,
		Name:             name,
		PiiID:            pii,
		Payload:          []byte(`{}`),
	}
}

// TestFetchByPiiID_AcrossAggregatesAndTenants exercises the memstore
// PiiLookup implementation: a subject whose events span two aggregates and
// two tenants, seen unscoped (everything) and scoped (only one tenant).
func TestFetchByPiiID_AcrossAggregatesAndTenants(t *testing.T) {
	ctx := context.Background()
	store := memstore.New()

	pii := uuid.New()
	aggA, aggB := uuid.New(), uuid.New()
	tenantA, tenantB := uuid.New(), uuid.New()

	// Committed separately: Commit requires every event in a batch to belong
	// to one aggregate. Commit order (→ GlobalID order) is: e0, e1, e2.
	e0 := ev(aggA, 1, "PersonRegistered", &pii, tenantA)
	e1 := ev(aggB, 1, "OrderPlaced", &pii, tenantA)
	e2 := ev(aggA, 2, "PersonRegistered", &pii, tenantB)
	for _, e := range []*es.DomainEvent{e0, e1, e2} {
		if err := store.Commit(ctx, []*es.DomainEvent{e}, 0); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}

	// Unscoped: everything, in commit (GlobalID) order.
	got, err := store.FetchByPiiID(ctx, pii)
	if err != nil {
		t.Fatalf("FetchByPiiID: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 events, got %d", len(got))
	}
	if got[0].Name != "PersonRegistered" || got[1].Name != "OrderPlaced" || got[2].Name != "PersonRegistered" {
		t.Fatalf("expected commit-order result, got %+v", got)
	}

	// es.WithAllTenants must behave exactly like unscoped.
	all, err := store.FetchByPiiID(ctx, pii, es.WithAllTenants())
	if err != nil {
		t.Fatalf("FetchByPiiID WithAllTenants: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected WithAllTenants to match unscoped (3 events), got %d", len(all))
	}

	// Tenant-scoped: only that tenant's events.
	onlyA, err := store.FetchByPiiID(ctx, pii, es.WithTenant(tenantA))
	if err != nil {
		t.Fatalf("FetchByPiiID tenantA: %v", err)
	}
	if len(onlyA) != 2 || onlyA[0].Name != "PersonRegistered" || onlyA[1].Name != "OrderPlaced" {
		t.Fatalf("expected tenant A's 2 events, got %+v", onlyA)
	}

	// A subject that never existed → ErrSubjectNotFound.
	if _, err := store.FetchByPiiID(ctx, uuid.New()); !errors.Is(err, es.ErrSubjectNotFound) {
		t.Fatalf("expected ErrSubjectNotFound, got %v", err)
	}
	// ...also when it exists but only in another tenant.
	if _, err := store.FetchByPiiID(ctx, pii, es.WithTenant(tenantB)); err != nil {
		t.Fatalf("expected tenant B to find its own event, got %v", err)
	}
	foreign := uuid.New()
	if _, err := store.FetchByPiiID(ctx, foreign, es.WithTenant(tenantB)); !errors.Is(err, es.ErrSubjectNotFound) {
		t.Fatalf("expected ErrSubjectNotFound for subject in the wrong tenant, got %v", err)
	}
}

// TestSubjectExporter_GroupsByAggregate proves Export turns the flat,
// global-cursor-ordered FetchByPiiID result into the subject-centric shape
// a DSAR/access-request consumer needs: grouped by (tenant, aggregate),
// events within an aggregate sorted by AggregateVersion.
func TestSubjectExporter_GroupsByAggregate(t *testing.T) {
	ctx := context.Background()
	store := memstore.New()

	pii := uuid.New()
	aggA, aggB := uuid.New(), uuid.New()
	tenant := uuid.New()

	// Interleaved so global order ≠ per-aggregate order: aggB's v1 lands
	// between aggA's v1 and v2. Grouping must still restore aggA = [v1, v2].
	a1 := ev(aggA, 1, "Registered", &pii, tenant)
	b1 := ev(aggB, 1, "OrderPlaced", &pii, tenant)
	a2 := ev(aggA, 2, "Updated", &pii, tenant)
	if err := store.Commit(ctx, []*es.DomainEvent{a1}, 0); err != nil {
		t.Fatalf("Commit a1: %v", err)
	}
	if err := store.Commit(ctx, []*es.DomainEvent{b1}, 0); err != nil {
		t.Fatalf("Commit b1: %v", err)
	}
	if err := store.Commit(ctx, []*es.DomainEvent{a2}, 1); err != nil {
		t.Fatalf("Commit a2: %v", err)
	}

	exporter := es.NewSubjectExporter(store)
	export, err := exporter.Export(ctx, pii, es.WithTenant(tenant))
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if export.PiiID != pii {
		t.Fatalf("expected PiiID %v, got %v", pii, export.PiiID)
	}
	if len(export.Aggregates) != 2 {
		t.Fatalf("expected 2 aggregate groups, got %d", len(export.Aggregates))
	}

	first := export.Aggregates[0]
	if first.AggregateID != aggA || len(first.Events) != 2 {
		t.Fatalf("expected aggA first with 2 events, got %+v", first)
	}
	if first.Events[0].AggregateVersion != 1 || first.Events[1].AggregateVersion != 2 {
		t.Fatalf("expected aggA events in version order, got %d, %d",
			first.Events[0].AggregateVersion, first.Events[1].AggregateVersion)
	}

	second := export.Aggregates[1]
	if second.AggregateID != aggB || second.TenantID != tenant || len(second.Events) != 1 {
		t.Fatalf("expected aggB second with 1 event, got %+v", second)
	}
}

// TestSubjectExporter_ErrSubjectNotFound proves the exporter surfaces the
// lookup's ErrSubjectNotFound instead of manufacturing an empty export.
func TestSubjectExporter_ErrSubjectNotFound(t *testing.T) {
	exporter := es.NewSubjectExporter(memstore.New())
	if _, err := exporter.Export(context.Background(), uuid.New()); !errors.Is(err, es.ErrSubjectNotFound) {
		t.Fatalf("expected ErrSubjectNotFound, got %v", err)
	}
}

// TestPiiEventStore_FetchByPiiID_Opens is the subject-exchange version of
// TestPiiEventStore_SealsOnCommitAndForgetsOnErasure: whatever reads an
// exported subject file through the PiiEventStore-wrapped store gets
// plaintext (or PiiUnrecoverable), same as any other read path.
func TestPiiEventStore_FetchByPiiID_Opens(t *testing.T) {
	ctx := context.Background()
	raw := memstore.New()
	keys := memstore.NewKeyRing()
	store := es.NewPiiEventStore(raw, newPiiRegistry(), es.NewPiiAnonymizer(keys))

	pii := uuid.New()
	agg := uuid.New()
	e := &es.DomainEvent{
		ID:               uuid.New(),
		AggregateID:      agg,
		AggregateVersion: 1,
		Name:             "PersonRegistered",
		PiiID:            &pii,
		Payload:          mustJSON(t, PersonRegistered{Email: "ann@example.com", Name: "Ann"}),
	}
	if err := store.Commit(ctx, []*es.DomainEvent{e}, 0); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Through the wrapping store: payload opened, exactly like Load.
	got, err := store.FetchByPiiID(ctx, pii)
	if err != nil {
		t.Fatalf("FetchByPiiID: %v", err)
	}
	if len(got) != 1 || got[0].PiiUnrecoverable {
		t.Fatalf("expected 1 opened event, got %+v", got)
	}
	var plain PersonRegistered
	if err := got[0].UnmarshalPayload(&plain); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if plain.Email != "ann@example.com" {
		t.Fatalf("expected opened email, got %+v", plain)
	}

	// Directly under the raw store: still ciphertext.
	rawGot, err := raw.FetchByPiiID(ctx, pii)
	if err != nil {
		t.Fatalf("raw FetchByPiiID: %v", err)
	}
	var sealed map[string]string
	if err := json.Unmarshal(rawGot[0].Payload, &sealed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if sealed["email"] == "ann@example.com" {
		t.Fatal("expected raw stored event to stay sealed")
	}
}

// TestPiiLookup_UnsupportedWrappers covers the delegating wrappers' failure
// mode: a store that doesn't implement es.PiiLookup surfaces
// ErrPiiLookupUnsupported rather than being silently filtered.
func TestPiiLookup_UnsupportedWrappers(t *testing.T) {
	ctx := context.Background()
	anon := es.NewPiiAnonymizer(memstore.NewKeyRing())
	reg := newPiiRegistry()

	bare := &fakeStore{} // implements EventStore only
	sealed := es.NewPiiEventStore(bare, reg, anon)
	if _, err := sealed.FetchByPiiID(ctx, uuid.New()); !errors.Is(err, es.ErrPiiLookupUnsupported) {
		t.Fatalf("expected ErrPiiLookupUnsupported from PiiEventStore, got %v", err)
	}

	scoped := es.NewTenantScopedStore(bare, uuid.New())
	if _, err := scoped.FetchByPiiID(ctx, uuid.New()); !errors.Is(err, es.ErrPiiLookupUnsupported) {
		t.Fatalf("expected ErrPiiLookupUnsupported from TenantScopedStore, got %v", err)
	}
}

// TestTenantScopedStore_FetchByPiiID_AlwaysScoped proves the bound tenant
// applies to subject lookups too — a per-tenant worker's subject export can
// never drift into another tenant's data.
func TestTenantScopedStore_FetchByPiiID_AlwaysScoped(t *testing.T) {
	ctx := context.Background()
	raw := memstore.New()

	pii := uuid.New()
	agg := uuid.New()
	tenantA, tenantB := uuid.New(), uuid.New()

	if err := raw.Commit(ctx, []*es.DomainEvent{ev(agg, 1, "PersonRegistered", &pii, tenantA)}, 0); err != nil {
		t.Fatalf("Commit tenantA: %v", err)
	}

	scoped := es.NewTenantScopedStore(raw, tenantA)
	got, err := scoped.FetchByPiiID(ctx, pii)
	if err != nil {
		t.Fatalf("FetchByPiiID: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected tenant A's event, got %+v", got)
	}

	// Even an explicit escape attempt (WithTenant(tenantB)) is overridden by
	// the bound tenant, so tenant A's event still comes back — a per-tenant
	// worker can never accidentally look at another tenant's subject file.
	override, err := scoped.FetchByPiiID(ctx, pii, es.WithTenant(tenantB))
	if err != nil {
		t.Fatalf("expected the bound tenant to win over WithTenant(tenantB), got %v", err)
	}
	if len(override) != 1 {
		t.Fatalf("expected tenant A's event despite the override attempt, got %+v", override)
	}

	// And a subject that exists only in tenant B is invisible to a worker
	// bound to tenant A.
	piiB := uuid.New()
	if err := raw.Commit(ctx, []*es.DomainEvent{ev(uuid.New(), 1, "PersonRegistered", &piiB, tenantB)}, 0); err != nil {
		t.Fatalf("Commit tenantB: %v", err)
	}
	if _, err := scoped.FetchByPiiID(ctx, piiB); !errors.Is(err, es.ErrSubjectNotFound) {
		t.Fatalf("expected ErrSubjectNotFound for the other tenant's subject, got %v", err)
	}
}
