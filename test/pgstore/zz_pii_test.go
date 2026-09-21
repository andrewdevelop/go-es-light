//go:build integration

// Filename starts with zz_ on purpose: it runs after pgstore_test.go, whose
// TestPgstore_ActorPiiTracking_OptionalColumns deliberately verifies the
// shared events table still has zero actor/pii columns before its
// tracking-enabled subtest creates them — and the tests here depend on those
// columns existing.
//
// These cover the subject-centric (es.PiiLookup) and KEK-wrapping
// (es.WrappedKeyRing) primitives against a real Postgres.
package pgstore_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/es/pgstore"
)

func piiEvent(aggID uuid.UUID, version uint64, name string, pii *uuid.UUID, tenant uuid.UUID) *es.DomainEvent {
	e := mkEvent(aggID, version, name, `{}`)
	e.PiiID = pii
	e.TenantID = tenant
	return e
}

type personPayload struct {
	Email string `json:"email"`
}

func (personPayload) EventName() string { return "Registered" }

func (personPayload) PiiFields() map[string][]string {
	return map[string][]string{"payload": {"email"}}
}

type erasurePayload struct{}

func (erasurePayload) EventName() string { return "Erased" }
func (erasurePayload) RequestsErasure()  {}

func TestPgstore_FetchByPiiID_AcrossAggregatesAndTenants(t *testing.T) {
	ctx := context.Background()
	store, pool := newStore(t, pgstore.WithPiiTracking())

	pii := uuid.New()
	aggA, aggB := uuid.New(), uuid.New()
	tenantA, tenantB := uuid.New(), uuid.New()

	if err := store.Commit(ctx, []*es.DomainEvent{piiEvent(aggA, 1, "Registered", &pii, tenantA)}, 0, es.WithTenant(tenantA)); err != nil {
		t.Fatalf("Commit A1: %v", err)
	}
	if err := store.Commit(ctx, []*es.DomainEvent{piiEvent(aggB, 1, "OrderPlaced", &pii, tenantA)}, 0, es.WithTenant(tenantA)); err != nil {
		t.Fatalf("Commit B1: %v", err)
	}
	// Same aggregate id under a different tenant starts its own version
	// sequence (per-tenant optimistic concurrency), so expectedVersion is 0.
	if err := store.Commit(ctx, []*es.DomainEvent{piiEvent(aggA, 2, "Updated", &pii, tenantB)}, 0, es.WithTenant(tenantB)); err != nil {
		t.Fatalf("Commit A2: %v", err)
	}

	// The events_pii_id_idx index must exist after the tracking-enabled
	// Migrate.
	t.Run("index exists", func(t *testing.T) {
		var count int
		if err := pool.QueryRowContext(ctx,
			`SELECT count(*) FROM pg_indexes WHERE indexname = 'events_pii_id_idx'`,
		).Scan(&count); err != nil {
			t.Fatalf("query pg_indexes: %v", err)
		}
		if count != 1 {
			t.Fatalf("expected events_pii_id_idx to exist, got %d rows", count)
		}
	})

	// Unscoped: all three, in commit order.
	got, err := store.FetchByPiiID(ctx, pii)
	if err != nil {
		t.Fatalf("FetchByPiiID: %v", err)
	}
	if len(got) != 3 || got[0].Name != "Registered" || got[1].Name != "OrderPlaced" || got[2].Name != "Updated" {
		t.Fatalf("expected 3 events in commit order, got %+v", got)
	}

	// Tenant-scoped: only that tenant's events.
	scoped, err := store.FetchByPiiID(ctx, pii, es.WithTenant(tenantA))
	if err != nil {
		t.Fatalf("FetchByPiiID tenantA: %v", err)
	}
	if len(scoped) != 2 {
		t.Fatalf("expected tenant A's 2 events, got %d", len(scoped))
	}

	// Unknown subject.
	if _, err := store.FetchByPiiID(ctx, uuid.New()); !errors.Is(err, es.ErrSubjectNotFound) {
		t.Fatalf("expected ErrSubjectNotFound, got %v", err)
	}
}

func TestPgstore_FetchByPiiID_RequiresTracking(t *testing.T) {
	store, _ := newStore(t) // no WithPiiTracking
	if _, err := store.FetchByPiiID(context.Background(), uuid.New()); err == nil {
		t.Fatal("expected FetchByPiiID to error on a Store built without WithPiiTracking")
	}
}

func TestPgstore_SubjectExport_EndToEnd(t *testing.T) {
	ctx := context.Background()
	store, pool := newStore(t, pgstore.WithPiiTracking())
	keys := pgstore.NewKeyRing(pool)
	if err := keys.Migrate(ctx); err != nil {
		t.Fatalf("KeyRing.Migrate: %v", err)
	}

	reg := es.NewRegistry()
	reg.Register("Registered", func() any { return &personPayload{} })

	anon := es.NewPiiAnonymizer(keys)
	sealed := es.NewPiiEventStore(store, reg, anon)
	exporter := es.NewSubjectExporter(sealed)

	pii := uuid.New()
	// The suite is designed to run against a fresh database; this test
	// issues a key it never erases, so forget it on the way out to keep the
	// shared pii_keys table clean for order-dependent suite neighbors.
	t.Cleanup(func() { _ = keys.Forget(ctx, pii) })
	agg := uuid.New()
	tenant := uuid.New()
	event := &es.DomainEvent{
		ID:               uuid.New(),
		AggregateID:      agg,
		AggregateVersion: 1,
		Version:          1,
		Name:             "Registered",
		PiiID:            &pii,
		Payload:          []byte(`{"email":"ann@example.com"}`),
	}
	if err := sealed.Commit(ctx, []*es.DomainEvent{event}, 0, es.WithTenant(tenant)); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	export, err := exporter.Export(ctx, pii, es.WithTenant(tenant))
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if len(export.Aggregates) != 1 || len(export.Aggregates[0].Events) != 1 {
		t.Fatalf("expected 1 aggregate with 1 event, got %+v", export)
	}
	if export.Aggregates[0].Events[0].PiiUnrecoverable {
		t.Fatal("expected the exported event to be opened, not flagged unrecoverable")
	}
	var plain personPayload
	if err := export.Aggregates[0].Events[0].UnmarshalPayload(&plain); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if plain.Email != "ann@example.com" {
		t.Fatalf("expected the exported event to decrypt, got %+v", plain)
	}
}

// TestPgstore_ErasureLedger_ChainRoundTrip: append-only pii_shreds rows
// round-trip into a hash chain that survives a Postgres round-trip (bytea
// hashes, timestamptz truncated to seconds by ShredRecordHash).
func TestPgstore_ErasureLedger_ChainRoundTrip(t *testing.T) {
	ctx := context.Background()
	_, pool := newStore(t)
	ledger := pgstore.NewErasureLedger(pool)
	if err := ledger.Migrate(ctx); err != nil {
		t.Fatalf("ErasureLedger.Migrate: %v", err)
	}

	now := time.Date(2026, 9, 21, 10, 30, 15, 123456000, time.UTC)
	if err := ledger.Append(ctx, uuid.New(), now, 1); err != nil {
		t.Fatalf("Append 1: %v", err)
	}
	if err := ledger.Append(ctx, uuid.New(), now.Add(time.Second), 2); err != nil {
		t.Fatalf("Append 2: %v", err)
	}

	entries, err := ledger.Entries(ctx)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 records, got %d", len(entries))
	}
	if err := es.CheckShredChain(entries); err != nil {
		t.Fatalf("expected a valid chain after a Postgres round-trip, got %v", err)
	}
}

// TestPgstore_ErasureVerifier_EndToEnd runs the provable-shred flow against
// a real Postgres: PiiEventStore with a mounted pgstore ledger, a real
// Registered → Erased lifecycle, and the verifier proving key + events are
// gone.
func TestPgstore_ErasureVerifier_EndToEnd(t *testing.T) {
	ctx := context.Background()
	store, pool := newStore(t, pgstore.WithPiiTracking())

	keys := pgstore.NewKeyRing(pool)
	if err := keys.Migrate(ctx); err != nil {
		t.Fatalf("KeyRing.Migrate: %v", err)
	}
	ledger := pgstore.NewErasureLedger(pool)
	if err := ledger.Migrate(ctx); err != nil {
		t.Fatalf("ErasureLedger.Migrate: %v", err)
	}

	reg := es.NewRegistry()
	reg.Register("Registered", func() any { return &personPayload{} })
	reg.Register("Erased", func() any { return &erasurePayload{} })

	sealed := es.NewPiiEventStore(store, reg, es.NewPiiAnonymizer(keys), es.WithErasureLedger(ledger))

	pii := uuid.New()
	agg := uuid.New()
	tenant := uuid.New()
	if err := sealed.Commit(ctx, []*es.DomainEvent{{
		ID:               uuid.New(),
		AggregateID:      agg,
		AggregateVersion: 1,
		Version:          1,
		Name:             "Registered",
		PiiID:            &pii,
		Payload:          []byte(`{"email":"ann@example.com"}`),
	}}, 0, es.WithTenant(tenant)); err != nil {
		t.Fatalf("Commit registered: %v", err)
	}
	if err := sealed.Commit(ctx, []*es.DomainEvent{{
		ID:               uuid.New(),
		AggregateID:      agg,
		AggregateVersion: 2,
		Version:          1,
		Name:             "Erased",
		PiiID:            &pii,
		Payload:          []byte(`{}`),
	}}, 1, es.WithTenant(tenant)); err != nil {
		t.Fatalf("Commit erasure: %v", err)
	}

	verifier := es.NewErasureVerifier(sealed, keys, ledger)
	proof, err := verifier.Verify(ctx, pii)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !proof.KeyForgotten || !proof.EventsUnrecoverable || len(proof.Erasures) != 1 {
		t.Fatalf("expected a complete proof, got %+v", proof)
	}

	// A never-erased subject is reported honestly.
	if _, err := verifier.Verify(ctx, uuid.New()); !errors.Is(err, es.ErrNoErasureRecord) {
		t.Fatalf("expected ErrNoErasureRecord for a never-erased subject, got %v", err)
	}
}

func TestPgstore_WrappedKeyRing_Rotate(t *testing.T) {
	ctx := context.Background()
	_, pool := newStore(t)
	keys := pgstore.NewKeyRing(pool)
	if err := keys.Migrate(ctx); err != nil {
		t.Fatalf("KeyRing.Migrate: %v", err)
	}

	kek1, err := es.NewAesGcmWrapper(bytes.Repeat([]byte{1}, 32), "kek:v1")
	if err != nil {
		t.Fatalf("NewAesGcmWrapper: %v", err)
	}
	ring := es.NewWrappedKeyRing(keys, kek1)

	subj := uuid.New()
	t.Cleanup(func() { _ = ring.Forget(ctx, subj) })
	key := bytes.Repeat([]byte{9}, 32)
	if err := ring.Put(ctx, subj, key); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The pii_keys row must hold a wrapped blob, not the plaintext DEK.
	blob, err := keys.Get(ctx, subj)
	if err != nil {
		t.Fatalf("raw keys.Get: %v", err)
	}
	if blob == nil || bytes.Equal(blob, key) {
		t.Fatal("expected the raw keyring to hold wrapped (non-plaintext) bytes")
	}

	// Subjects() must expose the subject for rotation.
	subjects, err := keys.Subjects(ctx)
	if err != nil {
		t.Fatalf("Subjects: %v", err)
	}
	if len(subjects) != 1 || subjects[0] != subj {
		t.Fatalf("expected exactly [%v], got %v", subj, subjects)
	}

	kek2, err := es.NewAesGcmWrapper(bytes.Repeat([]byte{2}, 32), "kek:v2")
	if err != nil {
		t.Fatalf("NewAesGcmWrapper: %v", err)
	}
	if err := ring.Rotate(ctx, kek2); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	got, err := ring.Get(ctx, subj)
	if err != nil {
		t.Fatalf("Get after rotate: %v", err)
	}
	if !bytes.Equal(got, key) {
		t.Fatalf("expected the DEK intact after rotation, got %x", got)
	}
}
