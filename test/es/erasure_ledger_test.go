package es_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/es/memstore"
)

// TestShredRecordHash_ReproducibleAcrossPrecision proves the hash is stable
// for the same wall-clock moment regardless of the source time's sub-second
// precision — a round-trip through a second-precision schemeless store
// (Postgres timestamptz) or a high-precision one (in-memory time.Time)
// recomputes the same link.
func TestShredRecordHash_ReproducibleAcrossPrecision(t *testing.T) {
	base := time.Date(2026, 9, 21, 10, 30, 15, 123456000, time.UTC)
	pii := uuid.New()

	fine := es.ShredRecordHash([32]byte{}, pii, base, 7)
	truncated := es.ShredRecordHash([32]byte{}, pii, base.Truncate(time.Second), 7)
	if fine != truncated {
		t.Fatal("expected the hash to be independent of sub-second precision")
	}
}

// TestShredRecordHash_OrderSensitive proves two records that share all
// fields but differ in one produce different hashes, and that the chain
// includes the predecessor — so records can't be silently reordered or
// substituted without breaking the chain.
func TestShredRecordHash_OrderSensitive(t *testing.T) {
	when := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	if es.ShredRecordHash([32]byte{}, uuid.New(), when, 1) == es.ShredRecordHash([32]byte{}, uuid.New(), when, 1) {
		t.Fatal("expected two different subjects to produce different hashes")
	}
	base := es.ShredRecordHash([32]byte{}, uuid.New(), when, 1)
	chained := es.ShredRecordHash(base, uuid.New(), when, 1)
	fresh := es.ShredRecordHash([32]byte{}, uuid.New(), when, 1)
	if chained == fresh {
		t.Fatal("expected the predecessor link to affect the hash")
	}
}

// TestCheckShredChain over direct-manipulated chains: valid, empty, single,
// and two distinct tampering cases (point edit and rewrite that forgets to
// fix the following link).
func TestCheckShredChain(t *testing.T) {
	ctx := context.Background()
	l := memstore.NewErasureLedger()
	when := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)

	if err := l.Append(ctx, uuid.New(), when, 1); err != nil {
		t.Fatalf("Append 1: %v", err)
	}
	if err := l.Append(ctx, uuid.New(), when.Add(time.Second), 2); err != nil {
		t.Fatalf("Append 2: %v", err)
	}

	chain, err := l.Entries(ctx)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if err := es.CheckShredChain(chain); err != nil {
		t.Fatalf("expected a valid chain, got %v", err)
	}
	if err := es.CheckShredChain(nil); err != nil {
		t.Fatalf("expected an empty chain to be valid: %v", err)
	}
	if err := es.CheckShredChain(chain[:1]); err != nil {
		t.Fatalf("expected a single-record chain to be valid: %v", err)
	}

	// Tamper: change a record's subject in place without relinking — must
	// break its own Hash.
	tampered := append([]es.ShredRecord(nil), chain...)
	tampered[0].PiiID = uuid.New()
	if err := es.CheckShredChain(tampered); err == nil {
		t.Fatal("expected a subject edit to break the chain")
	}

	// Tamper: re-hash the edited record but not the following link's PrevHash
	// — must break the link between records.
	rewritten := append([]es.ShredRecord(nil), chain...)
	rewritten[0] = es.ShredRecord{PiiID: rewritten[0].PiiID, ErasedAt: rewritten[0].ErasedAt, OriginGlobalID: rewritten[0].OriginGlobalID}
	if err := es.CheckShredChain(rewritten); err == nil {
		t.Fatal("expected a relinked-but-orphaned-follower edit to break the chain")
	}
}

// TestErasureVerifier_HappyPath is the full provable-shred flow through a
// PiiEventStore with a mounted ledger: register → erase → the verifier
// proves the key is gone, the PII-bearing event is unrecoverable, and the
// ledger's only record is this subject's.
func TestErasureVerifier_HappyPath(t *testing.T) {
	ctx := context.Background()
	raw := memstore.New()
	keys := memstore.NewKeyRing()
	ledger := memstore.NewErasureLedger()
	store := es.NewPiiEventStore(raw, newPiiRegistry(), es.NewPiiAnonymizer(keys), es.WithErasureLedger(ledger))

	pii := uuid.New()
	agg := uuid.New()
	reg := &es.DomainEvent{
		ID:               uuid.New(),
		AggregateID:      agg,
		AggregateVersion: 1,
		Version:          1,
		Name:             "PersonRegistered",
		PiiID:            &pii,
		Payload:          mustJSON(t, PersonRegistered{Email: "ann@example.com", Name: "Ann"}),
	}
	if err := store.Commit(ctx, []*es.DomainEvent{reg}, 0); err != nil {
		t.Fatalf("Commit registered: %v", err)
	}
	if _, err := keys.Get(ctx, pii); err != nil {
		t.Fatalf("Get key before erase: %v", err)
	}

	er := &es.DomainEvent{
		ID:               uuid.New(),
		AggregateID:      agg,
		AggregateVersion: 2,
		Version:          1,
		Name:             "PersonForgotten",
		PiiID:            &pii,
		Payload:          []byte(`{}`),
	}
	if err := store.Commit(ctx, []*es.DomainEvent{er}, 1); err != nil {
		t.Fatalf("Commit erasure: %v", err)
	}

	verifier := es.NewErasureVerifier(store, keys, ledger)
	proof, err := verifier.Verify(ctx, pii)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !proof.KeyForgotten || !proof.EventsUnrecoverable {
		t.Fatalf("expected both conditions met, got %+v", proof)
	}
	if len(proof.Erasures) != 1 {
		t.Fatalf("expected exactly 1 erasure record, got %d", len(proof.Erasures))
	}
	// OriginGlobalID is the erasure event's own global id (memstore assigns
	// it on Commit), so the record points at the exact trigger.
	if proof.Erasures[0].OriginGlobalID != er.GlobalID {
		t.Fatalf("expected origin %d to match the erasure event's global id, got %d", er.GlobalID, proof.Erasures[0].OriginGlobalID)
	}
}

// TestErasureVerifier_NeverErased: a subject with PII events but no erasure
// record is ErrNoErasureRecord regardless of how healthy the rest looks.
func TestErasureVerifier_NeverErased(t *testing.T) {
	ctx := context.Background()
	keys := memstore.NewKeyRing()
	store := es.NewPiiEventStore(memstore.New(), newPiiRegistry(), es.NewPiiAnonymizer(keys))
	ledger := memstore.NewErasureLedger()

	pii := uuid.New()
	if err := store.Commit(ctx, []*es.DomainEvent{{
		ID:               uuid.New(),
		AggregateID:      uuid.New(),
		AggregateVersion: 1,
		Version:          1,
		Name:             "PersonRegistered",
		PiiID:            &pii,
		Payload:          mustJSON(t, PersonRegistered{Email: "ann@example.com", Name: "Ann"}),
	}}, 0); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	_, err := es.NewErasureVerifier(store, keys, ledger).Verify(ctx, pii)
	if !errors.Is(err, es.ErrNoErasureRecord) {
		t.Fatalf("expected ErrNoErasureRecord, got %v", err)
	}
}

// fakeLedger wraps a valid chain but lets a test substitute doctored
// Entries — the verifier's contract is that it must not trust the ledger
// output, only the recomputable chain.
type fakeLedger struct {
	entries []es.ShredRecord
}

var _ es.ErasureLedger = (*fakeLedger)(nil)

func (f *fakeLedger) Append(_ context.Context, piiID uuid.UUID, erasedAt time.Time, originGlobalID uint64) error {
	var prev [32]byte
	if n := len(f.entries); n > 0 {
		prev = f.entries[n-1].Hash
	}
	f.entries = append(f.entries, es.ShredRecord{
		PiiID:          piiID,
		ErasedAt:       erasedAt,
		OriginGlobalID: originGlobalID,
		PrevHash:       prev,
		Hash:           es.ShredRecordHash(prev, piiID, erasedAt, originGlobalID),
	})
	return nil
}

func (f *fakeLedger) Entries(_ context.Context) ([]es.ShredRecord, error) {
	out := make([]es.ShredRecord, len(f.entries))
	copy(out, f.entries)
	return out, nil
}

// TestErasureVerifier_TamperedLedger: a genuinely erased subject fails
// verification the moment the ledger output has been doctored — that is the
// whole point of the tamper-evident chain.
func TestErasureVerifier_TamperedLedger(t *testing.T) {
	ctx := context.Background()
	keys := memstore.NewKeyRing()
	store := es.NewPiiEventStore(memstore.New(), newPiiRegistry(), es.NewPiiAnonymizer(keys))
	ledger := &fakeLedger{}

	pii := uuid.New()
	agg := uuid.New()
	if err := store.Commit(ctx, []*es.DomainEvent{{
		ID:               uuid.New(),
		AggregateID:      agg,
		AggregateVersion: 1,
		Version:          1,
		Name:             "PersonRegistered",
		PiiID:            &pii,
		Payload:          mustJSON(t, PersonRegistered{Email: "ann@example.com", Name: "Ann"}),
	}}, 0); err != nil {
		t.Fatalf("Commit registered: %v", err)
	}
	if err := store.Commit(ctx, []*es.DomainEvent{{
		ID:               uuid.New(),
		AggregateID:      agg,
		AggregateVersion: 2,
		Version:          1,
		Name:             "PersonForgotten",
		PiiID:            &pii,
		Payload:          []byte(`{}`),
	}}, 1); err != nil {
		t.Fatalf("Commit erasure: %v", err)
	}

	// Populate the ledger with the genuine record.
	if err := ledger.Append(ctx, pii, time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC), 2); err != nil {
		t.Fatalf("Append: %v", err)
	}
	verifier := es.NewErasureVerifier(store, keys, ledger)
	if _, err := verifier.Verify(ctx, pii); err != nil {
		t.Fatalf("expected a clean verification before tampering: %v", err)
	}

	// Doctored: the subject of the record is changed, nothing else relinked.
	doctored := append([]es.ShredRecord(nil), ledger.entries...)
	doctored[0].PiiID = uuid.New()
	doctoredLedger := &fakeLedger{entries: doctored}
	if _, err := es.NewErasureVerifier(store, keys, doctoredLedger).Verify(ctx, pii); err == nil {
		t.Fatal("expected verification to fail on doctored ledger output")
	}
}

// TestErasureVerifier_KeyStillPresent: an erasure record without the
// corresponding key destruction is a failed verification — deletion that
// never happened reveals itself.
func TestErasureVerifier_KeyStillPresent(t *testing.T) {
	ctx := context.Background()
	keys := memstore.NewKeyRing()
	anonymizer := es.NewPiiAnonymizer(keys)
	store := es.NewPiiEventStore(memstore.New(), newPiiRegistry(), anonymizer)
	ledger := &fakeLedger{}

	pii := uuid.New()
	event := &es.DomainEvent{
		ID:      uuid.New(),
		Name:    "PersonRegistered",
		PiiID:   &pii,
		Payload: mustJSON(t, PersonRegistered{Email: "ann@example.com", Name: "Ann"}),
	}
	if err := anonymizer.Seal(ctx, event, PersonRegistered{}); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	// Key exists, but the ledger claims an erasure happened.
	if err := ledger.Append(ctx, pii, time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC), 1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := es.NewErasureVerifier(store, keys, ledger).Verify(ctx, pii); err == nil {
		t.Fatal("expected verification to fail while the key still exists")
	}
}

// TestErasureVerifier_EventsStillDecrypt: a forgotten key with PII-bearing
// events read through a non-opening store still fails verification — the
// event condition is independent of the key condition.
func TestErasureVerifier_EventsStillDecrypt(t *testing.T) {
	ctx := context.Background()
	raw := memstore.New()
	keys := memstore.NewKeyRing()
	anonymizer := es.NewPiiAnonymizer(keys)
	sealed := es.NewPiiEventStore(raw, newPiiRegistry(), anonymizer)
	ledger := &fakeLedger{}

	pii := uuid.New()
	agg := uuid.New()
	if err := sealed.Commit(ctx, []*es.DomainEvent{{
		ID:               uuid.New(),
		AggregateID:      agg,
		AggregateVersion: 1,
		Version:          1,
		Name:             "PersonRegistered",
		PiiID:            &pii,
		Payload:          mustJSON(t, PersonRegistered{Email: "ann@example.com", Name: "Ann"}),
	}}, 0); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// Erase the key for real, and record the ledger entry.
	if err := anonymizer.Forget(ctx, pii); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if err := ledger.Append(ctx, pii, time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC), 1); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Verifier over the *raw* store: events read back without opening, so no
	// PiiUnrecoverable flag is ever set even though the key is gone.
	if _, err := es.NewErasureVerifier(raw, keys, ledger).Verify(ctx, pii); err == nil {
		t.Fatal("expected verification to fail when PII-bearing events still read as live")
	}
	// The same stack over the opening store passes.
	if _, err := es.NewErasureVerifier(sealed, keys, ledger).Verify(ctx, pii); err != nil {
		t.Fatalf("expected the opening store to verify cleanly: %v", err)
	}
}
