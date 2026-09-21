package es_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/es/memstore"
)

// --- fakes: a fallible KeyRing and a deterministic KeyWrapper ---

type fakeKeyRing struct {
	keys map[uuid.UUID][]byte
	subs []uuid.UUID

	getErr    error
	putErr    error
	forgetErr error
	subsErr   error
}

var _ es.KeyRing = (*fakeKeyRing)(nil)
var _ es.KeyRingLister = (*fakeKeyRing)(nil)

func newFakeKeyRing() *fakeKeyRing {
	return &fakeKeyRing{keys: map[uuid.UUID][]byte{}}
}

func (f *fakeKeyRing) Get(_ context.Context, piiID uuid.UUID) ([]byte, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.keys[piiID], nil
}

func (f *fakeKeyRing) Put(_ context.Context, piiID uuid.UUID, key []byte) error {
	if f.putErr != nil {
		return f.putErr
	}
	f.keys[piiID] = key
	return nil
}

func (f *fakeKeyRing) Forget(_ context.Context, piiID uuid.UUID) error {
	if f.forgetErr != nil {
		return f.forgetErr
	}
	delete(f.keys, piiID)
	return nil
}

func (f *fakeKeyRing) Subjects(_ context.Context) ([]uuid.UUID, error) {
	if f.subsErr != nil {
		return nil, f.subsErr
	}
	return f.subs, nil
}

type passthroughWrapper struct {
	label string

	wrapErr   error
	unwrapErr error
}

func (w passthroughWrapper) Wrap(_ context.Context, key []byte) ([]byte, error) {
	if w.wrapErr != nil {
		return nil, w.wrapErr
	}
	return key, nil
}

func (w passthroughWrapper) Unwrap(_ context.Context, wrapped []byte) ([]byte, error) {
	if w.unwrapErr != nil {
		return nil, w.unwrapErr
	}
	return wrapped, nil
}

func (w passthroughWrapper) Label() string { return w.label }

var _ es.KeyWrapper = passthroughWrapper{}

// --- es.WrappedKeyRing ---

// TestWrappedKeyRing_RoundTripAndComposition: the anonymous-ring +
// passthrough-wrapper case — Put stores the wrapped bytes (labelled), Get
// unwraps them back, composition with PiiAnonymizer still round-trips.
func TestWrappedKeyRing_RoundTripAndComposition(t *testing.T) {
	ctx := context.Background()
	ring := es.NewWrappedKeyRing(memstore.NewKeyRing(), passthroughWrapper{label: "kek:v1"})

	piiID := uuid.New()
	key := []byte("0123456789abcdef0123456789abcdef")

	if err := ring.Put(ctx, piiID, key); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := ring.Get(ctx, piiID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, key) {
		t.Fatalf("expected round-tripped key, got %x", got)
	}

	// Missing / forgotten subject: tolerant nil, like any KeyRing.
	missing, err := ring.Get(ctx, uuid.New())
	if err != nil || missing != nil {
		t.Fatalf("expected (nil, nil) for a missing subject, got %v, %v", missing, err)
	}

	// A full anonymizer stack on top of the wrapped ring.
	anonymizer := es.NewPiiAnonymizer(ring)
	event := &es.DomainEvent{
		Name:    "PersonRegistered",
		PiiID:   &piiID,
		Payload: mustJSON(t, PersonRegistered{Email: "ann@example.com", Name: "Ann"}),
	}
	if err := anonymizer.Seal(ctx, event, PersonRegistered{}); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	opened, err := anonymizer.Open(ctx, event)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var plain PersonRegistered
	if err := opened.UnmarshalPayload(&plain); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if plain.Email != "ann@example.com" {
		t.Fatalf("expected round-tripped email through the wrapped ring, got %+v", plain)
	}
}

// TestWrappedKeyRing_Get_LabelMismatch: a blob stored under one wrapper's
// label must refuse to be read through a different wrapper; that's the
// guard that makes Rotate honest instead of silently producing garbage.
func TestWrappedKeyRing_Get_LabelMismatch(t *testing.T) {
	ctx := context.Background()
	ring := es.NewWrappedKeyRing(memstore.NewKeyRing(), passthroughWrapper{label: "kek:v1"})

	piiID := uuid.New()
	if err := ring.Put(ctx, piiID, []byte("deadbeef")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Same KeyRing memory, different wrapper label.
	other := es.NewWrappedKeyRing(ring.Keys, passthroughWrapper{label: "kek:v2"})
	if _, err := other.Get(ctx, piiID); err == nil {
		t.Fatal("expected a label mismatch to error on Get")
	}
}

// TestWrappedKeyRing_Get_CorruptBlobAndWrapErrors covers the remaining Get/
// Put/Forget error branches.
func TestWrappedKeyRing_Get_CorruptBlobAndWrapErrors(t *testing.T) {
	ctx := context.Background()

	// Underlying Get error surfaces wrapped.
	failing := newFakeKeyRing()
	failing.getErr = errors.New("boom")
	ring := es.NewWrappedKeyRing(failing, passthroughWrapper{label: "kek:v1"})
	if _, err := ring.Get(ctx, uuid.New()); err == nil {
		t.Fatal("expected underlying Get error to propagate")
	}

	// A stored blob too short to hold a label frame.
	corrupt := newFakeKeyRing()
	corruptID := uuid.New()
	corrupt.keys[corruptID] = []byte{0x01}
	ring = es.NewWrappedKeyRing(corrupt, passthroughWrapper{label: "kek:v1"})
	if _, err := ring.Get(ctx, corruptID); err == nil {
		t.Fatal("expected corrupt blob to error on Get")
	}

	// Wrap error on Put.
	wrapping := es.NewWrappedKeyRing(newFakeKeyRing(), passthroughWrapper{label: "k", wrapErr: errors.New("kms down")})
	if err := wrapping.Put(ctx, uuid.New(), []byte("x")); err == nil {
		t.Fatal("expected wrap error to propagate")
	}

	// Unwrap error on Get.
	unwrapID := uuid.New()
	unwrapping := es.NewWrappedKeyRing(newFakeKeyRing(), passthroughWrapper{label: "k", unwrapErr: errors.New("bad tag")})
	if err := unwrapping.Put(ctx, unwrapID, []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := unwrapping.Get(ctx, unwrapID); err == nil {
		t.Fatal("expected unwrap error to propagate")
	}

	// Underlying Put error propagates.
	putFailing := newFakeKeyRing()
	putFailing.putErr = errors.New("full")
	ring = es.NewWrappedKeyRing(putFailing, passthroughWrapper{label: "k"})
	if err := ring.Put(ctx, uuid.New(), []byte("x")); err == nil {
		t.Fatal("expected underlying Put error to propagate")
	}

	// Forget error propagates.
	forgetFailing := newFakeKeyRing()
	forgetFailing.forgetErr = errors.New("gone")
	ring = es.NewWrappedKeyRing(forgetFailing, passthroughWrapper{label: "k"})
	if err := ring.Forget(ctx, uuid.New()); err == nil {
		t.Fatal("expected underlying Forget error to propagate")
	}
}

// TestPiiEventStore_CryptoShredsThroughWrappedRing: erasure still deletes
// the de-facto plaintext out of the wrapped ring — it only ever held
// wrapped bytes, but Forget's contract (data becomes unrecoverable) holds.
func TestPiiEventStore_CryptoShredsThroughWrappedRing(t *testing.T) {
	ctx := context.Background()
	rawKeys := memstore.NewKeyRing()
	ring := es.NewWrappedKeyRing(rawKeys, passthroughWrapper{label: "kek:v1"})
	store := es.NewPiiEventStore(memstore.New(), newPiiRegistry(), es.NewPiiAnonymizer(ring))

	piiID := uuid.New()
	agg := uuid.New()
	e := &es.DomainEvent{
		ID:               uuid.New(),
		AggregateID:      agg,
		AggregateVersion: 1,
		Name:             "PersonRegistered",
		PiiID:            &piiID,
		Payload:          mustJSON(t, PersonRegistered{Email: "ann@example.com", Name: "Ann"}),
	}
	if err := store.Commit(ctx, []*es.DomainEvent{e}, 0); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	erasure := &es.DomainEvent{
		ID:               uuid.New(),
		AggregateID:      agg,
		AggregateVersion: 2,
		Name:             "PersonForgotten",
		PiiID:            &piiID,
		Payload:          []byte(`{}`),
	}
	if err := store.Commit(ctx, []*es.DomainEvent{erasure}, 1); err != nil {
		t.Fatalf("Commit erasure: %v", err)
	}

	// The raw ring held a wrapped blob while it existed — and nothing after.
	rawBlob, err := rawKeys.Get(ctx, piiID)
	if err != nil {
		t.Fatalf("rawKeys.Get: %v", err)
	}
	if rawBlob != nil {
		t.Fatal("expected the wrapped ring to have forgotten the blob after erasure")
	}
	if _, err := ring.Get(ctx, piiID); err != nil {
		t.Fatalf("Get after erasure: %v", err)
	}
}

// TestWrappedKeyRing_Rotate re-wraps every surviving DEK under a new label,
// leaves forgotten subjects forgotten, and refuses to no-op on a non-lister
// or a half-finished key.
func TestWrappedKeyRing_Rotate(t *testing.T) {
	ctx := context.Background()

	ring := es.NewWrappedKeyRing(memstore.NewKeyRing(), passthroughWrapper{label: "kek:v1"})
	subjA, subjB := uuid.New(), uuid.New()
	keyA, keyB := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err := ring.Put(ctx, subjA, keyA); err != nil {
		t.Fatalf("Put A: %v", err)
	}
	if err := ring.Put(ctx, subjB, keyB); err != nil {
		t.Fatalf("Put B: %v", err)
	}
	// A forgotten subject must stay forgotten across rotation.
	if err := ring.Forget(ctx, subjB); err != nil {
		t.Fatalf("Forget B: %v", err)
	}

	if err := ring.Rotate(ctx, passthroughWrapper{label: "kek:v2"}); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	gotA, err := ring.Get(ctx, subjA)
	if err != nil {
		t.Fatalf("Get A after rotate: %v", err)
	}
	if !bytes.Equal(gotA, keyA) {
		t.Fatalf("expected key A intact after rotation, got %x", gotA)
	}
	// B: still forgotten — rotation never resurrects a shredded subject.
	if got, err := ring.Get(ctx, subjB); err != nil || got != nil {
		t.Fatalf("expected forgotten subject to stay gone after rotation, got %v, %v", got, err)
	}

	// The stored bytes must now carry the new label (re-wrapped, not
	// untouched): a wrapper still keyed to the old label can't read them.
	stale := es.NewWrappedKeyRing(ring.Keys, passthroughWrapper{label: "kek:v1"})
	if _, err := stale.Get(ctx, subjA); err == nil {
		t.Fatal("expected the old-wrapper view to be unreadable after rotation")
	}

	// Rotation over a non-lister keyring is refused.
	noLister := es.NewWrappedKeyRing(esWrappedKeyRingShim{}, passthroughWrapper{label: "kek:v1"})
	if err := noLister.Rotate(ctx, passthroughWrapper{label: "kek:v2"}); !errors.Is(err, es.ErrNoKeyLister) {
		t.Fatalf("expected ErrNoKeyLister, got %v", err)
	}
}

// esWrappedKeyRingShim unwraps: a KeyRing that deliberately does NOT list.
type esWrappedKeyRingShim struct{}

var _ es.KeyRing = esWrappedKeyRingShim{}

func (esWrappedKeyRingShim) Get(_ context.Context, _ uuid.UUID) ([]byte, error) { return nil, nil }
func (esWrappedKeyRingShim) Put(_ context.Context, _ uuid.UUID, _ []byte) error { return nil }
func (esWrappedKeyRingShim) Forget(_ context.Context, _ uuid.UUID) error        { return nil }

// TestWrappedKeyRing_Rotate_Errors covers every mid-rotation failure, each
// leaving the ring's active wrapper unchanged.
func TestWrappedKeyRing_Rotate_Errors(t *testing.T) {
	ctx := context.Background()

	// Subjects() error.
	failing := newFakeKeyRing()
	failing.subsErr = errors.New("list failed")
	ring := es.NewWrappedKeyRing(failing, passthroughWrapper{label: "kek:v1"})
	if err := ring.Rotate(ctx, passthroughWrapper{label: "kek:v2"}); err == nil {
		t.Fatal("expected Subjects error to propagate")
	}

	// Underlying Get error during rotation.
	getFailing := newFakeKeyRing()
	getFailing.subs = []uuid.UUID{uuid.New()}
	getFailing.getErr = errors.New("boom")
	ring = es.NewWrappedKeyRing(getFailing, passthroughWrapper{label: "kek:v1"})
	if err := ring.Rotate(ctx, passthroughWrapper{label: "kek:v2"}); err == nil {
		t.Fatal("expected rotation Get error to propagate")
	}

	// Corrupt stored blob during rotation.
	corrupt := newFakeKeyRing()
	pii := uuid.New()
	corrupt.subs = []uuid.UUID{pii}
	corrupt.keys[pii] = []byte{0x01} // not a valid label frame
	ring = es.NewWrappedKeyRing(corrupt, passthroughWrapper{label: "kek:v1"})
	if err := ring.Rotate(ctx, passthroughWrapper{label: "kek:v2"}); err == nil {
		t.Fatal("expected corrupt-blob error to propagate")
	}

	// Label mismatch during rotation (a blob from yet another wrapper).
	mismatch := newFakeKeyRing()
	other := uuid.New()
	mismatch.subs = []uuid.UUID{other}
	mismatch.keys[other] = []byte{0, 5, 'k', 'e', 'k', ':', 'x'} // label "kek:x" (5 chars)
	ring = es.NewWrappedKeyRing(mismatch, passthroughWrapper{label: "kek:v1"})
	if err := ring.Rotate(ctx, passthroughWrapper{label: "kek:v2"}); err == nil {
		t.Fatal("expected rotation label-mismatch error to propagate")
	}

	// Unwrap failure during rotation.
	unwrapFail := newFakeKeyRing()
	u := uuid.New()
	unwrapFail.subs = []uuid.UUID{u}
	unwrapFail.keys[u] = []byte("x")
	ring = es.NewWrappedKeyRing(unwrapFail, passthroughWrapper{label: "kek:v1", unwrapErr: errors.New("bad")})
	if err := ring.Rotate(ctx, passthroughWrapper{label: "kek:v2"}); err == nil {
		t.Fatal("expected rotation unwrap error to propagate")
	}

	// Re-wrap failure during rotation.
	rewrapFail := newFakeKeyRing()
	r := uuid.New()
	rewrapFail.subs = []uuid.UUID{r}
	rewrapFail.keys[r] = []byte("x")
	ring = es.NewWrappedKeyRing(rewrapFail, passthroughWrapper{label: "kek:v1"})
	if err := ring.Rotate(ctx, passthroughWrapper{label: "kek:v2", wrapErr: errors.New("kms down")}); err == nil {
		t.Fatal("expected rotation re-wrap error to propagate")
	}

	// Put failure during rotation.
	putFail := newFakeKeyRing()
	p := uuid.New()
	putFail.subs = []uuid.UUID{p}
	putFail.keys[p] = []byte("x")
	putFail.putErr = errors.New("full")
	ring = es.NewWrappedKeyRing(putFail, passthroughWrapper{label: "kek:v1"})
	if err := ring.Rotate(ctx, passthroughWrapper{label: "kek:v2"}); err == nil {
		t.Fatal("expected rotation Put error to propagate")
	}

	// After every failed rotation the active wrapper must be unchanged: the
	// ring still presents itself as kek:v1.
	if ring.Wrapper.Label() != "kek:v1" {
		t.Fatalf("expected the active wrapper to be untouched after failures, got %q", ring.Wrapper.Label())
	}
}

// --- es.AesGcmWrapper ---

func TestAesGcmWrapper_RoundTrip(t *testing.T) {
	ctx := context.Background()
	w, err := es.NewAesGcmWrapper(bytes.Repeat([]byte{1}, 32), "kek:v1")
	if err != nil {
		t.Fatalf("NewAesGcmWrapper: %v", err)
	}
	if w.Label() != "kek:v1" {
		t.Fatalf("expected explicit label, got %q", w.Label())
	}

	key := []byte("0123456789abcdef0123456789abcdef")
	wrapped, err := w.Wrap(ctx, key)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	if bytes.Equal(wrapped, key) {
		t.Fatal("expected wrapped ciphertext to differ from the key")
	}
	opened, err := w.Unwrap(ctx, wrapped)
	if err != nil {
		t.Fatalf("Unwrap: %v", err)
	}
	if !bytes.Equal(opened, key) {
		t.Fatalf("expected round-tripped key, got %x", opened)
	}

	// A different KEK (different label, different key) cannot open the blob.
	other, err := es.NewAesGcmWrapper(bytes.Repeat([]byte{2}, 32), "kek:v2")
	if err != nil {
		t.Fatalf("NewAesGcmWrapper other: %v", err)
	}
	if _, err := other.Unwrap(ctx, wrapped); err == nil {
		t.Fatal("expected a wrong-key unwrap to fail")
	}
}

func TestAesGcmWrapper_Errors(t *testing.T) {
	if _, err := es.NewAesGcmWrapper([]byte("short"), "l"); err == nil {
		t.Fatal("expected an invalid master key length to error")
	}

	w, err := es.NewAesGcmWrapper(bytes.Repeat([]byte{1}, 32), "")
	if err != nil {
		t.Fatalf("NewAesGcmWrapper: %v", err)
	}
	if got := w.Label(); len(got) < len("aes:") || got[:4] != "aes:" {
		t.Fatalf("expected a derived fingerprint label, got %q", got)
	}

	if _, err := w.Unwrap(context.Background(), []byte{0x01}); err == nil {
		t.Fatal("expected a too-short blob to error")
	}
	// Tampered ciphertext (flip one byte past the nonce).
	wrapped, err := w.Wrap(context.Background(), []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	wrapped[len(wrapped)-1] ^= 0xFF
	if _, err := w.Unwrap(context.Background(), wrapped); err == nil {
		t.Fatal("expected tampered ciphertext to fail authentication")
	}
}
