package es_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/es/memstore"
)

// --- a tiny PII-carrying aggregate used only by these tests ---

type PersonRegistered struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

func (PersonRegistered) EventName() string { return "PersonRegistered" }

func (PersonRegistered) PiiFields() map[string][]string {
	return map[string][]string{"payload": {"email", "name"}}
}

type PersonForgotten struct{}

func (PersonForgotten) EventName() string { return "PersonForgotten" }
func (PersonForgotten) RequestsErasure()  {}

type Person struct {
	es.BaseAggregate
	Email string
	Name  string
}

func (p *Person) Apply(event any) error {
	switch e := event.(type) {
	case *PersonRegistered:
		p.Email = e.Email
		p.Name = e.Name
	case *PersonForgotten:
		// no state change needed for this test
	}
	return nil
}

func newPiiRegistry() es.EventRegistry {
	r := es.NewRegistry()
	r.Register("PersonRegistered", func() any { return &PersonRegistered{} })
	r.Register("PersonForgotten", func() any { return &PersonForgotten{} })
	return r
}

func TestPiiAnonymizer_SealOpenRoundTrip(t *testing.T) {
	ctx := context.Background()
	anonymizer := es.NewPiiAnonymizer(memstore.NewKeyRing())

	piiID := uuid.New()
	event := &es.DomainEvent{
		Name:    "PersonRegistered",
		PiiID:   &piiID,
		Payload: mustJSON(t, PersonRegistered{Email: "ann@example.com", Name: "Ann"}),
	}

	if err := anonymizer.Seal(ctx, event, PersonRegistered{}); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	var sealed map[string]string
	if err := json.Unmarshal(event.Payload, &sealed); err != nil {
		t.Fatalf("unmarshal sealed payload: %v", err)
	}
	if sealed["email"] == "ann@example.com" || sealed["name"] == "Ann" {
		t.Fatalf("expected fields to be encrypted, got %+v", sealed)
	}
	if len(event.PiiFields) == 0 {
		t.Fatal("expected Seal to write a field manifest into PiiFields")
	}
	if len(event.Metadata) != 0 {
		t.Fatalf("expected Seal to leave Metadata untouched, got %s", event.Metadata)
	}

	opened, err := anonymizer.Open(ctx, event)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var plain PersonRegistered
	if err := opened.UnmarshalPayload(&plain); err != nil {
		t.Fatalf("unmarshal opened payload: %v", err)
	}
	if plain.Email != "ann@example.com" || plain.Name != "Ann" {
		t.Fatalf("expected round-tripped plaintext, got %+v", plain)
	}

	// The original event must never be mutated by Open.
	if err := json.Unmarshal(event.Payload, &sealed); err != nil {
		t.Fatalf("unmarshal original payload after Open: %v", err)
	}
	if sealed["email"] == "ann@example.com" {
		t.Fatal("Open must not mutate the original event's Payload")
	}
}

func TestPiiAnonymizer_ForgetIsTolerant(t *testing.T) {
	ctx := context.Background()
	anonymizer := es.NewPiiAnonymizer(memstore.NewKeyRing())

	subjectA := uuid.New()
	eventA := &es.DomainEvent{
		Name:    "PersonRegistered",
		PiiID:   &subjectA,
		Payload: mustJSON(t, PersonRegistered{Email: "a@example.com", Name: "A"}),
	}
	if err := anonymizer.Seal(ctx, eventA, PersonRegistered{}); err != nil {
		t.Fatalf("Seal A: %v", err)
	}

	subjectB := uuid.New()
	eventB := &es.DomainEvent{
		Name:    "PersonRegistered",
		PiiID:   &subjectB,
		Payload: mustJSON(t, PersonRegistered{Email: "b@example.com", Name: "B"}),
	}
	if err := anonymizer.Seal(ctx, eventB, PersonRegistered{}); err != nil {
		t.Fatalf("Seal B: %v", err)
	}

	if err := anonymizer.Forget(ctx, subjectA); err != nil {
		t.Fatalf("Forget A: %v", err)
	}

	// Subject A: key gone, Open must be tolerant — no error, fields remain
	// ciphertext (not decryptable, not a crash) — but PiiUnrecoverable must
	// be set so a consumer can tell the difference between "genuinely
	// decrypted" and "still ciphertext, don't use this as a real value."
	openedA, err := anonymizer.Open(ctx, eventA)
	if err != nil {
		t.Fatalf("Open forgotten subject must not error, got: %v", err)
	}
	if !openedA.PiiUnrecoverable {
		t.Fatal("expected PiiUnrecoverable to be true once the subject's key has been forgotten")
	}
	if !errors.Is(openedA.CheckPiiOpen(), es.ErrPiiUnrecoverable) {
		t.Fatalf("expected CheckPiiOpen to return ErrPiiUnrecoverable, got %v", openedA.CheckPiiOpen())
	}
	var stillSealed map[string]string
	if err := json.Unmarshal(openedA.Payload, &stillSealed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if stillSealed["email"] == "a@example.com" {
		t.Fatal("expected subject A's fields to remain ciphertext after Forget")
	}

	// Subject B: unaffected sibling event must still open cleanly, with no
	// PiiUnrecoverable flag set.
	openedB, err := anonymizer.Open(ctx, eventB)
	if err != nil {
		t.Fatalf("Open sibling subject: %v", err)
	}
	if openedB.PiiUnrecoverable {
		t.Fatal("expected PiiUnrecoverable to be false for a subject whose key still exists")
	}
	if err := openedB.CheckPiiOpen(); err != nil {
		t.Fatalf("expected CheckPiiOpen to return nil for a successfully opened event, got %v", err)
	}
	var plainB PersonRegistered
	if err := openedB.UnmarshalPayload(&plainB); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if plainB.Email != "b@example.com" {
		t.Fatalf("expected subject B still decryptable, got %+v", plainB)
	}
}

// TestPiiEventStore_SealsOnCommitAndForgetsOnErasure exercises the store
// wrapper directly (raw memstore + es.NewPiiEventStore) through a
// Repository built on top of it — Repository itself has no PII awareness
// at all, proving encryption really is a property of the store, not
// something Repository has to opt into.
func TestPiiEventStore_SealsOnCommitAndForgetsOnErasure(t *testing.T) {
	ctx := context.Background()
	raw := memstore.New()
	keys := memstore.NewKeyRing()
	anonymizer := es.NewPiiAnonymizer(keys)
	store := es.NewPiiEventStore(raw, newPiiRegistry(), anonymizer)
	repo := es.NewRepository(store, newPiiRegistry(), func() *Person { return &Person{} })

	piiID := uuid.New()
	id := uuid.New()
	person := &Person{}
	person.SetID(id)
	if err := person.RecordThat(person, &PersonRegistered{Email: "ann@example.com", Name: "Ann"}, es.WithPiiID(piiID)); err != nil {
		t.Fatalf("RecordThat: %v", err)
	}
	if err := repo.Save(ctx, person); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// The underlying raw store must hold ciphertext, proving PiiEventStore
	// sealed it before delegating Commit — Repository never touched Payload.
	rawEvents, err := raw.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load raw: %v", err)
	}
	var sealed map[string]string
	if err := json.Unmarshal(rawEvents[0].Payload, &sealed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if sealed["email"] == "ann@example.com" {
		t.Fatal("expected event to be sealed at rest")
	}

	// A key must have been issued for this subject.
	key, err := keys.Get(ctx, piiID)
	if err != nil || key == nil {
		t.Fatalf("expected a key to have been issued, got key=%v err=%v", key, err)
	}

	// Recording an erasure event and saving must crypto-shred the key.
	if err := person.RecordThat(person, &PersonForgotten{}, es.WithPiiID(piiID)); err != nil {
		t.Fatalf("RecordThat erasure: %v", err)
	}
	if err := repo.Save(ctx, person); err != nil {
		t.Fatalf("Save erasure: %v", err)
	}

	key, err = keys.Get(ctx, piiID)
	if err != nil {
		t.Fatalf("Get after erasure: %v", err)
	}
	if key != nil {
		t.Fatal("expected key to be forgotten after erasure event commit")
	}
}

// erroringKeyRing wraps a real es.KeyRing (memstore.KeyRing, so Get/Put/Seal
// still behave normally) but fails Forget once forgetErr is set — used to
// exercise PiiEventStore.Commit's error path after the underlying store has
// already committed successfully.
type erroringKeyRing struct {
	es.KeyRing
	forgetErr error
}

func (k *erroringKeyRing) Forget(ctx context.Context, piiID uuid.UUID) error {
	if k.forgetErr != nil {
		return k.forgetErr
	}
	return k.KeyRing.Forget(ctx, piiID)
}

// erroringLedger is a minimal es.ErasureLedger whose Append always fails —
// used to exercise PiiEventStore.Commit's error path when a mounted ledger
// can't record the shred (e.g. its hash chain is broken, or its backing
// store is unreachable).
type erroringLedger struct {
	appendErr error
}

func (l *erroringLedger) Append(context.Context, uuid.UUID, time.Time, uint64) error {
	return l.appendErr
}

func (l *erroringLedger) Entries(context.Context) ([]es.ShredRecord, error) {
	return nil, nil
}

var errForgetFailed = errors.New("keyring: forget failed")
var errLedgerAppendFailed = errors.New("ledger: append failed")

// TestPiiEventStore_Commit_ForgetErrorPropagates covers the branch where the
// underlying store's Commit has already succeeded (the erasure event is
// durably persisted) but the subsequent crypto-shred fails — Commit must
// still return that error rather than swallow it, so a caller doesn't
// mistake a failed erasure for a completed one.
func TestPiiEventStore_Commit_ForgetErrorPropagates(t *testing.T) {
	ctx := context.Background()
	raw := memstore.New()
	keys := &erroringKeyRing{KeyRing: memstore.NewKeyRing(), forgetErr: errForgetFailed}
	anonymizer := es.NewPiiAnonymizer(keys)
	store := es.NewPiiEventStore(raw, newPiiRegistry(), anonymizer)

	piiID := uuid.New()
	id := uuid.New()
	event := &es.DomainEvent{ID: uuid.New(), AggregateID: id, AggregateVersion: 1, Name: "PersonForgotten", PiiID: &piiID, Payload: []byte(`{}`)}

	err := store.Commit(ctx, []*es.DomainEvent{event}, 0)
	if !errors.Is(err, errForgetFailed) {
		t.Fatalf("expected Commit to propagate the Forget error, got %v", err)
	}

	// The event itself must still be durably committed to the underlying
	// store — PiiEventStore.Commit has no way to roll that back, and
	// shouldn't pretend otherwise by, say, returning a generic error that
	// hides whether the write actually landed.
	rawEvents, loadErr := raw.Load(ctx, id)
	if loadErr != nil || len(rawEvents) != 1 {
		t.Fatalf("expected the event to remain committed despite the Forget failure, got events=%v err=%v", rawEvents, loadErr)
	}
}

// TestPiiEventStore_Commit_LedgerAppendErrorPropagates covers the branch
// where Forget succeeds (the key really is gone) but the mounted
// es.ErasureLedger can't record that it happened — Commit must still
// surface the error, since a caller relying on WithErasureLedger for
// provable erasure needs to know the audit trail is now incomplete.
func TestPiiEventStore_Commit_LedgerAppendErrorPropagates(t *testing.T) {
	ctx := context.Background()
	raw := memstore.New()
	keys := memstore.NewKeyRing()
	anonymizer := es.NewPiiAnonymizer(keys)
	ledger := &erroringLedger{appendErr: errLedgerAppendFailed}
	store := es.NewPiiEventStore(raw, newPiiRegistry(), anonymizer, es.WithErasureLedger(ledger))

	piiID := uuid.New()
	id := uuid.New()
	// Seed a key for the subject so Forget below has something real to do.
	if err := keys.Put(ctx, piiID, make([]byte, 32)); err != nil {
		t.Fatalf("seed key: %v", err)
	}

	event := &es.DomainEvent{ID: uuid.New(), AggregateID: id, AggregateVersion: 1, Name: "PersonForgotten", PiiID: &piiID, Payload: []byte(`{}`)}

	err := store.Commit(ctx, []*es.DomainEvent{event}, 0)
	if !errors.Is(err, errLedgerAppendFailed) {
		t.Fatalf("expected Commit to propagate the ledger Append error, got %v", err)
	}

	// The key must still have been forgotten — Forget ran and succeeded
	// before the ledger write failed; the failure is purely in recording the
	// audit trail, not in the crypto-shred itself.
	key, getErr := keys.Get(ctx, piiID)
	if getErr != nil {
		t.Fatalf("Get after failed ledger append: %v", getErr)
	}
	if key != nil {
		t.Fatal("expected the key to still have been forgotten even though the ledger append failed")
	}
}

// TestPiiEventStore_LoadDecryptsBeforeReplay guards against a real bug an
// earlier version of this design had: whatever opens PII fields must do so
// before LoadFromHistory replays events, or Apply receives ciphertext
// instead of the real business value and the aggregate's in-memory state
// ends up permanently corrupted after any reload.
func TestPiiEventStore_LoadDecryptsBeforeReplay(t *testing.T) {
	ctx := context.Background()
	store := es.NewPiiEventStore(memstore.New(), newPiiRegistry(), es.NewPiiAnonymizer(memstore.NewKeyRing()))
	repo := es.NewRepository(store, newPiiRegistry(), func() *Person { return &Person{} })

	piiID := uuid.New()
	id := uuid.New()
	person := &Person{}
	person.SetID(id)
	if err := person.RecordThat(person, &PersonRegistered{Email: "ann@example.com", Name: "Ann"}, es.WithPiiID(piiID)); err != nil {
		t.Fatalf("RecordThat: %v", err)
	}
	if err := repo.Save(ctx, person); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := repo.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Email != "ann@example.com" || loaded.Name != "Ann" {
		t.Fatalf("expected replayed aggregate to hold decrypted business values, got %+v", loaded)
	}
}

// TestPiiEventStore_StreamAllOpensForRawConsumers is the architectural
// point of PiiEventStore: a consumer reading straight off StreamAll —
// exactly what a read-model projector does, bypassing Repository entirely —
// gets plaintext automatically, with no *PiiAnonymizer of its own and no
// awareness that encryption is happening at all.
func TestPiiEventStore_StreamAllOpensForRawConsumers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	raw := memstore.New()
	store := es.NewPiiEventStore(raw, newPiiRegistry(), es.NewPiiAnonymizer(memstore.NewKeyRing()))
	repo := es.NewRepository(store, newPiiRegistry(), func() *Person { return &Person{} })

	id := uuid.New()
	person := &Person{}
	person.SetID(id)
	if err := person.RecordThat(person, &PersonRegistered{Email: "ann@example.com", Name: "Ann"}, es.WithPiiID(uuid.New())); err != nil {
		t.Fatalf("RecordThat: %v", err)
	}
	if err := repo.Save(ctx, person); err != nil {
		t.Fatalf("Save: %v", err)
	}

	out, errc := store.StreamAll(ctx)
	select {
	case e := <-out:
		var plain PersonRegistered
		if err := e.UnmarshalPayload(&plain); err != nil {
			t.Fatalf("unmarshal streamed event: %v", err)
		}
		if plain.Email != "ann@example.com" {
			t.Fatalf("expected a raw stream consumer to see decrypted plaintext, got %+v", plain)
		}
	case err := <-errc:
		t.Fatalf("unexpected stream error: %v", err)
	}
}

// TestPiiEventStore_PiiUnrecoverablePropagatesAfterErasure is the store-level
// version of TestPiiAnonymizer_ForgetIsTolerant: it proves
// DomainEvent.PiiUnrecoverable reaches a consumer reading through
// Load/FetchAfter/StreamAll — not just through PiiAnonymizer.Open called
// directly — which is what actually matters for a read-model rebuild (see
// examples/ledger/main.go and examples/user/main.go's rebuild
// demonstrations): a fresh projector backfilling from scratch after an
// erasure sees exactly this.
func TestPiiEventStore_PiiUnrecoverablePropagatesAfterErasure(t *testing.T) {
	ctx := context.Background()
	raw := memstore.New()
	store := es.NewPiiEventStore(raw, newPiiRegistry(), es.NewPiiAnonymizer(memstore.NewKeyRing()))
	repo := es.NewRepository(store, newPiiRegistry(), func() *Person { return &Person{} })

	piiID := uuid.New()
	id := uuid.New()
	person := &Person{}
	person.SetID(id)
	if err := person.RecordThat(person, &PersonRegistered{Email: "ann@example.com", Name: "Ann"}, es.WithPiiID(piiID)); err != nil {
		t.Fatalf("RecordThat: %v", err)
	}
	if err := repo.Save(ctx, person); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := person.RecordThat(person, &PersonForgotten{}, es.WithPiiID(piiID)); err != nil {
		t.Fatalf("RecordThat erasure: %v", err)
	}
	if err := repo.Save(ctx, person); err != nil {
		t.Fatalf("Save erasure: %v", err)
	}

	assertUnrecoverable := func(t *testing.T, e *es.DomainEvent) {
		t.Helper()
		if !e.PiiUnrecoverable {
			t.Fatal("expected PiiUnrecoverable to be true for a PersonRegistered event read after its subject's erasure")
		}
		if !errors.Is(e.CheckPiiOpen(), es.ErrPiiUnrecoverable) {
			t.Fatalf("expected CheckPiiOpen to return ErrPiiUnrecoverable, got %v", e.CheckPiiOpen())
		}
	}

	loadEvents, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	assertUnrecoverable(t, loadEvents[0]) // PersonRegistered is the first event

	fetchEvents, err := store.FetchAfter(ctx, 0, 10)
	if err != nil {
		t.Fatalf("FetchAfter: %v", err)
	}
	if len(fetchEvents) == 0 {
		t.Fatal("expected FetchAfter to return at least the PersonRegistered event")
	}
	assertUnrecoverable(t, fetchEvents[0])

	streamCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	out, errc := store.StreamAll(streamCtx)
	select {
	case e := <-out:
		assertUnrecoverable(t, e)
	case err := <-errc:
		t.Fatalf("unexpected stream error: %v", err)
	case <-streamCtx.Done():
		t.Fatal("timed out waiting for StreamAll's backfill")
	}
}

// TestPiiAnonymizer_Open_TamperedCiphertextErrors is the security-critical
// counterpart to TestPiiAnonymizer_ForgetIsTolerant: Open's tolerance is
// specifically for a *forgotten* key, never for ciphertext that's been
// altered while the key still exists. AES-GCM's authentication tag must
// catch that, and Open must surface it as an error, not silently return
// garbage or an empty value that looks like a legitimate (if odd) result.
func TestPiiAnonymizer_Open_TamperedCiphertextErrors(t *testing.T) {
	ctx := context.Background()
	anonymizer := es.NewPiiAnonymizer(memstore.NewKeyRing())

	piiID := uuid.New()
	event := &es.DomainEvent{
		Name:    "PersonRegistered",
		PiiID:   &piiID,
		Payload: mustJSON(t, PersonRegistered{Email: "ann@example.com", Name: "Ann"}),
	}
	if err := anonymizer.Seal(ctx, event, PersonRegistered{}); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// Flip the payload's sealed "email" field to a still-valid-base64,
	// still-JSON-string value that isn't the real ciphertext GCM sealed —
	// simulates on-disk corruption or a tampering attempt, distinct from key
	// forgetting.
	var sealed map[string]string
	if err := json.Unmarshal(event.Payload, &sealed); err != nil {
		t.Fatalf("unmarshal sealed payload: %v", err)
	}
	tampered := []byte(sealed["email"])
	tampered[len(tampered)-2] ^= 0xFF // corrupt a byte inside the base64 ciphertext
	sealed["email"] = string(tampered)
	tamperedPayload, err := json.Marshal(sealed)
	if err != nil {
		t.Fatalf("marshal tampered payload: %v", err)
	}
	event.Payload = tamperedPayload

	if _, err := anonymizer.Open(ctx, event); err == nil {
		t.Fatal("expected Open to error on tampered ciphertext (GCM auth failure), got nil")
	}
}

// TestPiiAnonymizer_Open_UnknownManifestShapeIsTolerant covers Open's
// tolerant fallback when event.PiiFields isn't the manifest shape Seal
// writes (e.g. hand-constructed test fixtures, or data written by a future/
// incompatible version) — Open must return the event unmodified rather than
// error, since the field isn't something it knows how to interpret as
// ciphertext.
func TestPiiAnonymizer_Open_UnknownManifestShapeIsTolerant(t *testing.T) {
	ctx := context.Background()
	anonymizer := es.NewPiiAnonymizer(memstore.NewKeyRing())

	piiID := uuid.New()
	original := mustJSON(t, PersonRegistered{Email: "ann@example.com", Name: "Ann"})
	event := &es.DomainEvent{
		Name:      "PersonRegistered",
		PiiID:     &piiID,
		Payload:   original,
		PiiFields: json.RawMessage(`not-a-manifest`),
	}

	opened, err := anonymizer.Open(ctx, event)
	if err != nil {
		t.Fatalf("expected Open to tolerate an unrecognized PiiFields shape, got err: %v", err)
	}
	if string(opened.Payload) != string(original) {
		t.Fatalf("expected payload left untouched, got %s", opened.Payload)
	}
	if opened.PiiUnrecoverable {
		t.Fatal("expected PiiUnrecoverable to stay false for an unrecognized manifest, not a forgotten key")
	}
}

// TestPiiAnonymizer_Seal_NoOpCases exercises every documented no-op path of
// Seal: no PiiID, nil src, and a src that declares no "payload" fields —
// each must leave the event's Payload/PiiFields untouched rather than error
// or silently seal something that wasn't opted in.
func TestPiiAnonymizer_Seal_NoOpCases(t *testing.T) {
	ctx := context.Background()
	anonymizer := es.NewPiiAnonymizer(memstore.NewKeyRing())
	original := mustJSON(t, PersonRegistered{Email: "ann@example.com", Name: "Ann"})

	t.Run("no PiiID", func(t *testing.T) {
		event := &es.DomainEvent{Name: "PersonRegistered", Payload: append([]byte(nil), original...)}
		if err := anonymizer.Seal(ctx, event, PersonRegistered{}); err != nil {
			t.Fatalf("Seal: %v", err)
		}
		if string(event.Payload) != string(original) {
			t.Fatalf("expected untouched payload without a PiiID, got %s", event.Payload)
		}
		if len(event.PiiFields) != 0 {
			t.Fatalf("expected no manifest written without a PiiID, got %s", event.PiiFields)
		}
	})

	t.Run("nil src", func(t *testing.T) {
		piiID := uuid.New()
		event := &es.DomainEvent{Name: "PersonRegistered", PiiID: &piiID, Payload: append([]byte(nil), original...)}
		if err := anonymizer.Seal(ctx, event, nil); err != nil {
			t.Fatalf("Seal: %v", err)
		}
		if string(event.Payload) != string(original) {
			t.Fatalf("expected untouched payload for nil src, got %s", event.Payload)
		}
	})

	t.Run("src declares no payload fields", func(t *testing.T) {
		piiID := uuid.New()
		event := &es.DomainEvent{Name: "PersonRegistered", PiiID: &piiID, Payload: append([]byte(nil), original...)}
		if err := anonymizer.Seal(ctx, event, noPiiFieldsPayload{}); err != nil {
			t.Fatalf("Seal: %v", err)
		}
		if string(event.Payload) != string(original) {
			t.Fatalf("expected untouched payload when no fields are declared, got %s", event.Payload)
		}
		if len(event.PiiFields) != 0 {
			t.Fatalf("expected no manifest written when no fields are declared, got %s", event.PiiFields)
		}
	})
}

type noPiiFieldsPayload struct{}

func (noPiiFieldsPayload) EventName() string              { return "PersonRegistered" }
func (noPiiFieldsPayload) PiiFields() map[string][]string { return nil }

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}
