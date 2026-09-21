package es

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// ContainsPersonalData is implemented by event payload types that carry
// personal data, declaring which fields must be encrypted at rest.
// PiiFields is keyed by bucket — today only the "payload" bucket is
// understood by PiiAnonymizer, naming top-level keys of the event's JSON
// payload. Example: map[string][]string{"payload": {"email", "full_name"}}.
//
// Entirely optional: a payload type that doesn't implement this interface
// is simply never sealed, regardless of whether the event carries a PiiID.
type ContainsPersonalData interface {
	PiiFields() map[string][]string
}

// RequestsErasure is a marker interface for event payload types that
// represent a data subject asking to be forgotten. The event's PiiID (set
// via WithPiiID when it was recorded) identifies which subject. A
// PiiEventStore crypto-shreds that subject's key automatically after such
// an event commits successfully — see PiiEventStore.Commit.
//
// RequestsErasure() takes no arguments and returns nothing; it exists only
// so the interface has a method (an empty interface would be satisfied by
// every type, defeating the marker).
type RequestsErasure interface {
	RequestsErasure()
}

// ErrPiiUnrecoverable is the error-flavored equivalent of
// DomainEvent.PiiUnrecoverable — see CheckPiiOpen. Not returned by
// PiiAnonymizer.Open itself (which stays tolerant on purpose, see its doc
// comment); it exists for a consumer that would rather branch on
// errors.Is than check a bool, e.g. inside a validation pipeline that
// already threads errors through.
var ErrPiiUnrecoverable = errors.New("es: pii: subject's key has been forgotten; declared PII fields are unrecoverable ciphertext")

// CheckPiiOpen returns ErrPiiUnrecoverable if e.PiiUnrecoverable is true,
// nil otherwise. A convenience for a consumer about to use one of e's
// declared PII fields for something format-sensitive (writing it into a
// DB column with a CHECK constraint, validating it as an email/phone
// number, ...) that prefers an error-handling idiom over checking the bool
// field directly.
func (e DomainEvent) CheckPiiOpen() error {
	if e.PiiUnrecoverable {
		return ErrPiiUnrecoverable
	}
	return nil
}

// KeyRing stores per-data-subject encryption keys for crypto-shredding.
// Implementations must be durable and the key physically deletable —
// KeyRing is deliberately not part of EventStore or the events table,
// precisely because a key must be destroyable on request while the
// (now-unreadable) ciphertext it protected stays in the append-only log.
// This package defines only the contract; see memstore.KeyRing (in-memory,
// tests/examples) and pgstore.KeyRing (Postgres-backed) for
// implementations, mirroring how EventStore itself works.
type KeyRing interface {
	// Get returns the key for piiID, or (nil, nil) if none was ever issued
	// or it has since been forgotten.
	Get(ctx context.Context, piiID uuid.UUID) ([]byte, error)
	Put(ctx context.Context, piiID uuid.UUID, key []byte) error
	// Forget irreversibly destroys the key for piiID. No counter-action
	// exists on purpose.
	Forget(ctx context.Context, piiID uuid.UUID) error
}

// PiiAnonymizer encrypts and decrypts the PII fields of events at rest,
// using AES-256-GCM keyed per data subject (DomainEvent.PiiID) via Keys.
// Normally used indirectly, via PiiEventStore — wrapping an EventStore with
// one is what actually opts a store into this behavior; Seal/Open/Forget
// below are exported mainly so PiiEventStore (or a test) can call them.
type PiiAnonymizer struct {
	Keys KeyRing
}

// NewPiiAnonymizer returns a PiiAnonymizer backed by keys.
func NewPiiAnonymizer(keys KeyRing) *PiiAnonymizer {
	return &PiiAnonymizer{Keys: keys}
}

// Seal encrypts event's declared PII fields in place before it's committed,
// issuing a key for its PiiID via Keys if none exists yet, and writes a
// field manifest into event.PiiFields (a column of its own, separate from
// event.Metadata) so Open can later decrypt without needing src again.
// No-op if event has no PiiID, or src is nil, or src declares no "payload"
// fields.
func (a *PiiAnonymizer) Seal(ctx context.Context, event *DomainEvent, src ContainsPersonalData) error {
	if event.PiiID == nil || src == nil {
		return nil
	}
	fields := src.PiiFields()["payload"]
	if len(fields) == 0 {
		return nil
	}

	key, err := a.Keys.Get(ctx, *event.PiiID)
	if err != nil {
		return fmt.Errorf("es: pii: get key for %s: %w", *event.PiiID, err)
	}
	if key == nil {
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return fmt.Errorf("es: pii: generate key: %w", err)
		}
		if err := a.Keys.Put(ctx, *event.PiiID, key); err != nil {
			return fmt.Errorf("es: pii: store key for %s: %w", *event.PiiID, err)
		}
	}

	payload := map[string]json.RawMessage{}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("es: pii: unmarshal payload: %w", err)
	}

	for _, field := range fields {
		raw, ok := payload[field]
		if !ok {
			continue
		}
		ciphertext, err := piiEncrypt(key, raw)
		if err != nil {
			return fmt.Errorf("es: pii: encrypt field %q: %w", field, err)
		}
		enc, err := json.Marshal(ciphertext)
		if err != nil {
			return fmt.Errorf("es: pii: marshal encrypted field %q: %w", field, err)
		}
		payload[field] = enc
	}

	sealedPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("es: pii: marshal payload: %w", err)
	}
	event.Payload = sealedPayload

	manifest, err := json.Marshal(map[string][]string{"payload": fields})
	if err != nil {
		return fmt.Errorf("es: pii: marshal field manifest: %w", err)
	}
	event.PiiFields = manifest

	return nil
}

// Open decrypts event's PII fields for an authorized read, using the
// manifest Seal wrote into event.PiiFields (so the concrete payload type
// isn't needed). Returns a copy of event; the original is never mutated.
//
// Tolerant: if event has no PiiID, or no manifest, or the subject's key has
// already been forgotten, Open returns the event as-is (fields still
// ciphertext where applicable) with no error — replaying an aggregate must
// keep working for every OTHER field/event even after one subject has been
// crypto-shredded. An error is only returned for actual decryption failure
// (corrupt/tampered ciphertext), which is not the tolerant case.
//
// The forgotten-key case specifically also sets the returned event's
// PiiUnrecoverable to true — read that field's doc comment (or use
// ErrPiiUnrecoverable/DomainEvent.CheckPiiOpen) before feeding a declared
// PII field to anything format-sensitive: post-erasure, it's ciphertext,
// not a value in whatever shape it originally was (an email, a phone
// number, ...), and things like a DB column's CHECK constraint or an
// email/phone validator will reasonably reject it — or, worse, silently
// accept and store garbage if nothing rejects it.
func (a *PiiAnonymizer) Open(ctx context.Context, event *DomainEvent) (*DomainEvent, error) {
	out := *event // shallow copy; Payload is replaced wholesale below, not mutated in place

	if event.PiiID == nil || len(event.PiiFields) == 0 {
		return &out, nil
	}

	var manifest map[string][]string
	if err := json.Unmarshal(event.PiiFields, &manifest); err != nil {
		return &out, nil // not our manifest shape — leave untouched
	}
	fields := manifest["payload"]
	if len(fields) == 0 {
		return &out, nil
	}

	key, err := a.Keys.Get(ctx, *event.PiiID)
	if err != nil {
		return nil, fmt.Errorf("es: pii: get key for %s: %w", *event.PiiID, err)
	}
	if key == nil {
		out.PiiUnrecoverable = true
		return &out, nil // forgotten (or never issued) — tolerant no-op
	}

	payload := map[string]json.RawMessage{}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return nil, fmt.Errorf("es: pii: unmarshal payload: %w", err)
	}

	for _, field := range fields {
		raw, ok := payload[field]
		if !ok {
			continue
		}
		var ciphertext string
		if err := json.Unmarshal(raw, &ciphertext); err != nil {
			continue // not our encrypted-field shape — leave as-is
		}
		plaintext, err := piiDecrypt(key, ciphertext)
		if err != nil {
			return nil, fmt.Errorf("es: pii: decrypt field %q: %w", field, err)
		}
		payload[field] = plaintext
	}

	openedPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("es: pii: marshal payload: %w", err)
	}
	out.Payload = openedPayload

	return &out, nil
}

// Forget irreversibly destroys piiID's key via Keys. No counter-action
// exists on purpose.
func (a *PiiAnonymizer) Forget(ctx context.Context, piiID uuid.UUID) error {
	return a.Keys.Forget(ctx, piiID)
}

func piiEncrypt(key, plaintext []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

func piiDecrypt(key []byte, encoded string) (json.RawMessage, error) {
	blob, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(blob) < gcm.NonceSize() {
		return nil, errors.New("es: pii: ciphertext too short")
	}
	nonce, ciphertext := blob[:gcm.NonceSize()], blob[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(plaintext), nil
}
