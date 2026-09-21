package es

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// KeyWrapper seals and opens a data-subject key (a DEK) so the bytes a
// KeyRing persists are never the plaintext key itself. The wrapping key (a
// KEK) that powers a KeyWrapper lives outside the KeyRing — a cloud KMS, an
// HSM, an env var — which is the whole defense: a leaked keyring table, a
// stolen backup, a WAL archive, all yield only ciphertext worth nothing
// without the KEK. See WrappedKeyRing.
//
// Like KeyRing, this stays a small contract and the implementations live
// elsewhere; AesGcmWrapper is the stdlib-only, no-dependency one for
// deployments that want the wrapper defense without standing up a KMS.
type KeyWrapper interface {
	// Wrap seals key, returning the bytes a KeyRing should store. The result
	// is unreadable (and so effectively destroyable with the row) without
	// the KEK behind this wrapper.
	Wrap(ctx context.Context, key []byte) ([]byte, error)
	// Unwrap reverses Wrap. Corrupt/tampered/wrong-wrapper ciphertext is an
	// error, never a silent nil — that's how a mismatched or rotated-away
	// KEK surfaces instead of quietly degrading keys.
	Unwrap(ctx context.Context, wrapped []byte) ([]byte, error)
	// Label identifies which KEK produced the wrapping — a KMS key id, a
	// version tag ("kek:v1"), a fingerprint. WrappedKeyRing stores it beside
	// each wrapped blob and refuses to unwrap a blob whose label doesn't
	// match the current wrapper, which is exactly what makes Rotate honest.
	Label() string
}

// KeyRingLister is an optional capability of a KeyRing: enumerating every
// subject it holds a key for. WrappedKeyRing.Rotate needs it to re-wrap
// every stored DEK; it's deliberately not part of the KeyRing contract
// itself (a Get/Put/Forget-only implementation can still be perfectly
// good for everything except rotation).
type KeyRingLister interface {
	Subjects(ctx context.Context) ([]uuid.UUID, error)
}

// ErrNoKeyLister is returned by WrappedKeyRing.Rotate when the underlying
// KeyRing can't enumerate its subjects.
var ErrNoKeyLister = errors.New("es: wrapped keyring: underlying KeyRing does not implement KeyRingLister")

// WrappedKeyRing is an es.KeyRing decorator that makes the data it stores
// useless without a separately-held KEK: every key handed to it is wrapped
// via Wrapper before reaching the underlying KeyRing, and unwrapped again on
// Get. Forget still destroys the (now ciphertext-only) row, so crypto-
// shredding keeps working — the keyring table never held a key in a form a
// database dump could ever use.
//
// This also meaningfully weakens the "keys live in the same Postgres as the
// events" criticism of pgstore.KeyRing: an attacker who steals the database
// gets wrapped blobs, and the KEK — sitting in KMS/Vault/env — is a second,
// separate compromise. Rotate lets that KEK change without re-recording a
// single event.
type WrappedKeyRing struct {
	Keys    KeyRing
	Wrapper KeyWrapper
}

// NewWrappedKeyRing returns a WrappedKeyRing that stores its keys via keys,
// wrapped by wrapper. Use it wherever a KeyRing is expected, e.g.
// es.NewPiiAnonymizer(es.NewWrappedKeyRing(pgKeyRing, kmsWrapper)).
func NewWrappedKeyRing(keys KeyRing, wrapper KeyWrapper) *WrappedKeyRing {
	return &WrappedKeyRing{Keys: keys, Wrapper: wrapper}
}

// Get implements es.KeyRing. Returns (nil, nil) if no key exists for piiID
// or it has been forgotten — same tolerant contract as the underlying ring.
func (r *WrappedKeyRing) Get(ctx context.Context, piiID uuid.UUID) ([]byte, error) {
	wrapped, err := r.Keys.Get(ctx, piiID)
	if err != nil {
		return nil, fmt.Errorf("es: wrapped keyring: get wrapped key for %s: %w", piiID, err)
	}
	if wrapped == nil {
		return nil, nil
	}

	label, blob, err := decodeWrappedBlob(wrapped)
	if err != nil {
		return nil, fmt.Errorf("es: wrapped keyring: decode stored blob for %s: %w", piiID, err)
	}
	if label != r.Wrapper.Label() {
		return nil, fmt.Errorf("es: wrapped keyring: stored key for %s was wrapped with %q but the current wrapper is %q (rotate first)", piiID, label, r.Wrapper.Label())
	}

	key, err := r.Wrapper.Unwrap(ctx, blob)
	if err != nil {
		return nil, fmt.Errorf("es: wrapped keyring: unwrap key for %s: %w", piiID, err)
	}
	return key, nil
}

// Put implements es.KeyRing, wrapping key before it reaches the underlying
// ring.
func (r *WrappedKeyRing) Put(ctx context.Context, piiID uuid.UUID, key []byte) error {
	wrapped, err := r.Wrapper.Wrap(ctx, key)
	if err != nil {
		return fmt.Errorf("es: wrapped keyring: wrap key for %s: %w", piiID, err)
	}
	if err := r.Keys.Put(ctx, piiID, encodeWrappedBlob(r.Wrapper.Label(), wrapped)); err != nil {
		return fmt.Errorf("es: wrapped keyring: store wrapped key for %s: %w", piiID, err)
	}
	return nil
}

// Forget implements es.KeyRing. The row that disappears held nothing but
// KEK-wrapped ciphertext, so Forget really is (for the data in the log)
// destruction, not just capability revocation layered on plaintext.
func (r *WrappedKeyRing) Forget(ctx context.Context, piiID uuid.UUID) error {
	if err := r.Keys.Forget(ctx, piiID); err != nil {
		return fmt.Errorf("es: wrapped keyring: forget key for %s: %w", piiID, err)
	}
	return nil
}

// Rotate re-wraps every surviving data-subject key with newWrapper and
// switches the ring to it from then on — no event ever needs re-recording,
// only the DEKs' wrappings change. Requires the underlying KeyRing to
// implement KeyRingLister. A subject already forgotten (crypto-shredded)
// before the rotation has no key to re-wrap and stays gone.
//
// Best-effort by nature: if a re-wrap fails partway (a subject is listed but
// its key is corrupt, say), Rotate returns the error with r.Wrapper left
// unchanged — the keys already re-wrapped are readable with both the old
// and the new wrapper's labelling only if the caller re-runs Rotate, so on
// error, re-run with the same newWrapper to converge; there's no way to be
// transactionally atomic through an interface that isn't.
func (r *WrappedKeyRing) Rotate(ctx context.Context, newWrapper KeyWrapper) error {
	lister, ok := r.Keys.(KeyRingLister)
	if !ok {
		return ErrNoKeyLister
	}

	subjects, err := lister.Subjects(ctx)
	if err != nil {
		return fmt.Errorf("es: wrapped keyring: list subjects: %w", err)
	}

	for _, piiID := range subjects {
		wrapped, err := r.Keys.Get(ctx, piiID)
		if err != nil {
			return fmt.Errorf("es: wrapped keyring: rotation: get wrapped key for %s: %w", piiID, err)
		}
		label, blob, err := decodeWrappedBlob(wrapped)
		if err != nil {
			return fmt.Errorf("es: wrapped keyring: rotation: decode stored blob for %s: %w", piiID, err)
		}
		if label != r.Wrapper.Label() {
			return fmt.Errorf("es: wrapped keyring: rotation: stored key for %s was wrapped with %q but the current wrapper is %q", piiID, label, r.Wrapper.Label())
		}

		key, err := r.Wrapper.Unwrap(ctx, blob)
		if err != nil {
			return fmt.Errorf("es: wrapped keyring: rotation: unwrap key for %s: %w", piiID, err)
		}
		reWrapped, err := newWrapper.Wrap(ctx, key)
		if err != nil {
			return fmt.Errorf("es: wrapped keyring: rotation: re-wrap key for %s: %w", piiID, err)
		}
		if err := r.Keys.Put(ctx, piiID, encodeWrappedBlob(newWrapper.Label(), reWrapped)); err != nil {
			return fmt.Errorf("es: wrapped keyring: rotation: store re-wrapped key for %s: %w", piiID, err)
		}
	}

	r.Wrapper = newWrapper
	return nil
}

var _ KeyRing = (*WrappedKeyRing)(nil)

// encodeWrappedBlob frames a wrapper label and its wrapped key material so
// the label survives storage in a plain []byte KeyRing slot: a 2-byte
// big-endian label length, then the label, then the wrapped blob.
func encodeWrappedBlob(label string, blob []byte) []byte {
	out := make([]byte, 2+len(label)+len(blob))
	binary.BigEndian.PutUint16(out, uint16(len(label)))
	copy(out[2:], label)
	copy(out[2+len(label):], blob)
	return out
}

// decodeWrappedBlob is encodeWrappedBlob's inverse.
func decodeWrappedBlob(b []byte) (label string, blob []byte, err error) {
	if len(b) < 2 {
		return "", nil, errors.New("es: wrapped keyring: blob too short")
	}
	n := int(binary.BigEndian.Uint16(b[:2]))
	if n > len(b)-2 {
		return "", nil, errors.New("es: wrapped keyring: corrupt label length")
	}
	return string(b[2 : 2+n]), b[2+n:], nil
}

// AesGcmWrapper is a stdlib-only es.KeyWrapper: data-subject keys are
// sealed with AES-256-GCM under masterKey. masterKey is the KEK; it must
// stay out of the KeyRing (env var, secret manager, ...) for the wrapping to
// be worth anything. label is a human/key-version tag stored with every
// wrapped blob; if empty, a fingerprint of masterKey is derived instead.
type AesGcmWrapper struct {
	label string
	gcm   cipher.AEAD
}

// NewAesGcmWrapper builds a KeyWrapper from a 16/24/32-byte masterKey.
func NewAesGcmWrapper(masterKey []byte, label string) (*AesGcmWrapper, error) {
	if len(masterKey) != 16 && len(masterKey) != 24 && len(masterKey) != 32 {
		return nil, fmt.Errorf("es: aes wrapper: master key must be 16, 24 or 32 bytes, got %d", len(masterKey))
	}
	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return nil, fmt.Errorf("es: aes wrapper: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("es: aes wrapper: %w", err)
	}
	if label == "" {
		sum := sha256.Sum256(masterKey)
		label = "aes:" + hex.EncodeToString(sum[:4])
	}
	return &AesGcmWrapper{label: label, gcm: gcm}, nil
}

// Wrap implements es.KeyWrapper. The label is authenticated as additional
// data, so a wrapped blob can't be swapped between wrappers.
func (w *AesGcmWrapper) Wrap(_ context.Context, key []byte) ([]byte, error) {
	nonce := make([]byte, w.gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("es: aes wrapper: nonce: %w", err)
	}
	return w.gcm.Seal(nonce, nonce, key, []byte(w.label)), nil
}

// Unwrap implements es.KeyWrapper.
func (w *AesGcmWrapper) Unwrap(_ context.Context, wrapped []byte) ([]byte, error) {
	if len(wrapped) < w.gcm.NonceSize() {
		return nil, errors.New("es: aes wrapper: wrapped blob too short")
	}
	nonce, ct := wrapped[:w.gcm.NonceSize()], wrapped[w.gcm.NonceSize():]
	key, err := w.gcm.Open(nil, nonce, ct, []byte(w.label))
	if err != nil {
		return nil, fmt.Errorf("es: aes wrapper: open: %w", err)
	}
	return key, nil
}

// Label implements es.KeyWrapper.
func (w *AesGcmWrapper) Label() string { return w.label }

var _ KeyWrapper = (*AesGcmWrapper)(nil)
