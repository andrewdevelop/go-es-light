package es

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ShredRecord is one crypto-shredding in the erasure ledger: the subject
// whose key was destroyed, when, which committed event triggered it, and the
// hash chain link that makes the whole ledger tamper-evident.
//
// ErasedAt is second-precision and UTC on purpose — the hash is computed
// over the stored byte representation, so the exact same hash must be
// recomputable after a round-trip through any durable store (a Postgres
// timestamptz column, a serialized backup, ...) without the nanoseconds a
// time.Time carries being silently dropped by the round-trip.
type ShredRecord struct {
	PiiID          uuid.UUID `json:"pii_id"`
	ErasedAt       time.Time `json:"erased_at"`
	OriginGlobalID uint64    `json:"origin_global_id"`
	PrevHash       [32]byte  `json:"prev_hash"`
	Hash           [32]byte  `json:"hash"`
}

// shardHashInput is the fixed-width, order-sensitive byte layout
// ShredRecordHash hashes: prev_link || pii_id || erased_at_nano || origin.
// Fixed width and big-endian so two records differing only in a leading
// field (e.g. pii_ids 0x01... and 0x0100...) can never collide in the
// concatenation.
const (
	shredHashPrevBytes   = 32
	shredHashPiiBytes    = 16
	shredHashTimeBytes   = 8
	shredHashOriginBytes = 8
	shredHashInputSize   = shredHashPrevBytes + shredHashPiiBytes + shredHashTimeBytes + shredHashOriginBytes
)

// ShredRecordHash computes the SHA-256 link of one erasure record in the
// chain: H(prev_link || pii_id || erased_at_sec || origin_global_id). prev
// is the hash of the record that immediately preceded this one in the
// ledger (all zeros for the very first record). erasedAt is truncated to
// whole seconds first, matching what an ErasureLedger.Append stores, so the
// hash reproduces after any storage round-trip.
func ShredRecordHash(prev [32]byte, piiID uuid.UUID, erasedAt time.Time, originGlobalID uint64) [32]byte {
	buf := make([]byte, shredHashInputSize)
	copy(buf[:shredHashPrevBytes], prev[:])
	copy(buf[shredHashPrevBytes:shredHashPrevBytes+shredHashPiiBytes], piiID[:])
	binary.BigEndian.PutUint64(buf[shredHashPrevBytes+shredHashPiiBytes:], uint64(erasedAt.Truncate(time.Second).UTC().Unix()))
	binary.BigEndian.PutUint64(buf[shredHashPrevBytes+shredHashPiiBytes+shredHashTimeBytes:], originGlobalID)
	return sha256.Sum256(buf)
}

// CheckShredChain verifies a whole ledger's hash chain, in the order the
// records were appended. Each record's PrevHash must equal its
// predecessor's Hash (or the zero hash for the first record), and its own
// Hash must equal ShredRecordHash over it. Returns nil only if every link
// holds; otherwise an error naming the first broken index. An empty or
// single-record chain is valid.
func CheckShredChain(records []ShredRecord) error {
	var prev [32]byte
	for i, r := range records {
		if r.PrevHash != prev {
			return fmt.Errorf("es: pii: shred chain breaks at record %d: PrevHash mismatch", i)
		}
		want := ShredRecordHash(prev, r.PiiID, r.ErasedAt, r.OriginGlobalID)
		if r.Hash != want {
			return fmt.Errorf("es: pii: shred chain breaks at record %d: Hash mismatch", i)
		}
		prev = r.Hash
	}
	return nil
}

// ErrNoErasureRecord is returned by ErasureVerifier.Verify when the ledger
// holds no record for the given PiiID — the audit-triple equivalent of
// ErrSubjectNotFound / ErrAggregateNotFound: "this subject was never
// crypto-shredded here".
var ErrNoErasureRecord = errors.New("es: pii: no erasure record for the given PiiID")

// ErasureLedger is the durable, append-only record of every crypto-shred
// (keys destroyed via KeyRing.Forget). It exists to make deletion *provable*:
// the event log itself is append-only by design and can never be rewritten,
// so the ledger answers the audit question "was this subject erased, and
// when?" with a record that is itself tamper-evident — each entry links to
// the previous one via ShredRecordHash, and CheckShredChain recomputes the
// whole chain on demand.
//
// Where a KeyRing stores *what* protects the data (and says nothing about
// erasure history), the ledger stores the *fact* of erasure. Like KeyRing it
// lives physically outside the events table, and — unlike events — entries
// are only ever appended, never updated or deleted: that append-only
// property is exactly what makes them usable as evidence. Implementations:
// memstore.ErasureLedger (tests/examples) and pgstore.ErasureLedger
// (Postgres-backed).
type ErasureLedger interface {
	// Append records a crypto-shredding: k's key was forgotten at erasedAt,
	// in response to the committed event with global id originGlobalID
	// (pass e.GlobalID after Commit; 0 if unknown). The implementation
	// truncates erasedAt to whole seconds, links the new record to its
	// ledger tail, and persists both PrevHash and Hash. Appending is
	// idempotent-agnostic by design — the record is evidence, and callers
	// must only Append after a successful Forget.
	Append(ctx context.Context, piiID uuid.UUID, erasedAt time.Time, originGlobalID uint64) error
	// Entries returns every record in append order (oldest first) — the
	// input CheckShredChain expects.
	Entries(ctx context.Context) ([]ShredRecord, error)
}

// ErasureProof is a completed, evidence-backed answer to "is this subject's
// data provably gone?" See ErasureVerifier.Verify.
type ErasureProof struct {
	// Subject is the PiiID that was verified.
	Subject uuid.UUID
	// Erasures holds this subject's records, chain-validated with every
	// other record in the ledger.
	Erasures []ShredRecord
	// KeyForgotten is true if the keyring no longer holds a key for the
	// subject — so the wrapped/plaintext bytes are being destroyed, not
	// merely revoked.
	KeyForgotten bool
	// EventsUnrecoverable is true if every stored event of the subject that
	// declared PII fields comes back flagged PiiUnrecoverable (or the
	// subject has no events at all) — the ciphertext on disk is genuinely
	// unserializable.
	EventsUnrecoverable bool
}

// ErasureVerifier produces ErasureProof for a single subject, tying together
// the three sources of truth deletion touches — the events (Events), the
// keys (Keys), and the record of having shredded them (Ledger):
//
//   - every stored event of the subject that declared PII fields reads back
//     as PiiUnrecoverable (its key is gone, the ciphertext cannot be opened);
//   - the keyring no longer holds a key for the subject;
//   - the ledger contains a record for the subject, and the whole chain is
//     unbroken (nothing was doctored after the fact).
//
// It is the demonstrable-erasure hook a controller shows its regulator
// instead of asserting "we deleted it, trust us": each condition is
// independently presented, and an auditor can cross-check the ledger's
// chain against any export of it.
type ErasureVerifier struct {
	// Events must be an es.PiiEventStore-wrapped PiiLookup (a subject
	// file needs decryption awareness to know whether fields are plaintext
	// or unrecoverable ciphertext).
	Events PiiLookup
	// Keys is the same KeyRing the anonymizer was built on.
	Keys KeyRing
	// Ledger is the erasure ledger the shreds were appended to.
	Ledger ErasureLedger
}

// NewErasureVerifier returns an ErasureVerifier over the subject-data
// sources described on the type.
func NewErasureVerifier(events PiiLookup, keys KeyRing, ledger ErasureLedger) *ErasureVerifier {
	return &ErasureVerifier{Events: events, Keys: keys, Ledger: ledger}
}

// Verify produces the subject's ErasureProof, or an error describing the
// first unmet condition. The ledger is read first: a subject with no
// record at all yields ErrNoErasureRecord regardless of the other two
// sources — "never shreded" is an answer, not an omission. Beyond that,
// every independent condition is checked and the first failure is wrapped
// with enough context to act on it; proof is still returned and populated
// as far as the checks got, for a response that needs partial state anyway.
func (v *ErasureVerifier) Verify(ctx context.Context, piiID uuid.UUID) (*ErasureProof, error) {
	proof := &ErasureProof{Subject: piiID}

	all, err := v.Ledger.Entries(ctx)
	if err != nil {
		return nil, fmt.Errorf("es: pii: read erasure ledger: %w", err)
	}
	if err := CheckShredChain(all); err != nil {
		return nil, err
	}
	for _, r := range all {
		if r.PiiID == piiID {
			proof.Erasures = append(proof.Erasures, r)
		}
	}
	if len(proof.Erasures) == 0 {
		return proof, ErrNoErasureRecord
	}

	key, err := v.Keys.Get(ctx, piiID)
	if err != nil {
		return nil, fmt.Errorf("es: pii: read key for %s: %w", piiID, err)
	}
	proof.KeyForgotten = key == nil

	events, err := v.Events.FetchByPiiID(ctx, piiID)
	if err != nil {
		if !errors.Is(err, ErrSubjectNotFound) {
			return nil, fmt.Errorf("es: pii: read subject events: %w", err)
		}
		// No events left for the subject at all — vacuously unrecoverable.
		proof.EventsUnrecoverable = true
	} else {
		proof.EventsUnrecoverable = true
		for _, e := range events {
			if len(e.PiiFields) > 0 && !e.PiiUnrecoverable {
				proof.EventsUnrecoverable = false
				break
			}
		}
	}

	if !proof.KeyForgotten {
		return proof, fmt.Errorf("es: pii: verification of %s: key still exists in the keyring", piiID)
	}
	if !proof.EventsUnrecoverable {
		return proof, fmt.Errorf("es: pii: verification of %s: some PII-bearing events still decrypt", piiID)
	}
	return proof, nil
}
