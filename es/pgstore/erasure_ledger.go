package pgstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"go-es-light/es"
)

// ErasureLedger is a Postgres-backed es.ErasureLedger, storing one
// append-only row per crypto-shredding in a dedicated pii_shreds table —
// deliberately separate from both the events table (which must never allow
// UPDATE/DELETE) and its own predecessor rows (the hash chain is what makes
// the ledger tamper-evident). Unlike the append-only event log, the ledger's
// *addition* of rows is what matters, not their preservation from mutation.
//
// Because the keys themselves are wrapped by a WrappedKeyRing before they
// ever reach the keyring table, Postgres's WAL and any backup/dump of this
// database contain at most KEK-wrapped DEK blobs and these append-only
// erasure records — never a readable data-subject key after its shred.
type ErasureLedger struct {
	db DB
}

// NewErasureLedger wraps db. Call Migrate once before first use.
func NewErasureLedger(db DB) *ErasureLedger {
	return &ErasureLedger{db: db}
}

// Migrate idempotently creates the pii_shreds table. Safe to call on every
// startup; separate from Store.Migrate and KeyRing.Migrate since a
// deployment that doesn't opt into provable erasure has no reason to carry
// the table at all.
func (l *ErasureLedger) Migrate(ctx context.Context) error {
	_, err := l.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS pii_shreds (
			id                BIGSERIAL PRIMARY KEY,
			pii_id            UUID NOT NULL,
			erased_at         TIMESTAMPTZ NOT NULL,
			origin_global_id  BIGINT NOT NULL DEFAULT 0,
			prev_hash         BYTEA NOT NULL,
			hash              BYTEA NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("pgstore: migrate erasure ledger: %w", err)
	}
	return nil
}

// Append implements es.ErasureLedger. Concurrency is serialized with a
// transaction-scoped advisory lock keyed by the table itself, so two
// concurrent shreds can never both read the same tail and fork the chain —
// the equivalent of Store.Commit's per-aggregate lock, for the ledger.
func (l *ErasureLedger) Append(ctx context.Context, piiID uuid.UUID, erasedAt time.Time, originGlobalID uint64) (err error) {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("pgstore: erasure ledger: begin tx: %w", err)
	}
	defer func() {
		rbErr := tx.Rollback()
		if rbErr == nil || errors.Is(rbErr, sql.ErrTxDone) {
			return
		}
		if err != nil {
			err = fmt.Errorf("%w (additionally, rollback failed: %v)", err, rbErr)
		} else {
			err = fmt.Errorf("pgstore: erasure ledger: rollback: %w", rbErr)
		}
	}()

	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext('pii_shreds'))`); err != nil {
		return fmt.Errorf("pgstore: erasure ledger: lock: %w", err)
	}

	var prev []byte
	if err := tx.QueryRowContext(ctx, `SELECT hash FROM pii_shreds ORDER BY id DESC LIMIT 1`).Scan(&prev); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("pgstore: erasure ledger: read tail: %w", err)
		}
	}

	var prevHash [32]byte
	copy(prevHash[:], prev)
	hash := es.ShredRecordHash(prevHash, piiID, erasedAt, originGlobalID)

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO pii_shreds (pii_id, erased_at, origin_global_id, prev_hash, hash)
		VALUES ($1, $2, $3, $4, $5)
	`, piiID, erasedAt, originGlobalID, prevHash[:], hash[:]); err != nil {
		return fmt.Errorf("pgstore: erasure ledger: append: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("pgstore: erasure ledger: commit: %w", err)
	}
	return nil
}

// Entries implements es.ErasureLedger, returning every record in append
// order — the exact input CheckShredChain expects.
func (l *ErasureLedger) Entries(ctx context.Context) ([]es.ShredRecord, error) {
	rows, err := l.db.QueryContext(ctx, `
		SELECT pii_id, erased_at, origin_global_id, prev_hash, hash
		FROM pii_shreds
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("pgstore: erasure ledger: list: %w", err)
	}
	defer rows.Close()

	var out []es.ShredRecord
	for rows.Next() {
		var (
			r          es.ShredRecord
			prev, hash []byte
			erasedAt   time.Time
		)
		if err := rows.Scan(&r.PiiID, &erasedAt, &r.OriginGlobalID, &prev, &hash); err != nil {
			return nil, fmt.Errorf("pgstore: erasure ledger: scan: %w", err)
		}
		r.ErasedAt = erasedAt
		copy(r.PrevHash[:], prev)
		copy(r.Hash[:], hash)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgstore: erasure ledger: list: %w", err)
	}
	return out, nil
}

var _ es.ErasureLedger = (*ErasureLedger)(nil)
