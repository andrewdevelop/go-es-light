package pgstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"go-es-light/es"
)

// KeyRing is a Postgres-backed es.KeyRing, storing one row per data subject
// in a dedicated pii_keys table — deliberately separate from the events
// table (see es.KeyRing's doc comment: a key must be physically deletable,
// which the append-only event log itself must never be).
type KeyRing struct {
	db DB
}

// NewKeyRing wraps db. Call Migrate once before first use (see Migrate).
func NewKeyRing(db DB) *KeyRing {
	return &KeyRing{db: db}
}

// Migrate idempotently creates the pii_keys table. Safe to call on every
// startup, and separate from Store.Migrate since a deployment that doesn't
// use PII/erasure support has no reason to carry this table at all.
func (k *KeyRing) Migrate(ctx context.Context) error {
	_, err := k.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS pii_keys (
			pii_id UUID PRIMARY KEY,
			key    BYTEA NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("pgstore: migrate keyring: %w", err)
	}
	return nil
}

// Get implements es.KeyRing.
func (k *KeyRing) Get(ctx context.Context, piiID uuid.UUID) ([]byte, error) {
	var key []byte
	err := k.db.QueryRowContext(ctx, `SELECT key FROM pii_keys WHERE pii_id = $1`, piiID).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pgstore: keyring get: %w", err)
	}
	return key, nil
}

// Put implements es.KeyRing.
func (k *KeyRing) Put(ctx context.Context, piiID uuid.UUID, key []byte) error {
	_, err := k.db.ExecContext(ctx, `
		INSERT INTO pii_keys (pii_id, key) VALUES ($1, $2)
		ON CONFLICT (pii_id) DO UPDATE SET key = EXCLUDED.key
	`, piiID, key)
	if err != nil {
		return fmt.Errorf("pgstore: keyring put: %w", err)
	}
	return nil
}

// Forget implements es.KeyRing.
func (k *KeyRing) Forget(ctx context.Context, piiID uuid.UUID) error {
	_, err := k.db.ExecContext(ctx, `DELETE FROM pii_keys WHERE pii_id = $1`, piiID)
	if err != nil {
		return fmt.Errorf("pgstore: keyring forget: %w", err)
	}
	return nil
}

// Subjects implements es.KeyRingLister, so a WrappedKeyRing over this
// KeyRing can rotate its KEK (re-wrap every stored DEK). Order is
// unspecified.
func (k *KeyRing) Subjects(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := k.db.QueryContext(ctx, `SELECT pii_id FROM pii_keys`)
	if err != nil {
		return nil, fmt.Errorf("pgstore: keyring list subjects: %w", err)
	}
	defer rows.Close()

	var out []uuid.UUID
	for rows.Next() {
		var piiID uuid.UUID
		if err := rows.Scan(&piiID); err != nil {
			return nil, fmt.Errorf("pgstore: keyring scan subject: %w", err)
		}
		out = append(out, piiID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgstore: keyring list subjects: %w", err)
	}
	return out, nil
}

var _ es.KeyRing = (*KeyRing)(nil)
var _ es.KeyRingLister = (*KeyRing)(nil)
