package memstore

import (
	"context"
	"sync"

	"github.com/google/uuid"

	"go-es-light/es"
)

// KeyRing is a concurrency-safe, in-memory es.KeyRing. Like Store, it is
// meant for tests and examples — a forgotten key is only unrecoverable for
// the lifetime of the process, and nothing survives a restart. For
// production use a durable, physically-deletable store (e.g. pgstore.KeyRing,
// or a dedicated secrets manager) behind the same interface.
type KeyRing struct {
	mu   sync.Mutex
	keys map[uuid.UUID][]byte
}

// NewKeyRing returns an empty KeyRing.
func NewKeyRing() *KeyRing {
	return &KeyRing{keys: make(map[uuid.UUID][]byte)}
}

func (r *KeyRing) Get(_ context.Context, piiID uuid.UUID) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.keys[piiID], nil
}

func (r *KeyRing) Put(_ context.Context, piiID uuid.UUID, key []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys[piiID] = key
	return nil
}

func (r *KeyRing) Forget(_ context.Context, piiID uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.keys, piiID)
	return nil
}

// Subjects implements es.KeyRingLister, so a WrappedKeyRing over this
// KeyRing can rotate. Order is unspecified.
func (r *KeyRing) Subjects(_ context.Context) ([]uuid.UUID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]uuid.UUID, 0, len(r.keys))
	for piiID := range r.keys {
		out = append(out, piiID)
	}
	return out, nil
}

var _ es.KeyRing = (*KeyRing)(nil)
var _ es.KeyRingLister = (*KeyRing)(nil)
