package memstore

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"go-es-light/es"
)

// ErasureLedger is a concurrency-safe, in-memory es.ErasureLedger. Like
// Store and KeyRing, it means tests and examples — entries survive only for
// the lifetime of the process. For production use pgstore.ErasureLedger (or
// an equivalent durable, append-only store) behind the same interface; the
// chain verification (es.CheckShredChain) is storage-agnostic either way.
type ErasureLedger struct {
	mu      sync.Mutex
	records []es.ShredRecord
}

// NewErasureLedger returns an empty ErasureLedger.
func NewErasureLedger() *ErasureLedger {
	return &ErasureLedger{}
}

// Append implements es.ErasureLedger, linking the new record to the current
// tail via es.ShredRecordHash.
func (l *ErasureLedger) Append(_ context.Context, piiID uuid.UUID, erasedAt time.Time, originGlobalID uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var prev [32]byte
	if n := len(l.records); n > 0 {
		prev = l.records[n-1].Hash
	}
	record := es.ShredRecord{
		PiiID:          piiID,
		ErasedAt:       erasedAt,
		OriginGlobalID: originGlobalID,
		PrevHash:       prev,
	}
	record.Hash = es.ShredRecordHash(prev, piiID, erasedAt, originGlobalID)
	l.records = append(l.records, record)
	return nil
}

// Entries implements es.ErasureLedger, returning every record in append
// order under a copy so callers can't mutate the stored chain.
func (l *ErasureLedger) Entries(_ context.Context) ([]es.ShredRecord, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]es.ShredRecord, len(l.records))
	copy(out, l.records)
	return out, nil
}

var _ es.ErasureLedger = (*ErasureLedger)(nil)
