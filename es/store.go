package es

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

// ErrConcurrencyConflict is returned by EventStore.Commit when the expected
// version does not match the version currently stored for the aggregate —
// someone else committed events for the same aggregate first.
var ErrConcurrencyConflict = errors.New("event store concurrency conflict")

// ErrAggregateNotFound is returned by EventStore.Load (and by Repository.Load)
// when no events exist for the given aggregate id.
var ErrAggregateNotFound = errors.New("aggregate not found")

// Applier is the interface an aggregate must implement to accept events,
// both when recording new ones and when replaying history.
type Applier interface {
	Apply(event any) error
	GetID() uuid.UUID
}

// Reconstitutable is the interface the Repository needs in order to load,
// replay history into, and persist an aggregate.
type Reconstitutable interface {
	Applier
	SetID(id uuid.UUID)
	SetVersion(v uint64)
	GetVersion() uint64
	GetUncommittedEvents() []*DomainEvent // <-- Pointer
	ClearEvents()
}

// EventStore is the append-only log of domain events. Implementations must
// guarantee that Commit is atomic: either all events are persisted or none
// are, and that expectedVersion is enforced (optimistic concurrency).
type EventStore interface {
	// Commit appends events for a single aggregate. expectedVersion is the
	// aggregate version the caller believes is currently persisted (i.e. the
	// version before any of the events in this batch were applied). If the
	// store holds a different version, ErrConcurrencyConflict is returned
	// and nothing is persisted.
	Commit(ctx context.Context, events []*DomainEvent, expectedVersion uint64) error

	// Load returns all events for the given aggregate, ordered by
	// AggregateVersion ascending. Returns ErrAggregateNotFound if none exist.
	Load(ctx context.Context, id uuid.UUID) ([]*DomainEvent, error)

	// FetchAfter returns up to limit events with GlobalID > lastID, ordered
	// by GlobalID ascending. Used by projectors/pollers to catch up.
	FetchAfter(ctx context.Context, lastID uint64, limit int) ([]*DomainEvent, error)

	// StreamAll pushes every event as it is committed (and, depending on the
	// implementation, backfills history first) onto the returned channel.
	// The error channel receives at most one error before both channels are
	// closed. Callers must drain both channels and cancel ctx to stop.
	StreamAll(ctx context.Context) (<-chan *DomainEvent, <-chan error)
}

// Checkpointer stores the progress of a worker (projector, side-effect
// relay, ...) so it can resume from where it left off after a restart.
type Checkpointer interface {
	Get(ctx context.Context, name string) (uint64, error)
	Save(ctx context.Context, name string, lastID uint64) error
}
