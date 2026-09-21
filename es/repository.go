package es

import (
	"context"

	"github.com/google/uuid"
)

// Repository loads and persists aggregates of type T, translating between
// the domain object (T) and the raw DomainEvent log kept in an EventStore.
// T is normally a pointer to a struct embedding BaseAggregate, e.g.
// *SubscriptionAggregate.
//
// Repository has no notion of PII sealing/opening — that's the
// responsibility of the EventStore it's given, not this translation layer.
// See PiiEventStore: wrap the underlying store with it once, at the
// composition root, and Repository.Load/Save (along with any other
// consumer of that same EventStore, e.g. a projector's StreamAll) get
// transparently encrypted-at-rest data with no code here needing to know.
type Repository[T Reconstitutable] struct {
	store        EventStore
	registry     EventRegistry
	newAggregate func() T
}

// NewRepository builds a Repository. newAggregate must return a fresh,
// zero-valued T (e.g. func() *Sub { return &Sub{} }) ready to have history
// replayed into it.
func NewRepository[T Reconstitutable](store EventStore, registry EventRegistry, newAggregate func() T) *Repository[T] {
	return &Repository[T]{store: store, registry: registry, newAggregate: newAggregate}
}

// Load fetches every event recorded for id and replays it to rebuild the
// aggregate's current state. Returns ErrAggregateNotFound if nothing was
// ever recorded for id. opts is optional — see WithTenant.
func (r *Repository[T]) Load(ctx context.Context, id uuid.UUID, opts ...StoreOption) (T, error) {
	var zero T

	events, err := r.store.Load(ctx, id, opts...)
	if err != nil {
		return zero, err
	}
	if len(events) == 0 {
		return zero, ErrAggregateNotFound
	}

	agg := r.newAggregate()
	agg.SetID(id)

	if err := LoadFromHistory(agg, events, r.registry); err != nil {
		return zero, err
	}

	return agg, nil
}

// Save commits agg's uncommitted events to the store using optimistic
// concurrency (the version the aggregate had before those events were
// recorded). On success it clears the buffer so agg can keep accumulating
// new events. On ErrConcurrencyConflict the caller should reload the
// aggregate, re-apply the command and retry. opts is optional — see
// WithTenant.
func (r *Repository[T]) Save(ctx context.Context, agg T, opts ...StoreOption) error {
	events := agg.GetUncommittedEvents()
	if len(events) == 0 {
		return nil
	}

	expectedVersion := agg.GetVersion() - uint64(len(events))
	if err := r.store.Commit(ctx, events, expectedVersion, opts...); err != nil {
		return err
	}

	agg.ClearEvents()
	return nil
}
