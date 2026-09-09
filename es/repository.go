package es

import (
	"context"

	"github.com/google/uuid"
)

// Repository loads and persists aggregates of type T, translating between
// the domain object (T) and the raw DomainEvent log kept in an EventStore.
// T is normally a pointer to a struct embedding BaseAggregate, e.g.
// *SubscriptionAggregate.
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
// ever recorded for id.
func (r *Repository[T]) Load(ctx context.Context, id uuid.UUID) (T, error) {
	var zero T

	events, err := r.store.Load(ctx, id)
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
// aggregate, re-apply the command and retry.
func (r *Repository[T]) Save(ctx context.Context, agg T) error {
	events := agg.GetUncommittedEvents()
	if len(events) == 0 {
		return nil
	}

	expectedVersion := agg.GetVersion() - uint64(len(events))
	if err := r.store.Commit(ctx, events, expectedVersion); err != nil {
		return err
	}

	agg.ClearEvents()
	return nil
}
