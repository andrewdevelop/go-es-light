// Package memstore is an in-memory es.EventStore + es.Checkpointer. It is
// meant for unit tests and examples — nothing here survives a process
// restart. For production use es/pgstore (or any other durable backend that
// implements the same interfaces).
package memstore

import (
	"context"
	"sync"

	"github.com/google/uuid"

	"go-es-light/es"
)

// Store is a concurrency-safe, in-memory implementation of es.EventStore and
// es.Checkpointer.
type Store struct {
	mu sync.Mutex

	all          []*es.DomainEvent
	byAggregate  map[uuid.UUID][]*es.DomainEvent
	nextGlobalID uint64
	checkpoints  map[string]uint64
	subscribers  map[chan *es.DomainEvent]struct{}
}

// New returns an empty Store.
func New() *Store {
	return &Store{
		byAggregate: make(map[uuid.UUID][]*es.DomainEvent),
		checkpoints: make(map[string]uint64),
		subscribers: make(map[chan *es.DomainEvent]struct{}),
	}
}

// Commit implements es.EventStore.
func (s *Store) Commit(_ context.Context, events []*es.DomainEvent, expectedVersion uint64) error {
	if len(events) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	aggregateID := events[0].AggregateID
	current := s.byAggregate[aggregateID]

	var currentVersion uint64
	if n := len(current); n > 0 {
		currentVersion = current[n-1].AggregateVersion
	}
	if currentVersion != expectedVersion {
		return es.ErrConcurrencyConflict
	}

	for _, e := range events {
		s.nextGlobalID++
		e.GlobalID = s.nextGlobalID

		s.all = append(s.all, e)
		s.byAggregate[aggregateID] = append(s.byAggregate[aggregateID], e)

		for ch := range s.subscribers {
			ch <- e
		}
	}

	return nil
}

// Load implements es.EventStore.
func (s *Store) Load(_ context.Context, id uuid.UUID) ([]*es.DomainEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	events := s.byAggregate[id]
	if len(events) == 0 {
		return nil, es.ErrAggregateNotFound
	}

	out := make([]*es.DomainEvent, len(events))
	copy(out, events)
	return out, nil
}

// FetchAfter implements es.EventStore.
func (s *Store) FetchAfter(_ context.Context, lastID uint64, limit int) ([]*es.DomainEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []*es.DomainEvent
	for _, e := range s.all {
		if e.GlobalID <= lastID {
			continue
		}
		out = append(out, e)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

// StreamAll implements es.EventStore. It first replays every event already
// committed, then keeps streaming new ones as Commit is called, until ctx is
// cancelled.
func (s *Store) StreamAll(ctx context.Context) (<-chan *es.DomainEvent, <-chan error) {
	out := make(chan *es.DomainEvent)
	errc := make(chan error, 1)

	s.mu.Lock()
	backlog := make([]*es.DomainEvent, len(s.all))
	copy(backlog, s.all)

	live := make(chan *es.DomainEvent, 64)
	s.subscribers[live] = struct{}{}
	s.mu.Unlock()

	go func() {
		defer close(out)
		defer close(errc)
		defer func() {
			s.mu.Lock()
			delete(s.subscribers, live)
			s.mu.Unlock()
		}()

		for _, e := range backlog {
			select {
			case out <- e:
			case <-ctx.Done():
				return
			}
		}

		for {
			select {
			case e := <-live:
				select {
				case out <- e:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, errc
}

// Get implements es.Checkpointer.
func (s *Store) Get(_ context.Context, name string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkpoints[name], nil
}

// Save implements es.Checkpointer.
func (s *Store) Save(_ context.Context, name string, lastID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoints[name] = lastID
	return nil
}
