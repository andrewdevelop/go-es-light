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

// aggregateKey scopes an aggregate's events to the tenant they belong to
// (zero value if untenanted), so callers using es.WithTenant can never Load
// or Commit against another tenant's stream even if they guess the
// aggregate id.
type aggregateKey struct {
	tenantID    uuid.UUID
	aggregateID uuid.UUID
}

// Store is a concurrency-safe, in-memory implementation of es.EventStore,
// es.Checkpointer and es.PiiLookup.
type Store struct {
	mu sync.Mutex

	all          []*es.DomainEvent
	byAggregate  map[aggregateKey][]*es.DomainEvent
	byPii        map[uuid.UUID][]*es.DomainEvent
	nextGlobalID uint64
	checkpoints  map[string]uint64
	subscribers  map[chan *es.DomainEvent]struct{}
}

// New returns an empty Store.
func New() *Store {
	return &Store{
		byAggregate: make(map[aggregateKey][]*es.DomainEvent),
		byPii:       make(map[uuid.UUID][]*es.DomainEvent),
		checkpoints: make(map[string]uint64),
		subscribers: make(map[chan *es.DomainEvent]struct{}),
	}
}

// Commit implements es.EventStore. opts is optional — see es.WithTenant. If
// given, it's stamped onto every event that doesn't already carry a
// TenantID, and the optimistic-concurrency check + storage are scoped to
// that tenant, so one tenant can never collide with or overwrite another
// tenant's stream for the same aggregate id.
func (s *Store) Commit(_ context.Context, events []*es.DomainEvent, expectedVersion uint64, opts ...es.StoreOption) error {
	if len(events) == 0 {
		return nil
	}

	cfg := es.ResolveStoreConfig(opts...)
	if cfg.TenantID != nil {
		for _, e := range events {
			if e.TenantID == uuid.Nil {
				e.TenantID = *cfg.TenantID
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := aggregateKey{tenantID: events[0].TenantID, aggregateID: events[0].AggregateID}
	current := s.byAggregate[key]

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
		s.byAggregate[key] = append(s.byAggregate[key], e)

		if e.PiiID != nil {
			s.byPii[*e.PiiID] = append(s.byPii[*e.PiiID], e)
		}

		for ch := range s.subscribers {
			ch <- e
		}
	}

	return nil
}

// Load implements es.EventStore. opts is optional — see es.WithTenant. If
// given, only events committed under that tenant (plus any committed under
// es.GlobalTenantID) are visible; an aggregate id that exists under a
// different, non-global tenant behaves as not found.
func (s *Store) Load(_ context.Context, id uuid.UUID, opts ...es.StoreOption) ([]*es.DomainEvent, error) {
	cfg := es.ResolveStoreConfig(opts...)

	s.mu.Lock()
	defer s.mu.Unlock()

	var tenantID uuid.UUID
	if cfg.TenantID != nil {
		tenantID = *cfg.TenantID
	}
	events := s.byAggregate[aggregateKey{tenantID: tenantID, aggregateID: id}]
	if len(events) == 0 && cfg.TenantID != nil {
		events = s.byAggregate[aggregateKey{tenantID: es.GlobalTenantID, aggregateID: id}]
	}
	if len(events) == 0 {
		return nil, es.ErrAggregateNotFound
	}

	out := make([]*es.DomainEvent, len(events))
	copy(out, events)
	return out, nil
}

// FetchAfter implements es.EventStore. opts is optional — see es.WithTenant,
// which restricts results to that tenant plus any es.GlobalTenantID events.
func (s *Store) FetchAfter(_ context.Context, lastID uint64, limit int, opts ...es.StoreOption) ([]*es.DomainEvent, error) {
	cfg := es.ResolveStoreConfig(opts...)

	s.mu.Lock()
	defer s.mu.Unlock()

	var out []*es.DomainEvent
	for _, e := range s.all {
		if e.GlobalID <= lastID {
			continue
		}
		if cfg.TenantID != nil && e.TenantID != *cfg.TenantID && e.TenantID != es.GlobalTenantID {
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
// cancelled. opts is optional — see es.WithTenant.
func (s *Store) StreamAll(ctx context.Context, opts ...es.StoreOption) (<-chan *es.DomainEvent, <-chan error) {
	out := make(chan *es.DomainEvent)
	errc := make(chan error, 1)

	cfg := es.ResolveStoreConfig(opts...)

	s.mu.Lock()
	var backlog []*es.DomainEvent
	for _, e := range s.all {
		if cfg.TenantID != nil && e.TenantID != *cfg.TenantID && e.TenantID != es.GlobalTenantID {
			continue
		}
		backlog = append(backlog, e)
	}

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
				if cfg.TenantID != nil && e.TenantID != *cfg.TenantID && e.TenantID != es.GlobalTenantID {
					continue
				}
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

// FetchByPiiID implements es.PiiLookup: every event carrying piiID, across
// all aggregates and tenants, ordered by GlobalID (the order committed).
// opts is optional — see es.WithTenant, which restricts results to that
// tenant plus any es.GlobalTenantID events.
func (s *Store) FetchByPiiID(_ context.Context, piiID uuid.UUID, opts ...es.StoreOption) ([]*es.DomainEvent, error) {
	cfg := es.ResolveStoreConfig(opts...)

	s.mu.Lock()
	defer s.mu.Unlock()

	var out []*es.DomainEvent
	for _, e := range s.byPii[piiID] {
		if cfg.TenantID != nil && e.TenantID != *cfg.TenantID && e.TenantID != es.GlobalTenantID {
			continue
		}
		out = append(out, e)
	}
	if len(out) == 0 {
		return nil, es.ErrSubjectNotFound
	}

	copied := make([]*es.DomainEvent, len(out))
	copy(copied, out)
	return copied, nil
}

var _ es.PiiLookup = (*Store)(nil)

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
