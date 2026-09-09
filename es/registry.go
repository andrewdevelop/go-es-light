package es

import "sync"

// EventRegistry maps an event's Name (DomainEvent.Name / Event.EventName())
// to a factory producing a fresh, zero-valued instance of its payload type.
// It lets a Repository turn a stored DomainEvent back into a concrete Event
// before replaying it through Applier.Apply.
type EventRegistry interface {
	Register(name string, factory func() any)
	Factory(name string) (func() any, bool)
}

// NewRegistry returns an in-memory, concurrency-safe EventRegistry.
func NewRegistry() EventRegistry {
	return &registry{factories: make(map[string]func() any)}
}

type registry struct {
	mu        sync.RWMutex
	factories map[string]func() any
}

func (r *registry) Register(name string, factory func() any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[name] = factory
}

func (r *registry) Factory(name string) (func() any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.factories[name]
	return f, ok
}
