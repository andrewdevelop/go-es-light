package es

import (
	"context"
	"log"
	"strings"
	"sync"
)

// ListenerKind distinguishes how a listener's errors are treated by
// Dispatcher.Dispatch.
type ListenerKind int

const (
	// Projector builds a read model and needs strict ordering: Dispatch
	// runs every Projector for an event synchronously, one at a time, in
	// the order they were registered with Subscribe — never concurrently
	// with each other. A Projector can therefore rely on any Projector
	// registered before it having already run (see es.NotifierListener,
	// which depends on exactly this to only wake a reader once an earlier,
	// read-model-building Projector has actually applied the event). If a
	// Projector returns an error, Dispatch returns it and skips the rest
	// of the Projector chain for this event — but not any Reactors (see
	// below), which are unaffected by Projector failures.
	Projector ListenerKind = iota
	// Reactor triggers a side effect (call a partner API, send an email...)
	// where ordering — across different reactors, or relative to
	// Projectors — doesn't matter. Dispatch starts it in its own goroutine
	// and does not wait for it: a slow or failing side effect must never
	// delay whatever the caller does once Dispatch returns. Its error is
	// only logged, and the whole kind can be switched off via
	// Dispatcher.HandleSideEffects(false).
	Reactor
)

// EventHandler is the function-shaped form of an EventListener's work.
type EventHandler func(ctx context.Context, event *DomainEvent) error

// EventListener reacts to a dispatched DomainEvent. Kind determines whether
// its errors block the caller (Projector) or are best-effort (Reactor).
type EventListener interface {
	Kind() ListenerKind
	Handle(ctx context.Context, event *DomainEvent) error
}

// listenerFunc adapts a plain EventHandler into an EventListener.
type listenerFunc struct {
	kind ListenerKind
	fn   EventHandler
}

func (l listenerFunc) Kind() ListenerKind { return l.kind }

func (l listenerFunc) Handle(ctx context.Context, event *DomainEvent) error {
	return l.fn(ctx, event)
}

// NewProjector wraps fn as a Projector-kind EventListener.
func NewProjector(fn EventHandler) EventListener {
	return listenerFunc{kind: Projector, fn: fn}
}

// NewReactor wraps fn as a Reactor-kind EventListener.
func NewReactor(fn EventHandler) EventListener {
	return listenerFunc{kind: Reactor, fn: fn}
}

// ListenerProvider resolves which listeners should run for a given event,
// and in what order. Subscriptions are keyed by "pattern": the in-memory
// implementation treats a pattern as an exact match against the event's
// Name, unless it ends in "*" — event names are expected to use dot
// notation (e.g. "user.registered"), so a trailing "*" turns the rest into
// a prefix match: "user.*" matches "user.registered" and
// "user.email_changed", and the bare "*" matches every event (an empty
// prefix matches everything). GetListenersForEvent must return matches in
// the order they were registered — Dispatch depends on that order to run
// Projectors one at a time, deterministically.
type ListenerProvider interface {
	RegisterListener(pattern string, listener EventListener)
	GetListenersForEvent(event *DomainEvent) []EventListener
}

// listenerRegistration is one Subscribe call: which pattern it was for and
// which listener it registered, kept in call order.
type listenerRegistration struct {
	pattern  string
	listener EventListener
}

// InMemoryListenerProvider is a concurrency-safe, in-memory ListenerProvider.
// It keeps registrations in a single call-ordered slice — not bucketed by
// pattern in a map — specifically so GetListenersForEvent's result order
// (and therefore Dispatch's Projector execution order) is the order
// Subscribe was actually called in, regardless of how many different
// patterns are involved.
type InMemoryListenerProvider struct {
	mu            sync.RWMutex
	registrations []listenerRegistration
}

// NewInMemoryListenerProvider returns an empty InMemoryListenerProvider.
func NewInMemoryListenerProvider() ListenerProvider {
	return &InMemoryListenerProvider{}
}

func (p *InMemoryListenerProvider) RegisterListener(pattern string, listener EventListener) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.registrations = append(p.registrations, listenerRegistration{pattern: pattern, listener: listener})
}

func (p *InMemoryListenerProvider) GetListenersForEvent(event *DomainEvent) []EventListener {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var out []EventListener
	for _, r := range p.registrations {
		if patternMatches(r.pattern, event.Name) {
			out = append(out, r.listener)
		}
	}
	return out
}

// patternMatches reports whether name satisfies pattern: an exact match, or,
// if pattern ends in "*", a prefix match against everything before the "*"
// (so "*" itself — an empty prefix — matches any name).
func patternMatches(pattern, name string) bool {
	if pattern == name {
		return true
	}
	prefix, ok := strings.CutSuffix(pattern, "*")
	return ok && strings.HasPrefix(name, prefix)
}

// EventDispatcher fans a single DomainEvent out to every listener
// subscribed to it, is responsible for retrieving Listeners from a
// ListenerProvider for the Event dispatched, and invoking each Listener
// with that Event.
type EventDispatcher interface {
	Subscribe(pattern string, listener EventListener)
	Dispatch(ctx context.Context, event *DomainEvent) error // <-- Pointer
	HandleSideEffects(v bool) EventDispatcher
}

// Dispatcher is the default EventDispatcher: Projector listeners run one at
// a time, in registration order, and Dispatch waits for each before moving
// to the next; the first error stops the Projector chain and is returned
// to the caller. Reactor listeners are started and left to run detached —
// Dispatch never waits on a Reactor, and a Projector's error doesn't
// prevent later Reactors from firing. Reactors can be disabled entirely
// via HandleSideEffects(false) — e.g. while replaying history, where side
// effects (emails, webhooks) must not fire again.
type Dispatcher struct {
	provider        ListenerProvider
	withSideEffects bool
}

// NewEventDispatcher returns a Dispatcher backed by an
// InMemoryListenerProvider, with side effects (Reactor listeners) enabled.
func NewEventDispatcher() EventDispatcher {
	return &Dispatcher{
		provider:        NewInMemoryListenerProvider(),
		withSideEffects: true,
	}
}

// HandleSideEffects toggles whether Reactor listeners run at all. Returns
// the dispatcher itself so it can be chained after NewEventDispatcher.
func (d *Dispatcher) HandleSideEffects(v bool) EventDispatcher {
	d.withSideEffects = v
	return d
}

// Subscribe registers listener for pattern: an exact event Name, "*" for
// every event, or a dot-notation prefix like "user.*" for every event whose
// Name starts with "user.". Order matters for Projector listeners: see
// Dispatch.
func (d *Dispatcher) Subscribe(pattern string, listener EventListener) {
	d.provider.RegisterListener(pattern, listener)
}

// Dispatch runs every listener subscribed to event, in registration order.
// Each Projector listener runs synchronously, one at a time; if one
// returns an error, Dispatch returns it immediately and skips the
// remaining Projectors for this event. Every Reactor listener is started
// in its own goroutine and never waited for, regardless of its position
// relative to a failed Projector — side effects are independent of the
// read-model chain. A Reactor's error is only logged.
func (d *Dispatcher) Dispatch(ctx context.Context, event *DomainEvent) error { // <-- Pointer
	var projectorErr error

	for _, h := range d.provider.GetListenersForEvent(event) {
		if h.Kind() == Reactor {
			if !d.withSideEffects {
				continue
			}
			go func(l EventListener) {
				if err := l.Handle(ctx, event); err != nil {
					log.Printf("es: reactor error handling %q: %v", event.Name, err)
				}
			}(h)
			continue
		}

		if projectorErr != nil {
			continue // an earlier Projector in the chain already failed
		}
		if err := h.Handle(ctx, event); err != nil {
			projectorErr = err
		}
	}

	return projectorErr
}
