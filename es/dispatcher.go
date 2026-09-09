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
	// Projector builds a read model and needs strict ordering: its error is
	// reported back to the caller of Dispatch, so a failed projection isn't
	// silently swallowed.
	Projector ListenerKind = iota
	// Reactor triggers a side effect (call a partner API, send an email...)
	// where ordering across different reactors doesn't matter. Its error is
	// logged, never propagated, and it can be switched off wholesale via
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

// ListenerProvider resolves which listeners should run for a given event.
// Subscriptions are keyed by "pattern": the in-memory implementation treats
// a pattern as an exact match against the event's Name, unless it ends in
// "*" — event names are expected to use dot notation (e.g. "user.registered"),
// so a trailing "*" turns the rest into a prefix match: "user.*" matches
// "user.registered" and "user.email_changed", and the bare "*" matches
// every event (an empty prefix matches everything).
type ListenerProvider interface {
	RegisterListener(pattern string, listener EventListener)
	GetListenersForEvent(event *DomainEvent) []EventListener
}

// InMemoryListenerProvider is a concurrency-safe, in-memory ListenerProvider.
type InMemoryListenerProvider struct {
	mu        sync.RWMutex
	listeners map[string][]EventListener
}

// NewInMemoryListenerProvider returns an empty InMemoryListenerProvider.
func NewInMemoryListenerProvider() ListenerProvider {
	return &InMemoryListenerProvider{listeners: make(map[string][]EventListener)}
}

func (p *InMemoryListenerProvider) RegisterListener(pattern string, listener EventListener) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.listeners[pattern] = append(p.listeners[pattern], listener)
}

func (p *InMemoryListenerProvider) GetListenersForEvent(event *DomainEvent) []EventListener {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var out []EventListener
	for pattern, listeners := range p.listeners {
		if patternMatches(pattern, event.Name) {
			out = append(out, listeners...)
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

// Dispatcher is the default EventDispatcher: Projector listeners run
// concurrently but any of their errors is returned to the caller; Reactor
// listeners run concurrently, best-effort, their errors only logged, and
// can be disabled entirely via HandleSideEffects(false) — e.g. while
// replaying history, where side effects (emails, webhooks) must not fire
// again.
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
// Name starts with "user.".
func (d *Dispatcher) Subscribe(pattern string, listener EventListener) {
	d.provider.RegisterListener(pattern, listener)
}

// Dispatch invokes every listener subscribed to event concurrently. It
// waits for all of them to finish. A Projector's error is collected and
// returned (the first one, if several fail); a Reactor's error is only
// logged, never returned.
func (d *Dispatcher) Dispatch(ctx context.Context, event *DomainEvent) error { // <-- Pointer
	handlers := d.provider.GetListenersForEvent(event)

	var wg sync.WaitGroup
	errCh := make(chan error, len(handlers))

	for _, h := range handlers {
		kind := h.Kind()
		if !d.withSideEffects && kind == Reactor {
			continue
		}

		wg.Add(1)
		go func(l EventListener, k ListenerKind) {
			defer wg.Done()

			if k == Projector {
				if err := l.Handle(ctx, event); err != nil {
					errCh <- err
				}
				return
			}

			if err := l.Handle(ctx, event); err != nil {
				log.Printf("es: reactor error handling %q: %v", event.Name, err)
			}
		}(h, kind)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}
