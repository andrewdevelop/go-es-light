package es_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"go-es-light/es"
)

func TestDispatcher_ProjectorRuns(t *testing.T) {
	d := es.NewEventDispatcher()

	var got *es.DomainEvent
	var mu sync.Mutex
	d.Subscribe("SubscriptionActivated", es.NewProjector(func(_ context.Context, event *es.DomainEvent) error {
		mu.Lock()
		got = event
		mu.Unlock()
		return nil
	}))

	event := &es.DomainEvent{Name: "SubscriptionActivated"}
	if err := d.Dispatch(context.Background(), event); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if got != event {
		t.Fatal("expected projector to receive the dispatched event")
	}
}

func TestDispatcher_ProjectorErrorPropagates(t *testing.T) {
	d := es.NewEventDispatcher()
	wantErr := errors.New("projection failed")

	d.Subscribe("SubscriptionActivated", es.NewProjector(func(context.Context, *es.DomainEvent) error {
		return wantErr
	}))

	err := d.Dispatch(context.Background(), &es.DomainEvent{Name: "SubscriptionActivated"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected projector error to propagate, got %v", err)
	}
}

func TestDispatcher_ReactorErrorDoesNotPropagate(t *testing.T) {
	d := es.NewEventDispatcher()

	var called atomic.Bool
	d.Subscribe("SubscriptionActivated", es.NewReactor(func(context.Context, *es.DomainEvent) error {
		called.Store(true)
		return errors.New("side effect failed")
	}))

	err := d.Dispatch(context.Background(), &es.DomainEvent{Name: "SubscriptionActivated"})
	if err != nil {
		t.Fatalf("expected reactor errors to be swallowed (only logged), got %v", err)
	}
	if !called.Load() {
		t.Fatal("expected reactor to have run")
	}
}

func TestDispatcher_HandleSideEffectsFalseSkipsReactors(t *testing.T) {
	d := es.NewEventDispatcher().HandleSideEffects(false)

	var reactorCalled, projectorCalled atomic.Bool
	d.Subscribe("SubscriptionActivated", es.NewReactor(func(context.Context, *es.DomainEvent) error {
		reactorCalled.Store(true)
		return nil
	}))
	d.Subscribe("SubscriptionActivated", es.NewProjector(func(context.Context, *es.DomainEvent) error {
		projectorCalled.Store(true)
		return nil
	}))

	if err := d.Dispatch(context.Background(), &es.DomainEvent{Name: "SubscriptionActivated"}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if reactorCalled.Load() {
		t.Fatal("reactor must not run when side effects are disabled")
	}
	if !projectorCalled.Load() {
		t.Fatal("projector must still run when side effects are disabled")
	}
}

func TestDispatcher_WildcardAndNoListeners(t *testing.T) {
	d := es.NewEventDispatcher()

	var wildcardCalls atomic.Int32
	d.Subscribe("*", es.NewProjector(func(context.Context, *es.DomainEvent) error {
		wildcardCalls.Add(1)
		return nil
	}))

	if err := d.Dispatch(context.Background(), &es.DomainEvent{Name: "AnythingAtAll"}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if wildcardCalls.Load() != 1 {
		t.Fatalf("expected wildcard listener to run once, got %d", wildcardCalls.Load())
	}

	// No listeners registered at all for this name: Dispatch must be a safe no-op.
	d2 := es.NewEventDispatcher()
	if err := d2.Dispatch(context.Background(), &es.DomainEvent{Name: "NoSubscribers"}); err != nil {
		t.Fatalf("Dispatch with no listeners: %v", err)
	}
}

func TestDispatcher_DotNotationPrefixWildcard(t *testing.T) {
	d := es.NewEventDispatcher()

	var userCalls, otherCalls atomic.Int32
	d.Subscribe("user.*", es.NewProjector(func(context.Context, *es.DomainEvent) error {
		userCalls.Add(1)
		return nil
	}))
	d.Subscribe("order.*", es.NewProjector(func(context.Context, *es.DomainEvent) error {
		otherCalls.Add(1)
		return nil
	}))

	for _, name := range []string{"user.registered", "user.email_changed"} {
		if err := d.Dispatch(context.Background(), &es.DomainEvent{Name: name}); err != nil {
			t.Fatalf("Dispatch(%q): %v", name, err)
		}
	}
	if userCalls.Load() != 2 {
		t.Fatalf("expected \"user.*\" listener to run for both user.* events, got %d", userCalls.Load())
	}
	if otherCalls.Load() != 0 {
		t.Fatalf("expected \"order.*\" listener to not run for user.* events, got %d", otherCalls.Load())
	}

	// A name that merely contains the prefix elsewhere must not match.
	if err := d.Dispatch(context.Background(), &es.DomainEvent{Name: "not_user.registered"}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if userCalls.Load() != 2 {
		t.Fatalf("expected \"user.*\" to require a prefix match, got %d calls", userCalls.Load())
	}
}
