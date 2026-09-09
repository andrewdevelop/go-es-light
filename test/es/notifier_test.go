package es_test

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"go-es-light/es"
)

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestEventNotifier_SubscribeThenNotify(t *testing.T) {
	n := es.NewEventNotifier()
	aggID := uuid.New()

	ch := n.Subscribe(aggID, 2)
	if isClosed(ch) {
		t.Fatal("expected channel to not be closed before Notify reaches minVersion")
	}

	n.Notify(&es.DomainEvent{AggregateID: aggID, AggregateVersion: 1})
	if isClosed(ch) {
		t.Fatal("expected channel to stay open for a version below minVersion")
	}

	n.Notify(&es.DomainEvent{AggregateID: aggID, AggregateVersion: 2})
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected channel to close once minVersion was reached")
	}
}

func TestEventNotifier_NotifyThenSubscribe(t *testing.T) {
	n := es.NewEventNotifier()
	aggID := uuid.New()

	n.Notify(&es.DomainEvent{AggregateID: aggID, AggregateVersion: 5})

	// Subscribing for a version already reached must return an
	// already-closed channel — no race window where the caller could hang.
	ch := n.Subscribe(aggID, 3)
	if !isClosed(ch) {
		t.Fatal("expected an already-closed channel for a version already projected")
	}

	// A higher minVersion than what's been projected so far must still block.
	ch2 := n.Subscribe(aggID, 6)
	if isClosed(ch2) {
		t.Fatal("expected channel to stay open for a version not yet projected")
	}
}

func TestEventNotifier_UnrelatedAggregateDoesNotWake(t *testing.T) {
	n := es.NewEventNotifier()
	aggID := uuid.New()
	otherID := uuid.New()

	ch := n.Subscribe(aggID, 1)
	n.Notify(&es.DomainEvent{AggregateID: otherID, AggregateVersion: 100})

	if isClosed(ch) {
		t.Fatal("expected an unrelated aggregate's event to not wake this waiter")
	}
}

func TestEventNotifier_MultipleWaitersDifferentVersions(t *testing.T) {
	n := es.NewEventNotifier()
	aggID := uuid.New()

	low := n.Subscribe(aggID, 1)
	high := n.Subscribe(aggID, 3)

	n.Notify(&es.DomainEvent{AggregateID: aggID, AggregateVersion: 1})
	select {
	case <-low:
	case <-time.After(time.Second):
		t.Fatal("expected low waiter to wake at version 1")
	}
	if isClosed(high) {
		t.Fatal("expected high waiter to still be waiting")
	}

	n.Notify(&es.DomainEvent{AggregateID: aggID, AggregateVersion: 3})
	select {
	case <-high:
	case <-time.After(time.Second):
		t.Fatal("expected high waiter to wake at version 3")
	}
}

func TestEventNotifier_NotifyWithNoWaiters(t *testing.T) {
	n := es.NewEventNotifier()
	// Must be a safe no-op: nothing subscribed for this aggregate yet.
	n.Notify(&es.DomainEvent{AggregateID: uuid.New(), AggregateVersion: 1})
}

func TestEventNotifier_ConcurrentSubscribeAndNotify(t *testing.T) {
	n := es.NewEventNotifier()
	aggID := uuid.New()

	const waiters = 50
	var wg sync.WaitGroup
	wg.Add(waiters)

	for i := 0; i < waiters; i++ {
		go func() {
			defer wg.Done()
			ch := n.Subscribe(aggID, 1)
			<-ch
		}()
	}

	go n.Notify(&es.DomainEvent{AggregateID: aggID, AggregateVersion: 1})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for all concurrent waiters to wake")
	}
}
