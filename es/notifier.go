package es

import (
	"sync"

	"github.com/google/uuid"
)

// EventNotifier lets a command handler wait for the async read side to
// catch up to a specific write, without making the write itself
// synchronous: Save the aggregate, then Subscribe(id, aggregate.GetVersion())
// and wait on the returned channel before responding — giving read-your-
// writes consistency for that one request while every other reader still
// gets the read model whenever the projector gets to it.
//
// Subscribe is keyed by (aggregate ID, minimum AggregateVersion) rather
// than just the aggregate ID: waiting on "this aggregate got some update"
// is racy — if the projector processes and notifies before the handler
// subscribes, the handler hangs forever, and if it processes an unrelated
// concurrent write to the same aggregate, the handler wakes up too early,
// before its own write was actually projected. Tracking the version each
// aggregate has been projected through and comparing it against what the
// caller asked for removes both failure modes: Subscribe returns an
// already-closed channel if that version was already reached, and Notify
// only wakes waiters whose minVersion has actually been satisfied.
type EventNotifier struct {
	mu            sync.Mutex
	lastProjected map[uuid.UUID]uint64
	waiters       map[uuid.UUID][]*versionWaiter
}

type versionWaiter struct {
	minVersion uint64
	ch         chan struct{}
}

// NewEventNotifier returns an empty EventNotifier.
func NewEventNotifier() *EventNotifier {
	return &EventNotifier{
		lastProjected: make(map[uuid.UUID]uint64),
		waiters:       make(map[uuid.UUID][]*versionWaiter),
	}
}

// Subscribe returns a channel that closes once Notify has been called with
// an event for aggID whose AggregateVersion is >= minVersion. If that has
// already happened by the time Subscribe is called, the returned channel
// is already closed — callers don't need to check first.
func (n *EventNotifier) Subscribe(aggID uuid.UUID, minVersion uint64) <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()

	ch := make(chan struct{})
	if n.lastProjected[aggID] >= minVersion {
		close(ch)
		return ch
	}

	n.waiters[aggID] = append(n.waiters[aggID], &versionWaiter{minVersion: minVersion, ch: ch})
	return ch
}

// Notify records that event's aggregate has been projected through
// event.AggregateVersion, and wakes every waiter whose minVersion is now
// satisfied. Call it after the projector has successfully applied event —
// e.g. right after an es.EventDispatcher.Dispatch(ctx, event) call returns
// nil for that event.
func (n *EventNotifier) Notify(event *DomainEvent) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if event.AggregateVersion > n.lastProjected[event.AggregateID] {
		n.lastProjected[event.AggregateID] = event.AggregateVersion
	}

	pending := n.waiters[event.AggregateID]
	if len(pending) == 0 {
		return
	}

	remaining := pending[:0]
	for _, w := range pending {
		if event.AggregateVersion >= w.minVersion {
			close(w.ch)
		} else {
			remaining = append(remaining, w)
		}
	}

	if len(remaining) == 0 {
		delete(n.waiters, event.AggregateID)
	} else {
		n.waiters[event.AggregateID] = remaining
	}
}
