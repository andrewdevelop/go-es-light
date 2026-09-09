package es

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// BaseAggregate is embedded into concrete aggregates to give them identity,
// versioning and an uncommitted-events buffer for free.
type BaseAggregate struct {
	ID                uuid.UUID      `json:"id"`
	Version           uint64         `json:"version"`
	uncommittedEvents []*DomainEvent `json:"-"`
}

func (b *BaseAggregate) GetID() uuid.UUID {
	return b.ID
}

func (b *BaseAggregate) SetID(id uuid.UUID) {
	b.ID = id
}

func (b *BaseAggregate) GetVersion() uint64 {
	return b.Version
}

func (b *BaseAggregate) SetVersion(v uint64) {
	b.Version = v
}

func (b *BaseAggregate) GetUncommittedEvents() []*DomainEvent {
	return b.uncommittedEvents
}

func (b *BaseAggregate) ClearEvents() {
	b.uncommittedEvents = nil
}

// RecordOption customizes the DomainEvent envelope RecordThat builds,
// after it has been populated with its defaults (a fresh ID, schema
// Version 1, OccurredAt set to time.Now()). Options only ever touch the
// envelope metadata — never AggregateID, AggregateVersion or Name, which
// exist to keep the event log consistent and aren't meant to be
// overridden.
type RecordOption func(*DomainEvent)

// WithEventVersion overrides DomainEvent.Version — the schema version of
// this event's payload shape, not the aggregate's version. Bump it when an
// event's payload shape changes and consumers (registries, projections)
// need to tell old and new shapes apart. Defaults to 1.
func WithEventVersion(v int) RecordOption {
	return func(e *DomainEvent) { e.Version = v }
}

// WithOccurredAt overrides DomainEvent.OccurredAt, which otherwise defaults
// to time.Now().UTC(). Use it when the true occurrence time comes from
// somewhere other than "now" — e.g. importing events from an external
// system, or replaying a fixture in a test.
func WithOccurredAt(t time.Time) RecordOption {
	return func(e *DomainEvent) { e.OccurredAt = t }
}

// WithEventID overrides DomainEvent.ID, which otherwise defaults to a fresh
// uuid.New(). Use it when re-recording an event that was already assigned
// an ID by an external system, so identity survives the round trip.
func WithEventID(id uuid.UUID) RecordOption {
	return func(e *DomainEvent) { e.ID = id }
}

// RecordThat applies event to the aggregate (via ar.Apply, the aggregate's
// own business logic) and, only if that succeeds, buffers the corresponding
// DomainEvent to be persisted later by a Repository/EventStore. This is the
// single place where an aggregate's state changes. opts can override parts
// of the envelope RecordThat would otherwise default (see WithEventVersion,
// WithOccurredAt, WithEventID); most callers need none of them.
func (b *BaseAggregate) RecordThat(ar Applier, event Event, opts ...RecordOption) error {
	b.Version++
	if err := ar.Apply(event); err != nil {
		b.Version--
		return err
	}

	payload, err := json.Marshal(event)
	if err != nil {
		b.Version--
		return fmt.Errorf("es: marshal payload for %q: %w", event.EventName(), err)
	}

	e := &DomainEvent{
		ID:               uuid.New(),
		AggregateID:      ar.GetID(),
		AggregateVersion: b.Version,
		Version:          1,
		Name:             event.EventName(),
		Payload:          payload,
		OccurredAt:       time.Now().UTC(),
	}
	for _, opt := range opts {
		opt(e)
	}

	b.uncommittedEvents = append(b.uncommittedEvents, e)

	return nil
}

// LoadFromHistory rebuilds an aggregate's in-memory state by replaying
// previously persisted events through it. It looks up each event's concrete
// payload type in registry, unmarshals into a fresh instance and calls
// ar.Apply — the same code path RecordThat uses when the event was first
// recorded, so replay and live application never diverge.
func LoadFromHistory(ar Reconstitutable, events []*DomainEvent, registry EventRegistry) error {
	for _, e := range events {
		factory, ok := registry.Factory(e.Name)
		if !ok {
			return fmt.Errorf("es: no factory registered for event %q", e.Name)
		}

		payload := factory()
		if err := e.UnmarshalPayload(payload); err != nil {
			return fmt.Errorf("es: unmarshal payload for %q: %w", e.Name, err)
		}

		if err := ar.Apply(payload); err != nil {
			return fmt.Errorf("es: replay %q: %w", e.Name, err)
		}

		ar.SetVersion(e.AggregateVersion)
	}

	return nil
}
