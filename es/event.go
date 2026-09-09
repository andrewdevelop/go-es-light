package es

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// DomainEvent is an envelope for metadata and payload of an event.
type DomainEvent struct {
	GlobalID         uint64          `json:"global_id" db:"global_id"`
	ID               uuid.UUID       `json:"id" db:"id"`
	AggregateID      uuid.UUID       `json:"aggregate_id" db:"aggregate_id"`
	AggregateVersion uint64          `json:"aggregate_version" db:"aggregate_version"`
	Version          int             `json:"version" db:"version"`
	Name             string          `json:"name" db:"name"`
	Payload          json.RawMessage `json:"payload" db:"payload"`
	OccurredAt       time.Time       `json:"occurred_at" db:"occurred_at"`
}

// UnmarshalPayload decodes the event payload into v.
func (e DomainEvent) UnmarshalPayload(v any) error {
	return json.Unmarshal(e.Payload, v)
}

// Event is the interface every domain event payload must implement.
type Event interface {
	EventName() string
}
