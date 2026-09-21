package es

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// DomainEvent is an envelope for metadata and payload of an event.
//
// TenantID, ActorID, ActorType, PiiID, Metadata and PiiFields are all
// optional — zero/nil values mean "not set" and every existing
// single-tenant, no-PII caller can ignore them entirely. See WithTenant
// (store.go) for tenant scoping and WithActor/WithPiiID/WithMetadata
// (aggregate.go) for the rest. None of the optional fields use
// `json:",omitempty"`: an unset field serializes as an explicit
// `null`/zero value rather than disappearing from the JSON entirely, so a
// consumer decoding a DomainEvent always sees every key.
type DomainEvent struct {
	GlobalID         uint64          `json:"global_id" db:"global_id"`
	ID               uuid.UUID       `json:"id" db:"id"`
	TenantID         uuid.UUID       `json:"tenant_id" db:"tenant_id"`
	AggregateID      uuid.UUID       `json:"aggregate_id" db:"aggregate_id"`
	AggregateVersion uint64          `json:"aggregate_version" db:"aggregate_version"`
	Version          int             `json:"version" db:"version"`
	Name             string          `json:"name" db:"name"`
	Payload          json.RawMessage `json:"payload" db:"payload"`
	Metadata         json.RawMessage `json:"metadata" db:"metadata"`
	ActorID          *uuid.UUID      `json:"actor_id" db:"actor_id"`
	ActorType        *string         `json:"actor_type" db:"actor_type"`
	PiiID            *uuid.UUID      `json:"pii_id" db:"pii_id"`
	// PiiFields holds the field manifest PiiAnonymizer.Seal writes — which
	// payload fields it just encrypted — so Open can decrypt later without
	// needing the concrete payload type again. Kept in its own column
	// rather than folded into Metadata, so caller-supplied WithMetadata
	// fields and PiiAnonymizer's own bookkeeping never share storage.
	PiiFields json.RawMessage `json:"pii_fields" db:"pii_fields"`
	// PiiUnrecoverable is not a stored column — it's set in memory by
	// PiiAnonymizer.Open, true only when this event declared PII fields
	// (PiiID/PiiFields both set) but the subject's key has already been
	// forgotten (see RequestsErasure). Open leaves the fields as ciphertext
	// in that case rather than erroring, deliberately, so replaying the
	// rest of an aggregate's history keeps working — but that ciphertext
	// is not a valid value in whatever shape the original field was (an
	// email, a phone number, ...), so a consumer that's about to use a
	// declared PII field for something format-sensitive — writing it into
	// a DB column with a CHECK constraint, validating/parsing it, showing
	// it as-is in a UI — should check this first. See ErrPiiUnrecoverable
	// for the error-flavored equivalent of this same check.
	//
	// False does not by itself mean "definitely plaintext": it's only
	// meaningful for an event that actually went through
	// PiiAnonymizer.Open (i.e. came from an es.PiiEventStore-wrapped
	// store's Load/FetchAfter/StreamAll) — an event read from directly
	// under an unwrapped store was never opened at all, and this stays
	// false there too even though Payload may still be sealed.
	PiiUnrecoverable bool `json:"pii_unrecoverable"`

	OccurredAt time.Time `json:"occurred_at" db:"occurred_at"`
}

// UnmarshalPayload decodes the event payload into v.
func (e DomainEvent) UnmarshalPayload(v any) error {
	return json.Unmarshal(e.Payload, v)
}

// Event is the interface every domain event payload must implement.
type Event interface {
	EventName() string
}
