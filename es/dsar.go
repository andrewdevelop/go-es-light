package es

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// dsarEventDoc is one event as it appears in an exported subject document:
// the envelope a DSAR responder actually needs to ship (name, versions,
// timestamp, actor, any caller-supplied metadata, the opened payload, and
// whether the subject's key is gone so the payload fields are ciphertext).
type dsarEventDoc struct {
	ID               uuid.UUID       `json:"id"`
	Name             string          `json:"name"`
	Version          int             `json:"version"`
	AggregateVersion uint64          `json:"aggregate_version"`
	OccurredAt       time.Time       `json:"occurred_at"`
	Actor            *dsarActorDoc   `json:"actor,omitempty"`
	Metadata         json.RawMessage `json:"metadata,omitempty"`
	PiiUnrecoverable bool            `json:"pii_unrecoverable"`
	Payload          json.RawMessage `json:"payload"`
}

type dsarActorDoc struct {
	ID   uuid.UUID `json:"id"`
	Type string    `json:"type"`
}

type dsarAggregateDoc struct {
	TenantID    uuid.UUID      `json:"tenant_id"`
	AggregateID uuid.UUID      `json:"aggregate_id"`
	Events      []dsarEventDoc `json:"events"`
}

// dsarResponseDoc is the JSON shape SubjectExport.MarshalJSON (and so
// SubjectExporter.ExportJSON) produce: a complete, deterministic,
// subject-centric document ready to be handed to a data subject or their
// regulator. Aggregate groups and events keep the in-memory export's order
// (first-seen by global_id, then by aggregate_version), and every nested
// object's keys are emitted in sorted order by encoding/json, so two
// exports of the same log state are byte-for-byte identical.
type dsarResponseDoc struct {
	PiiID       uuid.UUID          `json:"pii_id"`
	GeneratedAt time.Time          `json:"generated_at"`
	Aggregates  []dsarAggregateDoc `json:"aggregates"`
}

// MarshalJSON turns the export into a deterministic, serializable DSAR/access
// response document — the "export API" output shape, where the in-memory
// SubjectExport above is the raw material. Every event carries the same
// fields any other consumer of the store sees: the payload is already opened
// (or flagged via PiiUnrecoverable post-erasure), actor and metadata survive
// intact, and occurred_at is preserved. For an erased subject the identity
// event's PiiUnrecoverable is set, so a responder can render "we no longer
// hold this in a readable form" without trying to decode ciphertext.
func (x *SubjectExport) MarshalJSON() ([]byte, error) {
	doc := dsarResponseDoc{
		PiiID:       x.PiiID,
		GeneratedAt: time.Now().UTC(),
	}
	for _, agg := range x.Aggregates {
		a := dsarAggregateDoc{TenantID: agg.TenantID, AggregateID: agg.AggregateID}
		for _, ev := range agg.Events {
			docEvent := dsarEventDoc{
				ID:               ev.ID,
				Name:             ev.Name,
				Version:          ev.Version,
				AggregateVersion: ev.AggregateVersion,
				OccurredAt:       ev.OccurredAt,
				PiiUnrecoverable: ev.PiiUnrecoverable,
				Payload:          ev.Payload,
			}
			if len(ev.Metadata) > 0 {
				docEvent.Metadata = ev.Metadata
			}
			if ev.ActorID != nil {
				actorType := ""
				if ev.ActorType != nil {
					actorType = *ev.ActorType
				}
				docEvent.Actor = &dsarActorDoc{ID: *ev.ActorID, Type: actorType}
			}
			a.Events = append(a.Events, docEvent)
		}
		doc.Aggregates = append(doc.Aggregates, a)
	}
	return json.Marshal(&doc)
}

// ExportJSON produces the ready-to-ship DSAR document for piiID in one call:
// Export (grouped by tenant/aggregate, PII fields opened or flagged) then
// MarshalJSON, indented for a human reviewer. Returns ErrSubjectNotFound if
// the log holds no event for this subject at all. opts is optional — see
// es.WithTenant.
func (e *SubjectExporter) ExportJSON(ctx context.Context, piiID uuid.UUID, opts ...StoreOption) ([]byte, error) {
	export, err := e.Export(ctx, piiID, opts...)
	if err != nil {
		return nil, err
	}
	return json.MarshalIndent(export, "", "  ")
}

var _ json.Marshaler = (*SubjectExport)(nil)
