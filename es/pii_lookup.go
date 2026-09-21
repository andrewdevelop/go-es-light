package es

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

// ErrSubjectNotFound is returned by PiiLookup.FetchByPiiID (and so by
// SubjectExporter.Export) when no event in the store carries the given
// PiiID — the subject-centric counterpart of ErrAggregateNotFound. A caller
// answering an access / data-subject request can distinguish "this person
// has no data here" from "the query failed" by errors.Is on this.
var ErrSubjectNotFound = errors.New("es: no events found for the given PiiID")

// ErrPiiLookupUnsupported is returned by a delegating wrapper (PiiEventStore,
// TenantScopedStore) when the store it wraps doesn't implement PiiLookup —
// i.e. the wrapped store can't answer a subject-centric query at all.
var ErrPiiLookupUnsupported = errors.New("es: underlying store does not implement PiiLookup")

// PiiLookup is the optional subject-centric complement to EventStore: where
// EventStore's four methods are aggregate- or global-cursor-oriented,
// PiiLookup answers the one question a data-subject request (access/erasure,
// GDPR Art. 15/17, Law 25, CCPA, ...) needs — "everything the log holds
// about this one PiiID", across all aggregates and tenants.
//
// Events come back ordered by GlobalID ascending (the order they were
// committed in), with the same optional scoping semantics as every other
// read: pass es.WithTenant to restrict to a single tenant plus any
// es.GlobalTenantID events, or nothing/es.WithAllTenants to search the
// whole log. The returned events' PII fields are whatever the store itself
// produces — wrap the store with PiiEventStore to get them opened (or
// flagged PiiUnrecoverable post-erasure) on the way back.
//
// It's a separate optional interface (like Checkpointer), not a fifth
// method on EventStore, because it needs a per-subject index: pii_id is a
// nullable, opt-in column, and implementations query it in SQL (or a map),
// exactly the argument TenantScopedStore's doc comment makes for tenant
// filtering — a generic wrapper could only post-filter, losing the index.
type PiiLookup interface {
	FetchByPiiID(ctx context.Context, piiID uuid.UUID, opts ...StoreOption) ([]*DomainEvent, error)
}

// AggregateHistory is one aggregate's slice of a data subject's file: every
// event recorded for that aggregate under that tenant that carries the
// subject's PiiID, in AggregateVersion order (which FetchByPiiID's GlobalID
// ordering implies, since both climb together per aggregate).
type AggregateHistory struct {
	TenantID    uuid.UUID
	AggregateID uuid.UUID
	Events      []*DomainEvent
}

// SubjectExport is a complete, subject-centric view of the log for one
// PiiID — the raw material of a data-subject access/portability response.
// Events are the same *DomainEvent values the store hands to any consumer,
// so PII payload fields are already opened (or flagged via
// PiiUnrecoverable/ErrPiiUnrecoverable if the subject's key has been
// crypto-shredded), and Actor/Metadata/Tenant/occurred_at are all intact.
type SubjectExport struct {
	PiiID      uuid.UUID
	Aggregates []AggregateHistory
}

// SubjectExporter produces SubjectExports over a PiiLookup-backed store. Hand
// it an es.PiiEventStore-wrapped store (so exported events arrive with PII
// fields opened) — typically also wrapped in a TenantScopedStore, or passed
// es.WithTenant per call, when scoping applies.
type SubjectExporter struct {
	lookup PiiLookup
}

// NewSubjectExporter returns a SubjectExporter over lookup. lookup should be
// a PiiEventStore-wrapped store; see SubjectExporter's doc comment.
func NewSubjectExporter(lookup PiiLookup) *SubjectExporter {
	return &SubjectExporter{lookup: lookup}
}

// Export gathers every event carrying piiID and groups it by
// (tenant, aggregate). Returns ErrSubjectNotFound if the log holds no event
// for this subject at all. opts is optional — see es.WithTenant /
// es.WithAllTenants.
func (e *SubjectExporter) Export(ctx context.Context, piiID uuid.UUID, opts ...StoreOption) (*SubjectExport, error) {
	events, err := e.lookup.FetchByPiiID(ctx, piiID, opts...)
	if err != nil {
		return nil, err
	}

	export := &SubjectExport{PiiID: piiID}
	firstSeen := map[[2]uuid.UUID]int{} // [tenant, aggregate] -> index in export.Aggregates

	for _, ev := range events {
		key := [2]uuid.UUID{ev.TenantID, ev.AggregateID}
		idx, ok := firstSeen[key]
		if !ok {
			idx = len(export.Aggregates)
			firstSeen[key] = idx
			export.Aggregates = append(export.Aggregates, AggregateHistory{
				TenantID:    ev.TenantID,
				AggregateID: ev.AggregateID,
			})
		}
		export.Aggregates[idx].Events = append(export.Aggregates[idx].Events, ev)
	}

	return export, nil
}
