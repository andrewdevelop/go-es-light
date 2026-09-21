package es_test

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/es/memstore"
)

// TestSubjectExport_MarshalJSON_Shape proves the DSAR document shape:
// grouped by (tenant, aggregate) with the full event envelope (id, versions,
// actor, metadata, opened payload), for a live subject's export.
func TestSubjectExport_MarshalJSON_Shape(t *testing.T) {
	ctx := context.Background()
	store := es.NewPiiEventStore(memstore.New(), newPiiRegistry(), es.NewPiiAnonymizer(memstore.NewKeyRing()))

	pii := uuid.New()
	agg := uuid.New()
	tenant := uuid.New()
	actorID := uuid.New()
	actorType := "customer"
	event := &es.DomainEvent{
		ID:               uuid.New(),
		TenantID:         tenant,
		AggregateID:      agg,
		AggregateVersion: 1,
		Version:          1,
		Name:             "PersonRegistered",
		PiiID:            &pii,
		ActorID:          &actorID,
		ActorType:        &actorType,
		Metadata:         json.RawMessage(`{"channel":"web"}`),
		Payload:          mustJSON(t, PersonRegistered{Email: "ann@example.com", Name: "Ann"}),
	}
	if err := store.Commit(ctx, []*es.DomainEvent{event}, 0, es.WithTenant(tenant)); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	doc, err := es.NewSubjectExporter(store).ExportJSON(ctx, pii, es.WithTenant(tenant))
	if err != nil {
		t.Fatalf("ExportJSON: %v", err)
	}

	var d struct {
		PiiID       uuid.UUID `json:"pii_id"`
		GeneratedAt string    `json:"generated_at"`
		Aggregates  []struct {
			TenantID    uuid.UUID `json:"tenant_id"`
			AggregateID uuid.UUID `json:"aggregate_id"`
			Events      []struct {
				Name             string          `json:"name"`
				ID               uuid.UUID       `json:"id"`
				Version          int             `json:"version"`
				AggregateVersion uint64          `json:"aggregate_version"`
				Actor            json.RawMessage `json:"actor"`
				Metadata         json.RawMessage `json:"metadata"`
				PiiUnrecoverable bool            `json:"pii_unrecoverable"`
				Payload          json.RawMessage `json:"payload"`
			} `json:"events"`
		} `json:"aggregates"`
	}
	if err := json.Unmarshal(doc, &d); err != nil {
		t.Fatalf("unmarshal doc: %v", err)
	}
	if d.PiiID != pii || d.GeneratedAt == "" {
		t.Fatalf("expected subject id and a generated_at timestamp, got %+v", d)
	}
	if len(d.Aggregates) != 1 || d.Aggregates[0].TenantID != tenant || d.Aggregates[0].AggregateID != agg {
		t.Fatalf("expected one aggregate group bound to the tenant, got %+v", d.Aggregates)
	}
	ev := d.Aggregates[0].Events[0]
	if ev.Name != "PersonRegistered" || ev.Version != 1 || ev.AggregateVersion != 1 || ev.ID == uuid.Nil {
		t.Fatalf("unexpected event envelope: %+v", ev)
	}
	if len(ev.Actor) == 0 || len(ev.Metadata) == 0 {
		t.Fatalf("expected actor and metadata to survive into the document, got %+v", ev)
	}
	if ev.PiiUnrecoverable {
		t.Fatal("expected a live subject's event not flagged unrecoverable")
	}
	var plain PersonRegistered
	if err := json.Unmarshal(ev.Payload, &plain); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if plain.Email != "ann@example.com" {
		t.Fatalf("expected the opened payload in the document, got %+v", plain)
	}
}

// TestSubjectExport_MarshalJSON_ErasedFlag proves an erased subject's export
// carries PiiUnrecoverable on the identity event instead of garbage, so a
// responder renders "no longer held readably" rather than emitting
// ciphertext.
func TestSubjectExport_MarshalJSON_ErasedFlag(t *testing.T) {
	ctx := context.Background()
	store := es.NewPiiEventStore(memstore.New(), newPiiRegistry(), es.NewPiiAnonymizer(memstore.NewKeyRing()))

	pii := uuid.New()
	agg := uuid.New()
	reg := &es.DomainEvent{
		ID:               uuid.New(),
		AggregateID:      agg,
		AggregateVersion: 1,
		Version:          1,
		Name:             "PersonRegistered",
		PiiID:            &pii,
		Payload:          mustJSON(t, PersonRegistered{Email: "ann@example.com", Name: "Ann"}),
	}
	if err := store.Commit(ctx, []*es.DomainEvent{reg}, 0); err != nil {
		t.Fatalf("Commit registered: %v", err)
	}
	er := &es.DomainEvent{
		ID:               uuid.New(),
		AggregateID:      agg,
		AggregateVersion: 2,
		Version:          1,
		Name:             "PersonForgotten",
		PiiID:            &pii,
		Payload:          []byte(`{}`),
	}
	if err := store.Commit(ctx, []*es.DomainEvent{er}, 1); err != nil {
		t.Fatalf("Commit erasure: %v", err)
	}

	doc, err := es.NewSubjectExporter(store).ExportJSON(ctx, pii)
	if err != nil {
		t.Fatalf("ExportJSON: %v", err)
	}
	var d struct {
		Aggregates []struct {
			Events []struct {
				Name             string          `json:"name"`
				PiiUnrecoverable bool            `json:"pii_unrecoverable"`
				Payload          json.RawMessage `json:"payload"`
			} `json:"events"`
		} `json:"aggregates"`
	}
	if err := json.Unmarshal(doc, &d); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	events := d.Aggregates[0].Events
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %+v", events)
	}
	if events[0].Name != "PersonRegistered" || !events[0].PiiUnrecoverable {
		t.Fatalf("expected the identity event flagged unrecoverable, got %+v", events[0])
	}
	// The payload is still present (unreadable ciphertext, not garbage), so a
	// responder can inspect the document without decoding it.
	if len(events[0].Payload) == 0 {
		t.Fatal("expected the payload to remain in the document")
	}
}

// TestSubjectExport_MarshalJSON_SameStateDeterministic marshals the same
// export twice and requires byte equality — the property an auditor
// comparing two exports of the same log state relies on. (GeneratedAt is
// stamped per marshaling, so this pins the *structure*, not that specific
// field.)
func TestSubjectExport_MarshalJSON_SameStateDeterministic(t *testing.T) {
	ctx := context.Background()
	store := es.NewPiiEventStore(memstore.New(), newPiiRegistry(), es.NewPiiAnonymizer(memstore.NewKeyRing()))

	pii := uuid.New()
	if err := store.Commit(ctx, []*es.DomainEvent{{
		ID:               uuid.New(),
		AggregateID:      uuid.New(),
		AggregateVersion: 1,
		Version:          1,
		Name:             "PersonRegistered",
		PiiID:            &pii,
		Payload:          mustJSON(t, PersonRegistered{Email: "ann@example.com", Name: "Ann"}),
	}}, 0); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	export, err := es.NewSubjectExporter(store).Export(ctx, pii)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	a, err := json.Marshal(export)
	if err != nil {
		t.Fatalf("marshal a: %v", err)
	}
	b, err := json.Marshal(export)
	if err != nil {
		t.Fatalf("marshal b: %v", err)
	}
	// Strip the per-marshal GeneratedAt stamp, then require byte identity:
	// the *structure* of the document depends only on the log state.
	re := regexp.MustCompile(`"generated_at": ?"[^"]*"`)
	if re.ReplaceAllString(string(a), "") != re.ReplaceAllString(string(b), "") {
		t.Fatal("expected two marshals of the same export to be identical apart from generated_at")
	}
}
