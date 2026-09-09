package es_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"go-es-light/es"
)

func TestRecordThat_ApplyError(t *testing.T) {
	sub := &Subscription{}
	sub.SetID(uuid.New())

	if err := sub.Activate("pro"); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if got := sub.GetVersion(); got != 1 {
		t.Fatalf("expected version 1 after activation, got %d", got)
	}

	// Activating an already-active subscription makes Apply return an
	// error; RecordThat must roll back the version bump and record no event.
	err := sub.Activate("pro")
	if err == nil {
		t.Fatal("expected error activating an already-active subscription")
	}
	if got := sub.GetVersion(); got != 1 {
		t.Fatalf("version must be rolled back on Apply error, got %d", got)
	}
	if got := len(sub.GetUncommittedEvents()); got != 1 {
		t.Fatalf("expected no new uncommitted event to be recorded, got %d", got)
	}
}

// unmarshalableEvent implements es.Event but cannot be marshalled to JSON,
// letting us exercise RecordThat's marshal-failure branch.
type unmarshalableEvent struct {
	Ch chan int
}

func (unmarshalableEvent) EventName() string { return "Unmarshalable" }

// permissiveAggregate accepts any event in Apply, so RecordThat gets past
// the Apply call and reaches json.Marshal.
type permissiveAggregate struct {
	es.BaseAggregate
}

func (p *permissiveAggregate) Apply(any) error { return nil }

func TestRecordThat_MarshalError(t *testing.T) {
	agg := &permissiveAggregate{}
	agg.SetID(uuid.New())

	err := agg.RecordThat(agg, unmarshalableEvent{Ch: make(chan int)})
	if err == nil {
		t.Fatal("expected marshal error")
	}
	if !strings.Contains(err.Error(), "marshal payload") {
		t.Fatalf("expected marshal error message, got %v", err)
	}
	if got := agg.GetVersion(); got != 0 {
		t.Fatalf("version must be rolled back on marshal error, got %d", got)
	}
	if got := len(agg.GetUncommittedEvents()); got != 0 {
		t.Fatalf("expected no uncommitted event, got %d", got)
	}
}

func TestRecordThat_DefaultsWithoutOptions(t *testing.T) {
	sub := &Subscription{}
	sub.SetID(uuid.New())

	before := time.Now()
	if err := sub.Activate("pro"); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	e := sub.GetUncommittedEvents()[0]
	if e.Version != 1 {
		t.Fatalf("expected default event schema version 1, got %d", e.Version)
	}
	if e.ID == uuid.Nil {
		t.Fatal("expected a generated event ID, got the nil UUID")
	}
	if e.OccurredAt.Before(before) {
		t.Fatalf("expected OccurredAt to default to roughly now, got %v (before %v)", e.OccurredAt, before)
	}
}

func TestRecordThat_WithOptionsOverridesEnvelope(t *testing.T) {
	sub := &Subscription{}
	sub.SetID(uuid.New())

	fixedID := uuid.New()
	fixedTime := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)

	err := sub.RecordThat(sub, &SubscriptionActivated{PlanID: "pro"},
		es.WithEventVersion(3),
		es.WithOccurredAt(fixedTime),
		es.WithEventID(fixedID),
	)
	if err != nil {
		t.Fatalf("RecordThat: %v", err)
	}

	e := sub.GetUncommittedEvents()[0]
	if e.Version != 3 {
		t.Fatalf("expected overridden event schema version 3, got %d", e.Version)
	}
	if !e.OccurredAt.Equal(fixedTime) {
		t.Fatalf("expected overridden OccurredAt %v, got %v", fixedTime, e.OccurredAt)
	}
	if e.ID != fixedID {
		t.Fatalf("expected overridden ID %v, got %v", fixedID, e.ID)
	}
	// Envelope fields RecordOption is not meant to touch stay derived from
	// the aggregate, regardless of options passed.
	if e.AggregateID != sub.GetID() {
		t.Fatalf("expected AggregateID %v, got %v", sub.GetID(), e.AggregateID)
	}
	if e.AggregateVersion != sub.GetVersion() {
		t.Fatalf("expected AggregateVersion %d, got %d", sub.GetVersion(), e.AggregateVersion)
	}
}

func newDomainEvent(aggID uuid.UUID, version uint64, name string, payload string) *es.DomainEvent {
	return &es.DomainEvent{
		ID:               uuid.New(),
		AggregateID:      aggID,
		AggregateVersion: version,
		Version:          1,
		Name:             name,
		Payload:          json.RawMessage(payload),
		OccurredAt:       time.Now(),
	}
}

func TestLoadFromHistory_UnknownFactory(t *testing.T) {
	sub := &Subscription{}
	aggID := uuid.New()
	sub.SetID(aggID)

	events := []*es.DomainEvent{newDomainEvent(aggID, 1, "NoSuchEvent", `{}`)}

	err := es.LoadFromHistory(sub, events, newRegistry())
	if err == nil || !strings.Contains(err.Error(), "no factory registered") {
		t.Fatalf("expected unknown-factory error, got %v", err)
	}
}

func TestLoadFromHistory_UnmarshalError(t *testing.T) {
	sub := &Subscription{}
	aggID := uuid.New()
	sub.SetID(aggID)

	// SubscriptionActivated.PlanID is a string; feeding it a JSON number
	// makes json.Unmarshal fail.
	events := []*es.DomainEvent{newDomainEvent(aggID, 1, "SubscriptionActivated", `{"plan_id": 42}`)}

	err := es.LoadFromHistory(sub, events, newRegistry())
	if err == nil || !strings.Contains(err.Error(), "unmarshal payload") {
		t.Fatalf("expected unmarshal error, got %v", err)
	}
}

func TestLoadFromHistory_ApplyError(t *testing.T) {
	sub := &Subscription{}
	aggID := uuid.New()
	sub.SetID(aggID)

	// Two activations in a row: the second Apply call fails because the
	// subscription is already active.
	events := []*es.DomainEvent{
		newDomainEvent(aggID, 1, "SubscriptionActivated", `{"plan_id":"pro"}`),
		newDomainEvent(aggID, 2, "SubscriptionActivated", `{"plan_id":"pro"}`),
	}

	err := es.LoadFromHistory(sub, events, newRegistry())
	if err == nil || !strings.Contains(err.Error(), "replay") {
		t.Fatalf("expected replay error, got %v", err)
	}
}

func TestRepository_Load_StoreError(t *testing.T) {
	wantErr := errors.New("boom: transport failure")
	repo := es.NewRepository[*Subscription](
		&fakeStore{loadErr: wantErr},
		newRegistry(),
		func() *Subscription { return &Subscription{} },
	)

	_, err := repo.Load(t.Context(), uuid.New())
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected store error to propagate, got %v", err)
	}
}

func TestRepository_Load_EmptyEventsWithoutError(t *testing.T) {
	// A store that returns an empty slice without ErrAggregateNotFound
	// (technically off-contract, but Repository defends against it anyway).
	repo := es.NewRepository[*Subscription](&fakeStore{}, newRegistry(), func() *Subscription { return &Subscription{} })

	_, err := repo.Load(t.Context(), uuid.New())
	if !errors.Is(err, es.ErrAggregateNotFound) {
		t.Fatalf("expected ErrAggregateNotFound, got %v", err)
	}
}

func TestRepository_Load_ReplayError(t *testing.T) {
	aggID := uuid.New()
	store := &fakeStore{loadEvents: []*es.DomainEvent{
		newDomainEvent(aggID, 1, "NoSuchEvent", `{}`),
	}}
	repo := es.NewRepository[*Subscription](store, newRegistry(), func() *Subscription { return &Subscription{} })

	_, err := repo.Load(t.Context(), aggID)
	if err == nil || !strings.Contains(err.Error(), "no factory registered") {
		t.Fatalf("expected replay error to propagate, got %v", err)
	}
}

func TestRepository_Save_NoOpWhenNoUncommittedEvents(t *testing.T) {
	store := &fakeStore{}
	repo := es.NewRepository[*Subscription](store, newRegistry(), func() *Subscription { return &Subscription{} })

	sub := &Subscription{}
	sub.SetID(uuid.New())

	if err := repo.Save(t.Context(), sub); err != nil {
		t.Fatalf("Save with no uncommitted events: %v", err)
	}
	if store.commitCalled {
		t.Fatal("Commit must not be called when there are no uncommitted events")
	}
}
