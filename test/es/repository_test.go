package es_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/es/memstore"
)

// --- a tiny aggregate used only by these tests ---

type SubscriptionActivated struct {
	PlanID string `json:"plan_id"`
}

func (SubscriptionActivated) EventName() string { return "SubscriptionActivated" }

type SubscriptionCancelled struct{}

func (SubscriptionCancelled) EventName() string { return "SubscriptionCancelled" }

type Subscription struct {
	es.BaseAggregate
	PlanID string
	Status string
}

func (s *Subscription) Apply(event any) error {
	switch e := event.(type) {
	case *SubscriptionActivated:
		if s.Status == "active" {
			return errors.New("subscription already active")
		}
		s.PlanID = e.PlanID
		s.Status = "active"
	case *SubscriptionCancelled:
		if s.Status != "active" {
			return errors.New("subscription not active")
		}
		s.Status = "cancelled"
	default:
		return fmt.Errorf("subscription: unknown event %T", event)
	}
	return nil
}

func (s *Subscription) Activate(planID string) error {
	return s.RecordThat(s, &SubscriptionActivated{PlanID: planID})
}

func (s *Subscription) Cancel() error {
	return s.RecordThat(s, &SubscriptionCancelled{})
}

func newRegistry() es.EventRegistry {
	r := es.NewRegistry()
	r.Register("SubscriptionActivated", func() any { return &SubscriptionActivated{} })
	r.Register("SubscriptionCancelled", func() any { return &SubscriptionCancelled{} })
	return r
}

func newRepo(store *memstore.Store) *es.Repository[*Subscription] {
	return es.NewRepository(store, newRegistry(), func() *Subscription { return &Subscription{} })
}

func TestRepository_SaveAndLoad(t *testing.T) {
	ctx := context.Background()
	store := memstore.New()
	repo := newRepo(store)

	id := uuid.New()
	sub := &Subscription{}
	sub.SetID(id)

	if err := sub.Activate("pro"); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if err := repo.Save(ctx, sub); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := repo.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Status != "active" || loaded.PlanID != "pro" {
		t.Fatalf("unexpected state after replay: %+v", loaded)
	}
	if loaded.GetVersion() != 1 {
		t.Fatalf("expected version 1, got %d", loaded.GetVersion())
	}

	if err := loaded.Cancel(); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := repo.Save(ctx, loaded); err != nil {
		t.Fatalf("Save (2nd): %v", err)
	}

	reloaded, err := repo.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load (2nd): %v", err)
	}
	if reloaded.Status != "cancelled" {
		t.Fatalf("expected cancelled, got %q", reloaded.Status)
	}
	if reloaded.GetVersion() != 2 {
		t.Fatalf("expected version 2, got %d", reloaded.GetVersion())
	}
}

func TestRepository_LoadNotFound(t *testing.T) {
	store := memstore.New()
	repo := newRepo(store)

	_, err := repo.Load(context.Background(), uuid.New())
	if !errors.Is(err, es.ErrAggregateNotFound) {
		t.Fatalf("expected ErrAggregateNotFound, got %v", err)
	}
}

func TestRepository_ConcurrencyConflict(t *testing.T) {
	ctx := context.Background()
	store := memstore.New()
	repo := newRepo(store)

	id := uuid.New()
	seed := &Subscription{}
	seed.SetID(id)
	if err := seed.Activate("pro"); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if err := repo.Save(ctx, seed); err != nil {
		t.Fatalf("Save seed: %v", err)
	}

	first, err := repo.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load first: %v", err)
	}
	second, err := repo.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load second: %v", err)
	}

	if err := first.Cancel(); err != nil {
		t.Fatalf("Cancel first: %v", err)
	}
	if err := repo.Save(ctx, first); err != nil {
		t.Fatalf("Save first: %v", err)
	}

	if err := second.Cancel(); err != nil {
		t.Fatalf("Cancel second: %v", err)
	}
	err = repo.Save(ctx, second)
	if !errors.Is(err, es.ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict, got %v", err)
	}
}

func TestStore_FetchAfterAndCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := memstore.New()
	repo := newRepo(store)

	id := uuid.New()
	sub := &Subscription{}
	sub.SetID(id)
	_ = sub.Activate("pro")
	if err := repo.Save(ctx, sub); err != nil {
		t.Fatalf("Save: %v", err)
	}

	last, err := store.Get(ctx, "projector:subscriptions")
	if err != nil {
		t.Fatalf("Get checkpoint: %v", err)
	}
	if last != 0 {
		t.Fatalf("expected fresh checkpoint to be 0, got %d", last)
	}

	events, err := store.FetchAfter(ctx, last, 10)
	if err != nil {
		t.Fatalf("FetchAfter: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	if err := store.Save(ctx, "projector:subscriptions", events[len(events)-1].GlobalID); err != nil {
		t.Fatalf("Save checkpoint: %v", err)
	}
	last, err = store.Get(ctx, "projector:subscriptions")
	if err != nil {
		t.Fatalf("Get checkpoint (2nd): %v", err)
	}
	if last != events[0].GlobalID {
		t.Fatalf("expected checkpoint %d, got %d", events[0].GlobalID, last)
	}
}
