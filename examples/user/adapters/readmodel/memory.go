// Package readmodel is the driven adapter for app.ReadModel: an in-memory
// stand-in for what would be a SQL projection table in a real application
// (no SQL views here, per the example's scope). It also exports
// UserProjector, an es.EventListener that keeps a Store in sync — wire it
// into an es.EventDispatcher directly (see main.go), no factory needed.
package readmodel

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/examples/user/app"
	"go-es-light/examples/user/domain"
)

var _ app.ReadModel = (*Store)(nil)

type Store struct {
	mu    sync.RWMutex
	views map[uuid.UUID]app.UserView
}

func New() *Store {
	return &Store{views: make(map[uuid.UUID]app.UserView)}
}

func (s *Store) Upsert(view app.UserView) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.views[view.ID] = view
}

func (s *Store) Get(id uuid.UUID) (app.UserView, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.views[id]
	return v, ok
}

var _ es.EventListener = (*UserProjector)(nil)

// UserProjector is the projector listener for a Store: main.go declares
// &UserProjector{Store: views} straight at the dispatcher.Subscribe call,
// no builder method in between. Kind fixes its role — Projector, so its
// error is reported back to Dispatch, unlike a Reactor's (see
// adapters/notifier) — and Handle carries the actual logic.
//
// This projector has no idea PII sealing/opening even exists: it reads
// events off whatever es.EventStore main.go's dispatcher loop streamed them
// from, and that store (an es.PiiEventStore — see main.go's composition
// root) has already decrypted them before they ever reach Dispatch.
// Encryption-at-rest is the store's property, not something every consumer
// re-implements.
type UserProjector struct {
	Store *Store
}

func (p *UserProjector) Kind() es.ListenerKind { return es.Projector }

func (p *UserProjector) Handle(_ context.Context, event *es.DomainEvent) error {
	view, _ := p.Store.Get(event.AggregateID)
	view.ID = event.AggregateID
	view.Version = event.AggregateVersion

	switch event.Name {
	case domain.UserRegistered:
		var payload domain.UserRegisteredEvent
		if err := event.UnmarshalPayload(&payload); err != nil {
			return err
		}
		// event.PiiUnrecoverable is true if this subject's key was already
		// forgotten by the time this event reached Open — payload.Email is
		// then ciphertext, not an email, and storing it in Email as-is
		// would be wrong (worse yet if this view ever fed a column with an
		// email format check). A live worker rarely hits this — it usually
		// processes UserRegistered long before any later erasure — but a
		// read model rebuilt from scratch after one will, every time. See
		// examples/ledger/main.go for a worked demonstration.
		if event.PiiUnrecoverable {
			view.Email = "[erased]"
		} else {
			view.Email = payload.Email
		}
		view.Name = payload.Name
	case domain.UserEmailChanged:
		var payload domain.UserEmailChangedEvent
		if err := event.UnmarshalPayload(&payload); err != nil {
			return err
		}
		if event.PiiUnrecoverable {
			view.Email = "[erased]"
		} else {
			view.Email = payload.Email
		}
	case domain.UserNameChanged:
		var payload domain.UserNameChangedEvent
		if err := event.UnmarshalPayload(&payload); err != nil {
			return err
		}
		view.Name = payload.Name
	case domain.UserErasureRequest:
		// No view field changes here — the whole point of crypto-shredding
		// is that the read model (like every other consumer) just stops
		// being able to make sense of this subject's PII fields on any
		// *future* replay, without this projector needing special-case
		// logic for it.
	default:
		return fmt.Errorf("readmodel: unknown event %q", event.Name)
	}

	p.Store.Upsert(view)
	return nil
}
