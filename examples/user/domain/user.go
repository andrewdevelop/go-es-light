package domain

import (
	"errors"
	"fmt"

	"github.com/google/uuid"

	"go-es-light/es"
)

// Actor identifies who performed the action that produced an event — see
// es.WithActor. ID/Type are deliberately unconstrained by this package
// (Type is caller-defined, e.g. "customer", "admin", "system"); an empty
// Actor{} means "no actor recorded," and Register/ChangeEmail/
// RequestErasure treat it that way — es.WithActor is only added to
// RecordThat when actor.ID is set.
type Actor struct {
	ID   uuid.UUID
	Type string
}

func (a Actor) isSet() bool { return a.ID != uuid.Nil }

var (
	ErrAlreadyRegistered = errors.New("user: already registered")
	ErrNotRegistered     = errors.New("user: not registered")
	ErrEmailRequired     = errors.New("user: email is required")
	ErrNameRequired      = errors.New("user: name is required")
)

// User is the aggregate: the only place its own business rules are
// enforced. Apply is where every event (live or replayed) mutates state;
// the exported methods (Register/ChangeEmail/ChangeName) are the only
// entry points a use case in package app is allowed to call.
type User struct {
	es.BaseAggregate
	Email string
	Name  string

	registered bool
}

func (u *User) Apply(event any) error {
	switch e := event.(type) {
	case *UserRegisteredEvent:
		if u.registered {
			return ErrAlreadyRegistered
		}
		u.Email = e.Email
		u.Name = e.Name
		u.registered = true
	case *UserEmailChangedEvent:
		if !u.registered {
			return ErrNotRegistered
		}
		u.Email = e.Email
	case *UserNameChangedEvent:
		if !u.registered {
			return ErrNotRegistered
		}
		u.Name = e.Name
	case *UserErasureRequestedEvent:
		if !u.registered {
			return ErrNotRegistered
		}
		// No state change: the erasure request is itself the domain fact
		// worth recording. What actually happens to Email (and any other
		// PII field) at rest is Repository.Save's job, driven by this
		// event implementing es.RequestsErasure — see events.go.
	default:
		return fmt.Errorf("user: unknown event %T", event)
	}
	return nil
}

// Register, ChangeEmail and RequestErasure all pass es.WithPiiID(u.GetID())
// to RecordThat: the user's own aggregate id doubles as the PII subject id,
// so every PII-bearing fact about this user (and, eventually, the request
// to forget it) keys into the same PiiAnonymizer/KeyRing entry. Each also
// passes es.WithActor(actor.ID, actor.Type) when actor is set, attributing
// the action to whoever performed it — the user themselves for a
// self-service signup, an admin for an erasure request actioned on the
// user's behalf, a "system" actor for something a background job did.

func (u *User) Register(email, name string, actor Actor) error {
	if email == "" {
		return ErrEmailRequired
	}
	if name == "" {
		return ErrNameRequired
	}
	opts := []es.RecordOption{es.WithPiiID(u.GetID())}
	if actor.isSet() {
		opts = append(opts, es.WithActor(actor.ID, actor.Type))
	}
	// WithMetadata is for ad-hoc, non-payload tags that don't warrant their
	// own event field — here, which channel the signup came through. In a
	// real app this would come from request context (a query param, a
	// referrer header, ...), not be hardcoded.
	opts = append(opts, es.WithMetadata(map[string]any{"signup_channel": "web"}))
	return u.RecordThat(u, &UserRegisteredEvent{Email: email, Name: name}, opts...)
}

func (u *User) ChangeEmail(email string, actor Actor) error {
	if email == "" {
		return ErrEmailRequired
	}
	opts := []es.RecordOption{es.WithPiiID(u.GetID())}
	if actor.isSet() {
		opts = append(opts, es.WithActor(actor.ID, actor.Type))
	}
	return u.RecordThat(u, &UserEmailChangedEvent{Email: email}, opts...)
}

func (u *User) ChangeName(name string) error {
	if name == "" {
		return ErrNameRequired
	}
	return u.RecordThat(u, &UserNameChangedEvent{Name: name})
}

// RequestErasure records that this user has asked to be forgotten (GDPR/CCPA
// "right to be forgotten"). See UserErasureRequestedEvent and
// es.PiiEventStore's erasure handling. actor is typically an admin or
// support agent processing the request on the user's behalf, not the user
// themselves — worth recording who actioned it, same as any other change.
func (u *User) RequestErasure(actor Actor) error {
	if !u.registered {
		return ErrNotRegistered
	}
	opts := []es.RecordOption{es.WithPiiID(u.GetID())}
	if actor.isSet() {
		opts = append(opts, es.WithActor(actor.ID, actor.Type))
	}
	return u.RecordThat(u, &UserErasureRequestedEvent{}, opts...)
}
