// Package domain is the hexagon's core: the User aggregate, its events and
// business rules. It imports nothing from app or adapters — everything
// points inward, per the dependency rule.
package domain

import "go-es-light/es"

// --- domain events ---
//
// Event names use dot notation (aggregate.action) so an es.EventDispatcher
// subscriber can match either one exactly or every user.* event with a
// single "user.*" pattern subscription (see main.go).

const (
	// AllUserEvents is the dot-notation prefix wildcard matching every
	// event name above; hand it to EventDispatcher.Subscribe.
	AllUserEvents      = "user.*"
	UserRegistered     = "user.registered"
	UserEmailChanged   = "user.email_changed"
	UserNameChanged    = "user.name_changed"
	UserErasureRequest = "user.erasure_requested"
)

type UserRegisteredEvent struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

func (UserRegisteredEvent) EventName() string { return UserRegistered }

// PiiFields implements es.ContainsPersonalData: Email is the personal data
// this event carries, so the es.PiiEventStore wrapping the store (see
// main.go's composition root) encrypts it at rest and decrypts it again on
// read.
func (UserRegisteredEvent) PiiFields() map[string][]string {
	return map[string][]string{"payload": {"email"}}
}

type UserEmailChangedEvent struct {
	Email string `json:"email"`
}

func (UserEmailChangedEvent) EventName() string { return UserEmailChanged }

// PiiFields — see UserRegisteredEvent.PiiFields.
func (UserEmailChangedEvent) PiiFields() map[string][]string {
	return map[string][]string{"payload": {"email"}}
}

type UserNameChangedEvent struct {
	Name string `json:"name"`
}

func (UserNameChangedEvent) EventName() string { return UserNameChanged }

// UserErasureRequestedEvent models a data subject asking to be forgotten
// (see domain.User.RequestErasure). It carries no payload of its own — the
// DomainEvent.PiiID set via es.WithPiiID when it's recorded already
// identifies which subject — and implements es.RequestsErasure, which is
// what tells the es.PiiEventStore wrapping the store to crypto-shred that
// subject's key right after this event commits.
type UserErasureRequestedEvent struct{}

func (UserErasureRequestedEvent) EventName() string { return UserErasureRequest }
func (UserErasureRequestedEvent) RequestsErasure()  {}

// Events returns the registry es.Repository needs to turn a stored payload
// back into one of the concrete event types above when replaying history.
// It lives in domain because only domain knows its own event shapes.
func Events() es.EventRegistry {
	r := es.NewRegistry()
	r.Register(UserRegistered, func() any { return &UserRegisteredEvent{} })
	r.Register(UserEmailChanged, func() any { return &UserEmailChangedEvent{} })
	r.Register(UserNameChanged, func() any { return &UserNameChangedEvent{} })
	r.Register(UserErasureRequest, func() any { return &UserErasureRequestedEvent{} })
	return r
}
