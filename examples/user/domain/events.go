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
	AllUserEvents    = "user.*"
	UserRegistered   = "user.registered"
	UserEmailChanged = "user.email_changed"
	UserNameChanged  = "user.name_changed"
)

type UserRegisteredEvent struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

func (UserRegisteredEvent) EventName() string { return UserRegistered }

type UserEmailChangedEvent struct {
	Email string `json:"email"`
}

func (UserEmailChangedEvent) EventName() string { return UserEmailChanged }

type UserNameChangedEvent struct {
	Name string `json:"name"`
}

func (UserNameChangedEvent) EventName() string { return UserNameChanged }

// Events returns the registry es.Repository needs to turn a stored payload
// back into one of the concrete event types above when replaying history.
// It lives in domain because only domain knows its own event shapes.
func Events() es.EventRegistry {
	r := es.NewRegistry()
	r.Register(UserRegistered, func() any { return &UserRegisteredEvent{} })
	r.Register(UserEmailChanged, func() any { return &UserEmailChangedEvent{} })
	r.Register(UserNameChanged, func() any { return &UserNameChangedEvent{} })
	return r
}
