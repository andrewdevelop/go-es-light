package domain

import (
	"errors"
	"fmt"

	"go-es-light/es"
)

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
	default:
		return fmt.Errorf("user: unknown event %T", event)
	}
	return nil
}

func (u *User) Register(email, name string) error {
	if email == "" {
		return ErrEmailRequired
	}
	if name == "" {
		return ErrNameRequired
	}
	return u.RecordThat(u, &UserRegisteredEvent{Email: email, Name: name})
}

func (u *User) ChangeEmail(email string) error {
	if email == "" {
		return ErrEmailRequired
	}
	return u.RecordThat(u, &UserEmailChangedEvent{Email: email})
}

func (u *User) ChangeName(name string) error {
	if name == "" {
		return ErrNameRequired
	}
	return u.RecordThat(u, &UserNameChangedEvent{Name: name})
}
