// Package app is the application layer: use cases (UserService) plus the
// ports they need from the outside world. It depends on domain but knows
// nothing about how those ports are actually implemented — that's for
// adapters to decide.
package app

import (
	"context"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/examples/user/domain"
)

// UserRepository is the driven port for persistence: es.Repository already
// is exactly that port (Load/Save over an EventStore), so there's nothing
// to add — this alias just names it the way the rest of this layer talks
// about it.
type UserRepository = es.Repository[*domain.User]

// UserView is the read model a query use case (or, in a real app, a GET
// handler) would read. No SQL here — see adapters/readmodel for the
// in-memory implementation.
type UserView struct {
	ID      uuid.UUID
	Email   string
	Name    string
	Version uint64 // aggregate version this view was built from — see es.EventNotifier
}

// ReadModel is the driven port the async projector writes to and a query
// use case reads from.
type ReadModel interface {
	Upsert(view UserView)
	Get(id uuid.UUID) (UserView, bool)
}

// Notifier is the driven port for the side effect of welcoming a newly
// registered user (send an email, call a partner API, ...). Kept separate
// from ReadModel because, per TASK.md, side effects have different
// ordering/failure semantics than projections (see es.Reactor).
type Notifier interface {
	NotifyRegistered(ctx context.Context, id uuid.UUID, email string) error
}
