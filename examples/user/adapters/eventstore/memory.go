// Package eventstore is the driven adapter for persistence: it wires
// memstore (an es.EventStore) together with domain's event registry into
// the UserRepository port app.UserService depends on. Swap this file for
// one built on es/pgstore and nothing above it (app, domain) has to change.
package eventstore

import (
	"go-es-light/es"
	"go-es-light/es/memstore"
	"go-es-light/examples/user/app"
	"go-es-light/examples/user/domain"
)

// New returns a ready-to-use UserRepository plus the underlying store, so
// callers can also subscribe to its event stream (see main.go's projector).
func New() (*app.UserRepository, *memstore.Store) {
	store := memstore.New()
	repo := es.NewRepository[*domain.User](store, domain.Events(), func() *domain.User { return &domain.User{} })
	return repo, store
}
