package es_test

import (
	"context"

	"github.com/google/uuid"

	"go-es-light/es"
)

// fakeStore is a minimal, fully-configurable es.EventStore used to exercise
// error paths in Repository that memstore has no reason to ever produce
// (e.g. a transport error unrelated to concurrency or missing data).
type fakeStore struct {
	loadEvents []*es.DomainEvent
	loadErr    error

	commitCalled bool
	commitErr    error
}

func (f *fakeStore) Commit(_ context.Context, _ []*es.DomainEvent, _ uint64) error {
	f.commitCalled = true
	return f.commitErr
}

func (f *fakeStore) Load(_ context.Context, _ uuid.UUID) ([]*es.DomainEvent, error) {
	return f.loadEvents, f.loadErr
}

func (f *fakeStore) FetchAfter(_ context.Context, _ uint64, _ int) ([]*es.DomainEvent, error) {
	return nil, nil
}

func (f *fakeStore) StreamAll(_ context.Context) (<-chan *es.DomainEvent, <-chan error) {
	out := make(chan *es.DomainEvent)
	errc := make(chan error)
	close(out)
	close(errc)
	return out, errc
}
