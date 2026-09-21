package memstore

import (
	"context"

	"go-es-light/es"
)

// AdvisoryLock is a no-op es.AdvisoryLock. An in-memory Store only ever
// makes sense within a single process, so there's no second replica to
// coordinate with — TryAcquire always succeeds immediately, and Release is
// a no-op. Exists purely so code written against es.AdvisoryLock (e.g. a
// projector that runs behind leader election in production, via
// pgstore.AdvisoryLock) can run unchanged against memstore in tests.
type AdvisoryLock struct{}

// NewAdvisoryLock returns an AdvisoryLock. name is accepted for API
// symmetry with pgstore.NewAdvisoryLock but is otherwise unused — a no-op
// lock doesn't need a key.
func NewAdvisoryLock(_ string) *AdvisoryLock {
	return &AdvisoryLock{}
}

func (*AdvisoryLock) TryAcquire(_ context.Context) (bool, error) {
	return true, nil
}

func (*AdvisoryLock) Release(_ context.Context) error {
	return nil
}

var _ es.AdvisoryLock = (*AdvisoryLock)(nil)
