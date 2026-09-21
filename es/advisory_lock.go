package es

import "context"

// AdvisoryLock elects a single leader among several replicas of the same
// worker (e.g. one projector instance per read-model), so exactly one
// replica ever builds a given read model at a time. Implementations vary
// per backend — see pgstore.AdvisoryLock (a real, cross-process Postgres
// advisory lock) and memstore.AdvisoryLock (a no-op: an in-memory Store
// only ever makes sense within a single process, so there's no second
// replica to coordinate with).
type AdvisoryLock interface {
	// TryAcquire attempts to become leader without blocking. false, nil
	// means another replica already holds the lock — the caller should
	// retry later.
	TryAcquire(ctx context.Context) (bool, error)

	// Release gives up leadership. Safe to call even if TryAcquire was
	// never called or already failed.
	Release(ctx context.Context) error
}
