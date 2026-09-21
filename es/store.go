package es

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

// ErrConcurrencyConflict is returned by EventStore.Commit when the expected
// version does not match the version currently stored for the aggregate —
// someone else committed events for the same aggregate first.
var ErrConcurrencyConflict = errors.New("event store concurrency conflict")

// ErrAggregateNotFound is returned by EventStore.Load (and by Repository.Load)
// when no events exist for the given aggregate id.
var ErrAggregateNotFound = errors.New("aggregate not found")

// Applier is the interface an aggregate must implement to accept events,
// both when recording new ones and when replaying history.
type Applier interface {
	Apply(event any) error
	GetID() uuid.UUID
}

// Reconstitutable is the interface the Repository needs in order to load,
// replay history into, and persist an aggregate.
type Reconstitutable interface {
	Applier
	SetID(id uuid.UUID)
	SetVersion(v uint64)
	GetVersion() uint64
	GetUncommittedEvents() []*DomainEvent // <-- Pointer
	ClearEvents()
}

// GlobalTenantID is the reserved sentinel TenantID (the nil UUID, matching
// uuid.UUID's zero value) for aggregates that are intentionally shared
// across every tenant — a global catalog, a shared pricing feed, etc. It's
// what an event's TenantID already defaults to when es.WithTenant is never
// used, so genuinely tenant-less streams need no special handling: just
// don't pass WithTenant when recording/committing them. EventStore
// implementations that support multitenancy treat rows carrying this
// TenantID as readable regardless of which tenant a Load/FetchAfter/
// StreamAll call is scoped to via WithTenant.
var GlobalTenantID = uuid.Nil

// StoreConfig holds optional, cross-cutting settings recognized by
// EventStore implementations that support them. Its zero value means "no
// scoping" — single-tenant callers and EventStore implementations that
// don't support multitenancy can ignore StoreOption entirely.
type StoreConfig struct {
	// TenantID, when set, scopes a Commit/Load/FetchAfter/StreamAll call to
	// a single tenant. An implementation that supports multitenancy filters
	// reads by it and, for Commit, stamps it onto every event that doesn't
	// already carry one.
	TenantID *uuid.UUID

	// AllTenants marks a call as deliberately unscoped — an operator
	// console, a system migration script, a support tool — rather than a
	// tenant-facing code path that simply forgot to pass WithTenant. It has
	// no effect on filtering: omitting both WithTenant and WithAllTenants
	// already means "no scope, see everything," exactly like AllTenants
	// does. The reason to use it anyway is everything WithTenant already
	// gives ordinary calls — a greppable, self-documenting call site, and
	// (for pgstore specifically) an observable marker in the DB session
	// rather than silence indistinguishable from an oversight. See
	// WithAllTenants and, for the stronger role-based version of this same
	// idea, pgstore.Store.GrantTenantBypass.
	AllTenants bool
}

// StoreOption customizes a single EventStore call via StoreConfig. See
// WithTenant. EventStore implementations that don't recognize a given
// option are expected to ignore it rather than error, so options compose
// across implementations of differing capability.
type StoreOption func(*StoreConfig)

// WithTenant scopes a Commit/Load/FetchAfter/StreamAll call to tenantID.
// Entirely optional — omit it for single-tenant use; EventStore
// implementations without multitenancy support simply ignore it.
func WithTenant(tenantID uuid.UUID) StoreOption {
	return func(c *StoreConfig) { c.TenantID = &tenantID }
}

// WithAllTenants explicitly marks a call as intentionally unscoped —
// see StoreConfig.AllTenants for why you'd bother, since simply omitting
// WithTenant already behaves identically. Meant for operator/admin code
// paths (a support console, a backfill script, a cross-tenant report), not
// as something a tenant-facing request handler ever passes.
func WithAllTenants() StoreOption {
	return func(c *StoreConfig) { c.AllTenants = true }
}

// ResolveStoreConfig applies opts in order and returns the resulting
// StoreConfig. EventStore implementations call this to read the options
// passed to them.
func ResolveStoreConfig(opts ...StoreOption) StoreConfig {
	var c StoreConfig
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// EventStore is the append-only log of domain events. Implementations must
// guarantee that Commit is atomic: either all events are persisted or none
// are, and that expectedVersion is enforced (optimistic concurrency).
type EventStore interface {
	// Commit appends events for a single aggregate. expectedVersion is the
	// aggregate version the caller believes is currently persisted (i.e. the
	// version before any of the events in this batch were applied). If the
	// store holds a different version, ErrConcurrencyConflict is returned
	// and nothing is persisted. opts is optional — see WithTenant.
	Commit(ctx context.Context, events []*DomainEvent, expectedVersion uint64, opts ...StoreOption) error

	// Load returns all events for the given aggregate, ordered by
	// AggregateVersion ascending. Returns ErrAggregateNotFound if none
	// exist (or none exist within the tenant scope given via WithTenant, if
	// any). opts is optional — see WithTenant.
	Load(ctx context.Context, id uuid.UUID, opts ...StoreOption) ([]*DomainEvent, error)

	// FetchAfter returns up to limit events with GlobalID > lastID, ordered
	// by GlobalID ascending. Used by projectors/pollers to catch up. opts is
	// optional — see WithTenant.
	FetchAfter(ctx context.Context, lastID uint64, limit int, opts ...StoreOption) ([]*DomainEvent, error)

	// StreamAll pushes every event as it is committed (and, depending on the
	// implementation, backfills history first) onto the returned channel.
	// The error channel receives at most one error before both channels are
	// closed. Callers must drain both channels and cancel ctx to stop. opts
	// is optional — see WithTenant.
	StreamAll(ctx context.Context, opts ...StoreOption) (<-chan *DomainEvent, <-chan error)
}

// Checkpointer stores the progress of a worker (projector, side-effect
// relay, ...) so it can resume from where it left off after a restart.
type Checkpointer interface {
	Get(ctx context.Context, name string) (uint64, error)
	Save(ctx context.Context, name string, lastID uint64) error
}
