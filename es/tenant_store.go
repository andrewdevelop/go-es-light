package es

import (
	"context"

	"github.com/google/uuid"
)

// TenantScopedStore wraps an EventStore, permanently binding every call to
// tenantID: Commit, Load, FetchAfter and StreamAll all get
// WithTenant(tenantID) appended after any options the caller passed, so
// the bound tenant always wins and can't be silently overridden or
// forgotten.
//
// Meant for a worker that only ever operates within a single tenant — a
// per-tenant projector, a per-tenant background job — the deployment shape
// this package's own docs point to for a real multitenant read model (one
// projector per tenant, each running its own store.StreamAll). Without
// this, every single call site in that worker has to remember to pass
// es.WithTenant(tenantID) itself; get it right everywhere but one call, and
// that one call silently processes every tenant's events instead of just
// its own.
//
// This complements WithTenant/WithAllTenants, it doesn't replace them:
// anything whose scope genuinely varies per call — an operator console, a
// Repository serving requests for different tenants over its lifetime —
// still needs the per-call option, since GRASP's Information Expert points
// at each EventStore implementation (not a wrapper) as the right place to
// actually perform tenant filtering — pgstore needs its own SQL WHERE
// clause and, if enabled, real RLS enforcement; a generic wrapper around
// EventStore could at best re-filter after the fact, losing both the index
// and the RLS guarantee. TenantScopedStore doesn't attempt that: it's pure
// convenience/safety for the fixed-scope case, layered on top of the
// filtering the underlying store already does correctly.
type TenantScopedStore struct {
	store    EventStore
	tenantID uuid.UUID
}

// NewTenantScopedStore returns a TenantScopedStore bound to tenantID.
func NewTenantScopedStore(store EventStore, tenantID uuid.UUID) *TenantScopedStore {
	return &TenantScopedStore{store: store, tenantID: tenantID}
}

// TenantID returns the tenant this store is permanently bound to.
func (s *TenantScopedStore) TenantID() uuid.UUID {
	return s.tenantID
}

// withTenant returns a copy of opts with WithTenant(s.tenantID) appended
// last, so it's applied after (and so overrides) anything the caller
// passed — including an accidental or malicious WithTenant(other) /
// WithAllTenants() from further up the call stack.
func (s *TenantScopedStore) withTenant(opts []StoreOption) []StoreOption {
	bound := make([]StoreOption, 0, len(opts)+1)
	bound = append(bound, opts...)
	return append(bound, WithTenant(s.tenantID))
}

// Commit implements EventStore, always scoped to TenantID().
func (s *TenantScopedStore) Commit(ctx context.Context, events []*DomainEvent, expectedVersion uint64, opts ...StoreOption) error {
	return s.store.Commit(ctx, events, expectedVersion, s.withTenant(opts)...)
}

// Load implements EventStore, always scoped to TenantID().
func (s *TenantScopedStore) Load(ctx context.Context, id uuid.UUID, opts ...StoreOption) ([]*DomainEvent, error) {
	return s.store.Load(ctx, id, s.withTenant(opts)...)
}

// FetchAfter implements EventStore, always scoped to TenantID().
func (s *TenantScopedStore) FetchAfter(ctx context.Context, lastID uint64, limit int, opts ...StoreOption) ([]*DomainEvent, error) {
	return s.store.FetchAfter(ctx, lastID, limit, s.withTenant(opts)...)
}

// StreamAll implements EventStore, always scoped to TenantID().
func (s *TenantScopedStore) StreamAll(ctx context.Context, opts ...StoreOption) (<-chan *DomainEvent, <-chan error) {
	return s.store.StreamAll(ctx, s.withTenant(opts)...)
}

// FetchByPiiID implements es.PiiLookup, always scoped to TenantID() — a
// per-tenant worker answering "everything this tenant holds about a subject"
// is exactly the fixed-scope case TenantScopedStore exists for.
func (s *TenantScopedStore) FetchByPiiID(ctx context.Context, piiID uuid.UUID, opts ...StoreOption) ([]*DomainEvent, error) {
	lookup, ok := s.store.(PiiLookup)
	if !ok {
		return nil, ErrPiiLookupUnsupported
	}
	return lookup.FetchByPiiID(ctx, piiID, s.withTenant(opts)...)
}

var _ EventStore = (*TenantScopedStore)(nil)
var _ PiiLookup = (*TenantScopedStore)(nil)
