package pgstore

import (
	"context"
	"fmt"

	"github.com/lib/pq"
)

// GrantTenantBypass grants role Postgres's BYPASSRLS attribute, so any
// connection authenticated as role always sees every tenant's rows —
// regardless of WithTenantIsolation's policy, and regardless of whether a
// given session ever calls SET app.tenant_id (i.e. whether the Go code
// happened to pass es.WithTenant or es.WithAllTenants). This is the
// role-based counterpart to es.WithAllTenants: that option only documents
// intent in application code and in the DB session state for a given call;
// it doesn't stop a *different* piece of code from accidentally seeing
// cross-tenant data if it forgets to scope itself. A dedicated operator/
// system role with BYPASSRLS is enforced by Postgres itself, independent of
// what any application code does or forgets to do.
//
// Requires the connected role to have privileges to alter role — normally
// superuser, or (Postgres 16+) CREATEROLE with ADMIN OPTION on role.
// role is validated as a SQL identifier (quoted via pq.QuoteIdentifier)
// since Postgres has no parameter placeholder for identifiers in DDL.
//
// Note this only matters if WithTenantIsolation was passed to Migrate at
// all — without RLS enabled, the WHERE tenant_id filtering Commit/Load/
// FetchAfter already do in Go is the only enforcement, and any role can
// already see every tenant simply by calling them with es.WithAllTenants
// (or without es.WithTenant at all).
func (s *Store) GrantTenantBypass(ctx context.Context, role string) error {
	stmt := "ALTER ROLE " + pq.QuoteIdentifier(role) + " BYPASSRLS"
	if _, err := s.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("pgstore: grant tenant bypass to %q: %w", role, err)
	}
	return nil
}

// RevokeTenantBypass removes role's BYPASSRLS attribute (NOBYPASSRLS),
// undoing GrantTenantBypass. Existing connections authenticated as role
// keep whichever behavior their session already had — Postgres checks role
// attributes at authentication, not per-query — so this only affects
// connections role opens after the revoke.
func (s *Store) RevokeTenantBypass(ctx context.Context, role string) error {
	stmt := "ALTER ROLE " + pq.QuoteIdentifier(role) + " NOBYPASSRLS"
	if _, err := s.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("pgstore: revoke tenant bypass from %q: %w", role, err)
	}
	return nil
}
