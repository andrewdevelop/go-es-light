package app

import (
	"github.com/google/uuid"

	"go-es-light/examples/user/domain"
)

// Commands are plain data — what an inbound adapter (HTTP handler, CLI,
// message consumer, ...) hands to UserService. None of them know how the
// command got here.
//
// TenantID is on every command because it's how UserService scopes every
// Repository.Load/Save call (see es.WithTenant), and Actor because it's
// what gets attributed on the resulting event (see es.WithActor) — a real
// inbound adapter would normally derive both from request auth (a JWT
// claim, a subdomain, a session's user id, ...), not accept them as raw
// fields, but this example has no real transport layer to pull them from.

type RegisterUserCommand struct {
	TenantID uuid.UUID
	UserID   uuid.UUID
	Email    string
	Name     string
	Actor    domain.Actor
}

type ChangeEmailCommand struct {
	TenantID uuid.UUID
	UserID   uuid.UUID
	Email    string
	Actor    domain.Actor
}

type ChangeNameCommand struct {
	TenantID uuid.UUID
	UserID   uuid.UUID
	Name     string
}

// ForgetUserCommand represents a data-subject erasure request (GDPR/CCPA
// "right to be forgotten"). See domain.User.RequestErasure. Actor is
// typically an admin/support agent processing the request, not the user.
type ForgetUserCommand struct {
	TenantID uuid.UUID
	UserID   uuid.UUID
	Actor    domain.Actor
}
