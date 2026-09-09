package app

import "github.com/google/uuid"

// Commands are plain data — what an inbound adapter (HTTP handler, CLI,
// message consumer, ...) hands to UserService. None of them know how the
// command got here.

type RegisterUserCommand struct {
	UserID uuid.UUID
	Email  string
	Name   string
}

type ChangeEmailCommand struct {
	UserID uuid.UUID
	Email  string
}

type ChangeNameCommand struct {
	UserID uuid.UUID
	Name   string
}
