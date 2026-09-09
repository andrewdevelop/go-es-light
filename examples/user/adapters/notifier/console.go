// Package notifier is the driven adapter for app.Notifier: a stand-in for
// whatever would really send the welcome email (SMTP, a transactional-email
// API, ...). It just prints to stdout. It also exports
// WelcomeEmailReactor, an es.EventListener — wire it into an
// es.EventDispatcher directly (see main.go), no factory needed.
package notifier

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/examples/user/app"
	"go-es-light/examples/user/domain"
)

var _ app.Notifier = (*Console)(nil)

type Console struct{}

func New() *Console {
	return &Console{}
}

func (c *Console) NotifyRegistered(_ context.Context, id uuid.UUID, email string) error {
	fmt.Printf("[notifier] welcome email queued for %s (user %s)\n", email, id)
	return nil
}

var _ es.EventListener = (*WelcomeEmailReactor)(nil)

// WelcomeEmailReactor is the reactor listener for a Console: main.go
// declares &WelcomeEmailReactor{Notifier: welcomeEmails} straight at the
// dispatcher.Subscribe call, no builder method in between. Kind fixes its
// role — Reactor, so its error is only logged by the dispatcher, never
// propagated, and the whole class of reactors can be switched off via
// es.EventDispatcher.HandleSideEffects(false) (e.g. while replaying
// history, where welcome emails must not fire again) — and Handle carries
// the actual logic.
type WelcomeEmailReactor struct {
	Notifier *Console
}

func (r *WelcomeEmailReactor) Kind() es.ListenerKind { return es.Reactor }

func (r *WelcomeEmailReactor) Handle(ctx context.Context, event *es.DomainEvent) error {
	if event.Name != domain.UserRegistered {
		return nil
	}
	var payload domain.UserRegisteredEvent
	if err := event.UnmarshalPayload(&payload); err != nil {
		return err
	}
	return r.Notifier.NotifyRegistered(ctx, event.AggregateID, payload.Email)
}
