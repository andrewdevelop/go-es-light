// Command user is a self-contained, runnable walk-through of registering
// and updating a user, structured like a real hexagonal-architecture
// application (minus an actual router and a real SQL read model — the
// read model here is an in-memory stand-in, see adapters/readmodel):
//
//	domain/               - the User aggregate, its events, its business rules
//	app/                  - use cases (UserService) + the ports they need
//	adapters/eventstore/  - driven adapter: persistence (memstore-backed)
//	adapters/readmodel/   - driven adapter: async read model (app.ReadModel)
//	adapters/notifier/    - driven adapter: side effect (app.Notifier)
//	main.go               - composition root; also stands in for the
//	                        driving/inbound adapter (HTTP/gRPC/CLI would
//	                        normally call app.UserService the same way)
//
// The dependency rule holds throughout: domain depends on nothing here,
// app depends only on domain, and every adapter depends inward on app (and,
// where it needs event shapes, on domain) — never the reverse.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/examples/user/adapters/eventstore"
	"go-es-light/examples/user/adapters/notifier"
	"go-es-light/examples/user/adapters/readmodel"
	"go-es-light/examples/user/app"
	"go-es-light/examples/user/domain"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- composition root: wire driven adapters behind their ports ---
	repo, store := eventstore.New()
	views := readmodel.New()
	welcomeEmails := notifier.New()

	userReadModelProjector := &readmodel.UserProjector{Store: views}
	userEmailReactor := &notifier.WelcomeEmailReactor{Notifier: welcomeEmails}

	// projectionReady lets a caller wait for the read model to actually
	// reflect a given write instead of guessing with a fixed sleep —
	// without making the write itself synchronous. Wired in as a
	// NotifierListener rather than called by hand from the projector loop
	// below: building the read model and waking up waiters are two
	// separate responsibilities, and the loop's only job stays "run
	// whatever's subscribed". Subscribed via the "*" wildcard so it
	// notifies for every event regardless of how many other listeners
	// (read-model projectors, reactors) are also registered.
	projectionReady := es.NewEventNotifier()

	dispatcher := es.NewEventDispatcher()
	dispatcher.Subscribe(domain.AllUserEvents, userReadModelProjector)
	dispatcher.Subscribe(domain.UserRegistered, userEmailReactor)
	dispatcher.Subscribe("*", &es.NotifierListener{Notifier: projectionReady})

	// In a real app this runs as its own worker, behind a Postgres advisory
	// lock (es/pgstore.AdvisoryLock) so exactly one instance ever builds the
	// read model. Here it's just a goroutine over the in-memory stream.
	go func() {
		out, errc := store.StreamAll(ctx)
		for e := range out {
			if err := dispatcher.Dispatch(ctx, e); err != nil {
				fmt.Println("dispatch error:", err)
			}
		}
		for err := range errc {
			if err != nil {
				fmt.Println("stream error:", err)
			}
		}
	}()

	svc := app.NewUserService(repo)

	// --- simulated inbound calls ---
	// No router: in a real app these three blocks would be HTTP/gRPC
	// handlers translating a request into a command and calling svc.
	userID := uuid.New()

	registered, err := svc.RegisterUser(ctx, app.RegisterUserCommand{
		UserID: userID,
		Email:  "ann@example.com",
		Name:   "Ann",
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("response (immediate, from aggregate): registered %+v\n", registered)

	updated, err := svc.ChangeEmail(ctx, app.ChangeEmailCommand{
		UserID: userID,
		Email:  "ann.new@example.com",
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("response (immediate, from aggregate): email changed %+v\n", updated)

	// Wait for this specific write to be projected — read-your-writes for
	// this one request, without making Save() itself synchronous: every
	// other reader still gets the read model whenever the projector gets
	// to it. See projectionReady's doc comment for why this waits on
	// (userID, updated.Version) rather than just userID.
	waitCtx, waitCancel := context.WithTimeout(ctx, 2*time.Second)
	select {
	case <-projectionReady.Subscribe(userID, updated.Version):
	case <-waitCtx.Done():
		fmt.Println("timed out waiting for the read model to catch up")
	}
	waitCancel()

	if view, ok := views.Get(userID); ok {
		fmt.Printf("query (read-your-writes, from read model): %+v\n", view)
	} else {
		fmt.Println("read model has not caught up yet")
	}
}
