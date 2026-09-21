// Command user is a self-contained, runnable walk-through of registering
// and updating a user, structured like a real hexagonal-architecture
// application (minus an actual router and a real SQL read model — the
// read model here is an in-memory stand-in, see adapters/readmodel):
//
//	domain/               - the User aggregate, its events, its business rules
//	app/                  - use cases (UserService) + the ports they need
//	adapters/readmodel/   - driven adapter: async read model (app.ReadModel)
//	adapters/notifier/    - driven adapter: side effect (app.Notifier)
//	main.go               - composition root; also stands in for the
//	                        driving/inbound adapter (HTTP/gRPC/CLI would
//	                        normally call app.UserService the same way)
//
// The dependency rule holds throughout: domain depends on nothing here,
// app depends only on domain, and every adapter depends inward on app (and,
// where it needs event shapes, on domain) — never the reverse.
//
// Persistence has no adapters/ package of its own: es/memstore (or
// es/pgstore) already is the driven adapter for app.UserRepository's
// underlying es.EventStore, wired up explicitly in main's composition root
// below rather than behind another constructor.
//
// It also demonstrates the opt-in features layered on top of the core
// engine, all of which app/domain/adapters above barely need to know about:
//   - multitenancy (es.WithTenant, es.WithAllTenants) — every command
//     carries a TenantID, UserService.* passes it straight through to
//     Repository.Load/Save;
//   - actor attribution (es.WithActor) — every command also carries an
//     Actor, attributed on the resulting event;
//   - ad-hoc metadata (es.WithMetadata) — domain.User.Register tags its
//     event with a signup channel;
//   - PII/erasure (es.WithPiiID, es.PiiAnonymizer) — Email is declared PII
//     on the events that carry it (see domain/events.go's PiiFields
//     methods), so it's encrypted at rest and decrypted again on read,
//     until UserService.ForgetUser crypto-shreds it for good — after
//     which es.DomainEvent.PiiUnrecoverable is what tells a projector
//     rebuilding its read model from scratch not to treat the leftover
//     ciphertext as a real email (see readmodel.UserProjector.Handle and
//     the rebuild demonstration near the end of this file).
//
// What it can't demonstrate: anything specific to es/pgstore (Migrate,
// WithActorTracking/WithPiiTracking, WithTenantIsolation,
// GrantTenantBypass/RevokeTenantBypass) — this example runs entirely on
// es/memstore (see the composition root below). Those are covered by
// es/pgstore's own integration tests (test/pgstore, test/user) against a
// real Postgres instead.
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/es/memstore"
	"go-es-light/examples/user/adapters/notifier"
	"go-es-light/examples/user/adapters/readmodel"
	"go-es-light/examples/user/app"
	"go-es-light/examples/user/domain"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- composition root: wire driven adapters behind their ports ---
	//
	// No adapters/eventstore package — a constructor hiding this wiring
	// behind a single New() call means a reader can't see what's actually
	// being assembled without jumping to another file. It's wired explicitly
	// right here instead, same as examples/ledger's composition root:
	//
	//   raw  - the unwrapped memstore.Store; kept around only so this file
	//          can deliberately read past PII encryption further down, the
	//          way an offline audit tool might.
	//   store - an es.PiiEventStore wrapping raw: every consumer below reads/
	//          writes through store, and none of them — repo, the projector,
	//          the reactor — need to know PII encryption is even happening.
	//   repo  - the UserRepository port app.UserService depends on, built
	//          directly over store.
	//
	// Swap raw for an es/pgstore.Store (plus pgstore.NewKeyRing for the
	// anonymizer) and nothing below this block has to change.
	raw := memstore.New()
	anonymizer := es.NewPiiAnonymizer(memstore.NewKeyRing())
	store := es.NewPiiEventStore(raw, domain.Events(), anonymizer)
	repo := es.NewRepository[*domain.User](store, domain.Events(), func() *domain.User { return &domain.User{} })
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
	//
	// It streams every tenant's events unscoped (no es.WithTenant) — fine
	// for this single-process demo's one shared read model, but a
	// per-tenant read model in a real multitenant deployment would instead
	// run one projector per tenant, each handed an es.NewTenantScopedStore
	// wrapping store and bound to its own tenantID: every call that worker
	// makes is then permanently scoped, so it never even processes another
	// tenant's events (rather than filtering them out after the fact), and
	// no individual call site inside it can forget to pass es.WithTenant.
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
	// No router: in a real app these blocks would be HTTP/gRPC handlers
	// translating a request into a command and calling svc. TenantID/Actor
	// would normally come from request auth (a JWT claim, a subdomain, a
	// session's user id, ...), not be picked here — see app/commands.go.
	tenantA := uuid.New()
	tenantB := uuid.New()
	userID := uuid.New()

	// Actor attribution (es.WithActor): a self-service signup is attributed
	// to the user themselves.
	annActor := domain.Actor{ID: userID, Type: "customer"}

	registered, err := svc.RegisterUser(ctx, app.RegisterUserCommand{
		TenantID: tenantA,
		UserID:   userID,
		Email:    "ann@example.com",
		Name:     "Ann",
		Actor:    annActor,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("response (immediate, from aggregate): registered %+v\n", registered)

	// Multitenancy: the same user id simply doesn't exist under a
	// different tenant. Repository.Load returns es.ErrAggregateNotFound,
	// exactly as if it had never been created — not a permissions error,
	// not a different kind of failure.
	if _, err := repo.Load(ctx, userID, es.WithTenant(tenantB)); errors.Is(err, es.ErrAggregateNotFound) {
		fmt.Println("isolation: tenant B cannot see tenant A's user, as expected")
	} else if err != nil {
		panic(err)
	} else {
		fmt.Println("isolation FAILED: tenant B could see tenant A's user")
	}

	// A second, unrelated user under tenant B — just so the WithAllTenants
	// demonstration further down has more than one tenant's data to show.
	if _, err := svc.RegisterUser(ctx, app.RegisterUserCommand{
		TenantID: tenantB,
		UserID:   uuid.New(),
		Email:    "bob@example.com",
		Name:     "Bob",
		Actor:    domain.Actor{ID: uuid.New(), Type: "customer"},
	}); err != nil {
		panic(err)
	}

	updated, err := svc.ChangeEmail(ctx, app.ChangeEmailCommand{
		TenantID: tenantA,
		UserID:   userID,
		Email:    "ann.new@example.com",
		Actor:    annActor,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("response (immediate, from aggregate): email changed %+v\n", updated)

	// PII at rest, plus actor attribution and ad-hoc metadata: read straight
	// from raw, the unwrapped memstore.Store — bypassing the es.PiiEventStore
	// that decrypts for every other reader in this file (Repository.Load,
	// the projector, the reactor) — to see that Email really is ciphertext
	// in the store, even though every response/view above showed the real
	// value. ActorID/ActorType/Metadata, by contrast, were never PII-sealed
	// (only Payload/PiiFields are), so they read back exactly as recorded
	// regardless of which store you read them from.
	rawEvents, err := raw.Load(ctx, userID, es.WithTenant(tenantA))
	if err != nil {
		panic(err)
	}
	first := rawEvents[0]
	fmt.Printf("raw event at rest (email is ciphertext): %s\n", rawEvents[len(rawEvents)-1].Payload)
	fmt.Printf("actor attribution (from the registration event): id=%s type=%s\n", *first.ActorID, *first.ActorType)
	fmt.Printf("ad-hoc metadata (from the registration event): %s\n", first.Metadata)

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

	// es.WithAllTenants: an operator/admin tool's cross-tenant report,
	// contrasted with an ordinary tenant-scoped read. Behaviorally
	// identical to just omitting es.WithTenant — the reason to use it
	// anyway is that it's a greppable, self-documenting call site for "this
	// is deliberately unscoped," not silence indistinguishable from a
	// tenant-facing handler that forgot to scope itself. See
	// es.WithAllTenants's doc comment, and — for the stronger, role-based
	// version of the same idea — pgstore.Store.GrantTenantBypass.
	scoped, err := store.FetchAfter(ctx, 0, 1000, es.WithTenant(tenantA))
	if err != nil {
		panic(err)
	}
	allTenants, err := store.FetchAfter(ctx, 0, 1000, es.WithAllTenants())
	if err != nil {
		panic(err)
	}
	fmt.Printf("operator report: %d events across all tenants (vs %d scoped to tenant A alone)\n", len(allTenants), len(scoped))

	// es.GlobalTenantID: the mechanism behind genuinely tenant-less,
	// shared/global data — a product catalog, a pricing feed — which
	// doesn't fit the User domain used elsewhere in this example, so this
	// talks to the store directly rather than through a domain.User. An
	// event committed without es.WithTenant lands under es.GlobalTenantID
	// (the nil UUID) and is then visible to a Load scoped to *any* tenant,
	// alongside that tenant's own data — see EventStore's "OR
	// tenant_id = GlobalTenantID" handling in Load/FetchAfter.
	globalAggID := uuid.New()
	globalEvent := &es.DomainEvent{
		ID:               uuid.New(),
		AggregateID:      globalAggID,
		AggregateVersion: 1,
		Version:          1,
		Name:             "example.GlobalCatalogEntryAdded",
		Payload:          []byte(`{"sku":"WIDGET-1"}`),
		OccurredAt:       time.Now().UTC(),
	}
	if err := store.Commit(ctx, []*es.DomainEvent{globalEvent}, 0); err != nil { // no es.WithTenant
		panic(err)
	}
	if seenByA, err := store.Load(ctx, globalAggID, es.WithTenant(tenantA)); err != nil {
		panic(err)
	} else if len(seenByA) == 1 {
		fmt.Println("global data: tenant A can see the tenant-less catalog entry, as expected")
	}
	if seenByB, err := store.Load(ctx, globalAggID, es.WithTenant(tenantB)); err != nil {
		panic(err)
	} else if len(seenByB) == 1 {
		fmt.Println("global data: tenant B can see it too — same tenant-less entry, shared by every tenant")
	}

	// Erasure: crypto-shred this user's PII key. The historical fact "this
	// user asked to be forgotten" is recorded like any other event — the
	// append-only log is never mutated — but every PII field tied to their
	// subject id becomes permanently unreadable from this point on. Here
	// it's an admin, not Ann herself, processing the request.
	adminActor := domain.Actor{ID: uuid.New(), Type: "admin"}
	if err := svc.ForgetUser(ctx, app.ForgetUserCommand{TenantID: tenantA, UserID: userID, Actor: adminActor}); err != nil {
		panic(err)
	}
	forgotten, err := repo.Load(ctx, userID, es.WithTenant(tenantA))
	if err != nil {
		panic(err)
	}
	fmt.Printf("after erasure, replayed aggregate (Email is now unrecoverable ciphertext): %+v\n", forgotten)

	// es.DomainEvent.PiiUnrecoverable: the live projector above already
	// processed (and cached) Ann's real email long before this erasure
	// request landed, so it never actually observes the flag — that's the
	// common case. A *fresh* projector backfilling from scratch, as if
	// standing up a new read model or recovering a corrupted one, hits her
	// UserRegistered event only after the key is already gone.
	rebuiltViews := readmodel.New()
	rebuiltDispatcher := es.NewEventDispatcher()
	rebuiltDispatcher.Subscribe(domain.AllUserEvents, &readmodel.UserProjector{Store: rebuiltViews})

	rebuildCtx, rebuildCancel := context.WithTimeout(ctx, 2*time.Second)
	rebuildOut, rebuildErrc := es.NewTenantScopedStore(store, tenantA).StreamAll(rebuildCtx)
rebuildLoop:
	for {
		select {
		case e, ok := <-rebuildOut:
			if !ok {
				break rebuildLoop
			}
			if err := rebuiltDispatcher.Dispatch(rebuildCtx, e); err != nil {
				panic(err)
			}
		case err := <-rebuildErrc:
			if err != nil {
				panic(err)
			}
		case <-rebuildCtx.Done():
			break rebuildLoop
		}
	}
	rebuildCancel()

	if view, ok := rebuiltViews.Get(userID); ok {
		fmt.Printf("read model rebuilt from scratch, after erasure (Email is a placeholder, not ciphertext): %+v\n", view)
	}
}
