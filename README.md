# go-es-light

A small event-sourcing library for Go: aggregates, an event store, an
optional Postgres backend — no message broker (RabbitMQ/NATS aren't needed
once events already live in Postgres; see the reasoning in `TASK.md`).

## Structure

- `es` — the core: `DomainEvent`, `BaseAggregate`, the `EventStore` /
  `Checkpointer` / `EventRegistry` interfaces, generic `Repository[T]`.
- `es/memstore` — an in-memory `EventStore` + `Checkpointer`, for tests and
  examples.
- `es/pgstore` — a Postgres backend directly on `database/sql` + `lib/pq`,
  no extra SQL helper library: optimistic concurrency via a
  transaction-scoped advisory lock, `StreamAll` via `LISTEN/NOTIFY` (a
  dedicated `pq.Listener`) + polling fallback, `AdvisoryLock` for projector
  leader election. Reading events (`Load`/`FetchAfter`) uses a fixed column
  order with positional `Rows.Scan`, no reflection. Schema in
  `es/pgstore/schema.sql`.
- `examples/user` — registering/updating a user, laid out the way a real
  application on a hexagonal architecture would be (no router, no real SQL
  views — the read model here is in-memory):
  - `domain/` — the `User` aggregate, events, business rules; depends on
    nothing outside itself;
  - `app/` — use cases (`UserService`) + the ports they need
    (`UserRepository` = `es.Repository[*domain.User]`, `ReadModel`,
    `Notifier`); depends only on `domain`;
  - `adapters/eventstore/` — driven adapter for persistence
    (`memstore`-backed `UserRepository`; swap it for `es/pgstore` and
    nothing above it needs to change);
  - `adapters/readmodel/` — driven adapter for `ReadModel`: an in-memory
    read model + the exported `UserProjector` (`es.EventListener`,
    `Kind() == Projector`) that keeps it in sync;
  - `adapters/notifier/` — driven adapter for `Notifier`: a side effect
    ("sending" a welcome email) via the exported `WelcomeEmailReactor`
    (`es.EventListener`, `Kind() == Reactor`);
  - `main.go` — the composition root; it also stands in for the
    driving/inbound adapter, calling `app.UserService` directly instead of
    an HTTP/gRPC handler.
- `test` — tests that only use the public API of the packages they cover:
  - `test/es`, `test/memstore` — unit tests, 100% statement coverage of
    the `es` and `es/memstore` packages.
  - `test/concurrency` — an in-process (memstore) concurrency test: N
    goroutines writing to the same aggregate at once with retry on
    `ErrConcurrencyConflict`, checking that no update is lost or duplicated.
  - `test/pgstore` (build tag `integration`) — integration tests against a
    real Postgres: round-trip, version conflicts, batch atomicity,
    `FetchAfter` pagination, `Checkpointer`, `StreamAll` (backfill +
    LISTEN/NOTIFY + graceful cancel), `AdvisoryLock` leader election, and
    the same concurrency test as `test/concurrency`, but against a real
    database.
  - `test/user` — the same for `examples/user`: untagged race tests
    (`UserService`/`readmodel.Store` under concurrent access, `-race`) plus
    integration tests against a real Postgres (build tag `integration`) —
    several `UserService` replicas, each with its own connection pool to
    one database: concurrent registration of distinct users, a race on the
    same user across replicas, projector leader election via
    `pgstore.AdvisoryLock` with leadership handed to another replica after
    `Release`.

## Usage

An aggregate embeds `es.BaseAggregate` and implements `Apply`.

Event names use dot notation (`aggregate.action`), held in constants rather
than scattered string literals:

```go
const (
	UserRegistered   = "user.registered"
	UserEmailChanged = "user.email_changed"
)

type UserRegisteredEvent struct{ Email, Name string }

func (UserRegisteredEvent) EventName() string { return UserRegistered }

type User struct {
	es.BaseAggregate
	Email string
	Name  string
}

func (u *User) Apply(event any) error {
	switch e := event.(type) {
	case *UserRegisteredEvent:
		u.Email, u.Name = e.Email, e.Name
	case *UserEmailChangedEvent:
		u.Email = e.Email
	default:
		return fmt.Errorf("unknown event %T", event)
	}
	return nil
}

func (u *User) Register(email, name string) error {
	// Invariant check here...
	return u.RecordThat(u, &UserRegisteredEvent{Email: email, Name: name})
}
```

By default `RecordThat` writes schema version `1`, a random `ID` and
`OccurredAt = time.Now().UTC()` into the `DomainEvent` envelope. These are
overridden with options (`AggregateID`/`AggregateVersion`/`Name` are left
alone — those fields are always derived from the aggregate):

```go
u.RecordThat(u, &UserEmailChangedEvent{Email: newEmail},
	es.WithEventVersion(2),        // the payload schema changed
	es.WithOccurredAt(importedAt), // the time came from an external system
	es.WithEventID(originalID),    // re-importing — keep the original ID
)
```

Every event is registered with an `EventRegistry`, so `Repository` can
deserialize the payload during replay:

```go
reg := es.NewRegistry()
reg.Register(UserRegistered, func() any { return &UserRegisteredEvent{} })
reg.Register(UserEmailChanged, func() any { return &UserEmailChangedEvent{} })

repo := es.NewRepository[*User](store, reg, func() *User { return &User{} })
```

A command handler (a use case on `app.UserService`): `repo.Load` → an
aggregate method → `repo.Save`. On a race, `Save` returns
`es.ErrConcurrencyConflict` — the handler should reload the aggregate and
retry:

```go
user, err := repo.Load(ctx, id)
if errors.Is(err, es.ErrAggregateNotFound) {
	user = &User{}
	user.SetID(id)
}
if err := user.Register(email, name); err != nil {
	return err
}
return repo.Save(ctx, user)
```

The response is returned immediately from the aggregate's in-memory
state — without waiting on the projection. The read model and side effects
read events asynchronously via `EventStore.FetchAfter` (polling with a
checkpoint) or `EventStore.StreamAll` (push, with backfill), usually
through `es.EventDispatcher` listeners.

A full working example (registering/updating a user, hexagonal-style —
domain/app/adapters):

```sh
go run ./examples/user
```

## Postgres

```sh
psql "$DATABASE_URL" -f es/pgstore/schema.sql
```

```go
import _ "github.com/lib/pq"

db, _ := sql.Open("postgres", dsn)
store := pgstore.New(db, dsn) // dsn is also used for StreamAll's dedicated pq.Listener
repo := es.NewRepository[*User](store, reg, func() *User { return &User{} })
```

`pgstore.New` and `pgstore.NewAdvisoryLock` don't take a `*sql.DB` — they
take a small `pgstore.DB` interface (`BeginTx`/`QueryContext`/
`QueryRowContext`/`ExecContext`/`Conn`). If a project already has a
`*sqlx.DB`, it satisfies that interface structurally (`sqlx.DB` just embeds
`*sql.DB` without overriding those signatures), so it can be passed in
as-is — no adapter, and no `sqlx` dependency added to this library:

```go
store := pgstore.New(existingSqlxDB, dsn)
```

For side effects (calling an external API in response to an event), use a
separate `outbox_jobs` table + `SELECT ... FOR UPDATE SKIP LOCKED` — that
pattern isn't part of the library, since it's specific to whatever the side
effect actually is (see `TASK.md`).

## Event dispatcher

`es.EventDispatcher` is an optional layer for fanning an event out to every
subscribed listener. Two kinds of listener, with different error
semantics:

- `Projector` — its error is returned to the caller (strict ordering is
  required; a failed projection can't be silently swallowed);
- `Reactor` — a side effect (calling an external API, sending an email,
  ...), its error is only logged; the whole class of reactors can be
  switched off at once via `HandleSideEffects(false)` — e.g. while
  replaying history, when emails and webhooks must not fire again.

The quick way — wrap a function with the `es.NewProjector`/`es.NewReactor`
helpers:

```go
d := es.NewEventDispatcher()
d.Subscribe(UserRegistered, es.NewProjector(sendToAuditLog))
d.Subscribe(UserRegistered, es.NewReactor(sendWelcomeEmail))
d.Subscribe("user.*", es.NewProjector(auditEveryUserEvent)) // dot-notation prefix wildcard

if err := d.Dispatch(ctx, event); err != nil {
	// an error from one of the Projector listeners
}
```

Or implement `es.EventListener` (`Kind() ListenerKind`,
`Handle(ctx, *DomainEvent) error`) as your own exported type and declare an
instance of it right at the `Subscribe` call — no builder method in
between. That's how `examples/user` does it:

```go
type UserProjector struct{ Store *Store }

func (p *UserProjector) Kind() es.ListenerKind { return es.Projector }
func (p *UserProjector) Handle(ctx context.Context, event *es.DomainEvent) error { ... }

d.Subscribe(domain.AllUserEvents, &readmodel.UserProjector{Store: views})
```

A live example with all three roles (`domain`/`app`/`adapters`, ports and
their in-memory implementations) — `examples/user`.

## Waiting for the projection (read-your-writes for the async read side)

CQRS with an async projection means a request right after a write can see
a stale read model — usually accepted as a trade-off. `es.EventNotifier`
gives a way to wait for it selectively: after `repo.Save`, a handler
subscribes to a specific aggregate version and blocks until the projector
has processed it — without making `Save` itself synchronous, and without
affecting any other reader.

The subscription is keyed by the **pair** (aggregate ID, version), not just
the ID. Keying by ID alone (the naive approach) has two failure modes: the
handler subscribes *after* `Save`, so there's a window where the projector
can process the event and fire the signal before the subscription is even
registered — the handler then hangs forever; and, the other way round, a
signal from someone else's concurrent update to the same aggregate wakes
the handler up before its own write has actually landed in the projection.
The version removes both: `Subscribe` returns an already-closed channel
immediately if that version has already been projected, and `Notify` only
wakes waiters whose `minVersion` has actually been reached.

```go
notifier := es.NewEventNotifier()

// in the projector — once Dispatch has run event's Projector listeners:
notifier.Notify(event)

// in the handler — after Save:
select {
case <-notifier.Subscribe(userID, updated.Version):
	// the read model is guaranteed to reflect this write
case <-ctx.Done():
	// timeout/cancellation — respond without that guarantee
}
```

Live example — `examples/user/main.go`: the response waits for its own
write specifically before reading the read model, instead of a
`time.Sleep`.

## Tests

```sh
# unit tests (100% coverage of es and es/memstore) + an in-process concurrency test
go test ./... -race

# coverage
go test ./test/es/... ./test/memstore/... -coverpkg=go-es-light/es,go-es-light/es/memstore -coverprofile=cover.out
go tool cover -func=cover.out
```

Integration tests against a real Postgres (build tag `integration`,
skipped without the environment variable):

```sh
docker run --rm -d --name go-es-light-pg -e POSTGRES_PASSWORD=postgres -p 5544:5432 postgres:16
psql "postgres://postgres:postgres@localhost:5544/postgres?sslmode=disable" -f es/pgstore/schema.sql

PGSTORE_TEST_DSN="postgres://postgres:postgres@localhost:5544/postgres?sslmode=disable" \
	go test -tags=integration ./test/pgstore/... ./test/user/... -race -v

docker stop go-es-light-pg
```
