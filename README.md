# go-es-light

A small event-sourcing library for Go: aggregates, an event store, an
optional Postgres backend — no message broker (RabbitMQ/NATS aren't needed
once events already live in Postgres; see the reasoning in `TASK.md`).

## Structure

- `es` — the core: `DomainEvent`, `BaseAggregate`, the `EventStore` /
  `Checkpointer` / `EventRegistry` interfaces, generic `Repository[T]`, and
  `PiiEventStore` — an `EventStore` decorator, not a `Repository` concern,
  since PII-at-rest is a property of the store itself (see below).
- `es/memstore` — an in-memory `EventStore` + `Checkpointer`, for tests and
  examples.
- `es/pgstore` — a Postgres backend directly on `database/sql` + `lib/pq`,
  no extra SQL helper library: optimistic concurrency via a
  transaction-scoped advisory lock, `StreamAll` via `LISTEN/NOTIFY` (a
  dedicated `pq.Listener`) + polling fallback, `AdvisoryLock` for projector
  leader election. Reading events (`Load`/`FetchAfter`) uses a fixed column
  order with positional `Rows.Scan`, no reflection. No schema file to apply
  by hand — call `Store.Migrate` once (idempotent, works against a
  brand-new empty database).
- `examples/user` — registering/updating a user, laid out the way a real
  application on a hexagonal architecture would be (no router, no real SQL
  views — the read model here is in-memory):
  - `domain/` — the `User` aggregate, events, business rules; depends on
    nothing outside itself;
  - `app/` — use cases (`UserService`) + the ports they need
    (`UserRepository` = `es.Repository[*domain.User]`, `ReadModel`,
    `Notifier`); depends only on `domain`;
  - persistence has no `adapters/` package of its own — `es/memstore`
    already is the driven adapter for `UserRepository`'s underlying
    `es.EventStore`, wired up explicitly in `main.go`'s composition root
    (no constructor hiding that wiring behind a `New()` call in another
    package); swap it for `es/pgstore` there and nothing above it needs to
    change;
  - `adapters/readmodel/` — driven adapter for `ReadModel`: an in-memory
    read model + the exported `UserProjector` (`es.EventListener`,
    `Kind() == Projector`) that keeps it in sync;
  - `adapters/notifier/` — driven adapter for `Notifier`: a side effect
    ("sending" a welcome email) via the exported `WelcomeEmailReactor`
    (`es.EventListener`, `Kind() == Reactor`);
  - `main.go` — the composition root; it also stands in for the
    driving/inbound adapter, calling `app.UserService` directly instead of
    an HTTP/gRPC handler.
- `examples/ledger` — more down-to-earth than `examples/user`, no hexagonal
  ceremony: a small multi-tenant general ledger (2 organizations, 3 accounts
  each, 3 transactions posted concurrently per account with reload-and-retry
  on `ErrConcurrencyConflict`), built to exercise essentially every opt-in
  feature together — `WithTenant`/`WithAllTenants`/`TenantScopedStore`,
  `WithActor`/`WithMetadata`/`WithOccurredAt`/`WithEventID`/
  `WithEventVersion`, `WithPiiID`/`PiiEventStore`/`RequestsErasure`
  (crypto-shredding), `GlobalTenantID`, an async per-organization read model
  via `EventDispatcher`/`EventNotifier`, and the full GDPR/DSAR toolkit —
  `SubjectExporter.Export`/`.ExportJSON`, `WithErasureLedger` +
  `ErasureVerifier.Verify` for provable erasure, and `WrappedKeyRing` +
  `.Rotate` for KEK rotation (see "GDPR: subject access and provable
  erasure" below). Ends with a self-contained block of end-to-end checks
  (`OK: ...` per line, exits non-zero on the first failure) proving balances
  survive concurrent posting, PII is sealed at rest and decrypts correctly
  except for an erased holder, a DSAR export (both Go struct and JSON) is
  correct before and after erasure, erasure is independently provable via
  the ledger, tenants stay isolated, and a tenant-less fact is visible to
  every organization.
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

```go
import _ "github.com/lib/pq"

db, _ := sql.Open("postgres", dsn)
store := pgstore.New(db, dsn) // dsn is also used for StreamAll's dedicated pq.Listener
if err := store.Migrate(ctx); err != nil {
	// handle err — idempotent, safe to call on every startup, works
	// against a brand-new empty database, no schema file to apply by hand
}
repo := es.NewRepository[*User](store, reg, func() *User { return &User{} })
```

Multitenancy and PII/erasure support are both opt-in and don't change any
of the above — see `es.WithTenant`/`es.WithAllTenants`,
`pgstore.WithTenantIsolation` (an option to `Store.Migrate`, enabling
row-level security as defense-in-depth), `es.WithActor`/`WithPiiID`/
`WithMetadata`, and `es.PiiEventStore` (wraps any EventStore, sealing PII on
Commit and opening it again on Load/FetchAfter/StreamAll — for every
consumer of that store, not just Repository, so a projector reading
straight off StreamAll never needs a *PiiAnonymizer of its own) +
`pgstore.KeyRing`/`memstore.KeyRing`. For a worker that only ever operates
within one tenant (a per-tenant projector, a per-tenant background job),
`es.NewTenantScopedStore(store, tenantID)` binds that tenant permanently so
no individual call inside the worker can forget `es.WithTenant` — it
composes with `PiiEventStore` (wrap in either order). Actor and PII columns
are themselves
optional: `pgstore.New(db, dsn, pgstore.WithActorTracking(), pgstore.WithPiiTracking())`
is what tells `Migrate` to create `actor_id`/`actor_type`/`pii_id` at all —
without those options the columns are never created or queried, and
`Commit` rejects (rather than silently drops) an event that carries an
actor or a `PiiID` anyway.

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

## GDPR: subject access and provable erasure

Built on top of `PiiEventStore`/`WithPiiID`/`RequestsErasure` (see above), a
small toolkit in `es` answers the two questions a data controller has to
answer under GDPR/Law 25 once a subject is identifiable by a `PiiID`:
"show me everything you hold on this person" (a Subject Access Request), and
"prove you actually erased it" (crypto-shredding needs paper trail, not just
a `KeyRing.Forget` call nobody can later verify happened).

**Subject Access Requests** — `es.PiiLookup` (`FetchByPiiID(ctx, piiID,
opts...)`) is a subject-centric query across every aggregate and tenant,
implemented by `memstore.Store` and `pgstore.Store` directly, and delegated
through by `PiiEventStore`/`TenantScopedStore` (so wrapping doesn't hide
it — a `PiiEventStore`-wrapped store still opens PII fields before handing
the events back, exactly like `Load`/`FetchAfter`/`StreamAll` already do).
`pgstore.Store` only implements it when constructed with
`pgstore.WithPiiTracking()` (the `pii_id` column and its index don't exist
otherwise); calling it on a store that doesn't support it returns
`es.ErrPiiLookupUnsupported`.

`es.SubjectExporter` turns that query into an actual export document,
grouped by (tenant, aggregate):

```go
exporter := es.NewSubjectExporter(store) // store must implement es.PiiLookup
export, err := exporter.Export(ctx, holderPiiID, es.WithTenant(orgID))
// export.Aggregates[i].Events[j] — plaintext, or PiiUnrecoverable: true
// for any event whose subject has since been erased

json, err := exporter.ExportJSON(ctx, holderPiiID, es.WithTenant(orgID))
// a deterministic DSAR response document — same JSON bytes for the same
// underlying event log, safe to hand to a compliance team or a regulator
```

**Provable erasure** — `PiiAnonymizer.Forget` alone only proves the key is
gone *now*; it says nothing about when a given crypto-shred happened or
whether the record of it could have been tampered with afterwards. Mount an
`es.ErasureLedger` on the `PiiEventStore` with `es.WithErasureLedger(ledger)`
and every successful erasure appends a hash-chained `es.ShredRecord`
(`es.CheckShredChain` verifies the whole chain, catching any record that was
altered or removed after the fact) — implementations: `memstore.NewErasureLedger()`
for tests/examples, `pgstore.NewErasureLedger(db)` (table `pii_shreds`,
appends serialized with an advisory lock) for production; call its
`Migrate(ctx)` once, same as `Store.Migrate`/`KeyRing.Migrate`.

`es.ErasureVerifier` ties the three independent sources of truth together
into one proof a regulator can be shown instead of "we deleted it, trust
us":

```go
verifier := es.NewErasureVerifier(store, keyRing, ledger)
proof, err := verifier.Verify(ctx, holderPiiID)
// err == nil (or es.ErrNoErasureRecord if this subject was never erased)
// proof.Erasures            — this subject's ledger records, chain-checked
// proof.KeyForgotten        — the keyring no longer holds a key for them
// proof.EventsUnrecoverable — every PII-bearing event they have reads back
//                             as ciphertext, not plaintext
```

**Key rotation** — `es.WrappedKeyRing` wraps any `KeyRing` (`memstore.KeyRing`,
`pgstore.KeyRing` — both implement the `KeyRingLister` it needs) in an
envelope/KMS scheme: the bytes actually persisted are a KEK-wrapped DEK, not
the raw per-subject key. `es.AesGcmWrapper` is a stdlib-only reference
`KeyWrapper`; swap in a real KMS client behind the same interface for
production. `.Rotate(ctx, newWrapper)` re-wraps every surviving subject's key
under a new KEK — no events are re-recorded, no re-encryption of any
payload, just the wrapping layer:

```go
keys := es.NewWrappedKeyRing(pgstore.NewKeyRing(db), es.NewAesGcmWrapper(kek1, "kek:v1"))
// ... time passes, kek1 needs retiring ...
if err := keys.Rotate(ctx, es.NewAesGcmWrapper(kek2, "kek:v2")); err != nil {
	// handle err
}
```

Live, end-to-end use of all four pieces together (including the negative
case — verifying a subject who was never erased) — `examples/ledger/main.go`.

## Event dispatcher

`es.EventDispatcher` is an optional layer for fanning an event out to every
subscribed listener. Two kinds of listener, with different ordering and
error semantics:

- `Projector` — runs synchronously, one at a time, **in the order it was
  registered with `Subscribe`**, never concurrently with another Projector.
  A later Projector can rely on an earlier one having already run for this
  event; its error is returned to the caller and stops the rest of the
  Projector chain for this event (a failed projection can't be silently
  swallowed);
- `Reactor` — a side effect (calling an external API, sending an email,
  ...), started in its own goroutine and never waited for — a slow or
  failing Reactor can't delay Dispatch's return or the Projector chain.
  Its error is only logged; the whole kind can be switched off at once via
  `HandleSideEffects(false)` — e.g. while replaying history, when emails
  and webhooks must not fire again.

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

Wire it in as an `es.NotifierListener` — a `Projector` itself — rather than
calling `Notify` by hand from inside a projector's event loop; that keeps
"update the read model" and "wake up waiters" as two separate listeners.
**Subscribe it after** every read-model `Projector` a waiter should be able
to rely on having run: Dispatch's registration-order guarantee (see "Event
dispatcher" above) is exactly what makes this safe — if `NotifierListener`
were registered first, or the read model were updated by a `Reactor`
instead of a `Projector`, a waiter could wake up before the read model
actually reflects the write.

```go
notifier := es.NewEventNotifier()

dispatcher.Subscribe(domain.AllUserEvents, &readmodel.UserProjector{Store: views}) // updates the read model
dispatcher.Subscribe("*", &es.NotifierListener{Notifier: notifier})                 // wakes waiters — registered last

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
skipped without the environment variable). `test/pgstore` and `test/user`
both point at the same PGSTORE_TEST_DSN database and both run schema
migrations (Store.Migrate, pgstore.KeyRing.Migrate) against its shared
`events` table — `go test` runs different packages' test binaries in
parallel by default, and concurrent `ALTER TABLE` from two processes can hit
Postgres's "tuple concurrently updated" error, so pass `-p 1` when running
both together:

```sh
docker run --rm -d --name go-es-light-pg -e POSTGRES_PASSWORD=postgres -p 5544:5432 postgres:16

PGSTORE_TEST_DSN="postgres://postgres:postgres@localhost:5544/postgres?sslmode=disable" \
	go test -tags=integration -p 1 ./test/pgstore/... ./test/user/... -race -v

docker stop go-es-light-pg
```
