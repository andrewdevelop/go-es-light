// Command ledger is a more down-to-earth, pragmatic walk-through than
// examples/user: a small multi-tenant general ledger — 2 organizations
// (tenants), 3 user accounts each, 3 transactions posted concurrently
// against every account — built to exercise essentially every opt-in
// feature this library has, together, under real concurrency:
//
//   - es.WithTenant / es.WithAllTenants / es.NewTenantScopedStore
//   - es.WithActor / es.WithMetadata / es.WithOccurredAt / es.WithEventID /
//     es.WithEventVersion
//   - es.WithPiiID / es.PiiEventStore / es.ContainsPersonalData /
//     es.RequestsErasure (crypto-shredding)
//   - es.WrappedKeyRing / es.AesGcmWrapper / Rotate — subject keys sealed
//     under a separate KEK (the demo's stand-in for a KMS), proving a leaked
//     keyring table is useless without it, and rotation without re-recording
//   - es.PiiLookup / es.SubjectExporter (.Export and .ExportJSON) — the
//     data-subject file behind a DSAR/access request, needing no
//     per-aggregate drilling
//   - es.ErasureLedger / es.ErasureVerifier — a tamper-evident,
//     hash-chained record of every crypto-shred, tying the ledger record,
//     the (now-forgotten) key and the unrecoverable events together into
//     one provable answer to "was this subject really erased?"
//   - es.GlobalTenantID (a tenant-less, platform-wide fact visible to
//     every organization)
//   - es.ErrConcurrencyConflict + reload-and-retry, with multiple
//     goroutines genuinely racing to post transactions against the very
//     same account
//   - es.EventDispatcher / es.EventListener (Projector) / es.EventNotifier
//     for an async, per-organization read model
//
// Like examples/user, this runs entirely on es/memstore — nothing here is
// specific to es/pgstore (Migrate, WithActorTracking/WithPiiTracking,
// WithTenantIsolation, GrantTenantBypass/RevokeTenantBypass); those are
// covered by es/pgstore's own integration tests against a real Postgres.
//
// The last section of this file is a self-contained set of end-to-end
// checks against everything above — every line printed is either "OK: ..."
// or the whole program exits non-zero on the first unmet expectation.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/es/memstore"
	"go-es-light/examples/ledger/domain"
)

// --- fixture: 2 organizations, 3 accounts each ---

type accountSpec struct {
	id          uuid.UUID
	org         uuid.UUID
	orgName     string
	holderName  string
	holderEmail string
}

// --- read model: one statement book per organization ---

type accountView struct {
	HolderName string
	Balance    int64
}

type statementBook struct {
	mu   sync.RWMutex
	byID map[uuid.UUID]accountView
}

func newStatementBook() *statementBook {
	return &statementBook{byID: make(map[uuid.UUID]accountView)}
}

func (b *statementBook) upsert(id uuid.UUID, v accountView) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.byID[id] = v
}

func (b *statementBook) get(id uuid.UUID) (accountView, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	v, ok := b.byID[id]
	return v, ok
}

var _ es.EventListener = (*statementProjector)(nil)

// statementProjector has no idea PII sealing even exists — same as
// examples/user/adapters/readmodel.UserProjector, and for the same
// reason: it reads off an es.PiiEventStore-wrapped stream, which has
// already decrypted every event before Dispatch ever sees it.
type statementProjector struct {
	book *statementBook
}

func (p *statementProjector) Kind() es.ListenerKind { return es.Projector }

func (p *statementProjector) Handle(_ context.Context, event *es.DomainEvent) error {
	cur, _ := p.book.get(event.AggregateID)

	switch event.Name {
	case domain.AccountOpened:
		var payload domain.AccountOpenedEvent
		if err := event.UnmarshalPayload(&payload); err != nil {
			return err
		}
		cur.Balance = payload.OpeningBalance
		// event.PiiUnrecoverable is true if the holder's key was already
		// forgotten by the time this event reached Open — a real
		// possibility a live worker won't usually hit (it processes
		// AccountOpened long before any later erasure request), but a
		// fresh read model rebuilt from scratch *after* the erasure will
		// hit every time. payload.HolderName is then ciphertext, not a
		// name — writing it into a format-validated column (or just
		// displaying it) as if it were real would be wrong, so store an
		// explicit placeholder instead. See es.DomainEvent.PiiUnrecoverable.
		if event.PiiUnrecoverable {
			cur.HolderName = "[erased]"
		} else {
			cur.HolderName = payload.HolderName
		}
	case domain.TransactionRecorded:
		var payload domain.TransactionRecordedEvent
		if err := event.UnmarshalPayload(&payload); err != nil {
			return err
		}
		cur.Balance += payload.AmountCents
	case domain.HolderErasureRequested:
		// No view field changes — see examples/user's readmodel for why.
	default:
		return fmt.Errorf("ledger: unknown event %q", event.Name)
	}

	p.book.upsert(event.AggregateID, cur)
	return nil
}

// orgWorker is what a real per-tenant projector process would be: its own
// es.NewTenantScopedStore bound to one organization (so it can never even
// process another organization's events, let alone leak them into its
// book), its own dispatcher, its own read model.
type orgWorker struct {
	store  es.EventStore
	book   *statementBook
	ready  *es.EventNotifier
	cancel context.CancelFunc
}

func startOrgWorker(ctx context.Context, store es.EventStore, orgID uuid.UUID) *orgWorker {
	scoped := es.NewTenantScopedStore(store, orgID)
	book := newStatementBook()
	ready := es.NewEventNotifier()

	dispatcher := es.NewEventDispatcher()
	dispatcher.Subscribe(domain.AllAccountEvents, &statementProjector{book: book})
	dispatcher.Subscribe("*", &es.NotifierListener{Notifier: ready})

	workerCtx, cancel := context.WithCancel(ctx)
	go func() {
		out, errc := scoped.StreamAll(workerCtx)
		for e := range out {
			if err := dispatcher.Dispatch(workerCtx, e); err != nil {
				fmt.Println("dispatch error:", err)
			}
		}
		for err := range errc {
			if err != nil {
				fmt.Println("stream error:", err)
			}
		}
	}()

	return &orgWorker{store: scoped, book: book, ready: ready, cancel: cancel}
}

// --- transactions posted against every account ---

type txSpec struct {
	amountCents int64
	description string
	channel     string
	actorID     uuid.UUID
	actorType   string
	opts        []es.RecordOption
}

// txSpecsFor returns the 3 transactions posted against acc: a self-service
// mobile top-up, a teller-handled branch deposit, and a backdated batch
// interest accrual — 3 different actor types, 3 different channels, one of
// them carrying es.WithOccurredAt/es.WithEventID as if it had been
// re-recorded from an external interest-calculation batch job that already
// timestamped and identified it.
func txSpecsFor(acc accountSpec, tellerID uuid.UUID, systemActorID uuid.UUID) []txSpec {
	return []txSpec{
		{
			amountCents: 10000, description: "Mobile top-up", channel: "mobile",
			actorID: acc.id, actorType: "customer", // self-service: actor is the holder
		},
		{
			amountCents: 5000, description: "Branch cash deposit", channel: "branch",
			actorID: tellerID, actorType: "teller",
		},
		{
			amountCents: 500, description: "Interest accrual (Q1)", channel: "batch",
			actorID: systemActorID, actorType: "system",
			opts: []es.RecordOption{
				// Interest accrues over the period it covers, not at
				// whatever wall-clock moment the batch job happened to run.
				es.WithOccurredAt(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
				// As if re-recorded from an external batch job that already
				// assigned this event its own id.
				es.WithEventID(uuid.New()),
			},
		},
	}
}

const expectedBalanceCents = 10000 + 5000 + 500 // = 15500, every account, regardless of posting order

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- composition root ---
	//
	// Two different mechanisms show up below, and they're deliberately not
	// the same shape:
	//
	//   - Decorators (raw -> sealedStore -> the TenantScopedStore each
	//     orgWorker builds below) wrap one EventStore around another. The
	//     store being wrapped is a *required* collaborator — a PiiEventStore
	//     with no underlying store to delegate to isn't a valid state — so
	//     it's a positional constructor argument, never a With option.
	//   - `With...` options (WithErasureLedger below; WithTenant/
	//     WithAllTenants at call sites) configure optional behavior a
	//     decorator works fine without. WithErasureLedger is exactly that:
	//     sealedStore seals/opens PII with or without a ledger mounted, the
	//     ledger only adds an audit trail on top.
	//
	// raw -> sealedStore is the whole "PII at rest" story for every reader
	// of sealedStore (Repository, the per-org projectors, the exporter
	// below) — none of them hold a *PiiAnonymizer of their own.
	raw := memstore.New()
	// Subject keys are wrapped by a KEK that never touches the keyring — the
	// demo's stand-in for a cloud KMS / HSM. A leaked keyring table (or a
	// stolen backup) is therefore useless without the KEK, and Rotate below
	// can swap the KEK without re-recording a single event.
	kek1, err := es.NewAesGcmWrapper(bytes.Repeat([]byte{1}, 32), "kek:v1")
	if err != nil {
		panic(err)
	}
	keyRing := es.NewWrappedKeyRing(memstore.NewKeyRing(), kek1)
	anonymizer := es.NewPiiAnonymizer(keyRing)
	// erasureLedger is the durable, tamper-evident, append-only record of
	// every crypto-shred — separate from both the events table (never
	// mutated) and the keyring (says only what protects the data now, not
	// its erasure history). Mounted on sealedStore via WithErasureLedger —
	// an optional extra, not a wrapper of its own — so every RequestsErasure
	// commit appends a record automatically, with no call site (including
	// UserService-equivalent code here) needing to remember to do it
	// separately.
	erasureLedger := memstore.NewErasureLedger()
	sealedStore := es.NewPiiEventStore(raw, domain.Events(), anonymizer, es.WithErasureLedger(erasureLedger))
	repo := es.NewRepository[*domain.Account](sealedStore, domain.Events(), func() *domain.Account { return &domain.Account{} })
	// The data-subject file behind a DSAR/access request: everything the log
	// holds for one holder, across every aggregate, through the same
	// PII-transparent store.
	exporter := es.NewSubjectExporter(sealedStore)

	orgA, orgB := uuid.New(), uuid.New()
	accounts := []accountSpec{
		{uuid.New(), orgA, "Acme Corp", "Alice Anderson", "alice@acme.example"},
		{uuid.New(), orgA, "Acme Corp", "Bob Baker", "bob@acme.example"},
		{uuid.New(), orgA, "Acme Corp", "Carol Chen", "carol@acme.example"},
		{uuid.New(), orgB, "Globex Inc", "Dave Diaz", "dave@globex.example"},
		{uuid.New(), orgB, "Globex Inc", "Erin Evans", "erin@globex.example"},
		{uuid.New(), orgB, "Globex Inc", "Frank Fisher", "frank@globex.example"},
	}
	tellerByOrg := map[uuid.UUID]uuid.UUID{orgA: uuid.New(), orgB: uuid.New()}
	systemActorID := uuid.New() // shared, platform-wide batch-job identity

	workers := map[uuid.UUID]*orgWorker{
		orgA: startOrgWorker(ctx, sealedStore, orgA),
		orgB: startOrgWorker(ctx, sealedStore, orgB),
	}
	defer func() {
		for _, w := range workers {
			w.cancel()
		}
	}()

	// --- concurrent writes: all 6 accounts in parallel, each account's 3
	// transactions racing each other with reload-and-retry on
	// es.ErrConcurrencyConflict, the same pattern test/user's concurrency
	// tests use against examples/user. ---
	var accountsWG sync.WaitGroup
	for _, acc := range accounts {
		accountsWG.Add(1)
		go func(acc accountSpec) {
			defer accountsWG.Done()

			opener := &domain.Account{}
			opener.SetID(acc.id)
			if err := opener.Open(acc.holderName, acc.holderEmail, 0, domain.Actor{ID: acc.id, Type: "customer"}); err != nil {
				panic(err)
			}
			if err := repo.Save(ctx, opener, es.WithTenant(acc.org)); err != nil {
				panic(err)
			}

			var txWG sync.WaitGroup
			for _, tx := range txSpecsFor(acc, tellerByOrg[acc.org], systemActorID) {
				txWG.Add(1)
				go func(tx txSpec) {
					defer txWG.Done()
					for {
						current, err := repo.Load(ctx, acc.id, es.WithTenant(acc.org))
						if err != nil {
							panic(err)
						}
						actor := domain.Actor{ID: tx.actorID, Type: tx.actorType}
						if err := current.RecordTransaction(tx.amountCents, tx.description, tx.channel, actor, tx.opts...); err != nil {
							panic(err)
						}
						err = repo.Save(ctx, current, es.WithTenant(acc.org))
						if err == nil {
							return
						}
						if errors.Is(err, es.ErrConcurrencyConflict) {
							continue // reload and retry — someone else's transaction landed first
						}
						panic(err)
					}
				}(tx)
			}
			txWG.Wait()
		}(acc)
	}
	accountsWG.Wait()

	// --- a tenant-less, platform-wide fact: committed without
	// es.WithTenant, so it lands under es.GlobalTenantID and is readable
	// by every organization alongside its own data. Named outside the
	// "ledger.*" namespace on purpose: domain.AllAccountEvents subscribes
	// every organization's statementProjector to that wildcard, and this
	// event isn't an account fact the projector knows how to fold into a
	// statement — it shouldn't be dispatched there at all.
	globalEvent := &es.DomainEvent{
		ID:               uuid.New(),
		AggregateID:      uuid.New(),
		AggregateVersion: 1,
		Version:          1,
		Name:             "platform.exchange_rate_updated",
		Payload:          []byte(`{"usd_to_eur":0.92}`),
		OccurredAt:       time.Now().UTC(),
	}
	if err := sealedStore.Commit(ctx, []*es.DomainEvent{globalEvent}, 0); err != nil {
		panic(err)
	}

	// --- erasure: Acme Corp's Alice asks to be forgotten. Processed by an
	// admin, not by Alice herself. ---
	erased := accounts[0]
	{
		acc, err := repo.Load(ctx, erased.id, es.WithTenant(erased.org))
		if err != nil {
			panic(err)
		}
		admin := domain.Actor{ID: uuid.New(), Type: "admin"}
		if err := acc.RequestHolderErasure(admin); err != nil {
			panic(err)
		}
		if err := repo.Save(ctx, acc, es.WithTenant(erased.org)); err != nil {
			panic(err)
		}
	}

	// --- wait for every organization's async read model to catch up to
	// each account's actual final version (source of truth: repo.Load,
	// synchronous), before reading statements below. ---
	finalByAccount := make(map[uuid.UUID]*domain.Account, len(accounts))
	for _, acc := range accounts {
		loaded, err := repo.Load(ctx, acc.id, es.WithTenant(acc.org))
		if err != nil {
			panic(err)
		}
		finalByAccount[acc.id] = loaded
	}
	for _, acc := range accounts {
		waitCtx, waitCancel := context.WithTimeout(ctx, 2*time.Second)
		select {
		case <-workers[acc.org].ready.Subscribe(acc.id, finalByAccount[acc.id].GetVersion()):
		case <-waitCtx.Done():
			fmt.Printf("timed out waiting for %s's read model to catch up on %s\n", acc.orgName, acc.holderName)
		}
		waitCancel()
	}

	// --- KEK rotation (es.WrappedKeyRing.Rotate): every surviving holder
	// key is re-wrapped under a new master key. Alice's was already
	// forgotten by her erasure request, so it stays gone; the other five
	// accounts' DEKs become readable only under the new KEK — no event is
	// re-recorded, and the old KEK can leave the building. ---
	kek2, err := es.NewAesGcmWrapper(bytes.Repeat([]byte{2}, 32), "kek:v2")
	if err != nil {
		panic(err)
	}
	if err := keyRing.Rotate(ctx, kek2); err != nil {
		panic(err)
	}
	fmt.Printf("keyring rotated to %s: all 5 surviving holder keys re-wrapped under the new KEK, %d older KEK no longer needed\n",
		kek2.Label(), len(accounts)-1)

	// --- provable erasure (es.ErasureVerifier): tie together the three
	// independent sources of truth deletion touches for Alice — the ledger
	// record, the (now-forgotten) key, and every stored event that
	// declared PII — into one answer a controller could show a regulator
	// instead of asserting "we deleted it, trust us". Checked *after* the
	// KEK rotation above on purpose: it proves rotation didn't accidentally
	// resurrect (or break the record of) a subject who was already gone.
	erasureVerifier := es.NewErasureVerifier(sealedStore, keyRing, erasureLedger)
	erasureProof, err := erasureVerifier.Verify(ctx, erased.id)
	if err != nil {
		panic(err)
	}
	fmt.Printf("erasure proof for %s: %d ledger record(s), key forgotten=%v, all PII-bearing events unrecoverable=%v\n",
		erased.holderName, len(erasureProof.Erasures), erasureProof.KeyForgotten, erasureProof.EventsUnrecoverable)

	// --- edge case: rebuilding a read model from scratch *after* an
	// erasure. Acme's live worker above already processed (and cached)
	// Alice's real name long before her erasure request landed, so it
	// never actually observes PiiUnrecoverable — that's the common case,
	// but not the only one. A fresh projector backfilling from global_id 0
	// — exactly what standing up a new read model, or recovering a
	// corrupted one, looks like — hits her AccountOpened event only after
	// the key is already gone. ---
	rebuiltBook := newStatementBook()
	rebuiltDispatcher := es.NewEventDispatcher()
	rebuiltDispatcher.Subscribe(domain.AllAccountEvents, &statementProjector{book: rebuiltBook})

	rebuildCtx, rebuildCancel := context.WithTimeout(ctx, 2*time.Second)
	rebuildOut, rebuildErrc := es.NewTenantScopedStore(sealedStore, orgA).StreamAll(rebuildCtx)
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

	// --- reports ---
	allEvents, err := sealedStore.FetchAfter(ctx, 0, 10_000, es.WithAllTenants())
	if err != nil {
		panic(err)
	}
	orgAEvents, err := workers[orgA].store.FetchAfter(ctx, 0, 10_000)
	if err != nil {
		panic(err)
	}
	orgBEvents, err := workers[orgB].store.FetchAfter(ctx, 0, 10_000)
	if err != nil {
		panic(err)
	}
	fmt.Printf("platform report: %d events total across all tenants (Acme: %d, Globex: %d, incl. the shared exchange-rate fact each can see)\n",
		len(allEvents), len(orgAEvents), len(orgBEvents))

	rawErased, err := raw.Load(ctx, erased.id, es.WithTenant(erased.org))
	if err != nil {
		panic(err)
	}
	fmt.Printf("raw event at rest for %s (holder fields are ciphertext): %s\n", erased.holderName, rawErased[0].Payload)

	fmt.Println()
	fmt.Println("--- end-to-end checks ---")

	// ============================================================
	// e2e assertions — every fact this program is supposed to prove.
	// ============================================================
	checks := 0
	check := func(cond bool, format string, args ...any) {
		checks++
		msg := fmt.Sprintf(format, args...)
		if !cond {
			fmt.Println("FAIL:", msg)
			os.Exit(1)
		}
		fmt.Println("OK:  ", msg)
	}

	for _, acc := range accounts {
		isErased := acc.id == erased.id

		loaded, err := repo.Load(ctx, acc.id, es.WithTenant(acc.org))
		check(err == nil, "%s: Repository.Load succeeds (err=%v)", acc.holderName, err)

		check(loaded.Balance == expectedBalanceCents,
			"%s: balance is %d cents regardless of the 3 concurrent transactions' landing order",
			acc.holderName, expectedBalanceCents)

		if isErased {
			check(loaded.HolderName != acc.holderName,
				"%s: holder name is unrecoverable ciphertext after erasure, not the real name", acc.holderName)
		} else {
			check(loaded.HolderName == acc.holderName && loaded.HolderEmail == acc.holderEmail,
				"%s: holder name/email decrypt correctly (PiiEventStore round-trip)", acc.holderName)
		}

		// Tenant isolation: the same account id under the *other*
		// organization simply doesn't exist.
		otherOrg := orgB
		if acc.org == orgB {
			otherOrg = orgA
		}
		_, err = repo.Load(ctx, acc.id, es.WithTenant(otherOrg))
		check(errors.Is(err, es.ErrAggregateNotFound),
			"%s: invisible under the other organization's tenant scope", acc.holderName)

		// PII at rest: raw (unwrapped) store shows ciphertext for holder
		// fields, for *every* account, not just the erased one — sealing
		// always happens on Commit, decryption is what Repository/
		// PiiEventStore provide on top of it.
		rawEvents, err := raw.Load(ctx, acc.id, es.WithTenant(acc.org))
		wantRawCount := 4 // 1 open + 3 transactions
		if isErased {
			wantRawCount = 5 // + 1 erasure request
		}
		check(err == nil && len(rawEvents) == wantRawCount,
			"%s: raw store holds exactly %d events", acc.holderName, wantRawCount)
		for i, e := range rawEvents {
			wantVersion := uint64(i + 1)
			check(e.AggregateVersion == wantVersion,
				"%s: event %d has aggregate_version %d (no gap/duplicate from the concurrent retries)",
				acc.holderName, i, wantVersion)
		}

		check(rawEvents[0].Name == domain.AccountOpened, "%s: first event is AccountOpened", acc.holderName)
		var rawOpen struct {
			HolderName string `json:"holder_name"`
		}
		check(rawEvents[0].UnmarshalPayload(&rawOpen) == nil, "%s: raw AccountOpened payload unmarshals cleanly (still valid JSON, just sealed)", acc.holderName)
		check(rawOpen.HolderName != acc.holderName, "%s: raw payload's holder_name is ciphertext, not plaintext", acc.holderName)

		// Subject-centric export (es.PiiLookup / es.SubjectExporter): the
		// data-subject file a DSAR/access response is built from. For the
		// erased holder the exported AccountOpened identity comes back
		// flagged PiiUnrecoverable; for everyone else it decrypts cleanly —
		// and the rotation above keyRing.Rotate never broke it.
		export, err := exporter.Export(ctx, acc.id, es.WithTenant(acc.org))
		check(err == nil, "%s: subject export succeeds (err=%v)", acc.holderName, err)
		check(export != nil && len(export.Aggregates) == 1 && len(export.Aggregates[0].Events) == wantRawCount,
			"%s: subject export holds exactly 1 aggregate stream of %d events", acc.holderName, wantRawCount)
		var exportedOpen domain.AccountOpenedEvent
		check(export != nil && export.Aggregates[0].Events[0].UnmarshalPayload(&exportedOpen) == nil,
			"%s: exported AccountOpened payload unmarshals cleanly", acc.holderName)
		if isErased {
			check(export != nil && export.Aggregates[0].Events[0].PiiUnrecoverable && exportedOpen.HolderName != acc.holderName,
				"%s: exported holder identity is flagged unrecoverable after erasure", acc.holderName)
		} else {
			check(export != nil && !export.Aggregates[0].Events[0].PiiUnrecoverable && exportedOpen.HolderName == acc.holderName,
				"%s: exported holder identity decrypts cleanly", acc.holderName)
		}

		// The same export, as the deterministic JSON document a DSAR
		// response would actually ship (SubjectExporter.ExportJSON /
		// SubjectExport.MarshalJSON) — decoded back into a generic document
		// here only to assert its shape; a real responder would just hand
		// the bytes to whoever asked.
		jsonDoc, err := exporter.ExportJSON(ctx, acc.id, es.WithTenant(acc.org))
		check(err == nil && len(jsonDoc) > 0, "%s: DSAR JSON export succeeds", acc.holderName)
		var decoded struct {
			PiiID      uuid.UUID `json:"pii_id"`
			Aggregates []struct {
				Events []struct {
					PiiUnrecoverable bool `json:"pii_unrecoverable"`
				} `json:"events"`
			} `json:"aggregates"`
		}
		check(json.Unmarshal(jsonDoc, &decoded) == nil, "%s: DSAR JSON export decodes cleanly", acc.holderName)
		check(decoded.PiiID == acc.id, "%s: DSAR JSON export's pii_id is the account id", acc.holderName)
		check(len(decoded.Aggregates) == 1 && len(decoded.Aggregates[0].Events) == wantRawCount,
			"%s: DSAR JSON export holds the same %d events as the in-memory export", acc.holderName, wantRawCount)
		check(decoded.Aggregates[0].Events[0].PiiUnrecoverable == isErased,
			"%s: DSAR JSON export's pii_unrecoverable on the identity event matches erasure status", acc.holderName)
		if isErased {
			fmt.Printf("DSAR export document (JSON) for %s, post-erasure:\n%s\n", acc.holderName, jsonDoc)
		}

		// Async read model (per-organization, tenant-scoped worker) agrees
		// with the synchronous source of truth.
		view, ok := workers[acc.org].book.get(acc.id)
		check(ok && view.Balance == expectedBalanceCents,
			"%s: %s's async statement shows the same balance as Repository.Load", acc.holderName, acc.orgName)

		// ...and does NOT leak into the *other* organization's book.
		_, leaked := workers[otherOrg].book.get(acc.id)
		check(!leaked, "%s: does not appear in the other organization's statement book", acc.holderName)
	}

	// Rebuilt-after-erasure read model: unlike the live worker's book,
	// which cached Alice's real name before she was erased, this one only
	// ever saw her AccountOpened event after the key was gone — proving
	// DomainEvent.PiiUnrecoverable actually gets set (not just theorized
	// about), and that statementProjector's placeholder branch runs.
	for _, acc := range accounts {
		if acc.org != orgA {
			continue
		}
		view, ok := rebuiltBook.get(acc.id)
		check(ok && view.Balance == expectedBalanceCents,
			"%s: rebuilt-from-scratch statement still shows the correct balance", acc.holderName)
		if acc.id == erased.id {
			check(view.HolderName == "[erased]",
				"%s: rebuilt statement shows the placeholder, not raw ciphertext, for the erased holder", acc.holderName)
		} else {
			check(view.HolderName == acc.holderName,
				"%s: rebuilt statement still shows the real name (this subject was never erased)", acc.holderName)
		}
	}

	// Provable erasure (es.ErasureVerifier): all three independent sources
	// of truth agree Alice is really gone, and the hash chain behind them
	// hasn't been tampered with (Verify itself already called
	// CheckShredChain over the whole ledger — reaching this far, with a nil
	// error, is that check passing).
	check(len(erasureProof.Erasures) >= 1, "erasure proof for %s cites at least one ledger record", erased.holderName)
	check(erasureProof.KeyForgotten, "erasure proof for %s confirms the key is gone from the keyring", erased.holderName)
	check(erasureProof.EventsUnrecoverable, "erasure proof for %s confirms every PII-bearing event is unrecoverable", erased.holderName)

	// A subject who was *never* erased has no ledger record at all —
	// ErrNoErasureRecord, not a false "proof" of something that never
	// happened.
	neverErased := accounts[1] // Bob: registered, never asked to be forgotten
	_, err = erasureVerifier.Verify(ctx, neverErased.id)
	check(errors.Is(err, es.ErrNoErasureRecord), "%s: erasure verification correctly fails — this subject was never erased", neverErased.holderName)

	// Global, tenant-less fact: both organizations' tenant-bound stores can
	// see it, alongside (not instead of) their own data.
	_, err = workers[orgA].store.Load(ctx, globalEvent.AggregateID)
	check(err == nil, "Acme's tenant-scoped store can see the shared exchange-rate fact")
	_, err = workers[orgB].store.Load(ctx, globalEvent.AggregateID)
	check(err == nil, "Globex's tenant-scoped store can see the same shared exchange-rate fact")

	// Platform-wide counts: 3 opens + 9 transactions each organization,
	// +1 erasure for Acme only, +1 shared global fact each org also sees.
	check(len(orgAEvents) == 3+9+1+1, "Acme's tenant-scoped report has exactly %d events", 3+9+1+1)
	check(len(orgBEvents) == 3+9+1, "Globex's tenant-scoped report has exactly %d events", 3+9+1)
	check(len(allEvents) == (3+9+1)+(3+9)+1, "es.WithAllTenants report has exactly %d events platform-wide", (3+9+1)+(3+9)+1)

	fmt.Println()
	fmt.Printf("ALL %d CHECKS PASSED\n", checks)
}
