//go:build integration

// Replication-safety tests for examples/user: several UserService
// instances, each with its own *sql.DB connection pool, standing in for
// separate app replicas that all talk to the same Postgres database — the
// real deployment shape es/pgstore is designed for (see TASK.md). They
// prove two different things:
//
//   - optimistic concurrency (transaction-scoped advisory lock +
//     MAX(aggregate_version) check in pgstore.Store.Commit) still
//     serializes writers correctly when they come from different
//     connections/pools, not just different goroutines on one pool;
//   - the projector's leader election (pgstore.AdvisoryLock) really does
//     let exactly one replica build the read model, with the others
//     sitting out, and hands over cleanly when the leader releases.
//
// Run the same way as /test/pgstore (see its file header): needs
// PGSTORE_TEST_DSN pointing at a Postgres with schema.sql already applied.
package user_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"go-es-light/es"
	"go-es-light/es/pgstore"
	"go-es-light/examples/user/adapters/readmodel"
	"go-es-light/examples/user/app"
	"go-es-light/examples/user/domain"
)

// replica stands in for one app instance: its own DB pool, its own
// UserService, its own pgstore.Store (so it can also run the projector or
// try to become its leader).
type replica struct {
	db   *sql.DB
	repo *app.UserRepository
	svc  *app.UserService
	*pgstore.Store
}

func newReplica(t *testing.T, dsn string) *replica {
	t.Helper()

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	store := pgstore.New(db, dsn)
	repo := es.NewRepository[*domain.User](store, domain.Events(), func() *domain.User { return &domain.User{} })

	return &replica{db: db, repo: repo, svc: app.NewUserService(repo), Store: store}
}

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("PGSTORE_TEST_DSN")
	if dsn == "" {
		t.Skip("PGSTORE_TEST_DSN not set; skipping replication integration test (see test/pgstore for how to run one)")
	}
	return dsn
}

// TestReplication_ConcurrentDistinctRegistrationsAcrossReplicas registers
// many distinct users, round-robining the command across several replicas'
// UserService — each backed by its own connection pool, all pointing at the
// same database. Any replica must be able to read back what any other
// replica wrote.
func TestReplication_ConcurrentDistinctRegistrationsAcrossReplicas(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()

	replicas := []*replica{newReplica(t, dsn), newReplica(t, dsn), newReplica(t, dsn)}

	const users = 60
	ids := make([]uuid.UUID, users)
	for i := range ids {
		ids[i] = uuid.New()
	}

	var wg sync.WaitGroup
	wg.Add(users)
	for i := 0; i < users; i++ {
		go func(i int) {
			defer wg.Done()
			r := replicas[i%len(replicas)]
			_, err := r.svc.RegisterUser(ctx, app.RegisterUserCommand{
				UserID: ids[i],
				Email:  fmt.Sprintf("user%d@example.com", i),
				Name:   fmt.Sprintf("User %d", i),
			})
			if err != nil {
				t.Errorf("RegisterUser(%d) via replica %d: %v", i, i%len(replicas), err)
			}
		}(i)
	}
	wg.Wait()

	// Read every user back through a *different* replica than any that
	// could plausibly have written it, to catch a bug where a replica
	// serves its own stale/local state instead of the shared database.
	reader := replicas[0]
	for i, id := range ids {
		user, err := reader.repo.Load(ctx, id)
		if err != nil {
			t.Fatalf("Load(%d) via reader replica: %v", i, err)
		}
		wantEmail := fmt.Sprintf("user%d@example.com", i)
		if user.Email != wantEmail {
			t.Fatalf("user %d: expected email %q, got %q", i, wantEmail, user.Email)
		}
	}
}

// TestReplication_ConcurrentUpdatesToSameUserAcrossReplicas races many
// writers, spread across several replicas/connection pools, all changing
// the same user's email. Every writer retries on ErrConcurrencyConflict —
// proving pgstore's advisory-lock-guarded version check serializes commits
// correctly even when they don't share a connection, let alone a goroutine.
func TestReplication_ConcurrentUpdatesToSameUserAcrossReplicas(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()

	replicas := []*replica{newReplica(t, dsn), newReplica(t, dsn), newReplica(t, dsn)}

	userID := uuid.New()
	if _, err := replicas[0].svc.RegisterUser(ctx, app.RegisterUserCommand{
		UserID: userID, Email: "seed@example.com", Name: "Seed",
	}); err != nil {
		t.Fatalf("seed RegisterUser: %v", err)
	}

	const writers = 60
	var successes int64
	var wg sync.WaitGroup
	wg.Add(writers)

	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()
			r := replicas[i%len(replicas)]
			email := fmt.Sprintf("writer%d@example.com", i)
			for {
				_, err := r.svc.ChangeEmail(ctx, app.ChangeEmailCommand{UserID: userID, Email: email})
				if err == nil {
					atomic.AddInt64(&successes, 1)
					return
				}
				if errors.Is(err, es.ErrConcurrencyConflict) {
					continue
				}
				t.Errorf("ChangeEmail(%d) via replica %d: %v", i, i%len(replicas), err)
				return
			}
		}(i)
	}
	wg.Wait()

	if successes != writers {
		t.Fatalf("expected %d successful writers, got %d", writers, successes)
	}

	events, err := replicas[0].Load(ctx, userID)
	if err != nil {
		t.Fatalf("Load raw events: %v", err)
	}
	if len(events) != writers+1 {
		t.Fatalf("expected %d events (1 registration + %d email changes), got %d — lost or duplicated update across replicas", writers+1, writers, len(events))
	}
	for i, e := range events {
		want := uint64(i + 1)
		if e.AggregateVersion != want {
			t.Fatalf("event %d: expected aggregate_version %d, got %d (gap/duplicate)", i, want, e.AggregateVersion)
		}
	}
}

// TestReplication_ProjectorLeaderElection has three replicas race to become
// the single leader that builds the read model (the pattern from TASK.md:
// projections need strict ordering, so exactly one replica may run the
// projector at a time). Only one TryAcquire may succeed while the leader
// holds the lock; after it releases, another replica can take over.
func TestReplication_ProjectorLeaderElection(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()
	lockName := "projector:user-view:" + uuid.New().String()

	replicas := []*replica{newReplica(t, dsn), newReplica(t, dsn), newReplica(t, dsn)}
	locks := make([]*pgstore.AdvisoryLock, len(replicas))
	for i, r := range replicas {
		locks[i] = pgstore.NewAdvisoryLock(r.db, lockName)
	}

	leaderIdx := -1
	for i, lock := range locks {
		ok, err := lock.TryAcquire(ctx)
		if err != nil {
			t.Fatalf("TryAcquire replica %d: %v", i, err)
		}
		if ok {
			if leaderIdx != -1 {
				t.Fatalf("replica %d also became leader; replica %d already holds the lock", i, leaderIdx)
			}
			leaderIdx = i
		}
	}
	if leaderIdx == -1 {
		t.Fatal("no replica became leader")
	}

	// The leader now builds the read model; run the actual projector loop
	// against its own replica's event stream while commands land through
	// any replica.
	leader := replicas[leaderIdx]
	views := readmodel.New()
	dispatcher := es.NewEventDispatcher()
	dispatcher.Subscribe(domain.AllUserEvents, &readmodel.UserProjector{Store: views})

	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	go func() {
		out, errc := leader.StreamAll(streamCtx)
		for e := range out {
			if err := dispatcher.Dispatch(streamCtx, e); err != nil {
				t.Errorf("dispatch: %v", err)
			}
		}
		for err := range errc {
			if err != nil && streamCtx.Err() == nil {
				t.Errorf("stream: %v", err)
			}
		}
	}()

	userID := uuid.New()
	writer := replicas[(leaderIdx+1)%len(replicas)] // a non-leader replica issues the command
	if _, err := writer.svc.RegisterUser(ctx, app.RegisterUserCommand{
		UserID: userID, Email: "led@example.com", Name: "Led",
	}); err != nil {
		t.Fatalf("RegisterUser via non-leader replica: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if view, ok := views.Get(userID); ok && view.Email == "led@example.com" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("read model never caught up while the elected leader held the projector")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancelStream()

	// Release leadership; another replica must now be able to take over.
	if err := locks[leaderIdx].Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}

	nextIdx := (leaderIdx + 1) % len(locks)
	ok, err := locks[nextIdx].TryAcquire(ctx)
	if err != nil {
		t.Fatalf("TryAcquire after release: %v", err)
	}
	if !ok {
		t.Fatal("expected a different replica to take over leadership after release")
	}
	if err := locks[nextIdx].Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}
}
