package user_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/examples/user/adapters/eventstore"
	"go-es-light/examples/user/adapters/readmodel"
	"go-es-light/examples/user/app"
	"go-es-light/examples/user/domain"
)

// TestUserService_ConcurrentDistinctRegistrations calls RegisterUser for
// many distinct users from many goroutines against a single UserService —
// the shape an HTTP server actually uses it in (one instance, many
// concurrent requests). Run with -race to catch any shared-state bug in
// the service/repository/aggregate path.
func TestUserService_ConcurrentDistinctRegistrations(t *testing.T) {
	ctx := context.Background()
	repo, _ := eventstore.New()
	svc := app.NewUserService(repo)

	const users = 200
	ids := make([]uuid.UUID, users)
	for i := range ids {
		ids[i] = uuid.New()
	}

	var wg sync.WaitGroup
	wg.Add(users)
	for i := 0; i < users; i++ {
		go func(i int) {
			defer wg.Done()
			_, err := svc.RegisterUser(ctx, app.RegisterUserCommand{
				UserID: ids[i],
				Email:  fmt.Sprintf("user%d@example.com", i),
				Name:   fmt.Sprintf("User %d", i),
			})
			if err != nil {
				t.Errorf("RegisterUser(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	for i, id := range ids {
		user, err := repo.Load(ctx, id)
		if err != nil {
			t.Fatalf("Load(%d): %v", i, err)
		}
		wantEmail := fmt.Sprintf("user%d@example.com", i)
		if user.Email != wantEmail {
			t.Fatalf("user %d: expected email %q, got %q (cross-talk between concurrent registrations)", i, wantEmail, user.Email)
		}
	}
}

// TestUserService_ConcurrentUpdatesToSameUser races many goroutines
// changing the *same* user's email, each retrying on
// es.ErrConcurrencyConflict the way a real command handler must (see
// TASK.md). It proves no update is lost or double-applied when several
// requests hit the same aggregate at once — the exact scenario
// UNIQUE(aggregate_id, aggregate_version) exists for — and, run alongside
// the projector goroutine below with -race, that building the read model
// concurrently with writes doesn't race either.
func TestUserService_ConcurrentUpdatesToSameUser(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo, store := eventstore.New()
	svc := app.NewUserService(repo)

	views := readmodel.New()
	dispatcher := es.NewEventDispatcher()
	dispatcher.Subscribe(domain.AllUserEvents, &readmodel.UserProjector{Store: views})

	go func() {
		out, errc := store.StreamAll(ctx)
		for e := range out {
			if err := dispatcher.Dispatch(ctx, e); err != nil {
				t.Errorf("dispatch: %v", err)
			}
		}
		for err := range errc {
			if err != nil {
				t.Errorf("stream: %v", err)
			}
		}
	}()

	userID := uuid.New()
	if _, err := svc.RegisterUser(ctx, app.RegisterUserCommand{UserID: userID, Email: "seed@example.com", Name: "Seed"}); err != nil {
		t.Fatalf("seed RegisterUser: %v", err)
	}

	const writers = 100
	var successes int64
	var wg sync.WaitGroup
	wg.Add(writers)

	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()
			email := fmt.Sprintf("writer%d@example.com", i)
			for {
				_, err := svc.ChangeEmail(ctx, app.ChangeEmailCommand{UserID: userID, Email: email})
				if err == nil {
					atomic.AddInt64(&successes, 1)
					return
				}
				if errors.Is(err, es.ErrConcurrencyConflict) {
					continue // reload-and-retry, exactly as a real handler must
				}
				t.Errorf("ChangeEmail(%d): %v", i, err)
				return
			}
		}(i)
	}
	wg.Wait()

	if successes != writers {
		t.Fatalf("expected %d successful writers, got %d", writers, successes)
	}

	events, err := store.Load(ctx, userID)
	if err != nil {
		t.Fatalf("Load raw events: %v", err)
	}
	if len(events) != writers+1 { // +1 for the seed registration
		t.Fatalf("expected %d events (1 registration + %d email changes), got %d — lost or duplicated update", writers+1, writers, len(events))
	}

	final, err := repo.Load(ctx, userID)
	if err != nil {
		t.Fatalf("final Load: %v", err)
	}

	// Poll instead of a fixed sleep: the read model is built asynchronously
	// by the projector goroutine above, so give it a bounded amount of time
	// to catch up to whatever the last successful write settled on.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if view, ok := views.Get(userID); ok && view.Email == final.Email {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("read model never caught up to final aggregate email %q", final.Email)
		}
		time.Sleep(time.Millisecond)
	}
}
