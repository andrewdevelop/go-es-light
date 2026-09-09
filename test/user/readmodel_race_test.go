// Package user_test exercises examples/user for thread-safety (many
// goroutines hitting the same in-process instances, run with -race) and,
// in replication_test.go (build tag integration), for safety when several
// app replicas share one real Postgres database.
package user_test

import (
	"sync"
	"testing"

	"github.com/google/uuid"

	"go-es-light/examples/user/adapters/readmodel"
	"go-es-light/examples/user/app"
)

// TestReadModelStore_ConcurrentAccess hammers Upsert/Get for both
// overlapping and distinct user IDs from many goroutines. It exists purely
// to catch data races in adapters/readmodel.Store under `go test -race` —
// the projector goroutine writes to it while query code reads from it
// concurrently in any real deployment.
func TestReadModelStore_ConcurrentAccess(t *testing.T) {
	store := readmodel.New()

	const (
		writers            = 50
		writesPerGoroutine = 20
		sharedIDs          = 5 // a handful of IDs every writer contends on
	)

	ids := make([]uuid.UUID, sharedIDs)
	for i := range ids {
		ids[i] = uuid.New()
	}

	var wg sync.WaitGroup
	wg.Add(writers * 2) // writers + readers

	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				id := ids[(i+j)%sharedIDs]
				store.Upsert(app.UserView{ID: id, Email: "race@example.com", Name: "Race"})
			}
		}(i)

		go func(i int) {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				id := ids[(i+j)%sharedIDs]
				store.Get(id) // return value intentionally ignored: only racing matters here
			}
		}(i)
	}

	wg.Wait()

	for _, id := range ids {
		if view, ok := store.Get(id); ok && view.ID != id {
			t.Fatalf("view for %v came back with mismatched ID %v", id, view.ID)
		}
	}
}
