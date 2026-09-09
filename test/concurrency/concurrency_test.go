// Package concurrency_test exercises the optimistic-concurrency contract of
// es.Repository/es.EventStore under real goroutine races: many writers
// racing on the same aggregate must never lose an update and must never
// double-apply one — exactly what TASK.md's UNIQUE(aggregate_id, seq_no)
// discussion is about. It runs against memstore (fast, in-process); the
// same contract is re-verified against a real Postgres in
// /test/pgstore (build tag integration).
package concurrency_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"go-es-light/es"
	"go-es-light/es/memstore"
)

type CounterIncremented struct{}

func (CounterIncremented) EventName() string { return "CounterIncremented" }

// Counter is a deliberately trivial aggregate: Increment has no business
// rule that can fail, so every conflict below is a pure concurrency
// conflict, not a domain rejection.
type Counter struct {
	es.BaseAggregate
	Value int
}

func (c *Counter) Apply(event any) error {
	switch event.(type) {
	case *CounterIncremented:
		c.Value++
	default:
		return fmt.Errorf("counter: unknown event %T", event)
	}
	return nil
}

func (c *Counter) Increment() error {
	return c.RecordThat(c, &CounterIncremented{})
}

func newCounterRepo(store es.EventStore) *es.Repository[*Counter] {
	reg := es.NewRegistry()
	reg.Register("CounterIncremented", func() any { return &CounterIncremented{} })
	return es.NewRepository(store, reg, func() *Counter { return &Counter{} })
}

// TestConcurrentIncrements_NoLostUpdates hammers Save on the same aggregate
// id from many goroutines simultaneously. Each writer retries on
// ErrConcurrencyConflict — the pattern TASK.md prescribes for the command
// handler — and the test asserts that every single increment survives:
// final count equals the number of writers, no more, no less.
func TestConcurrentIncrements_NoLostUpdates(t *testing.T) {
	const writers = 300

	ctx := context.Background()
	store := memstore.New()
	repo := newCounterRepo(store)
	id := uuid.New()

	var successes int64
	var wg sync.WaitGroup
	wg.Add(writers)

	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()

			for {
				counter, err := repo.Load(ctx, id)
				if errors.Is(err, es.ErrAggregateNotFound) {
					counter = &Counter{}
					counter.SetID(id)
				} else if err != nil {
					t.Errorf("Load: %v", err)
					return
				}

				if err := counter.Increment(); err != nil {
					t.Errorf("Increment: %v", err)
					return
				}

				err = repo.Save(ctx, counter)
				if err == nil {
					atomic.AddInt64(&successes, 1)
					return
				}
				if errors.Is(err, es.ErrConcurrencyConflict) {
					continue // reload and retry, as TASK.md's command handler must
				}
				t.Errorf("Save: %v", err)
				return
			}
		}()
	}

	wg.Wait()

	if successes != writers {
		t.Fatalf("expected %d successful writers, got %d", writers, successes)
	}

	final, err := repo.Load(ctx, id)
	if err != nil {
		t.Fatalf("final Load: %v", err)
	}
	if final.Value != writers {
		t.Fatalf("expected final value %d, got %d (lost or duplicated update)", writers, final.Value)
	}
	if final.GetVersion() != uint64(writers) {
		t.Fatalf("expected final version %d, got %d", writers, final.GetVersion())
	}

	events, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load raw events: %v", err)
	}
	if len(events) != writers {
		t.Fatalf("expected %d persisted events, got %d", writers, len(events))
	}
	for i, e := range events {
		wantVersion := uint64(i + 1)
		if e.AggregateVersion != wantVersion {
			t.Fatalf("event %d: expected aggregate_version %d, got %d (ordering/gap bug)", i, wantVersion, e.AggregateVersion)
		}
	}
}

// TestConcurrentIncrements_NonRetryingWritersConflictExactlyOnce starts two
// writers from the exact same loaded snapshot (no retry): exactly one must
// succeed and the other must see ErrConcurrencyConflict — never both
// succeeding, never both failing.
func TestConcurrentIncrements_NonRetryingWritersConflictExactlyOnce(t *testing.T) {
	ctx := context.Background()
	store := memstore.New()
	repo := newCounterRepo(store)
	id := uuid.New()

	seed := &Counter{}
	seed.SetID(id)
	if err := seed.Increment(); err != nil {
		t.Fatalf("seed Increment: %v", err)
	}
	if err := repo.Save(ctx, seed); err != nil {
		t.Fatalf("seed Save: %v", err)
	}

	const racers = 20
	results := make(chan error, racers)
	var start sync.WaitGroup
	start.Add(1)

	for i := 0; i < racers; i++ {
		snapshot, err := repo.Load(ctx, id)
		if err != nil {
			t.Fatalf("Load snapshot %d: %v", i, err)
		}
		if err := snapshot.Increment(); err != nil {
			t.Fatalf("Increment snapshot %d: %v", i, err)
		}

		go func(c *Counter) {
			start.Wait() // line every goroutine up before releasing them together
			results <- repo.Save(ctx, c)
		}(snapshot)
	}
	start.Done()

	var ok, conflicts int
	for i := 0; i < racers; i++ {
		switch err := <-results; {
		case err == nil:
			ok++
		case errors.Is(err, es.ErrConcurrencyConflict):
			conflicts++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if ok != 1 {
		t.Fatalf("expected exactly 1 winner among racers sharing one stale snapshot, got %d", ok)
	}
	if conflicts != racers-1 {
		t.Fatalf("expected %d conflicts, got %d", racers-1, conflicts)
	}
}
