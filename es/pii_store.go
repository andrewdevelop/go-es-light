package es

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// PiiEventStoreOption customizes a PiiEventStore at construction time. See
// WithErasureLedger.
type PiiEventStoreOption func(*PiiEventStore)

// WithErasureLedger mounts a durable erasure ledger on the store: every
// successful crypto-shred (a RequestsErasure payload triggering Forget) is
// also appends a ShredRecord to ledger, making each erasure provable later —
// see ErasureVerifier. Optional; without it the store behaves exactly as
// before, just with no audit record of erasures.
func WithErasureLedger(ledger ErasureLedger) PiiEventStoreOption {
	return func(s *PiiEventStore) { s.ledger = ledger }
}

// PiiEventStore wraps an EventStore, transparently sealing (encrypting)
// each event's declared PII fields before Commit persists it, and opening
// (decrypting) them again before Load/FetchAfter/StreamAll hand events back
// — to any caller: Repository, a projector reading the raw stream directly,
// a poller, anyone. This is what makes PII encryption a genuine property of
// the store itself ("data at rest"), rather than something every
// individual consumer of the underlying EventStore has to separately
// remember to handle. Also handles crypto-shredding: after a successful
// Commit, any event whose payload implements RequestsErasure has its
// subject's key forgotten via anonymizer — and, if an erasure ledger was
// mounted with WithErasureLedger, appends a tamper-evident record of that
// shred to it (see es.ShredRecord / es.ErasureVerifier).
//
// Wrap once at the composition root and hand the result to both
// NewRepository and anything else that reads from the same store directly
// (a projector's StreamAll, a poller's FetchAfter) — none of them need a
// *PiiAnonymizer of their own, or even need to know PiiEventStore exists.
type PiiEventStore struct {
	store      EventStore
	registry   EventRegistry
	anonymizer *PiiAnonymizer
	ledger     ErasureLedger
}

// NewPiiEventStore wraps store. registry is used the same way a Repository
// already uses one — to reconstruct a raw DomainEvent's concrete payload
// type, so it can be checked against ContainsPersonalData/RequestsErasure.
// opts is optional — see WithErasureLedger.
func NewPiiEventStore(store EventStore, registry EventRegistry, anonymizer *PiiAnonymizer, opts ...PiiEventStoreOption) *PiiEventStore {
	s := &PiiEventStore{store: store, registry: registry, anonymizer: anonymizer}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// payloadFor reconstructs the concrete payload instance for e via the
// registry, the same mechanism LoadFromHistory uses during replay. Returns
// ok=false only if no factory is registered for e.Name — a failed/partial
// Unmarshal (e.g. because Seal already encrypted some fields) still yields
// a validly-typed instance, which is all callers here need it for: type
// assertions against ContainsPersonalData/RequestsErasure, not field data.
func (s *PiiEventStore) payloadFor(e *DomainEvent) (any, bool) {
	factory, ok := s.registry.Factory(e.Name)
	if !ok {
		return nil, false
	}
	payload := factory()
	_ = e.UnmarshalPayload(payload)
	return payload, true
}

// Commit implements EventStore. Seals each event's declared PII fields
// before delegating to the wrapped store, then — after a successful
// commit — crypto-shreds the subject's key for any event whose payload
// implements RequestsErasure.
func (s *PiiEventStore) Commit(ctx context.Context, events []*DomainEvent, expectedVersion uint64, opts ...StoreOption) error {
	for _, e := range events {
		payload, ok := s.payloadFor(e)
		if !ok {
			continue
		}
		if src, ok := payload.(ContainsPersonalData); ok {
			if err := s.anonymizer.Seal(ctx, e, src); err != nil {
				return err
			}
		}
	}

	if err := s.store.Commit(ctx, events, expectedVersion, opts...); err != nil {
		return err
	}

	for _, e := range events {
		if e.PiiID == nil {
			continue
		}
		payload, ok := s.payloadFor(e)
		if !ok {
			continue
		}
		if _, ok := payload.(RequestsErasure); ok {
			id := *e.PiiID
			if err := s.anonymizer.Forget(ctx, id); err != nil {
				return err
			}
			if s.ledger != nil {
				if err := s.ledger.Append(ctx, id, time.Now().UTC(), e.GlobalID); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// openAll opens every event in events, returning a new slice — the inputs
// are never mutated, matching PiiAnonymizer.Open's own contract.
func (s *PiiEventStore) openAll(ctx context.Context, events []*DomainEvent) ([]*DomainEvent, error) {
	opened := make([]*DomainEvent, len(events))
	for i, e := range events {
		oe, err := s.anonymizer.Open(ctx, e)
		if err != nil {
			return nil, err
		}
		opened[i] = oe
	}
	return opened, nil
}

// FetchByPiiID implements es.PiiLookup by delegating to the wrapped store
// and opening every returned event's PII fields — so a subject export built
// over a PiiEventStore-autowired store gets plaintext (or a PiiUnrecoverable
// flag post-erasure) with no extra handling here, exactly like Load/
// FetchAfter/StreamAll.
func (s *PiiEventStore) FetchByPiiID(ctx context.Context, piiID uuid.UUID, opts ...StoreOption) ([]*DomainEvent, error) {
	lookup, ok := s.store.(PiiLookup)
	if !ok {
		return nil, ErrPiiLookupUnsupported
	}
	events, err := lookup.FetchByPiiID(ctx, piiID, opts...)
	if err != nil {
		return nil, err
	}
	return s.openAll(ctx, events)
}

// Load implements EventStore, opening every returned event's PII fields.
func (s *PiiEventStore) Load(ctx context.Context, id uuid.UUID, opts ...StoreOption) ([]*DomainEvent, error) {
	events, err := s.store.Load(ctx, id, opts...)
	if err != nil {
		return nil, err
	}
	return s.openAll(ctx, events)
}

// FetchAfter implements EventStore, opening every returned event's PII
// fields.
func (s *PiiEventStore) FetchAfter(ctx context.Context, lastID uint64, limit int, opts ...StoreOption) ([]*DomainEvent, error) {
	events, err := s.store.FetchAfter(ctx, lastID, limit, opts...)
	if err != nil {
		return nil, err
	}
	return s.openAll(ctx, events)
}

// StreamAll implements EventStore, opening every streamed event's PII
// fields before it reaches out — this is specifically what lets a
// projector or reactor subscribed to the raw stream see plaintext without
// holding a *PiiAnonymizer itself.
func (s *PiiEventStore) StreamAll(ctx context.Context, opts ...StoreOption) (<-chan *DomainEvent, <-chan error) {
	rawOut, rawErrc := s.store.StreamAll(ctx, opts...)
	out := make(chan *DomainEvent)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		for e := range rawOut {
			opened, err := s.anonymizer.Open(ctx, e)
			if err != nil {
				errc <- err
				return
			}
			select {
			case out <- opened:
			case <-ctx.Done():
				return
			}
		}
		for err := range rawErrc {
			if err != nil {
				errc <- err
				return
			}
		}
	}()

	return out, errc
}

var _ EventStore = (*PiiEventStore)(nil)
var _ PiiLookup = (*PiiEventStore)(nil)
