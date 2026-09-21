// Package domain is the ledger's core: the Account aggregate, its events,
// its business rules. Depends on nothing outside itself, same rule as
// examples/user/domain.
package domain

import "go-es-light/es"

// Event names use dot notation (aggregate.action) so an es.EventDispatcher
// subscriber can match either one exactly or every ledger.* event with a
// single wildcard subscription (see main.go).
const (
	AllAccountEvents       = "ledger.*"
	AccountOpened          = "ledger.account_opened"
	TransactionRecorded    = "ledger.transaction_recorded"
	HolderErasureRequested = "ledger.holder_erasure_requested"
)

// AccountOpenedEvent opens a new ledger account for one user within one
// organization (the tenant — see main.go's es.WithTenant usage; TenantID
// itself is never stored on the aggregate or the payload, exactly like
// examples/user/domain.User).
type AccountOpenedEvent struct {
	HolderName     string `json:"holder_name"`
	HolderEmail    string `json:"holder_email"`
	OpeningBalance int64  `json:"opening_balance_cents"`
}

func (AccountOpenedEvent) EventName() string { return AccountOpened }

// PiiFields implements es.ContainsPersonalData: the holder's name and
// email are personal data, so the es.PiiEventStore wrapping the store (see
// main.go) encrypts them at rest and decrypts them again on read. Balance
// is deliberately not PII — it must survive holder erasure intact (see
// HolderErasureRequestedEvent), since the ledger's arithmetic integrity
// matters for audit even after a subject has been forgotten.
func (AccountOpenedEvent) PiiFields() map[string][]string {
	return map[string][]string{"payload": {"holder_name", "holder_email"}}
}

// TransactionRecordedEvent posts one entry against an already-open
// account. AmountCents is signed: positive for a deposit/credit, negative
// for a withdrawal/debit. Carries no PII of its own.
type TransactionRecordedEvent struct {
	AmountCents int64  `json:"amount_cents"`
	Description string `json:"description"`
}

func (TransactionRecordedEvent) EventName() string { return TransactionRecorded }

// HolderErasureRequestedEvent models the account holder asking to be
// forgotten (GDPR/CCPA "right to be forgotten"). It carries no payload of
// its own — the DomainEvent.PiiID set via es.WithPiiID when it's recorded
// already identifies which subject — and implements es.RequestsErasure,
// which is what tells the es.PiiEventStore wrapping the store to
// crypto-shred that subject's key right after this event commits. Balance
// and every TransactionRecordedEvent amount stay exactly as they were:
// only HolderName/HolderEmail become permanently unreadable.
type HolderErasureRequestedEvent struct{}

func (HolderErasureRequestedEvent) EventName() string { return HolderErasureRequested }
func (HolderErasureRequestedEvent) RequestsErasure()  {}

// Events returns the registry es.Repository (and es.PiiEventStore) need to
// turn a stored payload back into one of the concrete event types above.
func Events() es.EventRegistry {
	r := es.NewRegistry()
	r.Register(AccountOpened, func() any { return &AccountOpenedEvent{} })
	r.Register(TransactionRecorded, func() any { return &TransactionRecordedEvent{} })
	r.Register(HolderErasureRequested, func() any { return &HolderErasureRequestedEvent{} })
	return r
}
