package domain

import (
	"errors"
	"fmt"

	"github.com/google/uuid"

	"go-es-light/es"
)

var (
	ErrAlreadyOpened  = errors.New("account: already opened")
	ErrNotOpened      = errors.New("account: not opened")
	ErrHolderRequired = errors.New("account: holder name and email are required")
	ErrZeroAmount     = errors.New("account: transaction amount must be non-zero")
)

// Actor identifies who performed the action that produced an event — see
// es.WithActor. Type is caller-defined (e.g. "customer", "teller",
// "system"); an empty Actor{} means "no actor recorded," and every method
// below treats it that way — es.WithActor is only added to RecordThat when
// actor.ID is set.
type Actor struct {
	ID   uuid.UUID
	Type string
}

func (a Actor) isSet() bool { return a.ID != uuid.Nil }

// Account is a per-user ledger account within one organization. Balance is
// deliberately never recorded as a fact of its own — it only ever exists
// as the running total Apply derives from OpeningBalance plus every
// TransactionRecordedEvent since. That's the point of sourcing a ledger
// from events at all: the balance is a projection of the log, not
// something that can drift from it.
type Account struct {
	es.BaseAggregate
	HolderName  string
	HolderEmail string
	Balance     int64 // cents
	opened      bool
}

func (a *Account) Apply(event any) error {
	switch e := event.(type) {
	case *AccountOpenedEvent:
		if a.opened {
			return ErrAlreadyOpened
		}
		a.HolderName = e.HolderName
		a.HolderEmail = e.HolderEmail
		a.Balance = e.OpeningBalance
		a.opened = true
	case *TransactionRecordedEvent:
		if !a.opened {
			return ErrNotOpened
		}
		a.Balance += e.AmountCents
	case *HolderErasureRequestedEvent:
		if !a.opened {
			return ErrNotOpened
		}
		// No state change: the erasure request is itself the domain fact
		// worth recording. What actually happens to HolderName/HolderEmail
		// at rest is the store's job (es.PiiEventStore), driven by this
		// event implementing es.RequestsErasure — see events.go.
	default:
		return fmt.Errorf("account: unknown event %T", event)
	}
	return nil
}

// Open records that this account now exists, with holderName/holderEmail
// as its PII-declared holder identity (see AccountOpenedEvent.PiiFields)
// and openingBalanceCents as its starting balance. actor is typically the
// account holder themselves (self-service signup) or an operator opening
// it on their behalf.
//
// WithEventVersion(1) is passed explicitly even though 1 is already
// RecordThat's default — schema versioning only starts to matter once a
// second version of this payload shape ships, but it's here so the
// mechanism (and where it would go) is visible.
func (a *Account) Open(holderName, holderEmail string, openingBalanceCents int64, actor Actor) error {
	if holderName == "" || holderEmail == "" {
		return ErrHolderRequired
	}
	opts := []es.RecordOption{es.WithPiiID(a.GetID()), es.WithEventVersion(1)}
	if actor.isSet() {
		opts = append(opts, es.WithActor(actor.ID, actor.Type))
	}
	return a.RecordThat(a, &AccountOpenedEvent{
		HolderName:     holderName,
		HolderEmail:    holderEmail,
		OpeningBalance: openingBalanceCents,
	}, opts...)
}

// RecordTransaction posts one entry (deposit if amountCents > 0, withdrawal
// if < 0) against an already-open account, tagged with an ad-hoc channel
// (e.g. "mobile", "branch", "batch") via es.WithMetadata. opts is optional
// — see WithBackdated/WithExternalID below, for entries that didn't
// originate from "now," in this process.
//
// Every transaction carries es.WithPiiID(a.GetID()) too: the transaction is
// the holder's activity data, so it belongs in the holder's subject file
// (es.SubjectExporter/DSAR) even though its payload declares no PII fields
// (TransactionRecordedEvent implements no es.ContainsPersonalData, so it's
// never sealed — the pii_id column alone ties it to the subject).
func (a *Account) RecordTransaction(amountCents int64, description, channel string, actor Actor, opts ...es.RecordOption) error {
	if amountCents == 0 {
		return ErrZeroAmount
	}
	all := append([]es.RecordOption{es.WithMetadata(map[string]any{"channel": channel}), es.WithPiiID(a.GetID())}, opts...)
	if actor.isSet() {
		all = append(all, es.WithActor(actor.ID, actor.Type))
	}
	return a.RecordThat(a, &TransactionRecordedEvent{AmountCents: amountCents, Description: description}, all...)
}

// RequestHolderErasure records that this account's holder has asked to be
// forgotten (GDPR/CCPA "right to be forgotten"). actor is typically an
// admin or support agent processing the request on the holder's behalf,
// not the holder themselves — worth recording who actioned it, same as any
// other change.
func (a *Account) RequestHolderErasure(actor Actor) error {
	if !a.opened {
		return ErrNotOpened
	}
	opts := []es.RecordOption{es.WithPiiID(a.GetID())}
	if actor.isSet() {
		opts = append(opts, es.WithActor(actor.ID, actor.Type))
	}
	return a.RecordThat(a, &HolderErasureRequestedEvent{}, opts...)
}
