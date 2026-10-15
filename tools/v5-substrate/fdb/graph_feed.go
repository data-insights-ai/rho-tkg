package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphFeedFormat = "fdb-snapshot-feed-v1"
const graphFeedPrepared, graphFeedActive, graphFeedReleased, graphFeedExpired = uint8(0), uint8(1), uint8(2), uint8(3)
const graphFeedControlBytes = 64 << 10

type graphFeedPolicy struct {
	PageRows, PageBytes                                            int
	MaxPages, MaxCalls, MaxControlBytes, MaxWorkRows, MaxWorkBytes uint64
	Read                                                           graphSnapshotPolicy
}

// Calls/work are cumulative per immutable Begin original. Record/byte/ticket
// admission is cumulative for the whole store. Two tickets per Begin and the
// fixed 64-ticket store cap also bound distinct originals; this is not a
// renewable/resumable capacity grant. The fresh workload owns exactly one.

// SDK range rows have no native byte-limit option. Page/control byte caps are
// post-SDK admission BEFORE JSON decode or owned cloning, not a hard client-RSS
// promise. Private v1 cleanup grants are fixed and separately prepaid.
func feedGet[T any](tr fdb.ReadTransaction, key fdb.Key) (value T, found bool, err error) {
	raw, err := tr.Get(key).Get()
	if err != nil {
		return value, false, err
	}
	if raw == nil {
		return value, false, nil
	}
	if len(raw) == 0 || len(raw) > graphFeedControlBytes {
		return value, false, errLimit
	}
	value, err = decode[T](raw)
	if err != nil {
		return value, false, err
	}
	canonical, err := wire(value)
	if err != nil || !bytes.Equal(canonical, raw) {
		return value, false, errors.Join(errMismatch, err)
	}
	return value, true, nil
}

func graphFeedPolicyFor(p graphCostProfile) graphFeedPolicy {
	pages := 3 + 2*p.Facts
	calls := 8 * (2*pages + 32)
	read := graphSelectorsPolicy(graphCostConfig("feed-policy", p)).Read
	return graphFeedPolicy{8, 64 << 10, pages, calls, (64 << 20) + pages*(2*graphFeedControlBytes+8192), calls * (4096 + 128*8), calls * (128 << 20), read}
}
func (p graphFeedPolicy) validate() error {
	if p.PageRows < 1 || p.PageRows > 128 || p.PageBytes < 4096 || p.PageBytes > graphFeedControlBytes || p.MaxPages < 1 || p.MaxPages > 4_000_000 || p.MaxCalls < 8 || p.MaxCalls > 1<<30 || p.MaxControlBytes < 1<<20 || p.MaxControlBytes > 1<<50 || p.MaxWorkRows < 128 || p.MaxWorkRows > 1<<40 || p.MaxWorkBytes < 8<<20 || p.MaxWorkBytes > 1<<60 || p.Read.validate() != nil || p.Read.Cleanup != graphSnapshotCleanupDefault() {
		return errInvalid
	}
	return nil
}

type graphFeedBinding struct {
	Version         uint8
	Config          config
	Source          graphHistoryBinding
	Policy          graphFeedPolicy
	Selector, Order string
	Admission       *graphTransportPolicy `json:",omitempty"`
}
type graphFeedLedger struct{ Records, Bytes, CleanupTickets uint64 }
type graphFeedStore struct {
	source  *graphHistoryRetention
	binding graphFeedBinding
}
type graphFeedOriginal struct {
	Version       uint8
	Request       string
	Secret        [32]byte
	Policy        graphFeedPolicy
	Selection     graphSnapshotRequest
	Cut           certificate
	Pin, Snapshot graphHistoryOwner
	Opening       graphSnapshotWork
	DeadlineNanos int64
	Admission     [32]byte `json:",omitzero"`
}

func (o graphFeedOriginal) validate() error {
	// Refuse variable-size caller envelopes BEFORE encoding/cloning them.
	if o.Selection.Mode != "Fresh" || o.Selection.After != nil || !bytes.Equal(o.Selection.Scope, []uint8{0, 1}) || !sameCertificate(o.Selection.At, certificate{}) || o.Selection.At.Scope != nil || o.Pin.PinRequest != "" || o.Pin.PinDigest != [32]byte{} {
		return errInvalid
	}
	for _, c := range []certificate{o.Cut, o.Pin.Cut, o.Pin.Required, o.Snapshot.Cut, o.Snapshot.Required} {
		if !validName(c.Graph) || !bytes.Equal(c.Scope, []uint8{0, 1}) || c.Round == 0 || c.Graph != o.Cut.Graph || c.Topology != o.Cut.Topology || c.Epochs != o.Cut.Epochs || c.Round > o.Cut.Round {
			return errInvalid
		}
		for g := range 2 {
			if c.Groups[g] > o.Cut.Groups[g] {
				return errInvalid
			}
		}
	}
	if !(o.Version == 1 && o.Admission == [32]byte{} || o.Version == 2 && o.Admission != [32]byte{}) || o.DeadlineNanos <= 0 || !validName(o.Request) || o.Secret == [32]byte{} || o.Policy.validate() != nil || o.Selection.Mode != "Fresh" || o.Selection.validate() != nil || !bytes.Equal(o.Selection.Scope, []byte{0, 1}) || !bytes.Equal(o.Cut.Scope, []byte{0, 1}) || o.Cut.Round == 0 || !historyOwnerValid(o.Pin) || !historyOwnerValid(o.Snapshot) || o.Pin.Kind != "pin" || o.Snapshot.Kind != "read" || o.Pin.State != historyActive || o.Snapshot.State != historyActive || !sameCertificate(o.Cut, o.Pin.Cut) || !sameCertificate(o.Cut, o.Snapshot.Cut) || !sameCertificate(o.Pin.Required, o.Snapshot.Required) || o.Snapshot.PinRequest != o.Pin.Request {
		return errInvalid
	}
	token := graphHistoryToken{o.Pin.Request, o.Cut, o.Pin.Secret}
	body, err := wire(token)
	if err != nil || o.Snapshot.PinDigest != sha256.Sum256(body) || o.Pin.Request != "feed-"+o.Request || o.Pin.Secret != feedIdentity(o.Secret, "pin") || o.Snapshot.Request != "source-read-"+hex.EncodeToString(o.Snapshot.Secret[:16]) || o.Pin.Lineage != [32]byte{} || o.Snapshot.Lineage != [32]byte{} {
		return errors.Join(errInvalid, err)
	}
	return nil
}

type graphFeedToken struct {
	Request          string
	Secret, Original [32]byte
	DeadlineNanos    int64
}
type graphFeedCursor struct {
	Version                           uint8
	Original                          [32]byte
	State                             uint8
	Positions                         [2]uint64
	Completed                         certificate
	Last                              outcome
	Pages, Calls, WorkRows, WorkBytes uint64
	Pending                           string
	TerminalRequest                   string
}
type graphFeedAction struct {
	Request, Kind string
	Input         [32]byte
	Invocations   uint8
}
type graphFeed struct {
	s        *graphFeedStore
	original graphFeedOriginal
	token    graphFeedToken
	reader   *graphHistoryRead
	lifetime *graphSnapshotLifetime
	mu       sync.Mutex
	busy     bool
}

func (s *graphFeedStore) key(kind, name string) fdb.Key {
	return s.source.h.key(0, "feed-v1/"+kind, name)
}
func newGraphFeedStore(ctx context.Context, source *graphHistoryRetention, p graphFeedPolicy) (*graphFeedStore, error) {
	return newGraphFeedStoreAdmission(ctx, source, p, nil)
}
func newGraphFeedStoreAdmission(ctx context.Context, source *graphHistoryRetention, p graphFeedPolicy, admission *graphTransportPolicy) (*graphFeedStore, error) {
	if ctx == nil || source == nil || p.validate() != nil {
		return nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := &graphFeedStore{source: source, binding: graphFeedBinding{Version: 1, Config: source.h.config, Source: source.binding, Policy: p, Selector: "full-scope;fixed-scalar-cross-edge;unfiltered-complete-before-after:v1", Order: "Round,Coordinator,Request;independent-coordinator-sequences:v1"}}
	if admission != nil {
		if admission.validate() != nil || source.h.config != graphFeedWorkloadConfig(admission.Spec, hex.EncodeToString(admission.Run[:])) {
			return nil, errInvalid
		}
		own := *admission // Fixed arrays/strings only, no caller-owned slices.
		s.binding.Version, s.binding.Admission = 2, &own
	}
	body, err := wire(s.binding)
	if err != nil {
		return nil, err
	}
	_, _, err = historyTransactContext(ctx, source.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		if err := s.admissionIn(tr); err != nil {
			return false, err
		}
		if err := source.h.checkConfig(tr, 0); err != nil {
			return false, err
		}
		key := s.key("meta", "binding")
		prior, err := tr.Get(key).Get()
		if err != nil {
			return false, err
		}
		if prior != nil {
			if !bytes.Equal(prior, body) {
				return false, errMismatch
			}
			return true, nil
		}
		if err := source.h.verifyCutIdentity(tr, cut{source.binding.Initial}); err != nil {
			return false, err
		}
		tr.Set(key, body)
		ledgerKey := s.key("meta", "ledger")
		maximum, err := wire(graphFeedLedger{math.MaxUint64, math.MaxUint64, math.MaxUint64})
		if err != nil {
			return false, err
		}
		if err := put(tr, ledgerKey, graphFeedLedger{Records: 1, Bytes: uint64(len(key) + len(body) + len(ledgerKey) + len(maximum))}); err != nil {
			return false, err
		}
		return true, nil
	})
	return s, err
}
func (s *graphFeedStore) checkIn(tr fdb.ReadTransaction) error {
	if s == nil || s.source == nil {
		return errInvalid
	}
	if err := s.admissionIn(tr); err != nil {
		return err
	}
	raw, err := tr.Get(s.key("meta", "binding")).Get()
	if err != nil {
		return err
	}
	body, err := wire(s.binding)
	if err != nil || !bytes.Equal(raw, body) {
		return errors.Join(errMismatch, err)
	}
	actual, err := s.source.h.historyRetentionIn(tr)
	if err != nil || actual == nil || !graphColumnSame(actual.binding, s.binding.Source) {
		return errors.Join(errUnavailable, err)
	}
	return nil
}
func (s *graphFeedStore) admissionIn(tr fdb.ReadTransaction) error {
	if s.binding.Admission == nil {
		if s.binding.Version != 1 {
			return errInvalid
		}
		return nil
	}
	if s.binding.Version != 2 {
		return errInvalid
	}
	body, err := wire(*s.binding.Admission)
	if err != nil {
		return err
	}
	actual, err := tr.Get(s.source.h.key(0, "workload-transport-v2", "binding")).Get()
	if err != nil || !bytes.Equal(actual, body) {
		return errors.Join(errMismatch, err)
	}
	return nil
}

// Reservation is for the largest future record, so a later ACK/terminal mutation
// never consumes an unfunded native record. The separate ledger is not graph KV.
func (s *graphFeedStore) createIn(tr fdb.Transaction, key fdb.Key, v any, reservation uint64) error {
	body, err := wire(v)
	if err != nil || len(body) > graphFeedControlBytes || uint64(len(key)+len(body)) > reservation {
		return errors.Join(errLimit, err)
	}
	prior, err := tr.Get(key).Get()
	if err != nil {
		return err
	}
	if prior != nil {
		if !bytes.Equal(prior, body) {
			return errMismatch
		}
		return nil
	}
	ledger, found, err := feedGet[graphFeedLedger](tr, s.key("meta", "ledger"))
	if err != nil || !found {
		return errors.Join(errUnavailable, err)
	}
	if ledger.Records >= 4*s.binding.Policy.MaxPages+128 || ledger.Bytes > s.binding.Policy.MaxControlBytes || reservation > s.binding.Policy.MaxControlBytes-ledger.Bytes {
		return errLimit
	}
	ledger.Records++
	ledger.Bytes += reservation
	tr.Set(key, body)
	return put(tr, s.key("meta", "ledger"), ledger)
}
func (s *graphFeedStore) originalIn(tr fdb.ReadTransaction, token graphFeedToken) (graphFeedOriginal, error) {
	if err := s.admissionIn(tr); err != nil {
		return graphFeedOriginal{}, err
	}
	var zero graphFeedOriginal
	if !validName(token.Request) || token.Secret == [32]byte{} {
		return zero, errInvalid
	}
	if err := s.checkIn(tr); err != nil {
		return zero, err
	}
	raw, err := tr.Get(s.key("original", token.Request)).Get()
	if err != nil {
		return zero, err
	}
	if len(raw) == 0 {
		return zero, errUnavailable
	}
	if len(raw) > graphFeedControlBytes {
		return zero, errLimit
	}
	o, err := decode[graphFeedOriginal](raw)
	if err != nil || o.validate() != nil || o.Request != token.Request || token.DeadlineNanos != o.DeadlineNanos || !historySecret(o.Secret, token.Secret) || sha256.Sum256(raw) != token.Original || o.Policy != s.binding.Policy || o.Cut.Graph != s.binding.Config.Graph || o.Cut.Topology != s.binding.Config.Topology || o.Cut.Epochs != s.binding.Config.Epochs {
		return zero, errors.Join(errMismatch, err)
	}
	canonical, err := wire(o)
	if err != nil || !bytes.Equal(canonical, raw) {
		return zero, errors.Join(errMismatch, err)
	}
	if o.Version != s.binding.Version {
		return zero, errMismatch
	}
	if p := s.binding.Admission; p != nil {
		digest, err := p.digest()
		if err != nil || o.Admission != digest || o.DeadlineNanos != p.DeadlineUnixNanos {
			return zero, errors.Join(errMismatch, err)
		}
	}
	return o, nil
}
func (s *graphFeedStore) cursorIn(tr fdb.ReadTransaction, token graphFeedToken) (graphFeedCursor, error) {
	o, err := s.originalIn(tr, token)
	if err != nil {
		return graphFeedCursor{}, err
	}
	c, found, err := feedGet[graphFeedCursor](tr, s.key("cursor", token.Request))
	if err != nil || !found {
		return c, errors.Join(errUnavailable, err)
	}
	if c.Version != 1 || c.Original != token.Original || c.State > graphFeedExpired || c.Calls > s.binding.Policy.MaxCalls || c.Pages > s.binding.Policy.MaxPages || c.WorkRows > s.binding.Policy.MaxWorkRows || c.WorkBytes > s.binding.Policy.MaxWorkBytes || c.Completed.Round < o.Cut.Round || !bytes.Equal(c.Completed.Scope, o.Cut.Scope) || c.Pending != "" && !validName(c.Pending) || c.State <= graphFeedActive && c.TerminalRequest != "" || c.State > graphFeedActive && !validName(c.TerminalRequest) {
		return c, errMismatch
	}
	if err := s.source.h.verifyCutIdentity(tr, cut{c.Completed}); err != nil {
		return c, err
	}
	for g := range 2 {
		if c.Positions[g] < o.Cut.Groups[g] || c.Positions[g] < c.Completed.Groups[g] {
			return c, errMismatch
		}
	}
	return c, nil
}
func feedIdentity(secret [32]byte, domain string) [32]byte {
	return sha256.Sum256(append(append([]byte("rho-fdb-feed-v1/"), []byte(domain)...), secret[:]...))
}

// Begin durably records the factory identity and finite policy BEFORE returning.
// It does not acquire a historical pin retroactively: Publish must still win the
// SAME native retirement guard. Repeating Begin authenticates the stored intent.
func (s *graphFeedStore) Begin(ctx context.Context, request string, secret [32]byte) (graphFeedOriginal, error) {
	var zero graphFeedOriginal
	if s == nil || ctx == nil || !validName(request) || secret == [32]byte{} {
		return zero, errInvalid
	}
	seed, err := s.source.readIdentity(cut{})
	if err != nil {
		return zero, err
	}
	pinSecret := feedIdentity(secret, "pin")
	deadline := time.Now().Add(30 * time.Minute)
	if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	if p := s.binding.Admission; p != nil {
		deadline = time.Unix(0, p.DeadlineUnixNanos)
		if time.Now().UnixNano() < p.AdmittedUnixNanos || !deadline.After(time.Now()) {
			return zero, context.DeadlineExceeded
		}
	}
	actual, _, err := historyTransactContext(ctx, s.source.h.db, func() error { return nil }, func(tr fdb.Transaction) (graphFeedOriginal, error) {
		if err := s.checkIn(tr); err != nil {
			return zero, err
		}
		previous, err := tr.Get(s.key("original", request)).Get()
		if err != nil {
			return zero, err
		}
		if previous != nil {
			if len(previous) > graphFeedControlBytes {
				return zero, errLimit
			}
			actual, err := decode[graphFeedOriginal](previous)
			if err != nil || actual.validate() != nil || actual.Request != request || !historySecret(actual.Secret, secret) || actual.Policy != s.binding.Policy {
				return zero, errors.Join(errMismatch, err)
			}
			token, err := graphFeedTokenFor(actual)
			if err != nil {
				return zero, err
			}
			if _, err := s.originalIn(tr, token); err != nil {
				return zero, err
			}
			c, err := s.cursorIn(tr, token)
			if err != nil {
				return zero, err
			}
			if err := s.debit(&c); err != nil {
				return zero, err
			}
			if err := put(tr, s.key("cursor", request), c); err != nil {
				return zero, err
			}
			return actual, nil
		}
		// The immutable Begin request chooses C and records its genuine factory
		// identity in ONE ordinary transaction. Unknown retry consults the original
		// first; it cannot select another C after a concurrent original commits.
		selectedValue, err := s.source.h.freshIn(tr, []uint8{0, 1}, nil)
		if err != nil {
			return zero, err
		}
		selected := selectedValue.(cut)
		required, err := s.source.readRequiredIn(tr, selected)
		if err != nil {
			return zero, err
		}
		token := graphHistoryToken{"feed-" + request, selected.record, pinSecret}
		identity := seed
		identity.Cut = selected.record
		reader, err := s.source.bindRead(identity, required, &token)
		if err != nil {
			return zero, err
		}
		// Feed-control native work is prepaid in the durable cursor. The snapshot's
		// separate opening/scan allowance starts at zero; the two are not relabelled.
		opening := graphSnapshotWork{}
		o := graphFeedOriginal{Version: s.binding.Version, Request: request, Secret: secret, Policy: s.binding.Policy, Selection: graphSnapshotRequest{Mode: "Fresh", Scope: []uint8{0, 1}}, Cut: selected.record, Pin: graphHistoryOwner{Kind: "pin", Request: token.Request, Cut: selected.record, Required: required, Secret: pinSecret, State: historyActive}, Snapshot: reader.id, Opening: opening, DeadlineNanos: deadline.UnixNano()}
		if p := s.binding.Admission; p != nil {
			o.Admission, err = p.digest()
			if err != nil {
				return zero, err
			}
		}
		if err := o.validate(); err != nil {
			return zero, err
		}
		body, err := wire(o)
		if err != nil {
			return zero, err
		}
		// Publication/cleanup state and its maximum future encoding are funded now,
		// even if the following acquisition or its reply is lost.
		if err := s.createIn(tr, s.key("original", request), o, uint64(len(s.key("original", request)))+graphFeedControlBytes); err != nil {
			return zero, err
		}
		c := graphFeedCursor{Version: 1, Original: sha256.Sum256(body), State: graphFeedPrepared, Positions: o.Cut.Groups, Completed: o.Cut}
		if err := s.debit(&c); err != nil {
			return zero, err
		}
		if err := s.createIn(tr, s.key("cursor", request), c, uint64(len(s.key("cursor", request)))+8192); err != nil {
			return zero, err
		}
		for _, owner := range []graphHistoryOwner{o.Pin, o.Snapshot} {
			if err := s.cleanupCreateIn(tr, o, owner); err != nil {
				return zero, err
			}
		}
		return o, nil
	})
	if err != nil {
		return zero, err
	}
	token, err := graphFeedTokenFor(actual)
	if err != nil {
		return zero, err
	}
	observed, _, err := graphCostParallelRead(ctx, s.source.h.db, func(tr fdb.ReadTransaction) (graphFeedOriginal, error) { return s.originalIn(tr, token) })
	if err != nil || !graphColumnSame(actual, observed) {
		return zero, errors.Join(errMismatch, err)
	}
	return observed, nil
}
func graphFeedTokenFor(o graphFeedOriginal) (graphFeedToken, error) {
	if err := o.validate(); err != nil {
		return graphFeedToken{}, err
	}
	body, err := wire(o)
	if err != nil || len(body) > graphFeedControlBytes {
		return graphFeedToken{}, errors.Join(errLimit, err)
	}
	return graphFeedToken{o.Request, o.Secret, sha256.Sum256(body), o.DeadlineNanos}, nil
}

func feedContext(ctx context.Context, token graphFeedToken) (context.Context, context.CancelFunc, error) {
	if ctx == nil || token.DeadlineNanos <= 0 {
		return nil, nil, errInvalid
	}
	bounded, cancel := context.WithDeadline(ctx, time.Unix(0, token.DeadlineNanos))
	return bounded, cancel, nil
}

// One invocation prepays the maximum native callback work of its bounded
// reservation/effect/observation calls, including SDK retries. This is declared
// logical admission, not measured network bytes or native-client RSS. A failed
// unknown invocation can overpay, never recover or reset its durable debit.
func (s *graphFeedStore) debit(c *graphFeedCursor) error {
	rows := uint64(4096 + 128*s.binding.Policy.PageRows)
	if c.Calls >= s.binding.Policy.MaxCalls || c.WorkRows > s.binding.Policy.MaxWorkRows || rows > s.binding.Policy.MaxWorkRows-c.WorkRows || c.WorkBytes > s.binding.Policy.MaxWorkBytes || 128<<20 > s.binding.Policy.MaxWorkBytes-c.WorkBytes {
		return errLimit
	}
	c.Calls++
	c.WorkRows += rows
	c.WorkBytes += 128 << 20
	return nil
}

// Each public invocation first consumes a durable action slot and reserves two
// capped SDK callback rounds (reservation + effect). Reopen/replay cannot reset
// these counters. Unknown outcomes retain the exact original action input.
func (s *graphFeedStore) reserve(ctx context.Context, token graphFeedToken, request, kind string, input any) error {
	if !validName(request) || !validName(kind) {
		return errInvalid
	}
	body, err := wire(input)
	if err != nil || len(body) > graphFeedControlBytes {
		return errors.Join(errLimit, err)
	}
	digest := sha256.Sum256(body)
	_, _, err = historyTransactContext(ctx, s.source.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		c, err := s.cursorIn(tr, token)
		if err != nil {
			return false, err
		}
		if err := s.debit(&c); err != nil {
			return false, err
		}
		action, found, err := feedGet[graphFeedAction](tr, s.key("action/"+token.Request, request))
		if err != nil {
			return false, err
		}
		if found && (action.Request != request || action.Kind != kind || action.Input != digest) {
			return false, errMismatch
		}
		if !found {
			action = graphFeedAction{request, kind, digest, 0}
		}
		if action.Invocations >= 8 {
			return false, errLimit
		}
		action.Invocations++
		if !found {
			if err := s.createIn(tr, s.key("action/"+token.Request, request), action, uint64(len(s.key("action/"+token.Request, request)))+1024); err != nil {
				return false, err
			}
		} else if err := put(tr, s.key("action/"+token.Request, request), action); err != nil {
			return false, err
		}
		return true, put(tr, s.key("cursor", token.Request), c)
	})
	return err
}

// The durable intent, not caller-supplied owner bytes, authorizes publication.
// Both source owners and the feed watermark are installed in one transaction.
func (s *graphFeedStore) Publish(ctx context.Context, o graphFeedOriginal) (*graphSnapshot, graphFeedToken, error) {
	token, err := graphFeedTokenFor(o)
	if err != nil || ctx == nil || s == nil {
		return nil, token, errors.Join(errInvalid, err)
	}
	ctx, cancel, err := feedContext(ctx, token)
	if err != nil {
		return nil, token, err
	}
	defer cancel()
	body, err := wire(o)
	if err != nil {
		return nil, token, err
	}
	o, err = decode[graphFeedOriginal](body)
	if err != nil {
		return nil, token, err
	}
	if err := s.reserve(ctx, token, "publish", "publish", o); err != nil {
		return nil, token, err
	}
	_, _, err = historyTransactContext(ctx, s.source.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		actual, err := s.originalIn(tr, token)
		if err != nil || !graphColumnSame(actual, o) {
			return false, errors.Join(errMismatch, err)
		}
		cursor, err := s.cursorIn(tr, token)
		if err != nil {
			return false, err
		}
		if cursor.State != graphFeedPrepared && cursor.State != graphFeedActive {
			return false, errHistoryExpired
		}
		if err := s.source.acquireIn(tr, o.Pin); err != nil {
			return false, err
		}
		r := &graphHistoryRead{s: s.source, id: o.Snapshot}
		pin := graphHistoryToken{o.Pin.Request, o.Cut, o.Pin.Secret}
		if err := r.openIn(tr, pin); err != nil {
			return false, err
		}
		cursor.State = graphFeedActive
		return true, put(tr, s.key("cursor", token.Request), cursor)
	})
	if err != nil {
		return nil, token, err
	}
	read := &graphHistoryRead{s: s.source, id: o.Snapshot}
	snapshot, err := s.source.observeGraphSnapshot(ctx, read, o.Selection, o.Policy.Read, o.Opening, s.lifetime(token, read))
	return snapshot, token, err
}

func (s *graphFeedStore) Open(ctx context.Context, token graphFeedToken, request string) (*graphFeed, error) {
	if s == nil || ctx == nil {
		return nil, errInvalid
	}
	ctx, cancel, boundErr := feedContext(ctx, token)
	if boundErr != nil {
		return nil, boundErr
	}
	defer cancel()
	if err := s.reserve(ctx, token, request, "open", token); err != nil {
		return nil, err
	}
	o, _, err := graphCostParallelRead(ctx, s.source.h.db, func(tr fdb.ReadTransaction) (graphFeedOriginal, error) {
		c, err := s.cursorIn(tr, token)
		if err != nil {
			return graphFeedOriginal{}, err
		}
		if c.State != graphFeedActive {
			return graphFeedOriginal{}, errHistoryExpired
		}
		return s.originalIn(tr, token)
	})
	if err != nil {
		return nil, err
	}
	pin := graphHistoryToken{o.Pin.Request, o.Cut, o.Pin.Secret}
	candidate, _, err := s.source.newReadContext(ctx, cut{o.Cut}, &pin, func() error { return nil })
	if err != nil {
		return nil, err
	}
	// Local reader factory identity is itself persisted before acquisition. A
	// replacement process authenticates the same original rather than minting
	// new authority after expiry or resetting a spent Close allowance.
	id, _, err := historyTransactContext(ctx, s.source.h.db, func() error { return nil }, func(tr fdb.Transaction) (graphHistoryOwner, error) {
		c, err := s.cursorIn(tr, token)
		if err != nil {
			return graphHistoryOwner{}, err
		}
		key := s.key("reader/"+token.Request, request)
		prior, found, err := feedGet[graphHistoryOwner](tr, key)
		if err != nil {
			return prior, err
		}
		if found {
			if !historyOwnerValid(prior) || prior.Kind != "read" || prior.State != historyActive || !sameCertificate(prior.Cut, o.Cut) || !sameCertificate(prior.Required, o.Pin.Required) || prior.PinRequest != pin.Request || prior.PinDigest != candidate.id.PinDigest || prior.Request != "source-read-"+hex.EncodeToString(prior.Secret[:16]) || prior.Lineage != [32]byte{} {
				return prior, errMismatch
			}
			return prior, nil
		}
		if c.State != graphFeedActive {
			return graphHistoryOwner{}, errHistoryExpired
		}
		if err := s.createIn(tr, key, candidate.id, uint64(len(key))+historyControlBytes); err != nil {
			return graphHistoryOwner{}, err
		}
		if err := s.cleanupCreateIn(tr, o, candidate.id); err != nil {
			return graphHistoryOwner{}, err
		}
		return candidate.id, nil
	})
	if err != nil {
		return nil, err
	}
	r := &graphHistoryRead{s: s.source, id: id}
	lifetime := s.lifetime(token, r)
	_, _, err = historyTransactContext(ctx, s.source.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		c, err := s.cursorIn(tr, token)
		if err != nil {
			return false, err
		}
		if c.State != graphFeedActive {
			return false, errHistoryExpired
		}
		return true, r.openIn(tr, pin)
	})
	if err != nil {
		return nil, errors.Join(err, lifetime.close())
	}
	return &graphFeed{s: s, original: o, token: token, reader: r, lifetime: lifetime}, nil
}
func (f *graphFeed) Close() error {
	if f == nil || f.lifetime == nil {
		return errInvalid
	}
	f.lifetime.mu.Lock()
	closed := f.lifetime.closed
	f.lifetime.mu.Unlock()
	if closed {
		return nil
	}
	return f.lifetime.close()
}
func (f *graphFeed) Work() graphSnapshotWork {
	if f == nil || f.lifetime == nil {
		return graphSnapshotWork{}
	}
	return f.lifetime.work()
}
func (s *graphFeedStore) Finish(ctx context.Context, token graphFeedToken, request string, expire bool) error {
	if ctx == nil || s == nil {
		return errInvalid
	}
	ctx, cancel, boundErr := feedContext(ctx, token)
	if boundErr != nil {
		return boundErr
	}
	defer cancel()
	o, _, err := graphCostParallelRead(ctx, s.source.h.db, func(tr fdb.ReadTransaction) (graphFeedOriginal, error) { return s.originalIn(tr, token) })
	if err != nil {
		return err
	}
	kind := historyReleased
	if expire {
		kind = historyExpired
	}
	_, err = s.cleanup(ctx, token, o.Pin.Request, request, kind)
	return err
}

// Terminal tickets are admitted BEFORE their owner can be acquired. Their
// funded record/call/work credit is disjoint from ordinary cursor traffic.
// The 64-ticket whole-store cap includes pin + snapshot + all process readers;
// it is an explicit fixture policy, not unlimited crash recovery or capacity.
type graphFeedCleanup struct {
	Original                          [32]byte
	Owner                             graphHistoryOwner
	OuterDeadlineNanos, DeadlineNanos int64
	Calls                             uint8
	Rows, Bytes                       uint64
	TerminalRequest                   string
	TerminalState                     uint8
	Closed                            bool
}

func (s *graphFeedStore) cleanupCreateIn(tr fdb.Transaction, o graphFeedOriginal, owner graphHistoryOwner) error {
	token, err := graphFeedTokenFor(o)
	if err != nil {
		return err
	}
	key := s.key("cleanup/"+o.Request, owner.Request)
	previous, found, err := feedGet[graphFeedCleanup](tr, key)
	if err != nil {
		return err
	}
	if found {
		if previous.Original != token.Original || !graphColumnSame(previous.Owner, owner) || previous.OuterDeadlineNanos != o.DeadlineNanos {
			return errMismatch
		}
		return nil
	}
	ledger, found, err := feedGet[graphFeedLedger](tr, s.key("meta", "ledger"))
	if err != nil || !found {
		return errors.Join(errUnavailable, err)
	}
	if ledger.CleanupTickets >= 64 {
		return errLimit
	}
	ticket := graphFeedCleanup{Original: token.Original, Owner: owner, OuterDeadlineNanos: o.DeadlineNanos}
	if err := s.createIn(tr, key, ticket, uint64(len(key))+historyControlBytes); err != nil {
		return err
	}
	ledger, _, err = feedGet[graphFeedLedger](tr, s.key("meta", "ledger"))
	if err != nil {
		return err
	}
	ledger.CleanupTickets++
	return put(tr, s.key("meta", "ledger"), ledger)
}
func (s *graphFeedStore) cleanupIn(tr fdb.ReadTransaction, token graphFeedToken, owner string) (graphFeedCleanup, error) {
	o, err := s.originalIn(tr, token)
	if err != nil {
		return graphFeedCleanup{}, err
	}
	ticket, found, err := feedGet[graphFeedCleanup](tr, s.key("cleanup/"+token.Request, owner))
	if err != nil || !found {
		return ticket, errors.Join(errUnavailable, err)
	}
	if ticket.Original != token.Original || ticket.OuterDeadlineNanos != o.DeadlineNanos || !historyOwnerValid(ticket.Owner) || ticket.Owner.Request != owner || ticket.Owner.State != historyActive || !sameCertificate(ticket.Owner.Cut, o.Cut) || !sameCertificate(ticket.Owner.Required, o.Pin.Required) || ticket.Calls > 8 || ticket.Rows != uint64(ticket.Calls)*1024 || ticket.Bytes != uint64(ticket.Calls)*(64<<20) || ticket.DeadlineNanos > ticket.OuterDeadlineNanos || ticket.DeadlineNanos < 0 || (ticket.Calls == 0) != (ticket.DeadlineNanos == 0) {
		return ticket, errMismatch
	}
	if ticket.Owner.Kind == "pin" {
		if !graphColumnSame(ticket.Owner, o.Pin) {
			return ticket, errMismatch
		}
	} else if ticket.Owner.Kind != "read" || ticket.Owner.PinRequest != o.Pin.Request || ticket.Owner.PinDigest != o.Snapshot.PinDigest {
		return ticket, errMismatch
	}
	return ticket, nil
}
func feedCleanupWork(t graphFeedCleanup) graphSnapshotWork {
	return graphSnapshotWork{CleanupRows: t.Rows, CleanupBytes: t.Bytes}
}

// Cleanup is SAME-original recovery only. It may fence an absent uncertain
// acquisition through the genuine closeIn reducer; it never reopens or mints
// an owner. Token expiry refuses NEW opens while this door remains usable.
func (s *graphFeedStore) cleanup(ctx context.Context, token graphFeedToken, owner, request string, terminal uint8) (graphSnapshotWork, error) {
	if s == nil || ctx == nil || !validName(owner) || !validName(request) || terminal != historyReleased && terminal != historyExpired {
		return graphSnapshotWork{}, errInvalid
	}
	ctx, outerCancel, boundErr := feedContext(ctx, token)
	if boundErr != nil {
		return graphSnapshotWork{}, boundErr
	}
	defer outerCancel()
	now := time.Now()
	ticket, _, err := historyTransactContext(ctx, s.source.h.db, func() error { return nil }, func(tr fdb.Transaction) (graphFeedCleanup, error) {
		t, err := s.cleanupIn(tr, token, owner)
		if err != nil {
			return t, err
		}
		if t.TerminalRequest != "" && (t.TerminalRequest != request || t.TerminalState != terminal) {
			return t, errMismatch
		}
		if t.Closed {
			return t, nil
		}
		if t.Calls >= 8 {
			return t, errLimit
		}
		if t.DeadlineNanos == 0 {
			t.DeadlineNanos = min(t.OuterDeadlineNanos, now.Add(30*time.Second).UnixNano())
		}
		// A recorded deadline never refreshes across process replacement. The
		// launcher also enforces the original monotonic whole-run watchdog.
		if time.Now().UnixNano() >= t.DeadlineNanos {
			return t, context.DeadlineExceeded
		}
		t.TerminalRequest, t.TerminalState = request, terminal
		t.Calls++
		t.Rows += 1024
		t.Bytes += 64 << 20
		return t, put(tr, s.key("cleanup/"+token.Request, owner), t)
	})
	if err != nil {
		return feedCleanupWork(ticket), err
	}
	if ticket.Closed {
		return feedCleanupWork(ticket), nil
	}
	bounded, cancel := context.WithDeadline(ctx, time.Unix(0, ticket.DeadlineNanos))
	defer cancel()
	_, _, err = historyTransactContext(bounded, s.source.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		actual, err := s.cleanupIn(tr, token, owner)
		if err != nil {
			return false, err
		}
		if actual.TerminalRequest != request || actual.TerminalState != terminal {
			return false, errMismatch
		}
		if actual.Closed {
			return true, nil
		}
		if actual.Owner.Kind == "read" {
			r := &graphHistoryRead{s: s.source, id: actual.Owner}
			if err := r.closeIn(tr); err != nil {
				return false, err
			}
		} else {
			c, err := s.cursorIn(tr, token)
			if err != nil {
				return false, err
			}
			desired := graphFeedReleased
			if terminal == historyExpired {
				desired = graphFeedExpired
			}
			if c.State == graphFeedPrepared {
				// This guard conflicts with Publish, fencing both prepared identities.
				if _, found, err := s.source.owner(tr, owner); err != nil || found {
					return false, errors.Join(errMismatch, err)
				}
			} else if c.State == graphFeedActive {
				if err := s.source.terminateIn(tr, owner, actual.Owner.Secret, terminal, request); err != nil {
					return false, err
				}
			} else if c.State != desired || c.TerminalRequest != request {
				return false, errMismatch
			}
			c.State, c.TerminalRequest = desired, request
			if err := put(tr, s.key("cursor", token.Request), c); err != nil {
				return false, err
			}
		}
		actual.Closed = true
		return true, put(tr, s.key("cleanup/"+token.Request, owner), actual)
	})
	return feedCleanupWork(ticket), err
}
func (s *graphFeedStore) lifetime(token graphFeedToken, r *graphHistoryRead) *graphSnapshotLifetime {
	l := newSnapshotLifetime(r, s.binding.Policy.Read.Cleanup)
	l.ownedClose = func() (graphSnapshotWork, error) {
		return s.cleanup(context.Background(), token, r.id.Request, "close", historyReleased)
	}
	return l
}
func (s *graphFeedStore) CloseOriginal(ctx context.Context, token graphFeedToken, owner string) (graphSnapshotWork, error) {
	// Exact stored ticket is the only identity input. This works after expiry or
	// client loss, including the initial snapshot; no active-pin acquisition.
	return s.cleanup(ctx, token, owner, "close", historyReleased)
}

// CloseReader resolves the caller-established immutable Open request, even if
// that process died BEFORE Open returned its factory owner. This is cleanup
// only: native intent + ticket must already exist; an expired pin is not opened
// and an absent/foreign request never creates acquisition authority.
func (s *graphFeedStore) CloseReader(ctx context.Context, token graphFeedToken, request string) (graphSnapshotWork, error) {
	if s == nil || !validName(request) {
		return graphSnapshotWork{}, errInvalid
	}
	ctx, cancel, err := feedContext(ctx, token)
	if err != nil {
		return graphSnapshotWork{}, err
	}
	defer cancel()
	owner, _, err := graphCostParallelRead(ctx, s.source.h.db, func(tr fdb.ReadTransaction) (graphHistoryOwner, error) {
		if _, err := s.originalIn(tr, token); err != nil {
			return graphHistoryOwner{}, err
		}
		owner, found, err := feedGet[graphHistoryOwner](tr, s.key("reader/"+token.Request, request))
		if err != nil || !found {
			return owner, errors.Join(errUnavailable, err)
		}
		ticket, err := s.cleanupIn(tr, token, owner.Request)
		if err != nil || !graphColumnSame(ticket.Owner, owner) || owner.Kind != "read" {
			return owner, errors.Join(errMismatch, err)
		}
		return owner, nil
	})
	if err != nil {
		return graphSnapshotWork{}, err
	}
	return s.cleanup(ctx, token, owner.Request, "close", historyReleased)
}

func newGraphFeedSecret() ([32]byte, error) {
	var secret [32]byte
	_, err := rand.Read(secret[:])
	return secret, err
}
