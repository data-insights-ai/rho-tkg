package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

var (
	errHistoryExpired = errors.New("fdb-source: retained cut or token expired")
	errHistoryBusy    = errors.New("fdb-source: durable history retirement in progress")
	errHistoryInUse   = errors.New("fdb-source: retained history has live owners")
	errHistoryBudget  = errors.New("fdb-source: next retirement page exceeds admission")
)

const historyActive, historyReleased, historyExpired = uint8(1), uint8(2), uint8(3)
const historyIdle, historyCensus, historyDeleting, historySealed = uint8(0), uint8(1), uint8(2), uint8(3)
const historyControlBytes = 4096

// Successful-page WorkBytes counts graph-data keys/values (current/history, CDC and
// original ID/request reads). Binding/control/config/cut checks and SDK retry
// reads are excluded. PageBytes is post-SDK admission, never native RSS.
type graphHistoryPolicy struct {
	MaxOwners, MaxControlBytes uint64
	PageRows, PageBytes        int
	MaxWorkRows, MaxWorkBytes  uint64
	MaxChainSteps              int
	MaxOutputBytes             uint64
}

func (p graphHistoryPolicy) validate() error {
	if p.MaxOwners < 2 || p.MaxOwners > 128 || p.MaxControlBytes < 8192 || p.MaxControlBytes > 16<<20 || p.PageRows < 1 || p.PageRows > 128 || p.PageBytes < 1024 || p.PageBytes > 4<<20 || p.MaxWorkRows < 1 || p.MaxWorkRows > 1<<40 || p.MaxWorkBytes < uint64(p.PageBytes) || p.MaxWorkBytes > 1<<50 || p.MaxChainSteps < 1 || p.MaxChainSteps > 128 || p.MaxOutputBytes < 2048 || p.MaxOutputBytes > 4096 {
		return errInvalid
	}
	return nil
}

type graphHistoryBinding struct {
	Version uint8
	Config  config
	Initial certificate
	Policy  graphHistoryPolicy
}
type graphHistoryLedger struct{ Records, Bytes uint64 }

// The immutable intent funds its eventual seal before the guard or any deletion.
type graphHistoryIntent struct {
	Request string
	Target  certificate
}
type graphHistorySeal struct {
	Request                                   string
	Cut                                       certificate
	Before, After                             [2]metadata
	Originals, Deleted, DeletedBytes, Anchors [2]uint64
	CensusSHA256                              [32]byte
	CumulativeDeleted                         [2]uint64
}
type graphHistoryState struct {
	Phase                                              uint8
	Request                                            string
	Target                                             certificate
	Before                                             [2]metadata
	Coordinator, Group                                 uint8
	Sequence                                           uint64
	Cursor                                             []byte
	Originals, Scanned, Deleted, DeletedBytes, Anchors [2]uint64
	WorkRows, WorkBytes                                uint64
	Chain                                              [32]byte
	Owners                                             uint64
	Completed                                          graphHistorySeal
	CumulativeDeleted                                  [2]uint64
}
type graphHistoryRetention struct {
	h       *harness
	binding graphHistoryBinding
}

func (h *harness) historyKey(name string) fdb.Key    { return h.key(0, "source-retention-v1", name) }
func (s *graphHistoryRetention) ownerPrefix() string { return s.h.prefix(0) + "source-owner-v1/" }
func (s *graphHistoryRetention) ownerKey(request string) fdb.Key {
	return fdb.Key(s.ownerPrefix() + hex.EncodeToString([]byte(request)))
}

// Explicit selection only, on a fresh graph and authentic initial cut. The
// immutable config stays untouched; source control quotas have a separate ledger.
func newGraphHistoryRetention(h *harness, initial cut, p graphHistoryPolicy) (*graphHistoryRetention, error) {
	if h == nil || p.validate() != nil || !slices.Equal(initial.record.Scope, []uint8{0, 1}) || initial.record.Groups != [2]uint64{} {
		return nil, errInvalid
	}
	b := graphHistoryBinding{1, h.config, initial.record, p}
	body, err := wire(b)
	if err != nil {
		return nil, err
	}
	b, err = decode[graphHistoryBinding](body)
	if err != nil {
		return nil, err
	}
	s := &graphHistoryRetention{h: h, binding: b}
	_, err = h.db.Transact(func(tr fdb.Transaction) (any, error) {
		for g := uint8(0); g < 2; g++ {
			if err := h.checkConfig(tr, g); err != nil {
				return nil, err
			}
		}
		if err := h.verifyCutIdentity(tr, cut{b.Initial}); err != nil {
			return nil, err
		}
		prior, err := tr.Get(h.historyKey("binding")).Get()
		if err != nil {
			return nil, err
		}
		if len(prior) != 0 {
			if !bytes.Equal(prior, body) {
				return nil, errMismatch
			}
			_, err := s.state(tr)
			return nil, err
		}
		// Never install optional retention underneath an already-admitted column
		// reader/publication or a foreign partial source namespace.
		for g := uint8(0); g < 2; g++ {
			for _, kind := range []string{"current/", "history/", "changes/", "id/", "request/"} {
				rows, err := tr.GetRange(prefixRange(h.prefix(g)+kind), fdb.RangeOptions{Limit: 1}).GetSliceWithError()
				if err != nil {
					return nil, err
				}
				if len(rows) != 0 {
					return nil, errUnavailable
				}
			}
		}
		for _, prefix := range []string{h.prefix(0) + "columns-v1/", h.prefix(0) + "source-retention-v1/"} {
			rows, err := tr.GetRange(prefixRange(prefix), fdb.RangeOptions{Limit: 1}).GetSliceWithError()
			if err != nil {
				return nil, err
			}
			if len(rows) != 0 {
				return nil, errUnavailable
			}
		}
		for g := uint8(0); g < 2; g++ {
			m, err := h.meta(tr, g)
			if err != nil {
				return nil, err
			}
			if m.Transactions != 0 || m.Versions != 0 || m.Groups != 0 {
				return nil, errUnavailable
			}
		}
		rows, err := tr.GetRange(prefixRange(s.ownerPrefix()), fdb.RangeOptions{Limit: 1}).GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if len(rows) != 0 {
			return nil, errUnavailable
		}
		for _, name := range []string{"ledger", "state"} {
			raw, err := tr.Get(h.historyKey(name)).Get()
			if err != nil {
				return nil, err
			}
			if len(raw) != 0 {
				return nil, errUnavailable
			}
		}
		reserved := uint64(len(h.historyKey("binding")) + len(body) + len(h.historyKey("ledger")) + 256 + len(h.historyKey("state")) + historyControlBytes)
		if reserved > p.MaxControlBytes {
			return nil, errLimit
		}
		tr.Set(h.historyKey("binding"), body)
		if err := put(tr, h.historyKey("state"), graphHistoryState{}); err != nil {
			return nil, err
		}
		return nil, put(tr, h.historyKey("ledger"), graphHistoryLedger{Bytes: reserved})
	})
	if err != nil {
		return nil, graphHistoryCommitError(err)
	}
	observed, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) { return h.historyRetentionIn(tr) })
	if err != nil {
		return nil, err
	}
	actual, ok := observed.(*graphHistoryRetention)
	if !ok || actual == nil {
		return nil, errUnavailable
	}
	if !graphColumnSame(actual.binding, b) {
		return nil, errMismatch
	}
	return actual, nil
}
func graphHistoryCommitError(err error) error {
	if _, ok := errors.AsType[fdb.Error](err); ok {
		_, maybe := graphCostParallelError(err)
		if maybe {
			return errors.Join(errUnknown, err)
		}
	}
	return err
}
func (h *harness) historyRetentionIn(tr fdb.ReadTransaction) (*graphHistoryRetention, error) {
	raw, err := tr.Get(h.historyKey("binding")).Get()
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		for _, prefix := range []string{h.prefix(0) + "source-retention-v1/", h.prefix(0) + "source-owner-v1/"} {
			rows, err := tr.GetRange(prefixRange(prefix), fdb.RangeOptions{Limit: 1}).GetSliceWithError()
			if err != nil {
				return nil, err
			}
			if len(rows) != 0 {
				return nil, errUnavailable
			}
		}
		return nil, nil
	}
	b, err := decode[graphHistoryBinding](raw)
	if err != nil {
		return nil, err
	}
	if b.Version != 1 || b.Config != h.config || b.Policy.validate() != nil || !slices.Equal(b.Initial.Scope, []uint8{0, 1}) || b.Initial.Groups != [2]uint64{} {
		return nil, errMismatch
	}
	for g := uint8(0); g < 2; g++ {
		if err := h.checkConfig(tr, g); err != nil {
			return nil, err
		}
	}
	if err := h.verifyCutIdentity(tr, cut{b.Initial}); err != nil {
		return nil, err
	}
	s := &graphHistoryRetention{h: h, binding: b}
	_, err = s.state(tr)
	return s, err
}
func (s *graphHistoryRetention) state(tr fdb.ReadTransaction) (graphHistoryState, error) {
	state, found, err := get[graphHistoryState](tr, s.h.historyKey("state"))
	if err != nil {
		return state, err
	}
	if !found || state.Phase > historySealed || state.Coordinator > 2 || state.Group > 2 || state.Owners > s.binding.Policy.MaxOwners || len(state.Cursor) > 1024 || state.WorkRows > s.binding.Policy.MaxWorkRows || state.WorkBytes > s.binding.Policy.MaxWorkBytes {
		return state, errUnavailable
	}
	if state.Phase != historyIdle && (!validName(state.Request) || !slices.Equal(state.Target.Scope, []uint8{0, 1}) || state.Target.Round == 0) {
		return state, errInvalid
	}
	if state.Phase == historyIdle && !graphColumnSame(state, graphHistoryState{Owners: state.Owners}) {
		return state, errMismatch
	}
	if state.Phase == historyCensus && (state.Coordinator >= 2 || state.Group != 0 || len(state.Cursor) != 0 || state.Scanned != [2]uint64{} || state.Deleted != [2]uint64{} || state.DeletedBytes != [2]uint64{} || state.Anchors != [2]uint64{}) || state.Phase == historyDeleting && (state.Coordinator != 2 || state.Sequence != 0 || state.Group >= 2) || state.Phase == historySealed && (state.Coordinator != 2 || state.Group != 2 || state.Sequence != 0 || len(state.Cursor) != 0 || state.Completed.Request != state.Request || !sameCertificate(state.Completed.Cut, state.Target)) {
		return state, errMismatch
	}
	if state.Phase != historyIdle {
		intent, found, err := get[graphHistoryIntent](tr, s.h.historyKey("intent/"+state.Request))
		if err != nil {
			return state, err
		}
		if !found || intent.Request != state.Request || !sameCertificate(intent.Target, state.Target) {
			return state, errUnavailable
		}
		if err := s.h.verifyCutIdentity(tr, cut{state.Target}); err != nil {
			return state, err
		}
		for g := range 2 {
			if state.Before[g].Groups != state.Target.Groups[g] || state.Before[g].Floor != state.Target.Round || state.Deleted[g] > state.Before[g].Versions || state.DeletedBytes[g] > state.Before[g].Bytes || state.Scanned[g] > state.Before[g].Versions || state.Anchors[g] > state.Scanned[g] || state.Deleted[g] > state.Scanned[g] {
				return state, errMismatch
			}
		}
		if state.Phase == historyCensus && state.Sequence > state.Target.Groups[state.Coordinator] {
			return state, errMismatch
		}
	}
	if state.Completed.Cut.Round != 0 {
		seal, found, err := get[graphHistorySeal](tr, s.h.historyKey("seal/"+state.Completed.Request))
		if err != nil {
			return state, err
		}
		if !found || !graphColumnSame(seal, state.Completed) || seal.CumulativeDeleted != state.CumulativeDeleted || !validName(seal.Request) || !slices.Equal(seal.Cut.Scope, []uint8{0, 1}) {
			return state, errUnavailable
		}
		if err := s.h.verifyCutIdentity(tr, cut{seal.Cut}); err != nil {
			return state, err
		}
	} else if state.CumulativeDeleted != [2]uint64{} {
		return state, errMismatch
	}
	l, found, err := get[graphHistoryLedger](tr, s.h.historyKey("ledger"))
	if err != nil {
		return state, err
	}
	if !found || l.Records > s.binding.Policy.MaxOwners || l.Bytes > s.binding.Policy.MaxControlBytes {
		return state, errUnavailable
	}
	return state, nil
}
func (s *graphHistoryRetention) writeState(tr fdb.Transaction, state graphHistoryState) error {
	body, err := wire(state)
	if err != nil || len(body) > historyControlBytes {
		return errors.Join(errLimit, err)
	}
	tr.Set(s.h.historyKey("state"), body)
	return nil
}
func (s *graphHistoryRetention) floor(state graphHistoryState) uint64 {
	if state.Phase == historyCensus || state.Phase == historyDeleting {
		return state.Target.Round
	}
	return state.Completed.Cut.Round
}
func (h *harness) historyCutIn(tr fdb.ReadTransaction, c cut) error {
	s, err := h.historyRetentionIn(tr)
	if err != nil || s == nil {
		return err
	}
	state, err := s.state(tr)
	if err != nil {
		return err
	}
	if c.record.Round < s.floor(state) {
		return errHistoryExpired
	}
	return nil
}
func (h *harness) historyWriteIn(tr fdb.ReadTransaction) error {
	s, err := h.historyRetentionIn(tr)
	if err != nil || s == nil {
		return err
	}
	state, err := s.state(tr)
	if err != nil {
		return err
	}
	if state.Phase == historyCensus || state.Phase == historyDeleting {
		return errHistoryBusy
	}
	return nil
}
func (h *harness) historyPublicationIn(tr fdb.ReadTransaction, m [2]metadata) ([2]uint64, error) {
	counts := [2]uint64{m[0].Versions, m[1].Versions}
	s, err := h.historyRetentionIn(tr)
	if err != nil || s == nil {
		return counts, err
	}
	state, err := s.state(tr)
	if err != nil {
		return counts, err
	}
	if state.Phase == historyCensus || state.Phase == historyDeleting {
		return counts, errHistoryBusy
	}
	for g := range 2 {
		if state.CumulativeDeleted[g] > math.MaxUint64-counts[g] {
			return counts, errLimit
		}
		counts[g] += state.CumulativeDeleted[g]
	}
	return counts, nil
}

type graphHistoryToken struct {
	Request string
	Cut     certificate
	Secret  [32]byte
}
type graphHistoryOwner struct {
	Kind, Request string
	Cut           certificate
	// Required protects the physical prefix walked, separately from answer Cut.
	Required           certificate
	Secret             [32]byte
	State              uint8
	PinRequest         string
	PinDigest, Lineage [32]byte
	TerminalRequest    string
}

func historyOwnerValid(o graphHistoryOwner) bool {
	return (o.Kind == "pin" || o.Kind == "read" || o.Kind == "column-pin" || o.Kind == "column-read") && validName(o.Request) && o.Secret != [32]byte{} && o.State >= historyActive && o.State <= historyExpired && (o.State == historyActive && o.TerminalRequest == "" || o.State != historyActive && validName(o.TerminalRequest))
}
func historyOwnerFloor(o graphHistoryOwner) uint64 {
	if o.Required.Round != 0 {
		return o.Required.Round
	}
	return o.Cut.Round
}
func historySecret(a, b [32]byte) bool { return subtle.ConstantTimeCompare(a[:], b[:]) == 1 }
func (s *graphHistoryRetention) owner(tr fdb.ReadTransaction, request string) (graphHistoryOwner, bool, error) {
	o, found, err := get[graphHistoryOwner](tr, s.ownerKey(request))
	if err != nil {
		return o, found, err
	}
	if found && (!historyOwnerValid(o) || o.Request != request) {
		return o, found, errMismatch
	}
	return o, found, nil
}
func (s *graphHistoryRetention) writeOwner(tr fdb.Transaction, o graphHistoryOwner, creating bool) error {
	body, err := wire(o)
	if err != nil || len(body) > historyControlBytes || !historyOwnerValid(o) {
		return errors.Join(errInvalid, err)
	}
	if creating {
		l, found, err := get[graphHistoryLedger](tr, s.h.historyKey("ledger"))
		if err != nil {
			return err
		}
		if !found {
			return errUnavailable
		}
		added := uint64(len(s.ownerKey(o.Request)) + historyControlBytes)
		if l.Records >= s.binding.Policy.MaxOwners || l.Bytes > s.binding.Policy.MaxControlBytes || added > s.binding.Policy.MaxControlBytes-l.Bytes {
			return errLimit
		}
		l.Records++
		l.Bytes += added
		if err := put(tr, s.h.historyKey("ledger"), l); err != nil {
			return err
		}
	}
	tr.Set(s.ownerKey(o.Request), body)
	return nil
}
func (s *graphHistoryRetention) acquireIn(tr fdb.Transaction, o graphHistoryOwner) error {
	state, err := s.state(tr)
	if err != nil {
		return err
	}
	prior, found, err := s.owner(tr, o.Request)
	if err != nil {
		return err
	}
	if found {
		if prior.Kind != o.Kind || !historySecret(prior.Secret, o.Secret) {
			return errMismatch
		}
		if prior.State != historyActive {
			return errHistoryExpired
		}
		if !graphColumnSame(prior, o) {
			return errMismatch
		}
		return nil
	}
	if o.Cut.Round < s.floor(state) || historyOwnerFloor(o) < s.floor(state) {
		return errHistoryExpired
	}
	if err := s.h.verifyCutIdentity(tr, cut{o.Cut}); err != nil {
		return err
	}
	if o.Required.Round != 0 {
		if err := s.h.verifyCutIdentity(tr, cut{o.Required}); err != nil {
			return err
		}
	}
	if state.Owners == s.binding.Policy.MaxOwners {
		return errLimit
	}
	if err := s.writeOwner(tr, o, true); err != nil {
		return err
	}
	state.Owners++
	return s.writeState(tr, state)
}
func (s *graphHistoryRetention) terminateIn(tr fdb.Transaction, request string, secret [32]byte, terminal uint8, terminalRequest string) error {
	if !validName(terminalRequest) {
		return errInvalid
	}
	state, err := s.state(tr)
	if err != nil {
		return err
	}
	o, found, err := s.owner(tr, request)
	if err != nil {
		return err
	}
	if !found {
		return errUnavailable
	}
	if !historySecret(o.Secret, secret) {
		return errMismatch
	}
	if o.State != historyActive {
		if o.State != terminal {
			return errHistoryExpired
		}
		if o.TerminalRequest != terminalRequest {
			return errMismatch
		}
		return nil
	}
	if state.Owners == 0 {
		return errMismatch
	}
	o.State = terminal
	o.TerminalRequest = terminalRequest
	if err := s.writeOwner(tr, o, false); err != nil {
		return err
	}
	state.Owners--
	return s.writeState(tr, state)
}
func (s *graphHistoryRetention) pin(request string, c cut, secret [32]byte) (graphHistoryToken, error) {
	if s == nil || !validName(request) || secret == [32]byte{} {
		return graphHistoryToken{}, errInvalid
	}
	token := graphHistoryToken{request, c.record, secret}
	body, err := wire(token)
	if err != nil {
		return token, err
	}
	token, err = decode[graphHistoryToken](body)
	if err != nil {
		return token, err
	}
	_, err = s.h.db.Transact(func(tr fdb.Transaction) (any, error) {
		return nil, s.acquireIn(tr, graphHistoryOwner{Kind: "pin", Request: token.Request, Cut: token.Cut, Secret: token.Secret, State: historyActive})
	})
	if err != nil {
		return graphHistoryToken{}, graphHistoryCommitError(err)
	}
	return token, nil
}
func (s *graphHistoryRetention) expire(request string, token graphHistoryToken) error {
	if s == nil || !validName(request) || !validName(token.Request) || token.Secret == [32]byte{} {
		return errInvalid
	}
	body, err := wire(token)
	if err != nil {
		return err
	}
	token, err = decode[graphHistoryToken](body)
	if err != nil {
		return err
	}
	_, err = s.h.db.Transact(func(tr fdb.Transaction) (any, error) {
		o, found, err := s.owner(tr, token.Request)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errUnavailable
		}
		if o.Kind != "pin" || !sameCertificate(o.Cut, token.Cut) {
			return nil, errMismatch
		}
		return nil, s.terminateIn(tr, token.Request, token.Secret, historyExpired, request)
	})
	return graphHistoryCommitError(err)
}
func (s *graphHistoryRetention) release(request string, token graphHistoryToken) error {
	if s == nil || !validName(request) || !validName(token.Request) || token.Secret == [32]byte{} {
		return errInvalid
	}
	body, err := wire(token)
	if err != nil {
		return err
	}
	token, err = decode[graphHistoryToken](body)
	if err != nil {
		return err
	}
	_, err = s.h.db.Transact(func(tr fdb.Transaction) (any, error) {
		o, found, err := s.owner(tr, token.Request)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errUnavailable
		}
		if o.Kind != "pin" || !sameCertificate(o.Cut, token.Cut) {
			return nil, errMismatch
		}
		return nil, s.terminateIn(tr, token.Request, token.Secret, historyReleased, request)
	})
	return graphHistoryCommitError(err)
}

type graphHistoryRead struct {
	s               *graphHistoryRetention
	id              graphHistoryOwner
	mu              sync.Mutex
	walking, closed bool
}
type graphHistoryReadPending struct {
	Owner *graphHistoryRead
	Cause error
}

func (e *graphHistoryReadPending) Error() string {
	return "fdb-source: reader cleanup pending; retry authenticated Close"
}
func (e *graphHistoryReadPending) Unwrap() error { return e.Cause }
func historyReadCleanup(r *graphHistoryRead) error {
	err := r.Close()
	if err != nil {
		return &graphHistoryReadPending{r, err}
	}
	return nil
}
func (s *graphHistoryRetention) newRead(c cut, pin *graphHistoryToken) (*graphHistoryRead, error) {
	if s == nil {
		return nil, errInvalid
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, err
	}
	o := graphHistoryOwner{Kind: "read", Request: "source-read-" + hex.EncodeToString(secret[:16]), Cut: c.record, Secret: secret, State: historyActive}
	o.Cut.Scope = slices.Clone(c.record.Scope)
	observed, err := s.h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		state, err := s.state(tr)
		if err != nil {
			return nil, err
		}
		if c.record.Round < s.floor(state) {
			return nil, errHistoryExpired
		}
		if state.Phase == historyCensus || state.Phase == historyDeleting {
			return nil, errHistoryBusy
		}
		required := s.binding.Initial
		if state.Completed.Cut.Round > required.Round {
			required = state.Completed.Cut
		}
		required.Scope = slices.Clone(required.Scope)
		return required, nil
	})
	if err != nil {
		return nil, err
	}
	o.Required = observed.(certificate)
	if pin != nil {
		if !sameCertificate(pin.Cut, c.record) {
			return nil, errMismatch
		}
		o.PinRequest = pin.Request
		body, err := wire(*pin)
		if err != nil {
			return nil, err
		}
		o.PinDigest = sha256.Sum256(body)
	}
	return &graphHistoryRead{s: s, id: o}, nil
}
func (s *graphHistoryRetention) open(ctx context.Context, token graphHistoryToken) (*graphHistoryRead, error) {
	if ctx == nil {
		return nil, errInvalid
	}
	body, err := wire(token)
	if err != nil {
		return nil, err
	}
	token, err = decode[graphHistoryToken](body)
	if err != nil {
		return nil, err
	}
	r, err := s.newRead(cut{token.Cut}, &token)
	if err != nil {
		return nil, err
	}
	_, err = s.h.db.Transact(func(tr fdb.Transaction) (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		o, found, err := s.owner(tr, token.Request)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errUnavailable
		}
		if o.Kind != "pin" || !sameCertificate(o.Cut, token.Cut) || !historySecret(o.Secret, token.Secret) {
			return nil, errMismatch
		}
		if o.State != historyActive {
			return nil, errHistoryExpired
		}
		return nil, s.acquireIn(tr, r.id)
	})
	if err != nil {
		err = graphHistoryCommitError(err)
		if errors.Is(err, errUnknown) {
			return nil, errors.Join(err, historyReadCleanup(r))
		}
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, historyReadCleanup(r))
	}
	return r, nil
}
func (r *graphHistoryRead) check(tr fdb.ReadTransaction) error {
	o, found, err := r.s.owner(tr, r.id.Request)
	if err != nil {
		return err
	}
	if !found {
		return errUnavailable
	}
	if !graphColumnSame(o, r.id) {
		return errMismatch
	}
	state, err := r.s.state(tr)
	if err != nil {
		return err
	}
	if state.Owners == 0 || o.State != historyActive {
		return errHistoryExpired
	}
	if err := r.s.h.verifyCutIdentity(tr, cut{o.Required}); err != nil {
		return err
	}
	return r.s.h.verifyCutIdentity(tr, cut{o.Cut})
}
func (r *graphHistoryRead) Close() error {
	if r == nil || r.s == nil || !historyOwnerValid(r.id) {
		return errInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.walking {
		return errHistoryInUse
	}
	if r.closed {
		return nil
	}
	_, err := r.s.h.db.Transact(func(tr fdb.Transaction) (any, error) {
		o, found, err := r.s.owner(tr, r.id.Request)
		if err != nil {
			return nil, err
		}
		if !found {
			cancelled := r.id
			cancelled.State = historyReleased
			cancelled.TerminalRequest = "close"
			return nil, r.s.writeOwner(tr, cancelled, true)
		}
		identity := o
		identity.State, identity.TerminalRequest = r.id.State, r.id.TerminalRequest
		if !graphColumnSame(identity, r.id) {
			return nil, errMismatch
		}
		return nil, r.s.terminateIn(tr, r.id.Request, r.id.Secret, historyReleased, "close")
	})
	if err == nil {
		r.closed = true
	}
	return graphHistoryCommitError(err)
}

// Every source value names a genuine committed original. Lookup consults both
// coordinator namespaces, then authenticates ID+request+digest and all effects.
func (h *harness) historyOriginalIn(tr fdb.ReadTransaction, group uint8, key string, v value) (changeGroup, uint64, error) {
	var match changeGroup
	matches := 0
	work := uint64(0)
	for coordinator := uint8(0); coordinator < 2; coordinator++ {
		idKey := h.key(coordinator, "id", v.TxID)
		raw, err := tr.Get(idKey).Get()
		if err != nil {
			return match, work, err
		}
		work += uint64(len(idKey) + len(raw))
		if len(raw) == 0 {
			continue
		}
		o, err := decode[outcome](raw)
		if err != nil {
			return match, work, err
		}
		if !o.Commit || o.Round != v.Round || o.ID != v.TxID || o.Coordinator != coordinator {
			continue
		}
		reqKey := h.key(coordinator, "request", o.Request)
		req, err := tr.Get(reqKey).Get()
		if err != nil {
			return match, work, err
		}
		work += uint64(len(reqKey) + len(req))
		if !bytes.Equal(raw, req) {
			return match, work, errMismatch
		}
		changeKey := h.changeKey(coordinator, o.Sequence, o.Request)
		body, err := tr.Get(changeKey).Get()
		if err != nil {
			return match, work, err
		}
		work += uint64(len(changeKey) + len(body))
		g, err := decode[changeGroup](body)
		if err != nil {
			return match, work, err
		}
		if err := h.validateChange(g); err != nil {
			return match, work, err
		}
		if g.Outcome != o {
			return match, work, errMismatch
		}
		for _, write := range g.Writes {
			if write.Group == group && write.Key == key && write.After == v {
				match = g
				matches++
			}
		}
	}
	if matches != 1 {
		return match, work, errUnavailable
	}
	return match, work, nil
}
func (h *harness) graphHistoryRow(row graphCostRawRow, round uint64) (string, value, error) {
	if row.Group > 1 || len(row.Key) > 1024 {
		return "", value{}, errInvalid
	}
	v, err := decode[value](row.Value)
	if err != nil || !validGraphValue(v) || v.Round > round {
		return "", v, errors.Join(errInvalid, err)
	}
	prefix := fmt.Sprintf("%shistory/%016x/", h.prefix(row.Group), v.Round)
	tail, found := strings.CutPrefix(string(row.Key), prefix)
	logical, err := hex.DecodeString(tail)
	if !found || err != nil || !validName(string(logical)) || hex.EncodeToString(logical) != tail {
		return "", v, errInvalid
	}
	return string(logical), v, nil
}
func graphHistoryDeletionAllowed(candidate, anchor value, floor uint64) bool {
	return candidate.Round > 0 && candidate.Round < floor && candidate.Version > 0 && candidate.Version < anchor.Version && candidate.Round < anchor.Round
}

// Follow the actual immutable CDC predecessor chain, not a guessed current
// value or per-key side table. The first revision at/below the certified floor
// is the required anchor. The configured chain/work caps are real refusals.
func (s *graphHistoryRetention) anchorIn(tr fdb.ReadTransaction, group uint8, key string, floor uint64) (value, uint64, error) {
	currentKey := s.h.key(group, "current", key)
	raw, err := tr.Get(currentKey).Get()
	work := uint64(len(currentKey) + len(raw))
	if err != nil {
		return value{}, work, err
	}
	v, err := decode[value](raw)
	if err != nil || !validGraphValue(v) {
		return v, work, errors.Join(errUnavailable, err)
	}
	for step := 0; step < s.binding.Policy.MaxChainSteps; step++ {
		g, used, err := s.h.historyOriginalIn(tr, group, key, v)
		work += used
		if err != nil {
			return v, work, err
		}
		if work > uint64(s.binding.Policy.PageBytes) {
			return v, work, errHistoryBudget
		}
		if v.Round <= floor {
			return v, work, nil
		}
		found := false
		for _, write := range g.Writes {
			if write.Group == group && write.Key == key && write.After == v && write.Before != nil {
				if write.Before.Version+1 != v.Version || write.Before.Round >= v.Round {
					return v, work, errMismatch
				}
				v = *write.Before
				found = true
				break
			}
		}
		if !found {
			return v, work, errUnavailable
		}
	}
	return v, work, errLimit
}

// A superseded row must be on the anchor's exact original predecessor chain;
// a matching key and merely smaller version/round cannot bless a foreign row.
func (s *graphHistoryRetention) ancestorIn(tr fdb.ReadTransaction, group uint8, key string, anchor, candidate value) (uint64, error) {
	work := uint64(0)
	v := anchor
	for step := 0; step < s.binding.Policy.MaxChainSteps; step++ {
		if v == candidate {
			return work, nil
		}
		if v.Version <= candidate.Version || v.Round <= candidate.Round {
			return work, errMismatch
		}
		original, used, err := s.h.historyOriginalIn(tr, group, key, v)
		work += used
		if err != nil {
			return work, err
		}
		if work > uint64(s.binding.Policy.PageBytes) {
			return work, errHistoryBudget
		}
		found := false
		for _, write := range original.Writes {
			if write.Group == group && write.Key == key && write.After == v && write.Before != nil {
				if write.Before.Version == math.MaxUint64 || write.Before.Version+1 != v.Version || write.Before.Round >= v.Round {
					return work, errMismatch
				}
				v = *write.Before
				found = true
				break
			}
		}
		if !found {
			return work, errUnavailable
		}
	}
	return work, errLimit
}
func (h *harness) historyLineageIn(tr fdb.ReadTransaction, group uint8, key string, v value) (uint64, error) {
	s, err := h.historyRetentionIn(tr)
	if err != nil || s == nil {
		return 0, errors.Join(errUnavailable, err)
	}
	state, err := s.state(tr)
	if err != nil {
		return 0, err
	}
	if state.Completed.Cut.Round == 0 || v.Round >= state.Completed.Cut.Round {
		if state.Phase == historyCensus || state.Phase == historyDeleting {
			return 0, errHistoryBusy
		}
		return 0, errUnavailable
	}
	if err := h.verifyCutIdentity(tr, cut{state.Completed.Cut}); err != nil {
		return 0, err
	}
	anchor, work, err := s.anchorIn(tr, group, key, state.Completed.Cut.Round)
	if err != nil {
		return work, err
	}
	if !graphHistoryDeletionAllowed(v, anchor, state.Completed.Cut.Round) {
		return work, errUnavailable
	}
	extra, err := s.ancestorIn(tr, group, key, anchor, v)
	work += extra
	if err != nil {
		return work, err
	}
	_, used, err := h.historyOriginalIn(tr, group, key, v)
	return work + used, err
}

// Every original effect must have its exact physical revision or an already
// complete retirement seal proving that precise revision superseded. Cardinality
// alone cannot substitute a different authentic retired row for a missing anchor.
func (h *harness) historyRevisionIn(tr fdb.ReadTransaction, group uint8, key string, v value) (uint64, error) {
	body, err := wire(v)
	if err != nil {
		return 0, err
	}
	historyKey := fdb.Key(fmt.Sprintf("%shistory/%016x/%x", h.prefix(group), v.Round, key))
	actual, err := tr.Get(historyKey).Get()
	work := uint64(len(historyKey) + len(actual))
	if err != nil {
		return work, err
	}
	if bytes.Equal(actual, body) {
		return work, nil
	}
	if actual != nil {
		return work, errMismatch
	}
	extra, err := h.historyLineageIn(tr, group, key, v)
	return work + extra, err
}

func historyColumnRequest(kind, request string) string {
	sum := sha256.Sum256([]byte(request))
	return "column-" + kind + "-" + hex.EncodeToString(sum[:])
}
func (s *graphHistoryRetention) columnRequirement(tr fdb.ReadTransaction, m graphColumnManifest) (certificate, error) {
	state, err := s.state(tr)
	if err != nil {
		return certificate{}, err
	}
	if state.Phase == historyCensus || state.Phase == historyDeleting {
		return certificate{}, errHistoryBusy
	}
	required := m.Generation.Initial
	if state.Completed.Cut.Round > required.Round {
		required = state.Completed.Cut
	}
	required.Scope = slices.Clone(required.Scope)
	return required, nil
}
func (h *harness) historyColumnAcquireIn(tr fdb.Transaction, id graphColumnOwnerID, m graphColumnManifest) error {
	s, err := h.historyRetentionIn(tr)
	if err != nil || s == nil {
		return err
	}
	if err := h.historyCutIn(tr, cut{m.Generation.Cut}); err != nil {
		return err
	}
	required, err := s.columnRequirement(tr, m)
	if err != nil {
		return err
	}
	digest, err := columnManifestDigest(m)
	if err != nil {
		return err
	}
	return s.acquireIn(tr, graphHistoryOwner{Kind: "column-read", Request: historyColumnRequest("read", id.Request), Cut: required, Secret: id.Secret, State: historyActive, Lineage: digest})
}
func (h *harness) historyColumnCheckIn(tr fdb.ReadTransaction, id graphColumnOwnerID, m graphColumnManifest) error {
	s, err := h.historyRetentionIn(tr)
	if err != nil || s == nil {
		return err
	}
	o, found, err := s.owner(tr, historyColumnRequest("read", id.Request))
	if err != nil {
		return err
	}
	if !found {
		return errUnavailable
	}
	digest, err := columnManifestDigest(m)
	if err != nil {
		return err
	}
	if o.Kind != "column-read" || !historySecret(o.Secret, id.Secret) || o.Lineage != digest {
		return errMismatch
	}
	if o.State != historyActive {
		return errHistoryExpired
	}
	state, err := s.state(tr)
	if err != nil {
		return err
	}
	if state.Owners == 0 {
		return errUnavailable
	}
	return h.verifyCutIdentity(tr, cut{o.Cut})
}
func (h *harness) historyColumnCloseIn(tr fdb.Transaction, id graphColumnOwnerID) error {
	s, err := h.historyRetentionIn(tr)
	if err != nil || s == nil {
		return err
	}
	request := historyColumnRequest("read", id.Request)
	o, found, err := s.owner(tr, request)
	if err != nil {
		return err
	}
	if !found {
		return s.writeOwner(tr, graphHistoryOwner{Kind: "column-read", Request: request, Secret: id.Secret, State: historyReleased, TerminalRequest: "close"}, true)
	}
	if o.Kind != "column-read" || !historySecret(o.Secret, id.Secret) {
		return errMismatch
	}
	return s.terminateIn(tr, request, id.Secret, historyReleased, "close")
}
func (h *harness) historyColumnPinIn(tr fdb.Transaction, token graphColumnPinToken, m graphColumnManifest) error {
	s, err := h.historyRetentionIn(tr)
	if err != nil || s == nil {
		return err
	}
	if err := h.historyCutIn(tr, cut{token.Cut}); err != nil {
		return err
	}
	required, err := s.columnRequirement(tr, m)
	if err != nil {
		return err
	}
	return s.acquireIn(tr, graphHistoryOwner{Kind: "column-pin", Request: historyColumnRequest("pin", token.Request), Cut: required, Secret: token.Secret, State: historyActive, Lineage: columnPinDigest(token)})
}
func (h *harness) historyColumnPinFinishIn(tr fdb.Transaction, token graphColumnPinToken, kind uint8, request string) error {
	s, err := h.historyRetentionIn(tr)
	if err != nil || s == nil {
		return err
	}
	o, found, err := s.owner(tr, historyColumnRequest("pin", token.Request))
	if err != nil {
		return err
	}
	if !found {
		return errUnavailable
	}
	if o.Kind != "column-pin" || o.Lineage != columnPinDigest(token) || !historySecret(o.Secret, token.Secret) {
		return errMismatch
	}
	return s.terminateIn(tr, o.Request, token.Secret, kind, request)
}

// Named references also own the source lineage needed by a later generation
// open. Their actual revision/digest, not a caller clock or generation label,
// authenticates replacement. Both owner updates share the column transaction.
func (h *harness) historyColumnReferenceIn(tr fdb.Transaction, request string, before, after graphColumnReference, store *graphColumnStore) error {
	s, err := h.historyRetentionIn(tr)
	if err != nil || s == nil {
		return err
	}
	identity := func(ref graphColumnReference) (string, [32]byte, error) {
		body, err := wire(ref)
		return historyColumnRequest("reference", fmt.Sprintf("%s/%d", ref.Name, ref.Version)), sha256.Sum256(body), err
	}
	if before.Generation != "" {
		id, digest, err := identity(before)
		if err != nil {
			return err
		}
		o, found, err := s.owner(tr, id)
		if err != nil {
			return err
		}
		if !found || o.Kind != "column-pin" || o.Lineage != digest || !historySecret(o.Secret, digest) {
			return errMismatch
		}
		if err := s.terminateIn(tr, id, digest, historyReleased, request); err != nil {
			return err
		}
	}
	if after.Generation == "" {
		return nil
	}
	m, err := store.manifestIn(tr, after.Generation)
	if err != nil {
		return err
	}
	if err := h.historyCutIn(tr, cut{m.Generation.Cut}); err != nil {
		return err
	}
	required, err := s.columnRequirement(tr, m)
	if err != nil {
		return err
	}
	id, digest, err := identity(after)
	if err != nil {
		return err
	}
	return s.acquireIn(tr, graphHistoryOwner{Kind: "column-pin", Request: id, Cut: required, Secret: digest, State: historyActive, Lineage: digest})
}

func (s *graphHistoryRetention) begin(request string, target cut) error {
	if s == nil || !validName(request) || !slices.Equal(target.record.Scope, []uint8{0, 1}) {
		return errInvalid
	}
	body, err := wire(target.record)
	if err != nil {
		return err
	}
	record, err := decode[certificate](body)
	if err != nil {
		return err
	}
	_, err = s.h.db.Transact(func(tr fdb.Transaction) (any, error) {
		if err := s.h.verifyCutIdentity(tr, cut{record}); err != nil {
			return nil, err
		}
		state, err := s.state(tr)
		if err != nil {
			return nil, err
		}
		if state.Request == request {
			if !sameCertificate(state.Target, record) {
				return nil, errMismatch
			}
			return nil, nil
		}
		old, found, err := get[graphHistorySeal](tr, s.h.historyKey("seal/"+request))
		if err != nil {
			return nil, err
		}
		if found {
			if old.Request != request || !sameCertificate(old.Cut, record) {
				return nil, errMismatch
			}
			return nil, nil
		}
		if state.Phase == historyCensus || state.Phase == historyDeleting {
			return nil, errHistoryBusy
		}
		if record.Round <= state.Completed.Cut.Round || record.Round < s.binding.Initial.Round {
			return nil, errHistoryExpired
		}
		owners, err := tr.GetRange(prefixRange(s.ownerPrefix()), fdb.RangeOptions{Limit: int(s.binding.Policy.MaxOwners) + 1}).GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if uint64(len(owners)) > s.binding.Policy.MaxOwners {
			return nil, errLimit
		}
		live := uint64(0)
		for _, raw := range owners {
			o, err := decode[graphHistoryOwner](raw.Value)
			if err != nil {
				return nil, err
			}
			if !historyOwnerValid(o) || !bytes.Equal(raw.Key, s.ownerKey(o.Request)) {
				return nil, errMismatch
			}
			if o.State == historyActive {
				live++
				if historyOwnerFloor(o) < record.Round {
					return nil, errHistoryInUse
				}
			}
		}
		if live != state.Owners {
			return nil, errUnavailable
		}
		next := graphHistoryState{Phase: historyCensus, Request: request, Target: record, Owners: state.Owners, Completed: state.Completed, CumulativeDeleted: state.CumulativeDeleted}
		for g := uint8(0); g < 2; g++ {
			if err := s.h.checkConfig(tr, g); err != nil {
				return nil, err
			}
			m, err := s.h.meta(tr, g)
			if err != nil {
				return nil, err
			}
			if m.Groups != record.Groups[g] || m.Floor != record.Round {
				return nil, errHistoryBusy
			}
			next.Before[g] = m
		}
		intentKey := s.h.historyKey("intent/" + request)
		intentBody, err := wire(graphHistoryIntent{request, record})
		if err != nil || len(intentBody) > historyControlBytes {
			return nil, errors.Join(errLimit, err)
		}
		priorIntent, err := tr.Get(intentKey).Get()
		if err != nil {
			return nil, err
		}
		if len(priorIntent) != 0 {
			return nil, errMismatch
		}
		ledger, found, err := get[graphHistoryLedger](tr, s.h.historyKey("ledger"))
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errUnavailable
		}
		added := uint64(len(intentKey) + len(s.h.historyKey("seal/"+request)) + 2*historyControlBytes)
		if ledger.Records > s.binding.Policy.MaxOwners-2 || added > s.binding.Policy.MaxControlBytes-ledger.Bytes {
			return nil, errLimit
		}
		ledger.Records += 2
		ledger.Bytes += added
		if err := put(tr, s.h.historyKey("ledger"), ledger); err != nil {
			return nil, err
		}
		tr.Set(intentKey, intentBody)
		return nil, s.writeState(tr, next)
	})
	return graphHistoryCommitError(err)
}

type graphHistoryBudget struct {
	Rows               int
	Bytes, OutputBytes uint64
}
type graphHistoryPage struct {
	Phase                 uint8
	WorkRows, WorkBytes   uint64
	Deleted, DeletedBytes [2]uint64
	Done                  bool
	Seal                  graphHistorySeal
}

func (p graphHistoryPolicy) admit(b graphHistoryBudget) error {
	if b.Rows < 1 || b.Rows > p.PageRows || b.Bytes < 1 || b.Bytes > uint64(p.PageBytes) || b.OutputBytes < 1 || b.OutputBytes > p.MaxOutputBytes {
		return errInvalid
	}
	return nil
}
func historyChain(previous [32]byte, key, body []byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write(previous[:])
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(key)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(key)
	binary.BigEndian.PutUint64(length[:], uint64(len(body)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(body)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
func (s *graphHistoryRetention) collect(ctx context.Context, request string, budget graphHistoryBudget) (graphHistoryPage, error) {
	if s == nil || ctx == nil || !validName(request) || s.binding.Policy.admit(budget) != nil {
		return graphHistoryPage{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return graphHistoryPage{}, err
	}
	result, err := s.h.db.Transact(func(tr fdb.Transaction) (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		state, err := s.state(tr)
		if err != nil {
			return nil, err
		}
		if state.Request != request {
			seal, found, err := get[graphHistorySeal](tr, s.h.historyKey("seal/"+request))
			if err != nil {
				return nil, err
			}
			if !found || seal.Request != request {
				return nil, errMismatch
			}
			if err := s.h.verifyCutIdentity(tr, cut{seal.Cut}); err != nil {
				return nil, err
			}
			page := graphHistoryPage{Phase: historySealed, Done: true, Seal: seal}
			body, err := wire(page)
			if err != nil || uint64(len(body)) > budget.OutputBytes {
				return nil, errors.Join(errHistoryBudget, err)
			}
			return page, nil
		}
		if state.Phase == historySealed {
			page := graphHistoryPage{Phase: historySealed, Done: true, Seal: state.Completed}
			body, err := wire(page)
			if err != nil || uint64(len(body)) > budget.OutputBytes {
				return nil, errors.Join(errHistoryBudget, err)
			}
			return page, nil
		}
		if state.Phase != historyCensus && state.Phase != historyDeleting {
			return nil, errUnavailable
		}
		var actual [2]metadata
		for g := uint8(0); g < 2; g++ {
			if err := s.h.checkConfig(tr, g); err != nil {
				return nil, err
			}
			m, err := s.h.meta(tr, g)
			if err != nil {
				return nil, err
			}
			if m.Groups != state.Target.Groups[g] || m.Floor != state.Target.Round || m.Generation != state.Before[g].Generation || m.Transactions != state.Before[g].Transactions || m.Certificates != state.Before[g].Certificates || m.Versions != state.Before[g].Versions-state.Deleted[g] || m.Bytes != state.Before[g].Bytes-state.DeletedBytes[g] {
				return nil, errMismatch
			}
			actual[g] = m
		}
		page := graphHistoryPage{Phase: state.Phase}
		var clear []fdb.Key
		if state.Phase == historyCensus {
			g := state.Coordinator
			if g >= 2 {
				return nil, errInvalid
			}
			lo := state.Sequence
			hi := state.Target.Groups[g]
			begin := fdb.Key(fmt.Sprintf("%schanges/%016x0", s.h.prefix(g), lo))
			end := fdb.Key(fmt.Sprintf("%schanges/%016x0", s.h.prefix(g), hi))
			rows, err := tr.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{Limit: budget.Rows}).GetSliceWithError()
			if err != nil {
				return nil, err
			}
			for _, row := range rows {
				if uint64(len(row.Key)+len(row.Value)) > budget.Bytes-page.WorkBytes {
					return nil, errHistoryBudget
				}
				group, err := decode[changeGroup](row.Value)
				if err != nil {
					return nil, err
				}
				if err := s.h.validateChange(group); err != nil {
					return nil, err
				}
				o := group.Outcome
				if o.Coordinator != g || o.Sequence != state.Sequence+1 || o.Sequence > hi || o.Round > state.Target.Round || !bytes.Equal(row.Key, s.h.changeKey(g, o.Sequence, o.Request)) {
					return nil, errMismatch
				}
				used := uint64(len(row.Key) + len(row.Value))
				ob, err := wire(o)
				if err != nil {
					return nil, err
				}
				for _, key := range []fdb.Key{s.h.key(g, "id", o.ID), s.h.key(g, "request", o.Request)} {
					raw, err := tr.Get(key).Get()
					if err != nil {
						return nil, err
					}
					used += uint64(len(key) + len(raw))
					if !bytes.Equal(raw, ob) {
						return nil, errMismatch
					}
				}
				// Before admitting deletion, independently prove every original
				// physical revision or the exact prior complete retirement lineage.
				// Equal census counts cannot bless a missing current anchor.
				for _, write := range group.Writes {
					extra, err := s.h.historyRevisionIn(tr, write.Group, write.Key, write.After)
					used += extra
					if err != nil {
						return nil, err
					}
					if used > budget.Bytes-page.WorkBytes {
						return nil, errHistoryBudget
					}
				}
				if used > budget.Bytes-page.WorkBytes {
					return nil, errHistoryBudget
				}
				page.WorkRows++
				page.WorkBytes += used
				state.Sequence = o.Sequence
				state.Chain = historyChain(state.Chain, row.Key, row.Value)
				for _, write := range group.Writes {
					if state.Originals[write.Group] == math.MaxUint64 {
						return nil, errLimit
					}
					state.Originals[write.Group]++
				}
			}
			if len(rows) < budget.Rows {
				if state.Sequence != hi {
					return nil, errUnavailable
				}
				state.Coordinator++
				state.Sequence = 0
				if state.Coordinator == 2 {
					for g := range 2 {
						if state.CumulativeDeleted[g] > math.MaxUint64-state.Before[g].Versions || state.Originals[g] != state.Before[g].Versions+state.CumulativeDeleted[g] {
							return nil, errUnavailable
						}
					}
					state.Phase = historyDeleting
				}
			}
		} else {
			g := state.Group
			if g >= 2 {
				return nil, errInvalid
			}
			begin := fdb.Key(s.h.prefix(g) + "history/")
			if len(state.Cursor) != 0 {
				begin = fdb.Key(append(bytes.Clone(state.Cursor), 0))
			}
			end := fdb.Key(fmt.Sprintf("%shistory/%016x0", s.h.prefix(g), state.Target.Round))
			rows, err := tr.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{Limit: budget.Rows}).GetSliceWithError()
			if err != nil {
				return nil, err
			}
			for _, row := range rows {
				if uint64(len(row.Key)+len(row.Value)) > budget.Bytes-page.WorkBytes {
					return nil, errHistoryBudget
				}
				logical, v, err := s.h.graphHistoryRow(graphCostRawRow{Group: g, Key: row.Key, Value: row.Value}, state.Target.Round)
				if err != nil {
					return nil, err
				}
				_, originalBytes, err := s.h.historyOriginalIn(tr, g, logical, v)
				if err != nil {
					return nil, err
				}
				anchor, anchorBytes, err := s.anchorIn(tr, g, logical, state.Target.Round)
				if err != nil {
					return nil, err
				}
				used := uint64(len(row.Key)+len(row.Value)) + originalBytes + anchorBytes
				if graphHistoryDeletionAllowed(v, anchor, state.Target.Round) {
					extra, err := s.ancestorIn(tr, g, logical, anchor, v)
					used += extra
					if err != nil {
						return nil, err
					}
				}
				if page.WorkBytes > budget.Bytes || used > budget.Bytes-page.WorkBytes {
					return nil, errHistoryBudget
				}
				page.WorkRows++
				page.WorkBytes += used
				state.Scanned[g]++
				state.Cursor = bytes.Clone(row.Key)
				if graphHistoryDeletionAllowed(v, anchor, state.Target.Round) {
					deleted := uint64(len(row.Key) + len(row.Value))
					if actual[g].Versions == 0 || actual[g].Bytes < s.h.metadataReservation(g) || deleted > actual[g].Bytes-s.h.metadataReservation(g) {
						return nil, errMismatch
					}
					actual[g].Versions--
					actual[g].Bytes -= deleted
					state.Deleted[g]++
					state.DeletedBytes[g] += deleted
					page.Deleted[g]++
					page.DeletedBytes[g] += deleted
					clear = append(clear, fdb.Key(bytes.Clone(row.Key)))
				} else {
					if v != anchor {
						return nil, errMismatch
					}
					state.Anchors[g]++
				}
			}
			if len(rows) < budget.Rows {
				if state.Scanned[g] != state.Before[g].Versions || state.Anchors[g] != actual[g].Versions {
					return nil, errUnavailable
				}
				state.Group++
				state.Cursor = nil
				if state.Group == 2 {
					state.Phase = historySealed
					state.Completed = graphHistorySeal{Request: request, Cut: state.Target, Before: state.Before, After: actual, Originals: state.Originals, Deleted: state.Deleted, DeletedBytes: state.DeletedBytes, Anchors: state.Anchors, CensusSHA256: state.Chain}
					for g := range 2 {
						if state.Deleted[g] > math.MaxUint64-state.CumulativeDeleted[g] {
							return nil, errLimit
						}
						state.CumulativeDeleted[g] += state.Deleted[g]
					}
					state.Completed.CumulativeDeleted = state.CumulativeDeleted
					page.Done = true
					page.Seal = state.Completed
				}
			}
		}
		if page.WorkRows > s.binding.Policy.MaxWorkRows-state.WorkRows || page.WorkBytes > s.binding.Policy.MaxWorkBytes-state.WorkBytes {
			return nil, errLimit
		}
		state.WorkRows += page.WorkRows
		state.WorkBytes += page.WorkBytes
		body, err := wire(page)
		if err != nil || uint64(len(body)) > budget.OutputBytes {
			return nil, errors.Join(errHistoryBudget, err)
		}
		stateBody, err := wire(state)
		if err != nil || len(stateBody) > historyControlBytes {
			return nil, errors.Join(errLimit, err)
		}
		if page.Done {
			sealBody, err := wire(state.Completed)
			if err != nil || len(sealBody) > historyControlBytes {
				return nil, errors.Join(errLimit, err)
			}
			key := s.h.historyKey("seal/" + request)
			prior, err := tr.Get(key).Get()
			if err != nil {
				return nil, err
			}
			if len(prior) != 0 && !bytes.Equal(prior, sealBody) {
				return nil, errMismatch
			}
			tr.Set(key, sealBody)
		}
		// All admissions and output checks precede every deletion/counter effect.
		for _, key := range clear {
			tr.Clear(key)
		}
		for g := uint8(0); g < 2; g++ {
			if err := put(tr, s.h.key(g, "meta", "state"), actual[g]); err != nil {
				return nil, err
			}
		}
		tr.Set(s.h.historyKey("state"), stateBody)
		return page, nil
	})
	if err != nil {
		return graphHistoryPage{}, graphHistoryCommitError(err)
	}
	return result.(graphHistoryPage), nil
}

// The active owner protects physical anchors for its cut. Native reads/callback
// work have finite policy bounds; external emit runs after ReadTransact returns.
func (r *graphHistoryRead) walk(ctx context.Context, emit func(graphCostRawRow) error) (rows [2]uint64, err error) {
	if r == nil || r.s == nil {
		return rows, errInvalid
	}
	return r.walkPages(ctx, r.s.binding.Policy.PageRows, emit, nil)
}
func (r *graphHistoryRead) walkPages(ctx context.Context, pageRows int, emit func(graphCostRawRow) error, between func()) (rows [2]uint64, err error) {
	if r == nil || ctx == nil || emit == nil {
		return rows, errInvalid
	}
	if pageRows < 1 || pageRows > r.s.binding.Policy.PageRows {
		return rows, errInvalid
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return rows, errHistoryExpired
	}
	if r.walking {
		r.mu.Unlock()
		return rows, errHistoryInUse
	}
	r.walking = true
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.walking = false; r.mu.Unlock() }()
	workRows, workBytes := uint64(0), uint64(0)
	for g := uint8(0); g < 2; g++ {
		if !slices.Contains(r.id.Cut.Scope, g) {
			continue
		}
		begin := fdb.Key(r.s.h.prefix(g) + "history/")
		end := fdb.Key(fmt.Sprintf("%shistory/%016x0", r.s.h.prefix(g), r.id.Cut.Round))
		for {
			result, readErr := r.s.h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if err := r.check(tr); err != nil {
					return nil, err
				}
				raw, err := tr.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{Limit: pageRows}).GetSliceWithError()
				if err != nil {
					return nil, err
				}
				page := make([]graphCostRawRow, 0, len(raw))
				used := uint64(0)
				for _, row := range raw {
					if uint64(len(row.Key)+len(row.Value)) > uint64(r.s.binding.Policy.PageBytes)-used {
						return nil, errHistoryBudget
					}
					v := graphCostRawRow{Group: g, Key: bytes.Clone(row.Key), Value: bytes.Clone(row.Value)}
					key, value, err := r.s.h.graphHistoryRow(v, r.id.Cut.Round)
					if err != nil {
						return nil, err
					}
					_, extra, err := r.s.h.historyOriginalIn(tr, g, key, value)
					if err != nil {
						return nil, err
					}
					used += uint64(len(row.Key)+len(row.Value)) + extra
					if used > uint64(r.s.binding.Policy.PageBytes) {
						return nil, errHistoryBudget
					}
					page = append(page, v)
				}
				return struct {
					Rows  []graphCostRawRow
					Bytes uint64
				}{page, used}, nil
			})
			if readErr != nil {
				return [2]uint64{}, readErr
			}
			page := result.(struct {
				Rows  []graphCostRawRow
				Bytes uint64
			})
			if uint64(len(page.Rows)) > r.s.binding.Policy.MaxWorkRows-workRows || page.Bytes > r.s.binding.Policy.MaxWorkBytes-workBytes {
				return [2]uint64{}, errLimit
			}
			workRows += uint64(len(page.Rows))
			workBytes += page.Bytes
			for _, row := range page.Rows {
				if err := emit(row); err != nil {
					return [2]uint64{}, err
				}
				rows[g]++
			}
			if between != nil {
				between()
			}
			if len(page.Rows) < pageRows {
				break
			}
			begin = fdb.Key(append(bytes.Clone(page.Rows[len(page.Rows)-1].Key), 0))
		}
	}
	// Independently verify every original effect and the complete prefix, not only
	// delivered rows. A reinserted retired row cannot hide a missing anchor.
	var originals [2]uint64
	type censusPage struct {
		Rows            int
		Bytes, Sequence uint64
		Originals       [2]uint64
	}
	for coordinator := uint8(0); coordinator < 2; coordinator++ {
		sequence := uint64(0)
		for {
			result, readErr := r.s.h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if err := r.check(tr); err != nil {
					return nil, err
				}
				begin := fdb.Key(fmt.Sprintf("%schanges/%016x0", r.s.h.prefix(coordinator), sequence))
				end := fdb.Key(fmt.Sprintf("%schanges/%016x0", r.s.h.prefix(coordinator), r.id.Cut.Groups[coordinator]))
				raw, err := tr.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{Limit: pageRows}).GetSliceWithError()
				if err != nil {
					return nil, err
				}
				page := censusPage{Rows: len(raw), Sequence: sequence}
				for _, row := range raw {
					used := uint64(len(row.Key) + len(row.Value))
					if used > uint64(r.s.binding.Policy.PageBytes)-page.Bytes {
						return nil, errHistoryBudget
					}
					group, err := decode[changeGroup](row.Value)
					if err != nil {
						return nil, err
					}
					if err := r.s.h.validateChange(group); err != nil {
						return nil, err
					}
					o := group.Outcome
					if o.Coordinator != coordinator || page.Sequence == math.MaxUint64 || o.Sequence != page.Sequence+1 || o.Sequence > r.id.Cut.Groups[coordinator] || touchesScope(group.Tx, r.id.Cut.Scope) && o.Round > r.id.Cut.Round || !bytes.Equal(row.Key, r.s.h.changeKey(coordinator, o.Sequence, o.Request)) {
						return nil, errMismatch
					}
					body, err := wire(o)
					if err != nil {
						return nil, err
					}
					for _, key := range []fdb.Key{r.s.h.key(coordinator, "id", o.ID), r.s.h.key(coordinator, "request", o.Request)} {
						original, err := tr.Get(key).Get()
						if err != nil {
							return nil, err
						}
						used += uint64(len(key) + len(original))
						if !bytes.Equal(original, body) {
							return nil, errMismatch
						}
					}
					if used > uint64(r.s.binding.Policy.PageBytes)-page.Bytes {
						return nil, errHistoryBudget
					}
					for _, write := range group.Writes {
						if slices.Contains(r.id.Cut.Scope, write.Group) {
							extra, err := r.s.h.historyRevisionIn(tr, write.Group, write.Key, write.After)
							if err != nil {
								return nil, err
							}
							used += extra
							if used > uint64(r.s.binding.Policy.PageBytes)-page.Bytes {
								return nil, errHistoryBudget
							}
							page.Originals[write.Group]++
						}
					}
					page.Bytes += used
					page.Sequence = o.Sequence
				}
				return page, nil
			})
			if readErr != nil {
				return [2]uint64{}, readErr
			}
			page := result.(censusPage)
			if uint64(page.Rows) > r.s.binding.Policy.MaxWorkRows-workRows || page.Bytes > r.s.binding.Policy.MaxWorkBytes-workBytes {
				return [2]uint64{}, errLimit
			}
			workRows += uint64(page.Rows)
			workBytes += page.Bytes
			sequence = page.Sequence
			for g := range 2 {
				if page.Originals[g] > math.MaxUint64-originals[g] {
					return [2]uint64{}, errLimit
				}
				originals[g] += page.Originals[g]
			}
			if page.Rows < pageRows {
				if sequence != r.id.Cut.Groups[coordinator] {
					return [2]uint64{}, errUnavailable
				}
				break
			}
		}
	}
	_, err = r.s.h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		if err := r.check(tr); err != nil {
			return nil, err
		}
		state, err := r.s.state(tr)
		if err != nil {
			return nil, err
		}
		for g := uint8(0); g < 2; g++ {
			if !slices.Contains(r.id.Cut.Scope, g) {
				continue
			}
			if state.CumulativeDeleted[g] > originals[g] || rows[g] != originals[g]-state.CumulativeDeleted[g] {
				return nil, errUnavailable
			}
		}
		return nil, nil
	})
	if err != nil {
		return [2]uint64{}, err
	}
	return rows, nil
}
