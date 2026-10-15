package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

var (
	errColumnExpired       = errors.New("fdb-column: retained generation or token expired")
	errColumnInUse         = errors.New("fdb-column: retained generation has live owners or references")
	errColumnCollectBudget = errors.New("fdb-column: next chunk exceeds remaining work budget")
)

const columnGuardActive, columnGuardRetiring, columnGuardCollected = uint8(1), uint8(2), uint8(3)
const columnOwnerActive, columnOwnerReleased, columnOwnerExpired = uint8(1), uint8(2), uint8(3)
const columnControlReservation = 4096

type graphColumnGuard struct {
	Manifest                  [32]byte
	State                     uint8
	References, Pins, Readers uint64
	Cursor, DeletedBytes      uint64
	Chain                     [32]byte
}
type graphColumnOwnerID struct {
	Request, Generation string
	Secret              [32]byte
}
type graphColumnReadRecord struct {
	Owner      graphColumnOwnerID
	Manifest   [32]byte
	PinRequest string
	PinToken   [32]byte
	State      uint8
}

// The caller-owned handle retains physical ownership until Close. Token expiry
// prevents NEW opens; it does not invalidate an already-admitted reader. No
// wall-clock authority, implicit lease timeout or automatic crash reaping.
type graphColumnRead struct {
	s               *graphColumnStore
	id              graphColumnOwnerID
	pin             *graphColumnPinToken
	mu              sync.Mutex
	walking, closed bool
	// Fresh context-owned callers retain ONE separately finite cleanup lifetime
	// on this exact pending handle. Normal cancellation cannot spend or reset it.
	cleanupDeadline  time.Time
	cleanupCallbacks int
}

// An uncertain cleanup retains the caller-created owner identity for an
// explicit authenticated Close retry. It grants no successful read/EOF and
// prints no secret. Unknown owners are never inferred absent or auto-reaped.
type graphColumnReadPending struct {
	Owner *graphColumnRead
	Cause error
}

func (e *graphColumnReadPending) Error() string {
	return "fdb-column: read-owner cleanup pending; retry authenticated Close"
}
func (e *graphColumnReadPending) Unwrap() error { return e.Cause }
func columnReadCleanup(r *graphColumnRead) error {
	err := r.Close()
	if err != nil {
		return &graphColumnReadPending{Owner: r, Cause: err}
	}
	return err
}

type columnControlWrite struct {
	Key   fdb.Key
	Value any
}

func columnManifestDigest(m graphColumnManifest) ([32]byte, error) {
	body, err := wire(m)
	return sha256.Sum256(body), err
}
func (s *graphColumnStore) guard(tr fdb.ReadTransaction, m graphColumnManifest) (graphColumnGuard, bool, error) {
	digest, err := columnManifestDigest(m)
	if err != nil {
		return graphColumnGuard{}, false, err
	}
	g, found, err := get[graphColumnGuard](tr, s.generationKey(m.Generation.ID, "owners"))
	if err != nil {
		return g, found, err
	}
	if !found {
		return graphColumnGuard{Manifest: digest, State: columnGuardActive}, false, nil
	}
	if g.Manifest != digest || g.State < columnGuardActive || g.State > columnGuardCollected || g.Cursor > m.Chunks || g.DeletedBytes > m.Bytes || g.References > 1<<32 || g.Pins > 1<<32 || g.Readers > 1<<32 || g.State != columnGuardActive && (g.References != 0 || g.Pins != 0 || g.Readers != 0) {
		return graphColumnGuard{}, false, errInvalid
	}
	if g.State == columnGuardActive && (g.Cursor != 0 || g.DeletedBytes != 0 || g.Chain != [32]byte{}) || g.State == columnGuardRetiring && (g.Cursor == 0 || g.Cursor >= m.Chunks) || g.State == columnGuardCollected && (g.Cursor != m.Chunks || g.DeletedBytes != m.Bytes || g.Chain != m.Chain) {
		return graphColumnGuard{}, false, errInvalid
	}
	return g, true, nil
}

// Controls reserve a constant mutable-record allowance in the ORIGINAL column
// byte ledger. Closed read owners and generation guards remain tombstones.
func (s *graphColumnStore) controls(tr fdb.Transaction, writes ...columnControlWrite) error {
	if len(writes) < 1 || len(writes) > 4 {
		return errInvalid
	}
	l, err := s.ledger(tr)
	if err != nil {
		return err
	}
	bodies := make([][]byte, len(writes))
	added := uint64(0)
	for i, write := range writes {
		for _, previous := range writes[:i] {
			if bytes.Equal(previous.Key, write.Key) {
				return errInvalid
			}
		}
		body, err := wire(write.Value)
		if err != nil {
			return err
		}
		if len(body) > columnControlReservation {
			return errLimit
		}
		prior, err := tr.Get(write.Key).Get()
		if err != nil {
			return err
		}
		if prior == nil {
			added += uint64(len(write.Key) + columnControlReservation)
		}
		bodies[i] = body
	}
	if added > s.binding.Policy.MaxRetainedBytes-l.Bytes {
		return errLimit
	}
	l.Bytes += added
	for i, write := range writes {
		tr.Set(write.Key, bodies[i])
	}
	return put(tr, s.key("ledger"), l)
}
func (s *graphColumnStore) ensureGuard(tr fdb.Transaction, m graphColumnManifest, allowCreate bool) error {
	g, found, err := s.guard(tr, m)
	if err != nil {
		return err
	}
	if g.State != columnGuardActive {
		return errColumnExpired
	}
	if found {
		return nil
	}
	if !allowCreate {
		return errUnavailable
	} // Never infer no owners for an older/missing guard.
	return s.controls(tr, columnControlWrite{s.generationKey(m.Generation.ID, "owners"), g})
}
func columnSameSecret(a, b [32]byte) bool { return subtle.ConstantTimeCompare(a[:], b[:]) == 1 }
func columnValidateOwner(id graphColumnOwnerID) error {
	if !validName(id.Request) || !validName(id.Generation) || id.Secret == [32]byte{} {
		return errInvalid
	}
	return nil
}
func newGraphColumnRead(s *graphColumnStore, generation string, pin *graphColumnPinToken) (*graphColumnRead, error) {
	if s == nil || !validName(generation) {
		return nil, errInvalid
	}
	if pin != nil {
		if err := columnValidatePin(s, *pin); err != nil {
			return nil, err
		}
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, err
	}
	r := &graphColumnRead{s: s, id: graphColumnOwnerID{Request: "read-" + hex.EncodeToString(secret[:16]), Generation: generation, Secret: secret}}
	if pin != nil {
		own := *pin
		own.Cut.Scope = slices.Clone(pin.Cut.Scope)
		r.pin = &own
	}
	return r, nil
}
func (r *graphColumnRead) key() fdb.Key {
	return r.s.key("read-owner/" + hex.EncodeToString([]byte(r.id.Request)))
}
func (r *graphColumnRead) open(ctx context.Context) error {
	if r == nil || ctx == nil || columnValidateOwner(r.id) != nil {
		return errInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := r.s.writeContext(ctx, func(tr fdb.Transaction) (any, error) {
		prior, found, err := get[graphColumnReadRecord](tr, r.key())
		if err != nil {
			return nil, err
		}
		if found {
			if prior.Owner.Generation != r.id.Generation || !columnSameSecret(prior.Owner.Secret, r.id.Secret) {
				return nil, errMismatch
			}
			if prior.State != columnOwnerActive {
				return nil, errColumnExpired
			}
			return nil, r.check(tr)
		}
		m, err := r.s.manifestIn(tr, r.id.Generation)
		if err != nil {
			return nil, err
		}
		g, guardFound, err := r.s.guard(tr, m)
		if err != nil {
			return nil, err
		}
		if !guardFound {
			return nil, errUnavailable
		}
		if g.State != columnGuardActive {
			return nil, errColumnExpired
		}
		record := graphColumnReadRecord{Owner: r.id, Manifest: g.Manifest, State: columnOwnerActive}
		if r.pin != nil {
			p, err := readGraphColumnPin(tr, r.s, *r.pin)
			if err != nil {
				return nil, err
			}
			if p.State != columnOwnerActive {
				return nil, errColumnExpired
			}
			if g.Pins == 0 {
				return nil, errMismatch
			}
			if p.Token.Generation != r.id.Generation || p.Token.Manifest != g.Manifest {
				return nil, errMismatch
			}
			record.PinRequest, record.PinToken = p.Token.Request, columnPinDigest(p.Token)
		}
		if g.Readers == 1<<32 {
			return nil, errLimit
		}
		g.Readers++
		if err := r.s.h.historyColumnAcquireIn(tr, r.id, m); err != nil {
			return nil, err
		}
		return nil, r.s.controls(tr, columnControlWrite{r.s.generationKey(r.id.Generation, "owners"), g}, columnControlWrite{r.key(), record})
	})
	if err == nil && ctx.Err() != nil {
		return errors.Join(ctx.Err(), columnReadCleanup(r))
	}
	return graphColumnCommitError(err)
}
func (r *graphColumnRead) check(tr fdb.ReadTransaction) error {
	record, found, err := get[graphColumnReadRecord](tr, r.key())
	if err != nil {
		return err
	}
	if !found {
		return errUnavailable
	}
	if record.Owner.Request != r.id.Request || record.Owner.Generation != r.id.Generation || !columnSameSecret(record.Owner.Secret, r.id.Secret) {
		return errMismatch
	}
	if record.State != columnOwnerActive {
		return errColumnExpired
	}
	m, err := r.s.manifestIn(tr, r.id.Generation)
	if err != nil {
		return err
	}
	g, guardFound, err := r.s.guard(tr, m)
	if err != nil {
		return err
	}
	if !guardFound {
		return errUnavailable
	}
	if err := r.s.h.historyColumnCheckIn(tr, r.id, m); err != nil {
		return err
	}
	if g.State != columnGuardActive || g.Readers == 0 || record.Manifest != g.Manifest {
		return errMismatch
	}
	if r.pin != nil {
		p, err := readGraphColumnPin(tr, r.s, *r.pin)
		if err != nil {
			return err
		}
		// A held owner survives authenticated release/expiry, but never a missing or
		// substituted original token. Its own active record protects physical bytes.
		if record.PinRequest != p.Token.Request || record.PinToken != columnPinDigest(p.Token) {
			return errMismatch
		}
	} else if record.PinRequest != "" || record.PinToken != [32]byte{} {
		return errMismatch
	}
	return nil
}
func (r *graphColumnRead) Close() error {
	if r == nil || columnValidateOwner(r.id) != nil {
		return errInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.walking {
		return errColumnInUse
	} // Never hold this mutex across consumer callbacks.
	if r.closed {
		return nil
	}
	_, err := r.closeNative(func(tr fdb.Transaction) (any, error) {
		record, found, err := get[graphColumnReadRecord](tr, r.key())
		if err != nil {
			return nil, err
		}
		if !found {
			// Authenticated cancellation conflicts with the SAME absent-key read in
			// acquisition. An uncertain late acquisition cannot commit past this close.
			if err := r.s.h.historyColumnCloseIn(tr, r.id); err != nil {
				return nil, err
			}
			return nil, r.s.controls(tr, columnControlWrite{r.key(), graphColumnReadRecord{Owner: r.id, State: columnOwnerReleased}})
		}
		if record.Owner.Request != r.id.Request || record.Owner.Generation != r.id.Generation || !columnSameSecret(record.Owner.Secret, r.id.Secret) {
			return nil, errMismatch
		}
		if record.State == columnOwnerReleased {
			return nil, r.s.h.historyColumnCloseIn(tr, r.id)
		}
		if record.State != columnOwnerActive {
			return nil, errInvalid
		}
		m, err := r.s.manifestIn(tr, r.id.Generation)
		if err != nil {
			return nil, err
		}
		g, guardFound, err := r.s.guard(tr, m)
		if err != nil {
			return nil, err
		}
		if !guardFound {
			return nil, errUnavailable
		}
		if g.State != columnGuardActive || g.Readers == 0 || g.Manifest != record.Manifest {
			return nil, errMismatch
		}
		g.Readers--
		record.State = columnOwnerReleased
		if err := r.s.h.historyColumnCloseIn(tr, r.id); err != nil {
			return nil, err
		}
		return nil, r.s.controls(tr, columnControlWrite{r.key(), record}, columnControlWrite{r.s.generationKey(r.id.Generation, "owners"), g})
	})
	if err == nil {
		r.closed = true
	}
	return graphColumnCommitError(err)
}

// Called only under r.mu. Nil keeps literal historical native Close IO; fresh
// owners never escape to that raw path after their normal run is canceled.
func (r *graphColumnRead) closeNative(fn func(fdb.Transaction) (any, error)) (any, error) {
	if r.s.ctx == nil {
		return r.s.h.db.Transact(fn)
	}
	if r.cleanupDeadline.IsZero() {
		r.cleanupDeadline = time.Now().Add(30 * time.Second)
	}
	ctx, cancel := context.WithDeadline(context.Background(), r.cleanupDeadline)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.cleanupCallbacks >= graphCostParallelCallbacks {
		return nil, errLimit
	}
	result, _, err := historyTransactContext(ctx, r.s.h.db, func() error {
		if r.cleanupCallbacks >= graphCostParallelCallbacks {
			return errLimit
		}
		r.cleanupCallbacks++
		return nil
	}, func(tr fdb.Transaction) (graphCostContextResult, error) {
		value, err := fn(tr)
		return graphCostContextResult{value}, err
	})
	return result.value, err
}
func (r *graphColumnRead) walk(ctx context.Context, at cut, cache *os.Root, emit func(graphColumnRow) error) (graphColumnManifest, error) {
	if r == nil {
		return graphColumnManifest{}, errInvalid
	}
	if r.pin != nil && !sameCertificate(r.pin.Cut, at.record) {
		return graphColumnManifest{}, errMismatch
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return graphColumnManifest{}, errColumnExpired
	}
	if r.walking {
		r.mu.Unlock()
		return graphColumnManifest{}, errColumnInUse
	}
	r.walking = true
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.walking = false; r.mu.Unlock() }()
	return r.s.walkOwned(ctx, r.id.Generation, at, cache, emit, r.check)
}

// Pin/control admission is separate from column/source bytes. Bounds are chosen
// explicitly by the embedding caller and persisted; no capacity acceptance.
// Collector work counts chunk key/value bytes and chunk reads, not fixed native
// binding/config/cut/owner metadata checks or client/replica RSS.
type graphColumnPinPolicy struct {
	MaxRecords, MaxBytes                        uint64
	PageChunks, PageBytes                       int
	MaxWorkChunks, MaxWorkBytes, MaxOutputBytes uint64
}

func (p graphColumnPinPolicy) validate() error {
	if p.MaxRecords < 2 || p.MaxRecords > 65536 || p.MaxBytes < 8192 || p.MaxBytes > 64<<20 || p.PageChunks < 1 || p.PageChunks > 32 || p.PageBytes < 1024 || p.PageBytes > 2<<20 || p.MaxWorkChunks < 1 || p.MaxWorkChunks > 4096 || p.MaxWorkBytes < uint64(p.PageBytes) || p.MaxWorkBytes > 64<<20 || p.MaxOutputBytes < 512 || p.MaxOutputBytes > 4096 {
		return errInvalid
	}
	return nil
}

type graphColumnPinBinding struct {
	Version uint8
	Columns [32]byte
	Policy  graphColumnPinPolicy
}
type graphColumnPinLedger struct{ Records, Bytes, ReservedRecords, ReservedBytes uint64 }
type graphColumnPins struct {
	s       *graphColumnStore
	binding graphColumnPinBinding
}

func newGraphColumnPins(s *graphColumnStore, p graphColumnPinPolicy) (*graphColumnPins, error) {
	if s == nil || p.validate() != nil || p.MaxRecords < 2 {
		return nil, errInvalid
	}
	v := &graphColumnPins{s: s, binding: graphColumnPinBinding{Version: 1, Columns: s.digest, Policy: p}}
	body, err := wire(v.binding)
	if err != nil {
		return nil, err
	}
	_, err = s.write(func(tr fdb.Transaction) (any, error) {
		columnBody, err := tr.Get(s.key("binding")).Get()
		if err != nil {
			return nil, err
		}
		expected, err := wire(s.binding)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(columnBody, expected) {
			return nil, errMismatch
		}
		for group := uint8(0); group < 2; group++ {
			if err := s.h.checkConfig(tr, group); err != nil {
				return nil, err
			}
		}
		actual, err := tr.Get(v.key("binding")).Get()
		if err != nil {
			return nil, err
		}
		if actual != nil {
			if !bytes.Equal(actual, body) {
				return nil, errMismatch
			}
			_, err := v.ledger(tr)
			return nil, err
		}
		rows, err := tr.GetRange(prefixRange(v.prefix()), fdb.RangeOptions{Limit: 1}).GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if len(rows) != 0 {
			return nil, errUnavailable
		}
		bytes := uint64(len(v.key("binding")) + len(body) + len(v.key("ledger")) + 256)
		if bytes > p.MaxBytes {
			return nil, errLimit
		}
		tr.Set(v.key("binding"), body)
		return nil, put(tr, v.key("ledger"), graphColumnPinLedger{Records: 2, Bytes: bytes})
	})
	if err != nil {
		return nil, graphColumnCommitError(err)
	}
	return v, nil
}
func (v *graphColumnPins) prefix() string          { return v.s.prefix() + "retention-v1/" }
func (v *graphColumnPins) key(name string) fdb.Key { return fdb.Key(v.prefix() + name) }
func (v *graphColumnPins) recordKey(kind, name string) fdb.Key {
	return v.key(kind + "/" + hex.EncodeToString([]byte(name)))
}
func (v *graphColumnPins) ledger(tr fdb.ReadTransaction) (graphColumnPinLedger, error) {
	body, err := tr.Get(v.key("binding")).Get()
	if err != nil {
		return graphColumnPinLedger{}, err
	}
	expected, err := wire(v.binding)
	if err != nil {
		return graphColumnPinLedger{}, err
	}
	if !bytes.Equal(body, expected) {
		return graphColumnPinLedger{}, errMismatch
	}
	l, found, err := get[graphColumnPinLedger](tr, v.key("ledger"))
	if err != nil {
		return l, err
	}
	if !found || l.Records < 2 || l.Records > v.binding.Policy.MaxRecords || l.ReservedRecords > v.binding.Policy.MaxRecords-l.Records || l.Bytes > v.binding.Policy.MaxBytes || l.ReservedBytes > v.binding.Policy.MaxBytes-l.Bytes {
		return l, errInvalid
	}
	return l, nil
}
func (v *graphColumnPins) put(tr fdb.Transaction, writes ...columnControlWrite) error {
	return v.putWithCredit(tr, 0, writes...)
}

// An active named reference reserves one maximum-sized FUTURE drop request.
// Detach consumes that credit, so full metadata quota cannot trap the reference.
func (v *graphColumnPins) putWithCredit(tr fdb.Transaction, credit int, writes ...columnControlWrite) error {
	if len(writes) < 1 || len(writes) > 4 {
		return errInvalid
	}
	l, err := v.ledger(tr)
	if err != nil {
		return err
	}
	if credit < -1 || credit > 1 {
		return errInvalid
	}
	futureBytes := uint64(len(v.prefix()) + len("reference-request/") + 256 + columnControlReservation)
	if credit == 1 {
		l.ReservedRecords++
		l.ReservedBytes += futureBytes
	}
	if credit == -1 {
		if l.ReservedRecords == 0 || l.ReservedBytes < futureBytes {
			return errInvalid
		}
		l.ReservedRecords--
		l.ReservedBytes -= futureBytes
	}
	bodies := make([][]byte, len(writes))
	added := uint64(0)
	for i, w := range writes {
		for _, prior := range writes[:i] {
			if bytes.Equal(w.Key, prior.Key) {
				return errInvalid
			}
		}
		b, err := wire(w.Value)
		if err != nil {
			return err
		}
		if len(b) > columnControlReservation {
			return errLimit
		}
		old, err := tr.Get(w.Key).Get()
		if err != nil {
			return err
		}
		if old == nil {
			added += uint64(len(w.Key) + columnControlReservation)
			l.Records++
		}
		bodies[i] = b
	}
	if l.Records > v.binding.Policy.MaxRecords || l.ReservedRecords > v.binding.Policy.MaxRecords-l.Records || added > v.binding.Policy.MaxBytes-l.Bytes || l.ReservedBytes > v.binding.Policy.MaxBytes-l.Bytes-added {
		return errLimit
	}
	l.Bytes += added
	for i, w := range writes {
		tr.Set(w.Key, bodies[i])
	}
	return put(tr, v.key("ledger"), l)
}

type graphColumnPinToken struct {
	Version                   uint8
	Request, Generation       string
	Columns, Manifest, Secret [32]byte
	Cut                       certificate
	FeedStart                 [2]uint64
}
type graphColumnPinRecord struct {
	Token           graphColumnPinToken
	Intent          [32]byte
	State           uint8
	TerminalRequest string
	TerminalKind    uint8
}

func columnPinDigest(token graphColumnPinToken) [32]byte {
	body, _ := wire(token)
	return sha256.Sum256(body)
}
func columnPinKey(s *graphColumnStore, request string) fdb.Key {
	return s.key("retention-v1/pin/" + hex.EncodeToString([]byte(request)))
}
func columnValidatePin(s *graphColumnStore, token graphColumnPinToken) error {
	if s == nil || token.Version != 1 || !validName(token.Request) || !validName(token.Generation) || token.Columns != s.digest || token.Manifest == [32]byte{} || token.Secret == [32]byte{} || !slices.Equal(token.Cut.Scope, []uint8{0, 1}) || token.Cut.Graph != s.h.config.Graph || token.Cut.Topology != s.h.config.Topology || token.Cut.Epochs != s.h.config.Epochs || token.Cut.Round == 0 || token.FeedStart != token.Cut.Groups {
		return errInvalid
	}
	return nil
}
func readGraphColumnPin(tr fdb.ReadTransaction, s *graphColumnStore, token graphColumnPinToken) (graphColumnPinRecord, error) {
	if err := columnValidatePin(s, token); err != nil {
		return graphColumnPinRecord{}, err
	}
	record, found, err := get[graphColumnPinRecord](tr, columnPinKey(s, token.Request))
	if err != nil {
		return record, err
	}
	if !found {
		return record, errUnavailable
	}
	if !columnSameSecret(record.Token.Secret, token.Secret) || !graphColumnSame(record.Token, token) {
		return record, errMismatch
	}
	if record.State < columnOwnerActive || record.State > columnOwnerExpired {
		return record, errInvalid
	}
	if record.State == columnOwnerActive && (record.TerminalRequest != "" || record.TerminalKind != 0) || record.State != columnOwnerActive && (!validName(record.TerminalRequest) || record.TerminalKind != record.State) {
		return record, errInvalid
	}
	return record, nil
}
func (v *graphColumnPins) pin(ctx context.Context, request, generation string, at cut) (graphColumnPinToken, error) {
	if v == nil || ctx == nil || !validName(request) || !validName(generation) || !slices.Equal(at.record.Scope, []uint8{0, 1}) {
		return graphColumnPinToken{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return graphColumnPinToken{}, err
	}
	if at.record.Graph != v.s.h.config.Graph || at.record.Topology != v.s.h.config.Topology || at.record.Epochs != v.s.h.config.Epochs {
		return graphColumnPinToken{}, errStale
	}
	if at.record.Round == 0 {
		return graphColumnPinToken{}, errInvalid
	}
	cutBody, err := wire(at.record)
	if err != nil {
		return graphColumnPinToken{}, err
	}
	at.record, err = decode[certificate](cutBody)
	if err != nil {
		return graphColumnPinToken{}, err
	}
	intentBody, err := wire(struct {
		Columns    [32]byte
		Generation string
		Cut        certificate
	}{v.s.digest, generation, at.record})
	if err != nil {
		return graphColumnPinToken{}, err
	}
	intent := sha256.Sum256(intentBody)
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return graphColumnPinToken{}, err
	}
	result, err := v.s.writeContext(ctx, func(tr fdb.Transaction) (any, error) {
		if _, err := v.ledger(tr); err != nil {
			return nil, err
		}
		prior, found, err := get[graphColumnPinRecord](tr, columnPinKey(v.s, request))
		if err != nil {
			return nil, err
		}
		if found {
			if prior.Intent != intent || prior.Token.Request != request || prior.Token.Generation != generation || !sameCertificate(prior.Token.Cut, at.record) {
				return nil, errMismatch
			}
			if err := columnValidatePin(v.s, prior.Token); err != nil {
				return nil, err
			}
			if prior.State != columnOwnerActive {
				return nil, errColumnExpired
			}
			m, err := v.s.manifestIn(tr, generation)
			if err != nil {
				return nil, err
			}
			if err := v.s.h.historyColumnPinIn(tr, prior.Token, m); err != nil {
				return nil, err
			}
			return prior.Token, nil
		}
		m, err := v.s.manifestIn(tr, generation)
		if err != nil {
			return nil, err
		}
		if err := v.s.h.verifyCut(tr, at); err != nil {
			return nil, err
		}
		if at.record.Round < m.Generation.Initial.Round || at.record.Round > m.Generation.Cut.Round {
			return nil, errInvalid
		}
		for g := range 2 {
			if at.record.Groups[g] > m.Generation.Cut.Groups[g] {
				return nil, errInvalid
			}
		}
		guard, guardFound, err := v.s.guard(tr, m)
		if err != nil {
			return nil, err
		}
		if !guardFound {
			return nil, errUnavailable
		}
		if guard.State != columnGuardActive {
			return nil, errColumnExpired
		}
		token := graphColumnPinToken{Version: 1, Request: request, Generation: generation, Columns: v.s.digest, Manifest: guard.Manifest, Secret: secret, Cut: at.record, FeedStart: at.record.Groups}
		if guard.Pins == 1<<32 {
			return nil, errLimit
		}
		guard.Pins++
		if err := v.s.h.historyColumnPinIn(tr, token, m); err != nil {
			return nil, err
		}
		if err := v.put(tr, columnControlWrite{columnPinKey(v.s, request), graphColumnPinRecord{Token: token, Intent: intent, State: columnOwnerActive}}); err != nil {
			return nil, err
		}
		if err := v.s.controls(tr, columnControlWrite{v.s.generationKey(generation, "owners"), guard}); err != nil {
			return nil, err
		}
		return token, nil
	})
	if err != nil {
		return graphColumnPinToken{}, graphColumnCommitError(err)
	}
	return result.(graphColumnPinToken), nil
}

// Only possession of the exact issued token authenticates release/expiration.
// The terminal request is scoped by the original pin request and occupies its
// already-reserved record. Full metadata quotas cannot prevent termination.
// No client wall-clock, deadline inference, missing-state abort or silent renewal.
func (v *graphColumnPins) finish(ctx context.Context, request string, token graphColumnPinToken, expire bool) error {
	if v == nil || ctx == nil || !validName(request) {
		return errInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := columnValidatePin(v.s, token); err != nil {
		return err
	}
	kind := columnOwnerReleased
	if expire {
		kind = columnOwnerExpired
	}
	_, err := v.s.writeContext(ctx, func(tr fdb.Transaction) (any, error) {
		if _, err := v.ledger(tr); err != nil {
			return nil, err
		}
		record, err := readGraphColumnPin(tr, v.s, token)
		if err != nil {
			return nil, err
		}
		if record.State != columnOwnerActive {
			if record.TerminalRequest == request && record.TerminalKind == kind {
				return nil, v.s.h.historyColumnPinFinishIn(tr, token, kind, request)
			}
			return nil, errColumnExpired
		}
		m, err := v.s.manifestIn(tr, token.Generation)
		if err != nil {
			return nil, err
		}
		g, found, err := v.s.guard(tr, m)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errUnavailable
		}
		if g.State != columnGuardActive || g.Pins == 0 || g.Manifest != token.Manifest {
			return nil, errMismatch
		}
		g.Pins--
		record.State = kind
		record.TerminalRequest = request
		record.TerminalKind = kind
		if err := v.s.h.historyColumnPinFinishIn(tr, token, kind, request); err != nil {
			return nil, err
		}
		if err := v.put(tr, columnControlWrite{columnPinKey(v.s, token.Request), record}); err != nil {
			return nil, err
		}
		return nil, v.s.controls(tr, columnControlWrite{v.s.generationKey(token.Generation, "owners"), g})
	})
	return graphColumnCommitError(err)
}
func (v *graphColumnPins) open(ctx context.Context, token graphColumnPinToken) (*graphColumnRead, error) {
	if v == nil {
		return nil, errInvalid
	}
	r, err := newGraphColumnRead(v.s, token.Generation, &token)
	if err != nil {
		return nil, err
	}
	if err = r.open(ctx); err != nil {
		if errors.Is(err, errUnknown) {
			err = errors.Join(err, columnReadCleanup(r))
		}
		return nil, err
	}
	return r, nil
}

type graphColumnReference struct {
	Name, Generation string
	Version          uint64
	Manifest         [32]byte
}
type graphColumnReferenceRequest struct {
	Intent    [32]byte
	Reference graphColumnReference
}

// expected is a revision, not a generation name: remove/re-add ABA cannot pass.
// A removed name keeps its revision tombstone. Empty generation explicitly drops
// the named reference; the original generation/manifest lineage stays retained.
func (v *graphColumnPins) reference(ctx context.Context, request, name, generation string, expected uint64) (graphColumnReference, error) {
	if v == nil || ctx == nil || !validName(request) || !validName(name) || generation != "" && !validName(generation) {
		return graphColumnReference{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return graphColumnReference{}, err
	}
	body, err := wire(struct {
		Name, Generation string
		Expected         uint64
	}{name, generation, expected})
	if err != nil {
		return graphColumnReference{}, err
	}
	intent := sha256.Sum256(body)
	result, err := v.s.writeContext(ctx, func(tr fdb.Transaction) (any, error) {
		if _, err := v.ledger(tr); err != nil {
			return nil, err
		}
		prior, found, err := get[graphColumnReferenceRequest](tr, v.recordKey("reference-request", request))
		if err != nil {
			return nil, err
		}
		if found {
			if prior.Intent != intent {
				return nil, errMismatch
			}
			return prior.Reference, nil
		}
		ref, found, err := get[graphColumnReference](tr, v.recordKey("reference", name))
		if err != nil {
			return nil, err
		}
		if ref.Version != expected || expected == ^uint64(0) || found && ref.Name != name {
			return nil, errMismatch
		}
		next := graphColumnReference{Name: name, Generation: generation, Version: expected + 1}
		var writes []columnControlWrite
		if ref.Generation != "" {
			m, err := v.s.manifestIn(tr, ref.Generation)
			if err != nil {
				return nil, err
			}
			g, guardFound, err := v.s.guard(tr, m)
			if err != nil {
				return nil, err
			}
			if !guardFound {
				return nil, errUnavailable
			}
			if g.State != columnGuardActive || g.References == 0 || g.Manifest != ref.Manifest {
				return nil, errMismatch
			}
			g.References--
			writes = append(writes, columnControlWrite{v.s.generationKey(ref.Generation, "owners"), g})
		}
		if generation != "" {
			m, err := v.s.manifestIn(tr, generation)
			if err != nil {
				return nil, err
			}
			g, guardFound, err := v.s.guard(tr, m)
			if err != nil {
				return nil, err
			}
			if !guardFound {
				return nil, errUnavailable
			}
			if g.State != columnGuardActive {
				return nil, errColumnExpired
			}
			next.Manifest = g.Manifest
			if generation == ref.Generation {
				writes = nil
			} else {
				if g.References == 1<<32 {
					return nil, errLimit
				}
				g.References++
				writes = append(writes, columnControlWrite{v.s.generationKey(generation, "owners"), g})
			}
		}
		credit := 0
		if ref.Generation == "" && generation != "" {
			credit = 1
		}
		if ref.Generation != "" && generation == "" {
			credit = -1
		}
		if err := v.s.h.historyColumnReferenceIn(tr, request, ref, next, v.s); err != nil {
			return nil, err
		}
		if err := v.putWithCredit(tr, credit, columnControlWrite{v.recordKey("reference", name), next}, columnControlWrite{v.recordKey("reference-request", request), graphColumnReferenceRequest{Intent: intent, Reference: next}}); err != nil {
			return nil, err
		}
		if len(writes) > 0 {
			if err := v.s.controls(tr, writes...); err != nil {
				return nil, err
			}
		}
		return next, nil
	})
	if err != nil {
		return graphColumnReference{}, graphColumnCommitError(err)
	}
	return result.(graphColumnReference), nil
}

type graphColumnCollectBudget struct{ Chunks, Bytes, OutputBytes uint64 }
type graphColumnCollectResult struct {
	Generation                                  string
	Cursor, DeletedBytes, WorkChunks, WorkBytes uint64
	Done                                        bool
}

func (v *graphColumnPins) collectAdmission(id string, b graphColumnCollectBudget) error {
	p := v.binding.Policy
	if !validName(id) || b.Chunks < 1 || b.Chunks > p.MaxWorkChunks || b.Bytes < 1 || b.Bytes > p.MaxWorkBytes || b.OutputBytes < 1 || b.OutputBytes > p.MaxOutputBytes {
		return errInvalid
	}
	worst, err := wire(graphColumnCollectResult{Generation: id, Cursor: ^uint64(0), DeletedBytes: ^uint64(0), WorkChunks: b.Chunks, WorkBytes: b.Bytes, Done: false})
	if err != nil {
		return err
	}
	if uint64(len(worst)) > b.OutputBytes {
		return errLimit
	}
	return nil
}

// Each native batch first checks SAME generation guard and all page/output/work
// admission, then atomically deletes ONLY derived chunk keys with its cursor and
// exact key/value-byte ledger decrement. Original manifest/progress/guard/pin
// lineage remains; an identical retired publication returns explicit expiry.
func (v *graphColumnPins) collect(ctx context.Context, id string, b graphColumnCollectBudget) (graphColumnCollectResult, error) {
	if v == nil || ctx == nil {
		return graphColumnCollectResult{}, errInvalid
	}
	if err := v.collectAdmission(id, b); err != nil {
		return graphColumnCollectResult{}, err
	}
	result := graphColumnCollectResult{Generation: id}
	for result.WorkChunks < b.Chunks {
		if err := ctx.Err(); err != nil {
			return graphColumnCollectResult{}, err
		}
		page, err := v.s.writeContext(ctx, func(tr fdb.Transaction) (any, error) {
			if _, err := v.ledger(tr); err != nil {
				return nil, err
			}
			m, err := v.s.manifestIn(tr, id)
			if err != nil {
				return nil, err
			}
			g, guardFound, err := v.s.guard(tr, m)
			if err != nil {
				return nil, err
			}
			if !guardFound {
				return nil, errUnavailable
			}
			if g.References != 0 || g.Pins != 0 || g.Readers != 0 {
				return nil, errColumnInUse
			}
			if g.State == columnGuardCollected {
				return graphColumnCollectResult{Generation: id, Cursor: g.Cursor, DeletedBytes: g.DeletedBytes, Done: true}, nil
			}
			maxChunks := min(uint64(v.binding.Policy.PageChunks), b.Chunks-result.WorkChunks, m.Chunks-g.Cursor)
			var keys []fdb.Key
			used := uint64(0)
			for n := uint64(0); n < maxChunks; n++ {
				key := v.s.chunkKey(id, g.Cursor)
				raw, err := tr.Get(key).Get()
				if err != nil {
					return nil, err
				}
				if raw == nil {
					return nil, errUnavailable
				}
				size := uint64(len(key) + len(raw))
				if size > uint64(v.binding.Policy.PageBytes)-used {
					if len(keys) == 0 {
						return nil, errLimit
					}
					break
				}
				if size > b.Bytes-result.WorkBytes-used {
					if len(keys) == 0 {
						return nil, errColumnCollectBudget
					}
					break
				}
				if _, err := decodeGraphColumnChunk(v.s.h, m.Generation, g.Cursor, g.Chain, raw, v.s.binding.Policy); err != nil {
					return nil, err
				}
				g.Chain = graphColumnChain(g.Chain, raw)
				g.Cursor++
				g.DeletedBytes += size
				used += size
				keys = append(keys, key)
			}
			done := g.Cursor == m.Chunks
			if done && (g.Chain != m.Chain || g.DeletedBytes != m.Bytes) {
				return nil, errMismatch
			}
			g.State = columnGuardRetiring
			if done {
				g.State = columnGuardCollected
			}
			output := graphColumnCollectResult{Generation: id, Cursor: g.Cursor, DeletedBytes: g.DeletedBytes, WorkChunks: uint64(len(keys)), WorkBytes: used, Done: done}
			encoded, err := wire(output)
			if err != nil {
				return nil, err
			}
			if uint64(len(encoded)) > b.OutputBytes {
				return nil, errLimit
			}
			if err := v.s.controls(tr, columnControlWrite{v.s.generationKey(id, "owners"), g}); err != nil {
				return nil, err
			}
			l, err := v.s.ledger(tr)
			if err != nil {
				return nil, err
			}
			if l.Chunks < uint64(len(keys)) || l.Bytes < used {
				return nil, errInvalid
			}
			l.Chunks -= uint64(len(keys))
			l.Bytes -= used
			for _, key := range keys {
				tr.Clear(key)
			}
			if err := put(tr, v.s.key("ledger"), l); err != nil {
				return nil, err
			}
			return output, nil
		})
		if errors.Is(err, errColumnCollectBudget) {
			if result.WorkChunks > 0 {
				return result, nil
			}
			return graphColumnCollectResult{}, errLimit
		}
		if err != nil {
			return graphColumnCollectResult{}, graphColumnCommitError(err)
		}
		value := page.(graphColumnCollectResult)
		result.Cursor, result.DeletedBytes, result.Done = value.Cursor, value.DeletedBytes, value.Done
		result.WorkChunks += value.WorkChunks
		result.WorkBytes += value.WorkBytes
		if result.Done || value.WorkChunks == 0 || result.WorkBytes == b.Bytes {
			return result, nil
		}
	}
	return result, nil
}
