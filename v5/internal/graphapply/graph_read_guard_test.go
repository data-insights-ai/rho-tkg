package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	pb "go.etcd.io/raft/v3/raftpb"
)

func conditionalWire(t *testing.T, f *graphFixture, r graphRequest) ([]byte, *graphReadSession) {
	t.Helper()
	s, err := openGraphRead(t.Context(), f.m, f.currentIndex(t))
	if err != nil {
		t.Fatal(err)
	}
	wire, err := s.Request(r.id, r.operations, r.revision, r.claims)
	if err != nil {
		t.Fatal(err)
	}
	return wire, s
}
func (f *graphFixture) currentIndex(t *testing.T) uint64 {
	t.Helper()
	i, _, err := f.s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	return i
}
func commitConditionalWire(t *testing.T, f *graphFixture, wire []byte) (outcome, raftlog.ApplicationBatch) {
	t.Helper()
	b, err := f.stage(t, wire)
	if err != nil {
		t.Fatal(err)
	}
	f.install(t, wire, b)
	o, err := decodeAnyOutcome(b.Outcome, f.n)
	if err != nil {
		t.Fatal(err)
	}
	return o, b
}
func assertNoReadSessionLeaks(t *testing.T, s *raftlog.Store, before raftlog.ApplicationUsage) {
	t.Helper()
	after, err := s.ApplicationUsage()
	if err != nil || after.Views != before.Views || after.ViewBytes != before.ViewBytes {
		t.Fatal(before, after, err)
	}
}
func boundReadyReadFixture(t *testing.T, id raftlog.ApplicationSemanticContractID) *graphFixture {
	t.Helper()
	seed := openGraphFixture(t)
	_, image, err := seed.s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	identity := raftlog.ApplicationIdentity{Graph: [16]byte(seed.n.graph), Partition: seed.n.partition, Group: [16]byte{77}}
	store, cfg := boundMaterializerStore(t, id, identity, image)
	m, err := newMaterializer(store, seed.n, 1, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	f := &graphFixture{s: store, m: m, n: seed.n, cfg: cfg}
	r := graphInit(t)
	r.schemas = []graphstate.PropertyDefinition{{Name: "answer", Owner: graphstate.Node, Type: graphstate.ScalarScope, Cardinality: graphstate.ScalarCardinality}}
	f.commit(t, r)
	f.control(t, codecRequests(t)[2])
	reserve := codecRequests(t)[3]
	reserve.sequence = 1
	reserve.count = 16
	f.control(t, reserve)
	return f // Opaque local atomic fixture, not fixed-three consensus acceptance.
}
func TestGraphReadSessionDirectOwnershipScopeAndClose(t *testing.T) {
	f := readyGraphFixture(t)
	before, err := f.s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	r := graphCodecRequest(t)
	wire, s := conditionalWire(t, f, r)
	if s.base.group != ([16]byte{}) || !s.base.valid(f.n) {
		t.Fatal(s.base)
	}
	v, err := s.ReadView()
	if err != nil {
		t.Fatal(err)
	}
	missing, err := v.Entity(t.Context(), 11)
	if err != nil || missing.Found {
		t.Fatal(missing, err)
	}
	work := s.Work()
	if work.Records < 7 || work.Bytes < graphReadSessionBytes {
		t.Fatal(work)
	}
	old := owned(wire)
	r.operations[0].Owner = 99
	if !bytes.Equal(wire, old) {
		t.Fatal("encoded request borrowed caller array")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil || s.Work() != work {
		t.Fatal("repeat Close changed result/work", err)
	}
	if _, err := s.ReadView(); !errors.Is(err, graphstore.ErrClosed) {
		t.Fatal(err)
	}
	if _, err := v.Entity(t.Context(), 11); !errors.Is(err, graphstore.ErrClosed) {
		t.Fatal(err)
	}
	if _, err := s.Request(r.id, r.operations, r.revision, r.claims); !errors.Is(err, graphstore.ErrClosed) {
		t.Fatal(err)
	}
	assertNoReadSessionLeaks(t, f.s, before)
	// Repeating a retained nonnil cleanup result must preserve identity, not wrap.
	cause := errors.New("original cleanup cause")
	closed := &graphReadSession{closed: true, closeErr: cause}
	firstClose, secondClose := closed.Close(), closed.Close()
	if firstClose != cause || secondClose != cause {
		t.Fatal("cleanup result grew/repeated")
	}
	var nilSession *graphReadSession
	if !errors.Is(nilSession.Close(), errInvalid) {
		t.Fatal("nil Close")
	}
	if _, err := nilSession.ReadView(); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := nilSession.Request(requestID{1}, nil, state.Revision{}, nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if nilSession.Work() != (graphstore.PageWork{}) {
		t.Fatal("nil Work")
	}
	if unsafe.Sizeof(graphReadSession{})+unsafe.Sizeof(reader{})+unsafe.Sizeof(graphstore.Root{})+unsafe.Sizeof(raftlog.ApplicationRoot{}) > graphReadSessionBytes {
		t.Fatal("session metadata undercount")
	}
	if unsafe.Sizeof(pb.HardState{})+unsafe.Sizeof(pb.ConfState{})+3*8 > graphReadScopeBytes {
		t.Fatal("scope metadata undercount")
	}
	if unsafe.Sizeof(graphRequest{}) > graphRequestMetadataBytes {
		t.Fatal("request metadata undercount")
	}
}
func TestGuardedGraphStaleNegativeReadsABAAndOriginalRecovery(t *testing.T) {
	f := readyGraphFixture(t)
	r := graphCodecRequest(t)
	conditional := r
	conditional.id = requestID{50}
	wire, s := conditionalWire(t, f, conditional)
	v, err := s.ReadView()
	if err != nil {
		t.Fatal(err)
	}
	missing, err := v.Entity(t.Context(), 11)
	if err != nil || missing.Found {
		t.Fatal(missing, err)
	}
	keys, err := v.ComponentKeys(t.Context(), graphstate.KeyPredicate{Owner: 11}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if err != nil || !keys.Complete || len(keys.Keys) != 0 {
		t.Fatal(keys, err)
	}
	s.Close()
	created, first := f.commit(t, r)
	if created.reason != reasonNone {
		t.Fatal(created)
	}
	conflict, b := commitConditionalWire(t, f, wire)
	if conflict.kind != guardedGraphOperations || conflict.reason != reasonReadConflict || len(b.Writes) != 1 || len(b.Changes) != 0 || !bytes.Equal(b.Image, first.Image) {
		t.Fatal(conflict, b)
	}
	original := conflict.index
	// Current data A -> B -> A retains different history and invalidates a read.
	fresh := r
	fresh.id = requestID{31}
	fresh.operations = []graphstate.Operation{r.operations[1]}
	fresh.claims = nil
	abaWire, abaSession := conditionalWire(t, f, fresh)
	abaSession.Close()
	change := fresh
	change.id = requestID{32}
	change.revision, _ = state.NewRevision(9, 0)
	alternate, err := temporal.Point(semanticPosition(t, r.operations[0].Scope.Axis(), 1))
	if err != nil {
		t.Fatal(err)
	}
	change.operations = []graphstate.Operation{r.operations[1]}
	change.operations[0].Value = graphstate.ScopeValue(alternate)
	change.operations[0].ValueID = 14
	change.claims = []freshBinding{{role: valueBinding, id: 14, grant: r.claims[0].grant}}
	f.commit(t, change)
	change.id = requestID{33}
	change.revision, _ = state.NewRevision(10, 0)
	change.operations[0].Value = r.operations[1].Value
	change.operations[0].ValueID = 999
	change.claims = nil
	_, latest := f.commit(t, change)
	aba, rejected := commitConditionalWire(t, f, abaWire)
	if aba.reason != reasonReadConflict || !bytes.Equal(rejected.Image, latest.Image) || len(rejected.Changes) != 0 {
		t.Fatal(aba, rejected)
	}
	f.control(t, codecRequests(t)[1])
	activate := codecRequests(t)[2]
	activate.id = requestID{40}
	activate.expectedEpoch = 1
	activate.session.Epoch = 2
	activate.session.Incarnation = [16]byte{99}
	f.control(t, activate)
	retry, replayed := commitConditionalWire(t, f, wire)
	if retry.disposition != requestReplay || retry.index != original || retry.reason != reasonReadConflict || len(replayed.Writes)+len(replayed.Changes) != 0 {
		t.Fatal(retry, replayed)
	}
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := f.cfg
	cfg.Create = false
	reopened, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	f.s = reopened
	f.m, err = newMaterializer(reopened, f.n, 1, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	retry, _ = commitConditionalWire(t, f, wire)
	if retry.index != original || retry.disposition != requestReplay {
		t.Fatal(retry)
	}
	changed := owned(wire)
	changed[61] ^= 1
	changed = seal(changed[:len(changed)-32])
	mismatch, unchanged := commitConditionalWire(t, f, changed)
	if mismatch.reason != reasonMismatch || len(unchanged.Writes) != 0 {
		t.Fatal(mismatch, unchanged)
	}
	retry, _ = commitConditionalWire(t, f, wire)
	if retry.index != original || retry.reason != reasonReadConflict {
		t.Fatal("original mapping replaced", retry)
	}
}
func TestGuardedGraphSuccessNoopControlsAndPhysicalOnlyCurrentBase(t *testing.T) {
	f := readyGraphFixture(t)
	r := graphCodecRequest(t)
	wire, s := conditionalWire(t, f, r)
	s.Close()
	// Service rotation preserves cached grants and the graph conditional base.
	f.control(t, codecRequests(t)[1])
	o, b := commitConditionalWire(t, f, wire)
	if o.reason != reasonNone || o.kind != guardedGraphOperations || len(b.Changes) == 0 {
		t.Fatal(o, b)
	}
	retry, replayed := commitConditionalWire(t, f, wire)
	if retry.index != o.index || retry.disposition != requestReplay || len(replayed.Writes)+len(replayed.Changes) != 0 || !bytes.Equal(replayed.Image, b.Image) {
		t.Fatal(retry, replayed)
	}
	noop := r
	noop.id = requestID{20}
	noop.operations = []graphstate.Operation{r.operations[1]}
	noop.operations[0].ValueID = 999
	noop.claims = nil
	noWire, s := conditionalWire(t, f, noop)
	base := s.base
	s.Close()
	n, nb := commitConditionalWire(t, f, noWire)
	if n.reason != reasonNone || len(nb.Changes) != 0 || !bytes.Equal(nb.Image, b.Image) {
		t.Fatal(n, nb)
	}
	idx := f.currentIndex(t)
	v, err := f.s.ApplicationView(idx)
	if err != nil {
		t.Fatal(err)
	}
	appBase, err := v.Root()
	v.Close()
	if err != nil {
		t.Fatal(err)
	}
	root, err := graphstore.DecodeRoot(appBase.Image)
	if err != nil {
		t.Fatal(err)
	}
	root, _, err = root.ReservePhysical(3)
	if err != nil {
		t.Fatal(err)
	}
	physical, err := graphstore.EncodeRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	installIncompleteFixture(t, f, raftlog.ApplicationBatch{BaseGeneration: appBase.Generation, BaseIndex: appBase.Index, BaseImageHash: appBase.ImageHash, Image: physical})
	if err := f.m.Restore(idx+1, physical); err != nil {
		t.Fatal(err)
	}
	current, err := openGraphRead(t.Context(), f.m, idx+1)
	if err != nil {
		t.Fatal(err)
	}
	if current.base != base {
		t.Fatal("physical cursor/image/index entered logical guard")
	}
	current.Close()
	noop.id = requestID{21}
	request, _ := decodeGraphRequest(noWire, f.m.limits)
	request.id = noop.id
	physicalWire, err := encodeGraphRequest(request, f.m.limits)
	if err != nil {
		t.Fatal(err)
	}
	n, nb = commitConditionalWire(t, f, physicalWire)
	if n.reason != reasonNone || !bytes.Equal(nb.Image, physical) || len(nb.Changes) != 0 {
		t.Fatal(n, nb)
	}
}
func TestGraphReadBoundGroupAndUnboundNonSingletonRefusal(t *testing.T) {
	bound := boundReadyReadFixture(t, SemanticContractID())
	r := graphCodecRequest(t)
	wire, s := conditionalWire(t, bound, r)
	if s.base.group != bound.s.ApplicationBinding().Identity.Group || s.base.group == ([16]byte{}) {
		t.Fatal(s.base)
	}
	s.Close()
	bad, err := decodeGraphRequest(wire, bound.m.limits)
	if err != nil {
		t.Fatal(err)
	}
	bad.readBase.group = [16]byte{}
	bad.id = requestID{44}
	badWire, err := encodeGraphRequest(bad, bound.m.limits)
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := bound.stage(t, badWire)
	if !errors.Is(err, errInvalid) || !reflect.DeepEqual(rejected, raftlog.ApplicationBatch{}) {
		t.Fatal(rejected, err)
	}
	o, _ := commitConditionalWire(t, bound, wire)
	if o.reason != reasonNone {
		t.Fatal(o)
	}
	unbound := boundReadyReadFixture(t, raftlog.ApplicationSemanticContractID{})
	before, _ := unbound.s.ApplicationUsage()
	if _, err := openGraphRead(t.Context(), unbound.m, unbound.currentIndex(t)); !errors.Is(err, errInvalid) {
		t.Fatal("zero binding authorized fixed-three scope", err)
	}
	assertNoReadSessionLeaks(t, unbound.s, before)
	legacy := readyGraphFixture(t)
	legacyWire, session := conditionalWire(t, legacy, r)
	session.Close()
	parsed, _ := decodeGraphRequest(legacyWire, legacy.m.limits)
	parsed.readBase.group = [16]byte{77}
	parsed.id = requestID{45}
	wrong, _ := encodeGraphRequest(parsed, legacy.m.limits)
	b, err := legacy.stage(t, wrong)
	if !errors.Is(err, errInvalid) || !reflect.DeepEqual(b, raftlog.ApplicationBatch{}) {
		t.Fatal(b, err)
	}
	identity := raftlog.ApplicationIdentity{Graph: [16]byte(legacy.n.graph), Partition: legacy.n.partition, Group: [16]byte{77}}
	old, _ := hex.DecodeString("65cf602d484307ec3d716f967111a5e75fe7ce61fec191620c8f490cf684384f")
	var oldID raftlog.ApplicationSemanticContractID
	copy(oldID[:], old)
	store, _ := boundMaterializerStore(t, oldID, identity, []byte("malformed-root"))
	if _, err := newMaterializer(store, legacy.n, 1, defaultMaterializerLimits()); !errors.Is(err, replica.ErrInvalid) || errors.Is(err, graphstore.ErrCorrupt) {
		t.Fatal("old agreement reached Restore", err)
	}
}
func TestGraphReadConstructorBoundsAndFailureCleanup(t *testing.T) {
	f := readyGraphFixture(t)
	index := f.currentIndex(t)
	s, err := openGraphRead(t.Context(), f.m, index)
	if err != nil {
		t.Fatal(err)
	}
	required := s.Work()
	s.Close()
	before, _ := f.s.ApplicationUsage()
	original := f.m.limits
	for _, dimension := range []string{"rows", "bytes"} {
		for _, delta := range []int{-1, 0, 1} {
			f.m.limits = original
			if dimension == "rows" {
				f.m.limits.sourceRows = required.Records + delta
			} else {
				f.m.limits.sourceBytes = required.Bytes + delta
			}
			opened, err := openGraphRead(t.Context(), f.m, index)
			if delta < 0 {
				if !errors.Is(err, graphstore.ErrResourceLimit) && !errors.Is(err, errLimit) {
					t.Fatal(dimension, delta, err)
				}
				if opened != nil {
					t.Fatal("partial session")
				}
			} else {
				if err != nil {
					t.Fatal(dimension, delta, err)
				}
				opened.Close()
			}
			assertNoReadSessionLeaks(t, f.s, before)
		}
	}
	f.m.limits = original
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, test := range []struct {
		ctx      context.Context
		m        *materializer
		index    uint64
		expected error
	}{{nil, f.m, index, errInvalid}, {t.Context(), nil, index, errInvalid}, {t.Context(), f.m, 0, errInvalid}, {ctx, f.m, index, context.Canceled}, {t.Context(), f.m, index + 100, raftlog.ErrInvalid}} {
		if opened, err := openGraphRead(test.ctx, test.m, test.index); !errors.Is(err, test.expected) || opened != nil {
			t.Fatal(opened, err)
		}
		assertNoReadSessionLeaks(t, f.s, before)
	}
	f.m.limits.outputBytes = 1
	if opened, err := openGraphRead(t.Context(), f.m, index); !errors.Is(err, errLimit) || opened != nil {
		t.Fatal(opened, err)
	}
	f.m.limits = original
	assertNoReadSessionLeaks(t, f.s, before)
	seed := openGraphFixture(t)
	if _, err := openGraphRead(t.Context(), seed.m, 1); !errors.Is(err, errNotInitialized) {
		t.Fatal(err)
	}
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openGraphRead(t.Context(), f.m, index); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
}
func TestGuardedGraphStrictCodecMatrixAndRepresentationBounds(t *testing.T) {
	f := readyGraphFixture(t)
	r := graphCodecRequest(t)
	wire, s := conditionalWire(t, f, r)
	s.Close()
	parsed, err := decodeGraphRequest(wire, f.m.limits)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) < 125 || wire[3] != 3 || wire[4] != 7 || binary.BigEndian.Uint64(wire[61:69]) != 1 || binary.BigEndian.Uint64(wire[85:93]) != parsed.readBase.guard.SemanticEpoch {
		t.Fatal("guard layout")
	}
	for _, version := range []byte{1, 2, 3, 4} {
		for _, kind := range []commandKind{initGraph, graphOperations, guardedGraphOperations, 8} {
			raw := owned(wire)
			raw[3], raw[4] = version, byte(kind)
			raw = seal(raw[:len(raw)-32])
			_, _, identityErr := commandIdentity(raw)
			valid := version == 2 && (kind == initGraph || kind == graphOperations) || version == 3 && kind == guardedGraphOperations
			if valid {
				if identityErr != nil {
					t.Fatal(version, kind, identityErr)
				}
			} else {
				if !errors.Is(identityErr, errCorrupt) {
					t.Fatal(version, kind, identityErr)
				}
				if _, err := decodeGraphRequest(raw, f.m.limits); !errors.Is(err, errCorrupt) {
					t.Fatal(version, kind, err)
				}
			}
		}
	}
	for _, offset := range []int{61, 69, 77, 85, 93} {
		raw := owned(wire)
		n := 8
		if offset == 93 {
			n = 32
		}
		clear(raw[offset : offset+n])
		raw = seal(raw[:len(raw)-32])
		if _, err := decodeGraphRequest(raw, f.m.limits); !errors.Is(err, errCorrupt) {
			t.Fatal(offset, err)
		}
	}
	for _, kind := range []commandKind{initGraph, graphOperations, guardedGraphOperations} {
		for why := reason(0); why <= 9; why++ {
			o := outcome{ns: f.n, kind: kind, identity: [16]byte{1}, hash: sha256.Sum256([]byte("fixed")), index: 1, disposition: applied, reason: why}
			valid := why == reasonNone || why == reasonInvalid || why == reasonMismatch || why == reasonStale && isGraphMutation(kind) || why == reasonAlreadyInitialized && kind == initGraph || why == reasonReadConflict && kind == guardedGraphOperations
			encoded, err := encodeGraphOutcome(o)
			if valid {
				if err != nil || len(encoded) != 120 {
					t.Fatal(kind, why, err)
				}
				if _, err := decodeGraphOutcome(encoded, f.n); err != nil {
					t.Fatal(err)
				}
			} else {
				if !errors.Is(err, errInvalid) {
					t.Fatal(kind, why, err)
				}
			}
		}
	}
	validOutcome := outcome{ns: f.n, kind: guardedGraphOperations, identity: [16]byte{1}, hash: sha256.Sum256(wire), index: 1, disposition: applied, reason: reasonReadConflict}
	encoded, _ := encodeGraphOutcome(validOutcome)
	for _, kind := range []commandKind{initGraph, graphOperations, reserveGrant} {
		bad := owned(encoded)
		bad[4] = byte(kind)
		bad = seal(bad[:len(bad)-32])
		if _, err := decodeGraphOutcome(bad, f.n); !errors.Is(err, errCorrupt) {
			t.Fatal(err)
		}
	}
	reserved := owned(encoded)
	binary.BigEndian.PutUint16(reserved[86:88], 7)
	reserved = seal(reserved[:len(reserved)-32])
	if _, err := decodeGraphOutcome(reserved, f.n); !errors.Is(err, errCorrupt) {
		t.Fatal("reserved reason7 accepted", err)
	}
	for _, delta := range []int{-1, 0, 1} {
		l := f.m.limits
		l.commandBytes = len(wire) + delta
		_, err := decodeGraphRequest(wire, l)
		if delta < 0 {
			if !errors.Is(err, errLimit) {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
	lo, hi := 1, f.m.limits.commandOwnedBytes
	for lo < hi {
		mid := (lo + hi) / 2
		l := f.m.limits
		l.commandOwnedBytes = mid
		if _, err := decodeGraphRequest(wire, l); err == nil {
			hi = mid
		} else if errors.Is(err, errLimit) {
			lo = mid + 1
		} else {
			t.Fatal(err)
		}
	}
	for _, delta := range []int{-1, 0, 1} {
		l := f.m.limits
		l.commandOwnedBytes = lo + delta
		_, err := decodeGraphRequest(wire, l)
		if delta < 0 {
			if !errors.Is(err, errLimit) {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
}
func TestGuardedGraphResourcePriorityAndAtomicRefusal(t *testing.T) {
	f := readyGraphFixture(t)
	r := graphCodecRequest(t)
	conditional := r
	conditional.id = requestID{50}
	wire, s := conditionalWire(t, f, conditional)
	s.Close()
	f.commit(t, r)
	beforeIndex, beforeImage, _ := f.s.Checkpoint()
	original := f.m.limits
	for _, dimension := range []string{"rows", "output", "decode"} {
		f.m.limits = original
		switch dimension {
		case "rows":
			f.m.limits.sourceRows = 6
		case "output":
			f.m.limits.graph.MaxOutputBytes = 1
		case "decode":
			f.m.limits.commandOwnedBytes = 1
		}
		b, err := f.stage(t, wire)
		expected := errLimit
		if dimension == "rows" || dimension == "output" {
			expected = graphstore.ErrResourceLimit
		}
		if !errors.Is(err, expected) || !reflect.DeepEqual(b, raftlog.ApplicationBatch{}) {
			t.Fatal(dimension, b, err)
		}
		index, image, _ := f.s.Checkpoint()
		if index != beforeIndex || !bytes.Equal(image, beforeImage) {
			t.Fatal("refusal changed root")
		}
	}
	f.m.limits = original
	for _, operational := range []error{errLimit, graphstore.ErrResourceLimit, graphstate.ErrResourceLimit, state.ErrResourceLimit, temporal.ErrResourceLimit, raftlog.ErrLimit, graphstore.ErrCorrupt, raftlog.ErrCorrupt, context.Canceled} {
		why, recognized := businessGraphReason(errors.Join(operational, graphstore.ErrReadConflict))
		if recognized || why != reasonNone {
			t.Fatal("operational joined conflict became durable", operational, why)
		}
	}
}

func TestGraphReadAllocatorIsValidatedOnSelectedHistoricalView(t *testing.T) {
	f := readyGraphFixture(t)
	old := f.currentIndex(t)
	v, err := f.s.ApplicationView(old)
	if err != nil {
		t.Fatal(err)
	}
	base, err := v.Root()
	v.Close()
	if err != nil {
		t.Fatal(err)
	}
	installIncompleteFixture(t, f, raftlog.ApplicationBatch{BaseGeneration: base.Generation, BaseIndex: base.Index, BaseImageHash: base.ImageHash, Image: base.Image, Writes: []raftlog.KV{{Key: allocatorKey(f.n), Deleted: true}}})
	before, _ := f.s.ApplicationUsage()
	historical, err := openGraphRead(t.Context(), f.m, old)
	if err != nil {
		t.Fatal("current allocator absence leaked into historical read", err)
	}
	historical.Close()
	assertNoReadSessionLeaks(t, f.s, before)
	if current, err := openGraphRead(t.Context(), f.m, old+1); !errors.Is(err, errCorrupt) || current != nil {
		t.Fatal("missing selected allocator accepted", current, err)
	}
	assertNoReadSessionLeaks(t, f.s, before)
	// A new conditional command cannot turn broken co-init into read conflict.
	r := graphCodecRequest(t)
	session, err := openGraphRead(t.Context(), f.m, old)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := session.Request(requestID{60}, r.operations, r.revision, r.claims)
	session.Close()
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.stage(t, wire)
	if !errors.Is(err, errCorrupt) || !reflect.DeepEqual(b, raftlog.ApplicationBatch{}) {
		t.Fatal(b, err)
	}
}
func TestGuardedGraphEncounteredDescriptorCorruptionBeatsStaleRead(t *testing.T) {
	f := openGraphFixture(t)
	init := graphInit(t)
	init.schemas = []graphstate.PropertyDefinition{{Name: "answer", Owner: graphstate.Node, Type: graphstate.ScalarScope, Cardinality: graphstate.ScalarCardinality}}
	_, initialized := f.commit(t, init)
	f.control(t, codecRequests(t)[2])
	reserve := codecRequests(t)[3]
	reserve.sequence = 1
	reserve.count = 16
	f.control(t, reserve)
	r := graphCodecRequest(t)
	conditional := r
	conditional.id = requestID{60}
	wire, s := conditionalWire(t, f, conditional)
	s.Close()
	f.commit(t, r)
	var descriptor raftlog.KV
	for _, row := range initialized.Writes {
		if len(row.Value) == 160 && bytes.Equal(row.Value[:3], []byte{'G', 'C', 1}) && bytes.Equal(row.Value[28:32], []byte{2, 2, 0, 0}) {
			descriptor = raftlog.KV{Key: owned(row.Key), Value: owned(row.Value)}
		}
	}
	if len(descriptor.Key) == 0 {
		t.Fatal("known initializer descriptor fixture missing")
	}
	binary.BigEndian.PutUint64(descriptor.Value[32:40], 2)
	index := f.currentIndex(t)
	view, err := f.s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	base, err := view.Root()
	view.Close()
	if err != nil {
		t.Fatal(err)
	}
	installIncompleteFixture(t, f, raftlog.ApplicationBatch{BaseGeneration: base.Generation, BaseIndex: base.Index, BaseImageHash: base.ImageHash, Image: base.Image, Writes: []raftlog.KV{descriptor}})
	before, _ := f.s.ApplicationUsage()
	b, err := f.stage(t, wire)
	if !errors.Is(err, graphstore.ErrCorrupt) || errors.Is(err, graphstore.ErrReadConflict) || !reflect.DeepEqual(b, raftlog.ApplicationBatch{}) {
		t.Fatal("corruption became durable conflict", b, err)
	}
	assertNoReadSessionLeaks(t, f.s, before)
}
func TestGuardedGraphSharedControlDedupAndStageOutputHeadroom(t *testing.T) {
	f := readyGraphFixture(t)
	r := graphCodecRequest(t)
	r.id = requestID{55}
	wire, s := conditionalWire(t, f, r)
	s.Close()
	control := codecRequests(t)[1]
	control.id = r.id
	original, _ := f.control(t, control)
	mismatch, b := commitConditionalWire(t, f, wire)
	if mismatch.reason != reasonMismatch || len(b.Writes)+len(b.Changes) != 0 || len(b.Outcome) != 120 {
		t.Fatal(mismatch, b)
	}
	recovered, _ := f.control(t, control)
	if recovered.disposition != requestReplay || recovered.index != original.index {
		t.Fatal("conditional payload replaced control mapping", recovered)
	}
	// A distinct stale guarded request records exactly one lean original mapping.
	r.id = requestID{56}
	staleWire, s := conditionalWire(t, f, r)
	s.Close()
	create := r
	create.id = requestID{57}
	f.commit(t, create)
	expected, err := f.stage(t, staleWire)
	if err != nil {
		t.Fatal(err)
	}
	if len(expected.Writes) != 1 || len(expected.Outcome) != 120 || len(expected.Writes[0].Value) != 120 {
		t.Fatal("unused grant padding or missing mapping", expected)
	}
	stageBytes := 128 + 2*len(expected.Writes[0].Key) + len(expected.Writes[0].Value) + 64
	// Coverage validation needs stager metadata/root plus its separate Work
	// even when the final rejection frame alone would fit a smaller bound.
	outputBytes := max(materializerOutputCost(expected), graphResultMetadataBytes+cap(expected.Image)+512+len(expected.Image)+64)
	originalLimits := f.m.limits
	beforeIndex, beforeImage, _ := f.s.Checkpoint()
	for _, dimension := range []string{"stage", "output"} {
		for _, delta := range []int{-1, 0, 1} {
			f.m.limits = originalLimits
			if dimension == "stage" {
				f.m.limits.stageBytes = stageBytes + delta
			} else {
				f.m.limits.outputBytes = outputBytes + delta
			}
			b, err := f.stage(t, staleWire)
			if delta < 0 {
				expectedError := errLimit
				if dimension == "output" {
					expectedError = graphstore.ErrResourceLimit
				}
				if !errors.Is(err, expectedError) || !reflect.DeepEqual(b, raftlog.ApplicationBatch{}) {
					t.Fatal(dimension, delta, b, err)
				}
			} else if err != nil || !reflect.DeepEqual(b, expected) {
				t.Fatal(dimension, delta, b, err)
			}
			i, image, _ := f.s.Checkpoint()
			if i != beforeIndex || !bytes.Equal(image, beforeImage) {
				t.Fatal("Stage installed a tentative outcome")
			}
		}
	}
	f.m.limits = originalLimits
}
