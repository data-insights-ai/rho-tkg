package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"testing"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

const fixtureImage = "opaque-allocation-only-fixture"

type allocationFixture struct {
	store  *raftlog.Store
	config raftlog.Config
	n      namespace
}

func openFixture(t *testing.T, fs vfs.FS) *allocationFixture {
	return openPolicyFixture(t, fs, raftlog.DefaultApplicationPolicy(1))
}
func openPolicyFixture(t *testing.T, fs vfs.FS, policy raftlog.ApplicationPolicy) *allocationFixture {
	t.Helper()
	cfg := raftlog.Config{Dir: "allocation", FS: fs, Create: true, Application: policy}
	s, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Initialize([]uint64{1}, []byte(fixtureImage)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return &allocationFixture{s, cfg, codecNamespace()}
}
func (f *allocationFixture) stage(t *testing.T, data []byte, l limits, p raftlog.ApplicationPolicy) (allocationEffects, error) {
	t.Helper()
	index, _, err := f.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	view, err := f.store.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	root, err := view.Root()
	if err != nil {
		t.Fatal(err)
	}
	return stageAllocation(t.Context(), view, f.n, applyPosition{index + 1, root.Generation}, data, l, p, f.store.Limits())
}
func effectBatch(e allocationEffects) raftlog.ApplicationBatch {
	return raftlog.ApplicationBatch{BaseGeneration: e.base.Generation, BaseIndex: e.base.Index, BaseImageHash: e.base.ImageHash, Image: owned(e.base.Image), Writes: e.writes, Changes: e.changes, Outcome: e.envelope}
}
func (f *allocationFixture) install(t *testing.T, data []byte, e allocationEffects) uint64 {
	t.Helper()
	index := e.base.Index + 1
	entry := &pb.Entry{Index: new(index), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: owned(data)}
	if err := f.store.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(index)}, Entries: []*pb.Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.InstallApplication(index, effectBatch(e)); err != nil {
		t.Fatal(err)
	}
	_, image, err := f.store.Checkpoint()
	if err != nil || !bytes.Equal(image, []byte(fixtureImage)) {
		t.Fatal("allocation helper changed outer image", image, err)
	}
	return index
}
func (f *allocationFixture) commit(t *testing.T, r request) (outcome, uint64) {
	t.Helper()
	data, err := encodeRequest(r, defaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	e, err := f.stage(t, data, defaultLimits(), f.store.ApplicationLimits())
	if err != nil {
		t.Fatal(err)
	}
	index := f.install(t, data, e)
	return e.outcome, index
}
func (f *allocationFixture) read(t *testing.T, index uint64, key []byte) ([]byte, bool) {
	t.Helper()
	view, err := f.store.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	row, found, err := view.Get(t.Context(), key, 1024)
	if err != nil {
		t.Fatal(err)
	}
	return row.Value, found && !row.Deleted
}
func (f *allocationFixture) raw(t *testing.T, key, value []byte, deleted bool) {
	t.Helper()
	index, image, err := f.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	view, err := f.store.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	root, err := view.Root()
	if err != nil {
		t.Fatal(err)
	}
	view.Close()
	e := allocationEffects{base: root, writes: []raftlog.KV{{Key: key, Value: value, Deleted: deleted}}}
	e.base.Image = image
	f.install(t, nil, e)
}
func initializedFixture(t *testing.T) *allocationFixture {
	t.Helper()
	f := openFixture(t, vfs.NewMem())
	o, _ := f.commit(t, codecRequests(t)[0])
	if o.reason != reasonNone {
		t.Fatal(o)
	}
	o, _ = f.commit(t, codecRequests(t)[2])
	if o.reason != reasonNone {
		t.Fatal(o)
	}
	return f
}
func TestAllocationRetryBeforeFencesAndPayloadComparison(t *testing.T) {
	f := initializedFixture(t)
	reserve := codecRequests(t)[3]
	first, index := f.commit(t, reserve)
	if first.reason != reasonNone || first.disposition != applied || first.grant.Reservation.First != 1 || first.grant.Reservation.Last != 2 {
		t.Fatal(first)
	}
	original, found := f.read(t, index, outcomeKey(reserve))
	if !found {
		t.Fatal("missing original mapping")
	}
	// Change BOTH independent epochs before retrying the exact old request.
	transfer := codecRequests(t)[1]
	if o, _ := f.commit(t, transfer); o.reason != reasonNone {
		t.Fatal(o)
	}
	activate := codecRequests(t)[2]
	activate.id = requestID{9}
	activate.expectedEpoch = 1
	activate.session.Incarnation[0] = 9
	activate.session.Epoch = 2
	if o, _ := f.commit(t, activate); o.reason != reasonNone {
		t.Fatal(o)
	}
	replay, replayIndex := f.commit(t, reserve)
	if replay.disposition != requestReplay || replay.index != index || replay.grant != first.grant {
		t.Fatal(replay)
	}
	current, found := f.read(t, replayIndex, outcomeKey(reserve))
	if !found || !bytes.Equal(current, original) {
		t.Fatal("replay changed original mapping")
	}
	// A different VALID command kind with the same RequestID is mismatch, not corruption.
	changed := transfer
	changed.id = reserve.id
	o, mismatchIndex := f.commit(t, changed)
	if o.reason != reasonMismatch || o.disposition != applied {
		t.Fatal(o)
	}
	current, _ = f.read(t, mismatchIndex, outcomeKey(reserve))
	if !bytes.Equal(current, original) {
		t.Fatal("cross-kind mismatch replaced original")
	}
	// A new request ID recovers the SAME persisted grant even with old epochs;
	// this is range inspection, never a new cursor grant or allocation.
	recover := reserve
	recover.id = requestID{10}
	o, _ = f.commit(t, recover)
	if o.disposition != grantRecovery || o.grant != first.grant || o.grantIndex != index {
		t.Fatal(o)
	}
	badCount := reserve
	badCount.id = requestID{11}
	badCount.count++
	if o, _ := f.commit(t, badCount); o.reason != reasonMismatch || o.grant != (idalloc.Grant{}) {
		t.Fatal(o)
	}
	stale := reserve
	stale.id = requestID{12}
	stale.sequence++
	if o, _ := f.commit(t, stale); o.reason != reasonStale {
		t.Fatal(o)
	}
	latest, _, _ := f.store.Checkpoint()
	old, _ := f.read(t, index, allocatorKey(f.n))
	now, _ := f.read(t, latest, allocatorKey(f.n))
	oldState, _ := idalloc.DecodeCheckpoint(old)
	newState, _ := idalloc.DecodeCheckpoint(now)
	oldView, _ := idalloc.Inspect(&oldState)
	newView, _ := idalloc.Inspect(&newState)
	if oldView.HighWater != 2 || newView.HighWater != 2 || oldView.Authority == newView.Authority {
		t.Fatal(oldView, newView)
	}
	cfg := f.config
	cfg.Create = false
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	f.store = reopened
	after, _ := f.read(t, latest, outcomeKey(reserve))
	if !bytes.Equal(after, original) {
		t.Fatal("reopen lost original request outcome")
	}
	after, _ = f.read(t, index, allocatorKey(f.n))
	if !bytes.Equal(after, old) {
		t.Fatal("historical allocator changed")
	}
	if err := reopened.ScrubApplication(t.Context()); err != nil {
		t.Fatal(err)
	}
}
func TestRecipientSequenceGapsServiceRotationAndForeignHome(t *testing.T) {
	f := initializedFixture(t)
	reserve := codecRequests(t)[3]
	reserve.sequence = 3
	first, _ := f.commit(t, reserve)
	if first.grant.Reservation.Request.Sequence != 1 {
		t.Fatal(first)
	}
	reserve.id = requestID{20}
	reserve.sequence = 99
	next, _ := f.commit(t, reserve)
	if next.grant.Reservation.Request.Sequence != 2 || next.grant.Reservation.First != 3 {
		t.Fatal(next)
	}
	// Replay activation under another RequestID must NOT reset the last sequence.
	activation := codecRequests(t)[2]
	activation.id = requestID{21}
	if o, _ := f.commit(t, activation); o.disposition != controlRecovery {
		t.Fatal(o)
	}
	index, _, _ := f.store.Checkpoint()
	stored, _ := f.read(t, index, recipientKey(f.n, activation.session.ID))
	record, err := decodeRecipient(stored, f.n)
	if err != nil || record.sequence != 99 {
		t.Fatal(record, err)
	}
	foreign := activation
	foreign.id = requestID{22}
	foreign.home++
	if o, _ := f.commit(t, foreign); o.reason != reasonInvalid {
		t.Fatal(o)
	}
	transfer := codecRequests(t)[1]
	f.commit(t, transfer)
	reserve.id = requestID{23}
	reserve.sequence = 100 // old service, same active recipient
	if o, _ := f.commit(t, reserve); o.reason != reasonStale {
		t.Fatal(o)
	}
	reserve.id = requestID{24}
	reserve.authority = transfer.replacement
	newGrant, _ := f.commit(t, reserve)
	if newGrant.reason != reasonNone || newGrant.grant.Reservation.Request.Sequence != 1 || newGrant.grant.Reservation.First != 5 {
		t.Fatal(newGrant)
	}
	cached := codecRequests(t)[3]
	cached.sequence = 3
	cached.id = requestID{25}
	if o, _ := f.commit(t, cached); o.disposition != grantRecovery || o.grant != first.grant {
		t.Fatal(o)
	}
}
func TestBootstrapRejectionsCannotPoisonEmptyProof(t *testing.T) {
	f := openFixture(t, vfs.NewMem())
	init := codecRequests(t)[0]
	wire, _ := encodeRequest(init, defaultLimits())
	malformed := owned(wire)
	binary.BigEndian.PutUint64(malformed[requestHeaderBytes+24:], 0)
	resign(malformed)
	for range 2 {
		e, err := f.stage(t, malformed, defaultLimits(), f.store.ApplicationLimits())
		if err != nil || !e.bootstrapRejection || len(e.writes) != 0 || e.outcome.reason != reasonInvalid {
			t.Fatal(e, err)
		}
		index := f.install(t, malformed, e)
		saved, err := f.store.ApplicationRecord(t.Context(), index, true, 1024)
		if err != nil || !bytes.Equal(saved, e.envelope) {
			t.Fatal(err)
		}
		view, err := f.store.ApplicationView(index)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := view.ProveNoApplicationData(t.Context()); err != nil {
			t.Fatal("rejection prevented legitimate init", err)
		}
		view.Close()
	}
	rejectedIndex, _, _ := f.store.Checkpoint()
	rejected, _ := f.store.ApplicationRecord(t.Context(), rejectedIndex, true, 1024)
	// Distinct later valid bootstrap attempt succeeds and retains its success mapping.
	init.attempt = bootstrapAttemptID{7}
	success, successIndex := f.commit(t, init)
	if success.reason != reasonNone {
		t.Fatal(success)
	}
	replay, _ := f.commit(t, init)
	if replay.disposition != requestReplay || replay.index != successIndex {
		t.Fatal(replay)
	}
	later := init
	later.authority = authority(t, 9, 1)
	if o, _ := f.commit(t, later); o.reason != reasonMismatch {
		t.Fatal(o)
	}
	old, err := f.store.ApplicationRecord(t.Context(), rejectedIndex, true, 1024)
	if err != nil || !bytes.Equal(old, rejected) {
		t.Fatal("later success overwrote bootstrap rejection", err)
	}
	oldMap, found := f.read(t, successIndex, outcomeKey(init))
	if !found {
		t.Fatal("success mapping missing")
	}
	now, _, _ := f.store.Checkpoint()
	current, _ := f.read(t, now, outcomeKey(init))
	if !bytes.Equal(oldMap, current) {
		t.Fatal("success mapping overwritten")
	}
}
func TestInitStageAndOutputLimitsAreRecoverableOrOperational(t *testing.T) {
	init := codecRequests(t)[0]
	data, _ := encodeRequest(init, defaultLimits())
	for _, dimension := range []string{"stage", "output"} {
		for _, delta := range []int{0, -1} {
			t.Run(dimension+string(rune('a'+delta+1)), func(t *testing.T) {
				f := openFixture(t, vfs.NewMem())
				base, err := f.stage(t, data, defaultLimits(), f.store.ApplicationLimits())
				if err != nil {
					t.Fatal(err)
				}
				l := defaultLimits()
				if dimension == "output" {
					l.outputBytes = outputCost(base) + delta
				} else {
					l.stageBytes = 128
					for _, w := range base.writes {
						l.stageBytes += 2*len(w.Key) + len(w.Value) + 64
					}
					l.stageBytes += delta
				}
				e, err := f.stage(t, data, l, f.store.ApplicationLimits())
				if err != nil {
					t.Fatal(err)
				}
				if delta == 0 {
					if e.outcome.reason != reasonNone {
						t.Fatal(e)
					}
				} else if e.outcome.reason != reasonLimit || !e.bootstrapRejection || len(e.writes) != 0 {
					t.Fatal(e)
				}
				f.install(t, data, e)
				if delta < 0 {
					init.attempt = bootstrapAttemptID{9}
					o, _ := f.commit(t, init)
					if o.reason != reasonNone {
						t.Fatal(o)
					}
				}
			})
		}
	}
	f := openFixture(t, vfs.NewMem())
	index, image, _ := f.store.Checkpoint()
	minimum := outcome{ns: f.n, kind: initAllocator, identity: init.identity(), hash: sha256.Sum256(data), index: index + 1, disposition: applied, reason: reasonInvalid}
	envelope, _ := encodeOutcome(minimum)
	l := defaultLimits()
	l.outputBytes = effectsMetadataBytes + len(image) + len(envelope) - 1
	if e, err := f.stage(t, data, l, f.store.ApplicationLimits()); !errors.Is(err, errLimit) || len(e.writes) != 0 {
		t.Fatal(e, err)
	}
}
func TestMissingStateProofAndOperationalBoundaries(t *testing.T) {
	f := openFixture(t, vfs.NewMem())
	init := codecRequests(t)[0]
	data, _ := encodeRequest(init, defaultLimits())
	view, err := f.store.ApplicationView(1)
	if err != nil {
		t.Fatal(err)
	}
	base, _ := view.Root()
	q := reader{ctx: t.Context(), view: view, base: base, limits: defaultLimits()}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	q.ctx = canceled
	if err := q.emptyProof(); !errors.Is(err, context.Canceled) || errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	q.ctx = t.Context()
	// Advance the application with an envelope-only no-op: old proof becomes stale.
	f.install(t, nil, allocationEffects{base: base})
	if err := q.emptyProof(); !errors.Is(err, raftlog.ErrInvalid) || errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if err := q.emptyProof(); !errors.Is(err, raftlog.ErrClosed) || errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	f.raw(t, []byte("hidden-data"), []byte{1}, false)
	if e, err := f.stage(t, data, defaultLimits(), f.store.ApplicationLimits()); !errors.Is(err, raftlog.ErrInvalid) || len(e.writes) != 0 {
		t.Fatal("hidden data initialized", e, err)
	}
	fresh := openFixture(t, vfs.NewMem())
	ordinary, _ := encodeRequest(codecRequests(t)[1], defaultLimits())
	if _, err := fresh.stage(t, ordinary, defaultLimits(), fresh.store.ApplicationLimits()); !errors.Is(err, errNotInitialized) {
		t.Fatal(err)
	}
	rootView, _ := fresh.store.ApplicationView(1)
	defer rootView.Close()
	pos := applyPosition{2, 0}
	p := fresh.store.ApplicationLimits()
	rl := fresh.store.Limits()
	for _, bad := range []struct {
		ctx  context.Context
		view *raftlog.ApplicationView
		n    namespace
		pos  applyPosition
	}{{nil, rootView, fresh.n, pos}, {t.Context(), nil, fresh.n, pos}, {t.Context(), rootView, namespace{}, pos}, {t.Context(), rootView, fresh.n, applyPosition{3, 0}}, {t.Context(), rootView, fresh.n, applyPosition{2, 1}}} {
		if _, err := stageAllocation(bad.ctx, bad.view, bad.n, bad.pos, data, defaultLimits(), p, rl); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
	}
	if _, err := stageAllocation(t.Context(), rootView, fresh.n, pos, bytes.Repeat([]byte{1}, maxRequestBytes+1), defaultLimits(), p, rl); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	if _, err := stageAllocation(canceled, rootView, fresh.n, pos, data, defaultLimits(), p, rl); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestAllocationOverflowAndRealInstallQuota(t *testing.T) {
	f := initializedFixture(t)
	reserve := codecRequests(t)[3]
	// A valid historical allocator checkpoint at the final high-water refuses
	// allocation before any unsigned addition can wrap to zero.
	state, _ := idalloc.NewState(f.n.graph, reserve.authority, 16)
	image, _ := idalloc.MarshalCheckpoint(&state)
	binary.BigEndian.PutUint64(image[45:53], math.MaxUint64)
	binary.BigEndian.PutUint32(image[117:], crc32.Checksum(image[:117], crc32.MakeTable(crc32.Castagnoli)))
	f.raw(t, allocatorKey(f.n), image, false)
	if o, _ := f.commit(t, reserve); o.reason != reasonExhausted || o.grant != (idalloc.Grant{}) {
		t.Fatal(o)
	}
	rotate := codecRequests(t)[2]
	rotate.id = requestID{88}
	rotate.expectedEpoch = math.MaxUint64
	rotate.session.Epoch = math.MaxUint64
	if o, _ := f.commit(t, rotate); o.reason != reasonExhausted {
		t.Fatal(o)
	}
	// Policy Preflight is NOT remaining-quota reservation: an accepted private
	// effect can still be denied by actual admission/installation.
	f = initializedFixture(t)
	data, _ := encodeRequest(reserve, defaultLimits())
	e, err := f.stage(t, data, defaultLimits(), f.store.ApplicationLimits())
	if err != nil {
		t.Fatal(err)
	}
	policy := f.store.ApplicationLimits()
	batch := effectBatch(e)
	_, err = policy.Preflight(batch, f.store.Limits())
	if err != nil {
		t.Fatal(err)
	}
	// Find the exact framing threshold by binary search, retaining valid policy.
	lo, hi := 2048, policy.MaxInstallBytes
	for lo < hi {
		mid := (lo + hi) / 2
		probe := policy
		probe.MaxInstallBytes = mid
		if _, err := probe.Preflight(batch, f.store.Limits()); err == nil {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	exact := policy
	exact.MaxInstallBytes = lo
	if _, err := exact.Preflight(batch, f.store.Limits()); err != nil {
		t.Fatal(err)
	}
	below := exact
	below.MaxInstallBytes--
	if _, err := below.Preflight(batch, f.store.Limits()); !errors.Is(err, raftlog.ErrLimit) {
		t.Fatal(err)
	}
	// Cached request maps must remain intact when a deterministic stage limit
	// rejects the NEW grant: only the rejection mapping may be returned.
	l := defaultLimits()
	l.stageRows = 3
	rejected, err := f.stage(t, data, l, policy)
	if err != nil || rejected.outcome.reason != reasonLimit || len(rejected.writes) != 1 || !bytes.Equal(rejected.writes[0].Key, outcomeKey(reserve)) {
		t.Fatal(rejected, err)
	}
	f.install(t, data, rejected)
	latest, _, _ := f.store.Checkpoint()
	b, _ := f.read(t, latest, allocatorKey(f.n))
	decoded, _ := idalloc.DecodeCheckpoint(b)
	metadata, _ := idalloc.Inspect(&decoded)
	if metadata.HighWater != 0 {
		t.Fatal("partial reservation escaped", metadata)
	}
}

func TestPurePreflightDoesNotReserveActualQuota(t *testing.T) {
	probe := initializedFixture(t)
	before, err := probe.store.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	reserve := codecRequests(t)[3]
	data, _ := encodeRequest(reserve, defaultLimits())
	for _, delta := range []uint64{0, 1} {
		policy := raftlog.DefaultApplicationPolicy(1)
		policy.MaxInstallWrites = 4
		policy.MaxInstallBytes = 8192
		policy.RetainedApplicationBytes = before.RetainedBytes + 2*8192 - delta
		f := openPolicyFixture(t, vfs.NewMem(), policy)
		f.commit(t, codecRequests(t)[0])
		f.commit(t, codecRequests(t)[2])
		usage, _ := f.store.ApplicationUsage()
		if usage.RetainedBytes != before.RetainedBytes {
			t.Fatal(usage, before)
		}
		e, err := f.stage(t, data, defaultLimits(), policy)
		if err != nil {
			t.Fatal("pure preflight unexpectedly used remaining quota", err)
		}
		err = f.store.AdmitApplication(len(data))
		if delta == 0 {
			if err != nil {
				t.Fatal(err)
			}
			f.install(t, data, e)
		} else {
			if !errors.Is(err, raftlog.ErrLimit) {
				t.Fatal(err)
			}
			after, _ := f.store.ApplicationUsage()
			if after != usage {
				t.Fatal("denied admission mutated durable state", usage, after)
			}
		}
	}
}

func TestControlRejectionsNeverChangeAllocatorOrRecipient(t *testing.T) {
	f := openFixture(t, vfs.NewMem())
	f.commit(t, codecRequests(t)[0])
	reserve := codecRequests(t)[3]
	assertControlRejection(t, f, reserve, reasonStale)
	activate := codecRequests(t)[2]
	f.commit(t, activate)
	invalidTransfer := codecRequests(t)[1]
	invalidTransfer.id = requestID{30}
	invalidTransfer.replacement = authority(t, 5, 5)
	assertControlRejection(t, f, invalidTransfer, reasonInvalid)
	staleTransfer := invalidTransfer
	staleTransfer.id = requestID{31}
	staleTransfer.authority = authority(t, 9, 1)
	staleTransfer.replacement = authority(t, 10, 2)
	assertControlRejection(t, f, staleTransfer, reasonStale)
	tooLarge := reserve
	tooLarge.id = requestID{32}
	tooLarge.count = 17
	assertControlRejection(t, f, tooLarge, reasonInvalid)
	badEpoch := activate
	badEpoch.id = requestID{33}
	badEpoch.expectedEpoch = 2
	badEpoch.session.Epoch = 3
	assertControlRejection(t, f, badEpoch, reasonStale)
	badEpoch.id = requestID{34}
	badEpoch.expectedEpoch = 1
	badEpoch.session.Epoch = 8
	assertControlRejection(t, f, badEpoch, reasonInvalid)
	reserve.id = requestID{35}
	reserve.sequence = 99
	f.commit(t, reserve)
	abandoned := reserve
	abandoned.id = requestID{36}
	abandoned.sequence = 50
	assertControlRejection(t, f, abandoned, reasonMismatch)
	transfer := codecRequests(t)[1]
	f.commit(t, transfer)
	transfer.id = requestID{37}
	if o, _ := f.commit(t, transfer); o.disposition != controlRecovery {
		t.Fatal(o)
	}
	for _, entry := range []struct {
		sequence uint64
		epoch    uint64
	}{{math.MaxUint64, 1}, {0, math.MaxUint64}} {
		a := authority(t, 2, entry.epoch)
		state, _ := idalloc.NewState(f.n.graph, a, 16)
		image, _ := idalloc.MarshalCheckpoint(&state)
		if entry.sequence != 0 {
			r := idalloc.Request{Graph: f.n.graph, Authority: a, Sequence: entry.sequence, Count: 1}
			binary.BigEndian.PutUint64(image[45:53], math.MaxUint64)
			binary.BigEndian.PutUint64(image[61:69], entry.sequence)
			binary.BigEndian.PutUint64(image[69:77], 1)
			binary.BigEndian.PutUint64(image[77:85], math.MaxUint64)
			hash := r.Digest()
			copy(image[85:117], hash[:])
			binary.BigEndian.PutUint32(image[117:], crc32.Checksum(image[:117], crc32.MakeTable(crc32.Castagnoli)))
		}
		f.raw(t, allocatorKey(f.n), image, false)
		if entry.sequence != 0 {
			r := reserve
			r.id = requestID{40}
			r.sequence = 100
			r.authority = a
			assertControlRejection(t, f, r, reasonExhausted)
		} else {
			r := transfer
			r.id = requestID{41}
			r.authority = a
			r.replacement = authority(t, 8, math.MaxUint64)
			assertControlRejection(t, f, r, reasonExhausted)
		}
	}
}
func TestStoredBindingsAndReadBudgetsFailClosed(t *testing.T) {
	for _, kind := range []string{"allocator", "recipient", "grant", "dedup", "tombstone"} {
		t.Run(kind, func(t *testing.T) {
			f := initializedFixture(t)
			r := codecRequests(t)[3]
			switch kind {
			case "allocator":
				f.raw(t, allocatorKey(f.n), []byte{1}, false)
			case "recipient":
				record := recipientRecord{session: codecSession(), home: 7, index: 2}
				record.session.ID[0] = 8
				wire, _ := encodeRecipient(f.n, record)
				f.raw(t, recipientKey(f.n, r.session.ID), wire, false)
			case "grant":
				g := grantRecord{grant: codecGrant(t), index: math.MaxUint64}
				wire, _ := encodeGrant(f.n, g)
				f.raw(t, grantKey(f.n, r.session, r.sequence), wire, false)
			case "dedup":
				f.raw(t, outcomeKey(r), []byte{1}, false)
			case "tombstone":
				f.raw(t, allocatorKey(f.n), nil, true)
			}
			data, _ := encodeRequest(r, defaultLimits())
			if e, err := f.stage(t, data, defaultLimits(), f.store.ApplicationLimits()); !errors.Is(err, errCorrupt) || len(e.writes) != 0 {
				t.Fatal(e, err)
			}
		})
	}
	f := initializedFixture(t)
	index, _, _ := f.store.Checkpoint()
	view, _ := f.store.ApplicationView(index)
	defer view.Close()
	base, _ := view.Root()
	key := allocatorKey(f.n)
	q := reader{ctx: t.Context(), view: view, ns: f.n, base: base, limits: defaultLimits()}
	q.limits.readBytes = len(key) + 64 + idalloc.CheckpointSize - 1
	if _, _, err := q.get(key); !errors.Is(err, errLimit) || !errors.Is(err, raftlog.ErrLimit) {
		t.Fatal(err)
	}
	q = reader{ctx: t.Context(), view: view, ns: f.n, base: base, limits: defaultLimits()}
	q.limits.readRows = 1
	if _, _, err := q.get(key); err != nil {
		t.Fatal(err)
	}
	if _, _, err := q.get(key); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	q.ctx = ctx
	if _, _, err := q.get(key); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	r := codecRequests(t)[3]
	data, _ := encodeRequest(r, defaultLimits())
	l := defaultLimits()
	l.readRows = 1
	if e, err := f.stage(t, data, l, f.store.ApplicationLimits()); !errors.Is(err, errLimit) || len(e.writes) != 0 {
		t.Fatal(e, err)
	}
	// Remaining output/installation policy is checked with both sentinel layers.
	e, err := f.stage(t, data, defaultLimits(), f.store.ApplicationLimits())
	if err != nil {
		t.Fatal(err)
	}
	p := f.store.ApplicationLimits()
	p.MaxInstallWrites = 1
	if err := checkEffects(e, defaultLimits(), p, f.store.Limits()); !errors.Is(err, errLimit) || !errors.Is(err, raftlog.ErrLimit) {
		t.Fatal(err)
	}
	p = raftlog.ApplicationPolicy{}
	if err := checkEffects(e, defaultLimits(), p, f.store.Limits()); !errors.Is(err, errInvalid) || !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestStageAndProofInputsCannotInventFreshness(t *testing.T) {
	f := openFixture(t, vfs.NewMem())
	view, _ := f.store.ApplicationView(1)
	defer view.Close()
	base, _ := view.Root()
	q := reader{ctx: t.Context(), view: view, base: base, limits: defaultLimits()}
	q.base.Generation++
	if err := q.emptyProof(); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	q.base = base
	q.limits.readBytes = len(base.Image) - 1
	if err := q.emptyProof(); !errors.Is(err, errLimit) || errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	init := codecRequests(t)[0]
	data, _ := encodeRequest(init, defaultLimits())
	p := f.store.ApplicationLimits()
	rl := f.store.Limits()
	pos := applyPosition{2, base.Generation}
	if _, err := stageAllocation(t.Context(), view, f.n, pos, data, limits{}, p, rl); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := stageAllocation(t.Context(), view, f.n, pos, data, defaultLimits(), raftlog.ApplicationPolicy{}, rl); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	l := defaultLimits()
	l.readBytes = len(base.Image) - 1
	if _, err := stageAllocation(t.Context(), view, f.n, pos, data, l, p, rl); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	foreign := codecRequests(t)[1]
	foreign.ns.graph[0]++
	wire, _ := encodeRequest(foreign, defaultLimits())
	if _, err := stageAllocation(t.Context(), view, f.n, pos, wire, defaultLimits(), p, rl); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	ordinary, _ := encodeRequest(codecRequests(t)[1], defaultLimits())
	ordinary[4] = 99
	resign(ordinary)
	if _, err := stageAllocation(t.Context(), view, f.n, pos, ordinary, defaultLimits(), p, rl); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	// Known valid bootstrap bytes above ONLY the input limit get a fixed, bounded
	// envelope and no KV, keeping later legitimate initialization possible.
	l = defaultLimits()
	l.inputBytes = len(data) - 1
	e, err := stageAllocation(t.Context(), view, f.n, pos, data, l, p, rl)
	if err != nil || !e.bootstrapRejection || len(e.writes) != 0 || e.outcome.reason != reasonLimit {
		t.Fatal(e, err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := stageAllocation(t.Context(), view, f.n, pos, data, defaultLimits(), p, rl); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
}

func assertControlRejection(t *testing.T, f *allocationFixture, r request, want reason) {
	t.Helper()
	beforeIndex, _, err := f.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	keys := [][]byte{allocatorKey(f.n), recipientKey(f.n, codecSession().ID)}
	type value struct {
		bytes []byte
		found bool
	}
	before := make([]value, len(keys))
	for i, key := range keys {
		before[i].bytes, before[i].found = f.read(t, beforeIndex, key)
	}
	o, afterIndex := f.commit(t, r)
	if o.reason != want {
		t.Fatal(o, want)
	}
	for i, key := range keys {
		after, found := f.read(t, afterIndex, key)
		if found != before[i].found || !bytes.Equal(after, before[i].bytes) {
			t.Fatalf("rejected%v changed allocator/recipient key%x beforeFound%v afterFound%v", r.kind, key, before[i].found, found)
		}
	}
}
func TestOutputLedgerCoversTypedResultAndBuffers(t *testing.T) {
	f := initializedFixture(t)
	r := codecRequests(t)[3]
	data, _ := encodeRequest(r, defaultLimits())
	e, err := f.stage(t, data, defaultLimits(), f.store.ApplicationLimits())
	if err != nil {
		t.Fatal(err)
	}
	// Independent representation-size evidence: do not merely restate the
	// ledger's arithmetic. The typed result contains a decoded Grant in addition
	// to the encoded outcome and every KV buffer/backing-array slot.
	if unsafe.Sizeof(e) > effectsMetadataBytes {
		t.Fatal("fixed metadata allowance too small", unsafe.Sizeof(e))
	}
	if unsafe.Sizeof(raftlog.KV{}) > 64 {
		t.Fatal("KV slot allowance too small", unsafe.Sizeof(raftlog.KV{}))
	}
	actual := int(unsafe.Sizeof(e)) + cap(e.base.Image) + cap(e.envelope) + cap(e.changes) + cap(e.writes)*int(unsafe.Sizeof(raftlog.KV{}))
	for _, w := range e.writes {
		actual += cap(w.Key) + cap(w.Value)
	}
	if outputCost(e) < actual {
		t.Fatal("decoded outcome/fixed metadata not charged", outputCost(e), actual)
	}
}
