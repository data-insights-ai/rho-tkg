package raftlog

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"testing"
)

func TestApplicationRecoveryPooledCandidateBoundAndExactView(t *testing.T) {
	p := DefaultApplicationPolicy(1)
	b := ApplicationBatch{Image: []byte("root"), Outcome: []byte("outcome"), Writes: []KV{{Key: []byte{0, 1, 0}, Value: []byte("before")}}, ControlPuts: []ApplicationControlPut{{Key: []byte{0, 2, 0}, Value: []byte("control")}}}
	m := recoveryPoolFixture()
	for i := range maxApplicationTerminalOwners {
		m.Terminal = append(m.Terminal, ApplicationRecoveryReservation{ID: [32]byte{byte(i + 1)}, Digest: [32]byte{42}, Credits: []ApplicationRecoveryCredit{{1, 128, 8192, 16}, {2, 128, 8192, 16}, {3, 128, 8192, 16}}})
	}
	actual, records, err := p.measureNativeRecovery(b, m)
	if err != nil {
		t.Fatal(err)
	}
	bound, maximum, work, err := p.PooledRecoveryBatchBound(b, 0)
	if err != nil || bound < actual || maximum < records || work < 2*bound {
		t.Fatal("complete roots/control/key-escape bound", actual, bound, records, maximum, work, err)
	}
	more, _, _, err := p.PooledRecoveryBatchBound(b, 17)
	if err != nil || more != bound+17 {
		t.Fatal("padding discounted/reset candidate", err)
	}
	if _, _, _, err = p.PooledRecoveryBatchBound(b, math.MaxUint64); !errors.Is(err, ErrLimit) {
		t.Fatal("overflow", err)
	}
	bad := b
	bad.Writes = []KV{{Key: nil}}
	if _, _, _, err = p.PooledRecoveryBatchBound(bad, 0); !errors.Is(err, ErrInvalid) {
		t.Fatal("malformed key", err)
	}
	var absent *ApplicationView
	if _, _, err = absent.RecoveryStepBudget(t.Context(), ApplicationRecoveryUse{}, ReadBudget{2, 1 << 20}); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil view", err)
	}
	s, _, v := recoveryPoolStore(t)
	spanRecoveryInstall(t, s, v, "register")
	view, err := s.ApplicationView(s.meta.Applied)
	if err != nil {
		t.Fatal(err)
	}
	limits := view.ReadLimits()
	step := ApplicationRecoveryUse{ID: v.reserve.ID, Digest: v.reserve.Digest, CommandDigest: [32]byte{1}, Phase: 100}
	got, _, err := view.RecoveryStepBudget(t.Context(), step, limits)
	if err != nil || got.Pool != v.reserve.Spans[0].BytePool || got.Maximum.Phase != 100 {
		t.Fatal("exact native pool", got, err)
	}
	spanRecoveryInstall(t, s, v, "step")
	old, _, err := view.RecoveryStepBudget(t.Context(), step, limits)
	if err != nil || old != got {
		t.Fatal("held old pool changed after actual debit", old, got, err)
	}
	current, err := s.ApplicationView(s.meta.Applied)
	if err != nil {
		t.Fatal(err)
	}
	currentUse := step
	currentUse.Phase++
	now, _, err := current.RecoveryStepBudget(t.Context(), currentUse, current.ReadLimits())
	if err != nil || now.Pool.RetainedRemaining >= got.Pool.RetainedRemaining || now.Pool.LogRemaining >= got.Pool.LogRemaining || now.Maximum.Phase != 101 {
		t.Fatal("current pool did not debit", now, err)
	}
	if err = current.Close(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*ApplicationRecoveryUse){func(u *ApplicationRecoveryUse) { u.ID[0]++ }, func(u *ApplicationRecoveryUse) { u.Digest[0]++ }, func(u *ApplicationRecoveryUse) { u.CommandDigest = [32]byte{} }, func(u *ApplicationRecoveryUse) { u.Phase++ }, func(u *ApplicationRecoveryUse) { u.RetireBefore = u.Phase }} {
		bad := step
		mutate(&bad)
		if _, _, err := view.RecoveryStepBudget(t.Context(), bad, limits); !errors.Is(err, ErrInvalid) {
			t.Fatal("forged owner/phase/hash/terminal", err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err = view.RecoveryStepBudget(ctx, step, limits); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled", err)
	}
	view.retention.RecoveryFloor = view.index + 1
	if _, _, err = view.RecoveryStepBudget(t.Context(), step, limits); !errors.Is(err, ErrApplicationExpired) {
		t.Fatal("expired", err)
	}
	view.retention.RecoveryFloor = 0
	if err = view.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err = view.RecoveryStepBudget(t.Context(), step, limits); !errors.Is(err, ErrClosed) {
		t.Fatal("closed", err)
	}
	if err = s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	export, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	manifest := buildPublished(t, export, ReadBudget{1, 8192})
	chunks := collectPublished(t, export, ReadBudget{1, 8192})
	if err = export.Close(); err != nil {
		t.Fatal(err)
	}
	receiver, _, _ := recoveryPoolStore(t)
	prepared, _ := semanticPrepared(t, receiver, manifest, chunks)
	read, err := prepared.OpenReadView(t.Context(), manifest.Index, 172)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err = read.RecoveryStepBudget(t.Context(), currentUse, read.ReadLimits())
	if err != nil || got.Pool != now.Pool {
		t.Fatal("AS9 prepared pool", got, err)
	}
	if err = read.Close(); err != nil {
		t.Fatal(err)
	}
	if err = prepared.Close(); err != nil {
		t.Fatal(err)
	}
}

func recoveryPoolFixture() applicationRecoveryMetadata {
	return applicationRecoveryMetadata{Config: ApplicationRecoveryConfig{Version: 4}, Reservation: ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Spans: []ApplicationRecoverySpan{
		{First: 1, Next: 1, Last: 2, CommandBytes: 1, RetainedBytes: 80, RetainedRecords: 2, BytePool: ApplicationRecoveryBytePool{100, 100, 168, 168}},
		{First: 10, Next: 10, Last: 10, CommandBytes: 1, RetainedBytes: 80, RetainedRecords: 2},
	}}}
}

func TestApplicationRecoveryBytePoolProtectedWindowAndReplayBacking(t *testing.T) {
	p, l := DefaultApplicationPolicy(1), DefaultLimits()
	// Formula fixture, not an opened policy: exact byte amounts isolate the
	// window from unrelated codec and minimum-install policy requirements.
	p.MaxInstallBytes, p.MaxInstallWrites, p.MaxTailEntries = 80, 1, 2
	p.RetainedApplicationBytes = 340 // pool100 + terminal80 + protected window160.
	m := metadata{Applied: 1, Last: 1}
	m.App.Policy, m.Recovery = p, recoveryPoolFixture()
	queue := func(index uint64, phase uint32) applicationRecoveryQueued {
		u := recoveryPoolUse(phase)
		return applicationRecoveryQueued{Index: index, CommandDigest: u.CommandDigest, Purpose: applicationRecoveryQueuedPurpose{Use: &u}, Credit: ApplicationRecoveryCredit{Phase: phase, CommandBytes: 1, RetainedBytes: 80, RetainedRecords: 2}, ActualLogBytes: 84}
	}
	for _, queued := range [][]applicationRecoveryQueued{{}, {queue(2, 1)}, {queue(2, 1), queue(3, 2)}} {
		m.Last, m.Recovery.Queued = 1+uint64(len(queued)), queued // #nosec G115 -- at most2 fixed entries.
		g, pending, _, err := recoveryProjected(m, l, uint64(len(queued)))
		if err != nil || g.bytes+pending != 340 {
			t.Fatalf("window must be charged ONCE across a multi-entry Ready: %+v pending%d %v", g, pending, err)
		}
		if g.logBytes+m.Last*84 != 336 { // phase logs168 + terminal84 + fixed initial84.
			t.Fatalf("actual pending log must replace only its own pool charge: %+v", g)
		}
	}
	// A duplicate phase and an ordinary pending entry cannot borrow the window:
	// neither spends the pool at installation. Unknown/reclassified SAME entries
	// remain repeatable projection, never a durable debit or a credit refill.
	for _, queued := range [][]applicationRecoveryQueued{{queue(2, 1), queue(3, 1)}, {queue(2, 1)}} {
		m.Last, m.Recovery.Queued = 3, queued
		before := cloneRecoveryReservation(m.Recovery.Reservation)
		for range 2 {
			g, pending, _, err := recoveryProjected(m, l, 2)
			if err != nil || g.bytes+pending != 420 || !sameRecoveryReservation(before, m.Recovery.Reservation) {
				t.Fatalf("ordinary/replay spent pending window or changed durable totals: %+v %d %v", g, pending, err)
			}
			if err := recoveryFits(g, pending, 0, 0, 0, p, l); !errors.Is(err, ErrLimit) {
				t.Fatalf("unprotected ordinary traffic admitted near quota: %v", err)
			}
		}
	}
	spent, _, err := m.Recovery.consumeMeasured(recoveryPoolUse(1), 10, 84)
	if err != nil {
		t.Fatal(err)
	}
	spent, _, err = spent.consumeMeasured(recoveryPoolUse(2), 70, 84)
	if err != nil {
		t.Fatal(err)
	}
	terminal := queue(2, 10)
	terminal.Purpose.Use.RetireBefore = 10
	m.Last, m.Recovery = 2, spent
	m.Recovery.Queued = []applicationRecoveryQueued{terminal}
	g, pending, _, err := recoveryProjected(m, l, 1)
	if err != nil || g.bytes != 20+160 || pending != 80 {
		t.Fatalf("queued terminal released unspent pool/window: %+v %d %v", g, pending, err)
	}
	if !terminalRecoveryUseFor(4, terminal, spent.Reservation) {
		t.Fatal("fresh terminal dependency lost exact pooled owner")
	}
	m.Last, m.Recovery.Queued = 2, []applicationRecoveryQueued{queue(2, 1)}
	forged := m.Recovery.Queued[0]
	forged.ActualLogBytes = 85 // genuine one-byte command has a maximum84.
	if _, _, err := advanceRecoveryQueued(m.Recovery, forged, p, l); !errors.Is(err, ErrInvalid) {
		t.Fatalf("forged actual log backing: %v", err)
	}
	wire := appendRecoveryQueue(nil, m.Recovery.Queued, 4)
	decoded, tail, err := decodeRecoveryQueue(wire, p, l, 4)
	if err != nil || len(tail) != 0 || len(decoded) != 1 || decoded[0].ActualLogBytes != 84 || decoded[0].Index != 2 || decoded[0].CommandDigest != forged.CommandDigest {
		t.Fatalf("RQ4 lost original index/hash/actual backing: %v", err)
	}
	if _, _, err := decodeRecoveryQueue(wire, p, l, 3); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("RQ4 accepted as RQ3: %v", err)
	}
	p.MaxTailEntries = math.MaxUint64
	if _, err := recoveryPoolPendingWindow(m.Recovery.Reservation, p, 0); !errors.Is(err, ErrLimit) {
		t.Fatalf("pending-window multiplication overflow: %v", err)
	}
}
func recoveryPoolUse(phase uint32) ApplicationRecoveryUse {
	return ApplicationRecoveryUse{ID: [32]byte{1}, Digest: [32]byte{2}, CommandDigest: [32]byte{3}, Phase: phase}
}

func TestApplicationRecoveryBytePoolQueueActualDebitsAndTerminalSurplus(t *testing.T) {
	p, l := DefaultApplicationPolicy(1), DefaultLimits()
	for _, actual := range [][2]uint64{{10, 70}, {80, 20}} {
		m := recoveryPoolFixture()
		shadow := m
		for i := range 2 {
			u := recoveryPoolUse(uint32(i + 1)) // #nosec G115 -- fixed1..2 test phases.
			q := applicationRecoveryQueued{Index: uint64(i + 1), CommandDigest: u.CommandDigest, Purpose: applicationRecoveryQueuedPurpose{Use: &u}, Credit: ApplicationRecoveryCredit{Phase: u.Phase, CommandBytes: 1, RetainedBytes: 80, RetainedRecords: 2}, ActualLogBytes: 84}
			var err error
			shadow, _, err = advanceRecoveryQueued(shadow, q, p, l)
			if err != nil || shadow.Reservation.Spans[0].BytePool.RetainedRemaining != 100 {
				t.Fatalf("queue must retain the whole actual pool: %+v %v", shadow, err)
			}
		}
		for i, n := range actual {
			var err error
			m, _, err = m.consumeMeasured(recoveryPoolUse(uint32(i+1)), n, 84) // #nosec G115 -- fixed1..2 phases.
			if err != nil {
				t.Fatalf("actual %v step%d: %v", actual, i, err)
			}
		}
		span := m.Reservation.Spans[0]
		if !span.Exhausted || span.BytePool.RetainedRemaining != 100-actual[0]-actual[1] || span.BytePool.LogRemaining != 0 {
			t.Fatalf("wrong actual pool: %+v", span)
		}
		if _, err := m.credit(recoveryPoolUse(2)); !errors.Is(err, ErrInvalid) {
			t.Fatalf("exhausted phase: %v", err)
		}
		growth, err := recoveryGrowth(m.Reservation, p, l)
		if err != nil || growth.bytes != span.BytePool.RetainedRemaining+80 || growth.records != 2 || growth.logRecords != 1 {
			t.Fatalf("surplus must stay protected: %+v %v", growth, err)
		}
		terminal := recoveryPoolUse(10)
		terminal.RetireBefore = 10
		m, _, err = m.consumeMeasured(terminal, 1, 84)
		if err != nil || m.Reservation.ID != ([32]byte{}) {
			t.Fatalf("actual terminal must release unused pool: %+v %v", m, err)
		}
	}
	before := recoveryPoolFixture()
	m, _, err := before.consumeMeasured(recoveryPoolUse(1), 80, 84)
	if err != nil {
		t.Fatal(err)
	}
	unchanged := cloneRecoveryReservation(m.Reservation)
	if _, _, err = m.consumeMeasured(recoveryPoolUse(2), 21, 84); !errors.Is(err, ErrLimit) || !sameRecoveryReservation(unchanged, m.Reservation) {
		t.Fatalf("over-pool must refuse without effect: %v", err)
	}
	forged := recoveryPoolUse(1)
	forged.ID[0]++
	if _, _, err = before.consumeMeasured(forged, 1, 84); !errors.Is(err, ErrInvalid) {
		t.Fatalf("forged owner: %v", err)
	}
	pooledTerminal := before
	pooledTerminal.Reservation = cloneRecoveryReservation(before.Reservation)
	pooledTerminal.Reservation.Spans[1].BytePool = ApplicationRecoveryBytePool{80, 80, 84, 84}
	terminal := recoveryPoolUse(10)
	terminal.RetireBefore = 10
	shape, _, err := pooledTerminal.consume(terminal)
	if err != nil {
		t.Fatal(err)
	}
	installed, _, err := pooledTerminal.consumeMeasured(terminal, 20, 84)
	if err != nil || nativeRecoveryRootBytes(shape) != nativeRecoveryRootBytes(installed) || installed.Reservation.ID != ([32]byte{}) {
		t.Fatalf("pooled terminal measurement used a different prospective root: %v", err)
	}
}

func TestApplicationRecoveryBytePoolCodecBoundsAndOldSpanIsolation(t *testing.T) {
	p, l := DefaultApplicationPolicy(1), DefaultLimits()
	r := recoveryPoolFixture().Reservation
	wire := appendRecoveryPoolRoot(r)
	if len(wire) != 72+2*72 {
		t.Fatalf("wrong fixed root size%d", len(wire))
	}
	decoded, n, err := readRecoveryPoolRoot(wire, p, l)
	if err != nil || n != len(wire) || !sameRecoveryReservation(r, decoded) {
		t.Fatalf("pool root roundtrip: %v", err)
	}
	wire[72+40] ^= 1
	if !sameRecoveryReservation(r, decoded) {
		t.Fatal("decode borrowed caller backing")
	}
	for _, mutate := range []func([]byte){
		func(b []byte) { binary.BigEndian.PutUint32(b[68:72], 9) },
		func(b []byte) { binary.BigEndian.PutUint64(b[104:112], 4) },
		func(b []byte) { binary.BigEndian.PutUint64(b[120:128], 101) },
	} {
		bad := appendRecoveryPoolRoot(r)
		mutate(bad)
		if _, _, err = readRecoveryPoolRoot(bad, p, l); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("forged pool codec: %v", err)
		}
	}
	if _, _, err = readRecoveryPoolRoot(nil, p, l); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("nil codec: %v", err)
	}
	if err = recoverySpanProfile(ApplicationRecoveryConfig{Version: 3}, r); !errors.Is(err, ErrInvalid) {
		t.Fatalf("legacy profile accepted byte pool: %v", err)
	}
	old := recoverySpanFixture()
	oldWire := appendRecoverySpanRoot(old)
	if _, _, err = readRecoveryPoolRoot(oldWire, p, l); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("RS3 relabelled: %v", err)
	}
	if !bytes.Equal(oldWire, appendRecoverySpanRoot(old)) {
		t.Fatal("legacy span changed")
	}
	bad := r
	bad.Spans = slices.Clone(r.Spans)
	bad.Spans[0].BytePool.RetainedLimit = 0
	if _, err = recoveryGrowth(bad, p, l); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero pool bound: %v", err)
	}
	for _, mutate := range []func(*ApplicationRecoveryReservation){
		func(r *ApplicationRecoveryReservation) { r.Spans[0].Next++ },
		func(r *ApplicationRecoveryReservation) { r.Spans[1].First = r.Spans[0].Last },
		func(r *ApplicationRecoveryReservation) { r.Spans[0].BytePool.RetainedRemaining-- },
	} {
		candidate := cloneRecoveryReservation(r)
		mutate(&candidate)
		if _, err := (applicationRecoveryMetadata{Config: ApplicationRecoveryConfig{4}}).reserve(candidate, p, l); !errors.Is(err, ErrInvalid) {
			t.Fatalf("fresh reservation admitted partial/overlapping/refilled bands: %v", err)
		}
	}
}
