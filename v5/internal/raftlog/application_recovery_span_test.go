package raftlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"reflect"
	"testing"
)

func recoverySpanFixture() ApplicationRecoveryReservation {
	return ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Spans: []ApplicationRecoverySpan{
		{First: 100, Next: 100, Last: 102, CommandBytes: 10, RetainedBytes: 100, RetainedRecords: 4},
		{First: 200, Next: 200, Last: 202, CommandBytes: 20, RetainedBytes: 200, RetainedRecords: 8},
		{First: 300, Next: 300, Last: 300, CommandBytes: 30, RetainedBytes: 300, RetainedRecords: 12},
	}}
}
func TestApplicationRecoverySpansExactNextGrowthAndRetirement(t *testing.T) {
	p, l := DefaultApplicationPolicy(1), DefaultLimits()
	r := recoverySpanFixture()
	g, err := recoveryGrowth(r, p, l)
	// Seven separate log entries, each with74-byte framing and9-byte key.
	if err != nil || g != (applicationRecoveryGrowth{1200, 48, 701, 7}) {
		t.Fatal(g, err)
	}
	for _, mutate := range []func(*applicationRecoveryGrowth){func(g *applicationRecoveryGrowth) { g.bytes = p.RetainedApplicationBytes + 1 }, func(g *applicationRecoveryGrowth) { g.records = p.RetainedApplicationRecords + 1 }, func(g *applicationRecoveryGrowth) { g.logBytes = l.MaxRetainedBytes + 1 }, func(g *applicationRecoveryGrowth) { g.logRecords = l.MaxRetainedEntries + 1 }} {
		bad := g
		mutate(&bad)
		if !errors.Is(recoveryFits(bad, 0, 0, 0, 0, p, l), ErrLimit) {
			t.Fatal("missing aggregate quota", bad)
		}
	}
	for _, n := range []uint64{1, 2, math.MaxUint64} {
		if _, err := checkedRecoveryMul(math.MaxUint64, n); n > 1 && !errors.Is(err, ErrLimit) || n == 1 && err != nil {
			t.Fatal(n, err)
		}
	}
	for _, mutate := range []func(*ApplicationRecoveryReservation){
		func(r *ApplicationRecoveryReservation) { r.ID = [32]byte{} }, func(r *ApplicationRecoveryReservation) { r.Digest = [32]byte{} },
		func(r *ApplicationRecoveryReservation) { r.Spans[0].First = 0 }, func(r *ApplicationRecoveryReservation) { r.Spans[0].Next = 99 }, func(r *ApplicationRecoveryReservation) { r.Spans[0].Next = 103 },
		func(r *ApplicationRecoveryReservation) { r.Spans[1].First = 102 }, func(r *ApplicationRecoveryReservation) { r.Spans[0].RetainedBytes = 0 }, func(r *ApplicationRecoveryReservation) { r.Spans[0].RetainedRecords = 0 },
		func(r *ApplicationRecoveryReservation) { r.Spans[0].CommandBytes = -1 }, func(r *ApplicationRecoveryReservation) { r.Credits = []ApplicationRecoveryCredit{{Phase: 1}} },
		func(r *ApplicationRecoveryReservation) { r.Spans = append(r.Spans, r.Spans...) },
	} {
		bad := cloneRecoveryReservation(r)
		mutate(&bad)
		if _, err := recoveryGrowth(bad, p, l); !errors.Is(err, ErrInvalid) {
			t.Fatal(bad, err)
		}
	}
	overflow := cloneRecoveryReservation(r)
	overflow.Spans = overflow.Spans[:1]
	overflow.Spans[0].Last = math.MaxUint32
	overflow.Spans[0].RetainedBytes = 1 << 40
	large := p
	large.RetainedApplicationBytes = 1 << 40
	if _, err := recoveryGrowth(overflow, large, l); !errors.Is(err, ErrLimit) {
		t.Fatal("aggregate multiplication overflow", err)
	}
	m, err := (applicationRecoveryMetadata{Config: ApplicationRecoveryConfig{3}}).reserve(r, p, l)
	if err != nil {
		t.Fatal(err)
	}
	r.Spans[0].RetainedBytes++
	if m.Reservation.Spans[0].RetainedBytes != 100 {
		t.Fatal("caller span alias")
	}
	u := ApplicationRecoveryUse{ID: m.Reservation.ID, Digest: m.Reservation.Digest, Phase: 100, CommandDigest: sha256.Sum256([]byte("actual"))}
	for _, mutate := range []func(*ApplicationRecoveryUse){func(u *ApplicationRecoveryUse) { u.ID[0]++ }, func(u *ApplicationRecoveryUse) { u.Digest[0]++ }, func(u *ApplicationRecoveryUse) { u.Phase++ }, func(u *ApplicationRecoveryUse) { u.RetireBefore = u.Phase }} {
		bad := u
		mutate(&bad)
		if _, err := m.credit(bad); !errors.Is(err, ErrInvalid) {
			t.Fatal(bad, err)
		}
	}
	next, credit, err := m.consume(u)
	if err != nil || credit.Phase != 100 || next.Reservation.Spans[0].Next != 101 || m.Reservation.Spans[0].Next != 100 {
		t.Fatal(next, credit, err)
	}
	if _, _, err := next.consume(u); !errors.Is(err, ErrInvalid) {
		t.Fatal("duplicate actual spend", err)
	}
	terminal := u
	terminal.Phase = 300
	terminal.RetireBefore = 300
	q := applicationRecoveryQueued{Index: 9, CommandDigest: terminal.CommandDigest, Purpose: applicationRecoveryQueuedPurpose{Use: &terminal}, Credit: ApplicationRecoveryCredit{300, 30, 300, 12}}
	projected, _, err := advanceRecoveryQueued(next, q, p, l)
	if err != nil || len(projected.Reservation.Spans) != 2 {
		t.Fatal("queued terminal retired unused suffix", projected, err)
	}
	installed, _, err := next.consume(terminal)
	if err != nil || recoveryEntries(installed.Reservation) != 0 {
		t.Fatal("actual terminal left suffix", installed, err)
	}
	for _, version := range []uint32{0, 1, 2} {
		if _, err := (applicationRecoveryMetadata{Config: ApplicationRecoveryConfig{version}}).reserve(recoverySpanFixture(), p, l); !errors.Is(err, ErrInvalid) {
			t.Fatal("mixed old profile", version, err)
		}
	}
}

func TestApplicationRecoverySpansQueueCodecOwnershipAndLegacyBytes(t *testing.T) {
	p, l := DefaultApplicationPolicy(1), DefaultLimits()
	r := recoverySpanFixture()
	m := applicationRecoveryMetadata{Config: ApplicationRecoveryConfig{3}, Reservation: r}
	hash := sha256.Sum256([]byte("exact command"))
	u := ApplicationRecoveryUse{ID: r.ID, Digest: r.Digest, CommandDigest: hash, Phase: 100}
	credit, err := m.credit(u)
	if err != nil {
		t.Fatal(err)
	}
	q := applicationRecoveryQueued{Index: 2, CommandDigest: hash, Purpose: applicationRecoveryQueuedPurpose{Use: &u}, Credit: credit}
	projected, _, err := advanceRecoveryQueued(m, q, p, l)
	if err != nil {
		t.Fatal(err)
	}
	replayed, _, err := advanceRecoveryQueued(projected, q, p, l)
	if err != nil || !sameRecoveryReservation(replayed.Reservation, projected.Reservation) {
		t.Fatal("duplicate projection spent twice", err)
	}
	// Actual installation starts from applied state, never the projected copy.
	installed, _, err := m.consume(u)
	if err != nil || !sameRecoveryReservation(installed.Reservation, projected.Reservation) {
		t.Fatal("reclassified installation refill", err)
	}
	reserve := applicationRecoveryQueued{Index: 3, ReservationOriginIndex: 2, CommandDigest: hash, Purpose: applicationRecoveryQueuedPurpose{Reserve: &r}}
	replayed, _, err = advanceRecoveryQueued(installed, reserve, p, l)
	if err != nil || !sameRecoveryReservation(replayed.Reservation, installed.Reservation) {
		t.Fatal("unknown registration replay refilled", err)
	}
	m.Queued = []applicationRecoveryQueued{q, reserve}
	wire := appendRecoveryMetadata(nil, m)
	if len(wire) != recoveryMetadataBytes(m) || !bytes.Equal(wire[:4], []byte{'R', 'Y', 5, 0}) {
		t.Fatal("exact metadata accounting", len(wire), recoveryMetadataBytes(m))
	}
	decoded, tail, err := decodeRecoveryMetadata(wire, p, l)
	if err != nil || len(tail) != 0 || !reflect.DeepEqual(decoded, m) {
		t.Fatal(decoded, err)
	}
	decoded.Reservation.Spans[0].Next++
	if m.Reservation.Spans[0].Next != 100 {
		t.Fatal("decoder alias")
	}
	for _, mutate := range []func([]byte){func(b []byte) { b[2] = 4 }, func(b []byte) { binary.BigEndian.PutUint32(b[72:76], math.MaxUint32) }, func(b []byte) { binary.BigEndian.PutUint32(b[80:84], 99) }} {
		bad := bytes.Clone(wire)
		mutate(bad)
		if _, _, err := decodeRecoveryMetadata(bad, p, l); !errors.Is(err, ErrCorrupt) {
			t.Fatal("malformed fresh metadata", err)
		}
	}
	if _, _, err := decodeRecoveryMetadata(make([]byte, maxMetadataBytes+1), p, l); !errors.Is(err, ErrCorrupt) {
		t.Fatal("unbounded parser", err)
	}
	// Literal disabled/one/two representations remain unchanged, including the
	// historical queue wrapper with no optional profile selection.
	old := ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Credits: []ApplicationRecoveryCredit{{1, 10, 100, 4}}}
	expected, _ := hex.DecodeString("525301000100000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000000001000000010000000a00000000000000640000000000000004")
	if !bytes.Equal(appendRecoveryRoot(old), expected) {
		t.Fatal("RS1 literal changed")
	}
	old2 := applicationRecoveryMetadata{Config: ApplicationRecoveryConfig{2}, Reservation: old}
	expected2 := bytes.Clone(expected)
	expected2[2] = 2
	expected2 = append(expected2, []byte{'T', 'B', 1, 0, 0, 0, 0, 0}...)
	if !bytes.Equal(appendNativeRecoveryRoot(old2), expected2) {
		t.Fatal("RS2 literal changed")
	}
	for _, version := range []uint32{1, 2} {
		literal := []byte{'R', 'Y', byte(version + 2), 0, 0, 0, 0, byte(version)}
		literal = append(literal, expected[4:]...)
		literal = append(literal, []byte{'R', 'Q', 2, 0, 0, 0, 0, 0}...)
		literal = append(literal, make([]byte, 48)...)
		if version == 2 {
			literal = append(literal, []byte{'T', 'B', 1, 0, 0, 0, 0, 0}...)
		}
		if !bytes.Equal(appendRecoveryMetadata(nil, applicationRecoveryMetadata{Config: ApplicationRecoveryConfig{version}, Reservation: old}), literal) {
			t.Fatal("old metadata literal changed", version)
		}
	}
	if !bytes.Equal(appendRecoveryQueue(nil, nil), []byte{'R', 'Q', 2, 0, 0, 0, 0, 0}) {
		t.Fatal("RQ2 literal changed")
	}
	if _, err := ApplicationContractForPolicyWithRecovery(p, ApplicationControlConfig{1}, ApplicationRecoveryConfig{3}); !errors.Is(err, ErrInvalid) {
		t.Fatal("implicit profile upgrade", err)
	}
	c, err := ApplicationContractForPolicyWithRetention(p, ApplicationControlConfig{1}, ApplicationRecoveryConfig{3}, ApplicationRetentionConfig{1})
	if err != nil || c.Version != 6 {
		t.Fatal(c, err)
	}
}
