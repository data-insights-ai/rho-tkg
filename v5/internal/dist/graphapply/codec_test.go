package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
)

func codecNamespace() namespace { return namespace{idalloc.GraphID{1}, 7} }
func authority(t *testing.T, owner byte, epoch uint64) idalloc.Authority {
	t.Helper()
	a, err := idalloc.NewAuthority([16]byte{owner}, epoch)
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func codecSession() idalloc.RecipientSession {
	return idalloc.RecipientSession{ID: [16]byte{3}, Incarnation: [16]byte{4}, Epoch: 1}
}
func codecRequests(t *testing.T) []request {
	return []request{
		{ns: codecNamespace(), kind: initAllocator, attempt: bootstrapAttemptID{1}, authority: authority(t, 2, 1), maxBlock: 16},
		{ns: codecNamespace(), kind: transferAllocator, id: requestID{2}, authority: authority(t, 2, 1), replacement: authority(t, 5, 2)},
		{ns: codecNamespace(), kind: activateRecipient, id: requestID{3}, session: codecSession(), home: 7},
		{ns: codecNamespace(), kind: reserveGrant, id: requestID{4}, authority: authority(t, 2, 1), session: codecSession(), sequence: 7, count: 2},
	}
}
func codecGrant(t *testing.T) idalloc.Grant {
	t.Helper()
	r := codecRequests(t)[3].grantRequest()
	return idalloc.Grant{Request: r, Reservation: idalloc.Reservation{Request: idalloc.Request{Graph: r.Graph, Authority: authority(t, 2, 1), Sequence: 1, Count: r.Count}, First: 1, Last: 2}}
}
func resign(b []byte) { h := sha256.Sum256(b[:len(b)-32]); copy(b[len(b)-32:], h[:]) }
func TestRequestCodecRejectsBeforeAllocatingCollections(t *testing.T) {
	l := defaultLimits()
	for _, r := range codecRequests(t) {
		wire, err := encodeRequest(r, l)
		if err != nil || len(wire) != requestSize(r.kind) || cap(wire) != len(wire) {
			t.Fatal(len(wire), err)
		}
		got, err := decodeRequest(wire, l)
		if err != nil || got != r {
			t.Fatal(got, err)
		}
		for i := range len(wire) {
			if got, err := decodeRequest(wire[:i], l); !errors.Is(err, errCorrupt) || got != (request{}) {
				t.Fatal(i, got, err)
			}
		}
		if _, err := decodeRequest(append(bytes.Clone(wire), 0), l); !errors.Is(err, errCorrupt) {
			t.Fatal(err)
		}
		for _, offset := range []int{0, 3, 4, len(wire) - 1} {
			bad := bytes.Clone(wire)
			bad[offset] ^= 0x7f
			if _, err := decodeRequest(bad, l); !errors.Is(err, errCorrupt) {
				t.Fatal(offset, err)
			}
		}
		tight := l
		tight.inputBytes = len(wire) - 1
		if got, err := decodeRequest(wire, tight); !errors.Is(err, errLimit) || got != (request{}) {
			t.Fatal(got, err)
		}
		if _, err := encodeRequest(r, tight); !errors.Is(err, errLimit) {
			t.Fatal(err)
		}
		extra := r
		extra.home = 99
		if extra.kind == activateRecipient {
			extra.maxBlock = 1
		}
		if _, err := encodeRequest(extra, l); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
	}
	init := codecRequests(t)[0]
	wire, _ := encodeRequest(init, l)
	malformed := bytes.Clone(wire)
	binary.BigEndian.PutUint64(malformed[requestHeaderBytes+24:], 0)
	resign(malformed)
	if _, err := decodeRequest(malformed, l); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	recognized, ok := recognizeBootstrap(malformed)
	if !ok || recognized.attempt != init.attempt || recognized.ns != init.ns {
		t.Fatal(recognized, ok)
	}
	for _, b := range [][]byte{nil, wire[:3], bytes.Repeat([]byte{0}, maxRequestBytes+1)} {
		if _, ok := recognizeBootstrap(b); ok {
			t.Fatal("recognized invalid bootstrap identity")
		}
	}
	ordinary, _ := encodeRequest(codecRequests(t)[1], l)
	if _, ok := recognizeBootstrap(ordinary); ok {
		t.Fatal("ordinary identity is not bootstrap")
	}
	bad := init
	bad.id = requestID{1}
	if _, err := encodeRequest(bad, l); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	bad = init
	bad.ns.partition = 0
	if _, err := encodeRequest(bad, l); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := decodeRequest(wire, limits{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := encodeRequest(init, limits{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	max := codecRequests(t)[3]
	max.sequence = math.MaxUint64
	wire, err := encodeRequest(max, l)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := decodeRequest(wire, l); err != nil || got.sequence != math.MaxUint64 {
		t.Fatal(got, err)
	}
}
func TestFixedRecordCodecsBindNamespaceAndExactGrant(t *testing.T) {
	n := codecNamespace()
	g := codecGrant(t)
	recipient := recipientRecord{session: codecSession(), home: 7, index: 3, sequence: math.MaxUint64}
	grant := grantRecord{grant: g, index: 4}
	req := codecRequests(t)[3]
	out := outcome{ns: n, kind: reserveGrant, identity: req.identity(), hash: [32]byte{1}, index: 4, disposition: applied, grantIndex: 4, grant: g}
	rw, err := encodeRecipient(n, recipient)
	if err != nil || len(rw) != recipientBytes {
		t.Fatal(len(rw), err)
	}
	gw, err := encodeGrant(n, grant)
	if err != nil || len(gw) != grantBytes {
		t.Fatal(len(gw), err)
	}
	ow, err := encodeOutcome(out)
	if err != nil || len(ow) != outcomeBytes {
		t.Fatal(len(ow), err)
	}
	if got, err := decodeRecipient(rw, n); err != nil || got != recipient {
		t.Fatal(got, err)
	}
	if got, err := decodeGrant(gw, n); err != nil || got != grant {
		t.Fatal(got, err)
	}
	if got, err := decodeOutcome(ow, n); err != nil || got != out {
		t.Fatal(got, err)
	}
	cases := []struct {
		wire   []byte
		decode func([]byte, namespace) error
	}{
		{rw, func(b []byte, n namespace) error { _, e := decodeRecipient(b, n); return e }},
		{gw, func(b []byte, n namespace) error { _, e := decodeGrant(b, n); return e }},
		{ow, func(b []byte, n namespace) error { _, e := decodeOutcome(b, n); return e }},
	}
	for _, c := range cases {
		for i := range len(c.wire) {
			if err := c.decode(c.wire[:i], n); !errors.Is(err, errCorrupt) {
				t.Fatal(i, err)
			}
		}
		if err := c.decode(append(bytes.Clone(c.wire), 0), n); !errors.Is(err, errCorrupt) {
			t.Fatal(err)
		}
		bad := bytes.Clone(c.wire)
		bad[3] = 2
		resign(bad)
		if err := c.decode(bad, n); !errors.Is(err, errCorrupt) {
			t.Fatal(err)
		}
		foreign := n
		foreign.partition++
		if err := c.decode(c.wire, foreign); !errors.Is(err, errCorrupt) {
			t.Fatal(err)
		}
	}
	badg := grant
	badg.grant.Reservation.Last++
	if _, err := encodeGrant(n, badg); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	badr := recipient
	badr.home++
	if _, err := encodeRecipient(n, badr); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	for _, edit := range []func(*outcome){func(o *outcome) { o.index = 0 }, func(o *outcome) { o.reason = reasonMismatch }, func(o *outcome) { o.disposition = 0 }, func(o *outcome) { o.grantIndex = 0 }, func(o *outcome) { o.grant.Reservation.Last++ }} {
		bad := out
		edit(&bad)
		if _, err := encodeOutcome(bad); !errors.Is(err, errInvalid) {
			t.Fatal(bad, err)
		}
	}
	rejection := outcome{ns: n, kind: initAllocator, identity: [16]byte{9}, hash: [32]byte{9}, index: 2, disposition: applied, reason: reasonInvalid}
	wire, err := encodeOutcome(rejection)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := decodeOutcome(wire, n); err != nil || got != rejection {
		t.Fatal(got, err)
	}
	wire[4] ^= 1
	if rejection.ns != n {
		t.Fatal("codec retained bytes")
	}
}

func TestOutcomeSemanticCombinationCorruptionAndGrantCause(t *testing.T) {
	n := codecNamespace()
	r := codecRequests(t)[3]
	o := outcome{ns: n, kind: reserveGrant, identity: r.identity(), hash: [32]byte{1}, index: 4, disposition: applied, grantIndex: 4, grant: codecGrant(t)}
	wire, err := encodeOutcome(o)
	if err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func([]byte){
		func(b []byte) { b[28] = byte(transferAllocator) },
		func(b []byte) { binary.BigEndian.PutUint64(b[88:96], 5) },
		func(b []byte) { b[96] = 2 },
		func(b []byte) { binary.BigEndian.PutUint64(b[88:96], 3) }, // applied grant must be created HERE
		func(b []byte) { b[85] = byte(grantRecovery) },             // recovery must refer to an EARLIER grant

	} {
		bad := owned(wire)
		edit(bad)
		resign(bad)
		if _, err := decodeOutcome(bad, n); !errors.Is(err, errCorrupt) {
			t.Fatal(err)
		}
	}
	rejection := outcome{ns: n, kind: initAllocator, identity: [16]byte{2}, hash: [32]byte{2}, index: 3, disposition: applied, reason: reasonInvalid}
	wire, _ = encodeOutcome(rejection)
	for _, mode := range []disposition{grantRecovery, controlRecovery} {
		bad := owned(wire)
		bad[85] = byte(mode)
		resign(bad)
		if _, err := decodeOutcome(bad, n); !errors.Is(err, errCorrupt) {
			t.Fatal(err)
		}
	}
	control := outcome{ns: n, kind: transferAllocator, identity: [16]byte{3}, hash: [32]byte{3}, index: 3, disposition: controlRecovery}
	controlWire, err := encodeOutcome(control)
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint16(controlWire[86:88], uint16(reasonInvalid))
	resign(controlWire)
	if _, err := decodeOutcome(controlWire, n); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	g := grantRecord{grant: codecGrant(t), index: 4}
	wire, _ = encodeGrant(n, g)
	// Authority epoch sits after request72 + reservation graph16 + owner16.
	bad := owned(wire)
	binary.BigEndian.PutUint64(bad[36+72+16+16:], 0)
	resign(bad)
	if _, err := decodeGrant(bad, n); !errors.Is(err, errCorrupt) || !errors.Is(err, idalloc.ErrInvalid) {
		t.Fatal(err)
	}
	g.grant.Reservation.Last++
	if _, err := encodeGrant(n, g); !errors.Is(err, errInvalid) || !errors.Is(err, idalloc.ErrInvalid) {
		t.Fatal(err)
	}
}
