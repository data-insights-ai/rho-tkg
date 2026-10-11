package graphapply

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"testing"
)

// These are inert codec guards under the independently recorded E15 missing-
// capability RED. Syntactically valid bodies never establish source authority.
func graphCutWireFixture(kind byte) graphCutAuthorityBody {
	s := allocationScope{graph: [16]byte{1}, partition: 3, group: [16]byte{4}, ownership: 5, topology: 6, declaration: [32]byte{7}, semantic: certifiedGraphSemanticContractID()}
	p := graphCutAuthorityBody{scope: s, id: allocationReadID{session: [16]byte{8}, sequence: 9}, question: graphCutQuery{kind: kind, nonce: [16]byte{10}}, observation: graphCutObservation{scope: s, configuration: [32]byte{11}, index: 20, sourceRows: 13, sourceBytes: 1024, outputBytes: 256}}
	switch kind {
	case 1:
		p.observation.floor = 12
		p.observation.pending, p.observation.coordinator = [16]byte{13}, 8
	case 2:
		p.question.round = 12
		p.observation.certificate = graphCertificate{scope: s, configuration: p.observation.configuration, round: 12, index: 19, sourceRound: 10, sourceIndex: 18, image: [172]byte{14}}
	case 3:
		p.question.request = [16]byte{15}
		p.observation.local = graphLocalReceipt{scope: s, identity: p.question.request, digest: [32]byte{16}, round: 12, index: 19}
	case 4:
		p.question.request, p.question.digest = [16]byte{17}, [32]byte{18}
		p.observation.participant = graphParticipantObservation{transaction: p.question.request, digest: p.question.digest, phase: crossResolve}
	}
	return p
}

func TestGraphCutWireRequestAllowanceLiteralAndAdmission(t *testing.T) {
	// Independently assembled GCQ2: kind1, nonce9, no round/request/digest,
	// followed by three request-bound u32 allowances at offsets77/81/85.
	raw := make([]byte, 89)
	copy(raw, []byte{'G', 'C', 'Q', 2})
	raw[4], raw[13] = 1, 9
	binary.BigEndian.PutUint32(raw[77:81], 24)
	binary.BigEndian.PutUint32(raw[81:85], 1024)
	binary.BigEndian.PutUint32(raw[85:89], 2048)
	literal := seal(raw)
	want := graphCutQuery{kind: 1, nonce: [16]byte{9}, sourceRows: 24, sourceBytes: 1024, outputBytes: 2048}
	got, err := decodeGraphCutQuery(literal)
	if err != nil || got != want {
		t.Fatal("request allowance literal changed", got, err)
	}
	encoded, err := encodeGraphCutQuery(want)
	if err != nil || !bytes.Equal(encoded, literal) {
		t.Fatal("request allowance encoder changed independent literal", err)
	}
	for _, allowance := range [][3]int{{0, 0, 0}, {1, 1, 1}, {1 << 20, 64 << 20, 64 << 20}} {
		q := want
		q.sourceRows, q.sourceBytes, q.outputBytes = allowance[0], allowance[1], allowance[2]
		wire, err := encodeGraphCutQuery(q)
		if err != nil {
			t.Fatal("legacy-zero or exact finite allowance refused", allowance, err)
		}
		got, err := decodeGraphCutQuery(wire)
		if err != nil || got != q {
			t.Fatal("allowance roundtrip dropped a field", allowance, got, err)
		}
	}
	for _, allowance := range [][3]int{{-1, 1, 1}, {1, -1, 1}, {1, 1, -1}, {0, 1, 1}, {1, 0, 1}, {1, 1, 0}, {1, 0, 0}, {(1 << 20) + 1, 1, 1}, {1, (64 << 20) + 1, 1}, {1, 1, (64 << 20) + 1}} {
		q := want
		q.sourceRows, q.sourceBytes, q.outputBytes = allowance[0], allowance[1], allowance[2]
		if wire, err := encodeGraphCutQuery(q); wire != nil || !errors.Is(err, errInvalid) {
			t.Fatal("partial/negative/oversized request allowances admitted", allowance, err)
		}
	}
	for i, offset := range []int{77, 81, 85} {
		bad := bytes.Clone(literal)
		binary.BigEndian.PutUint32(bad[offset:offset+4], []uint32{(1 << 20) + 1, (64 << 20) + 1, (64 << 20) + 1}[i])
		wireGuardReseal(bad)
		if got, err := decodeGraphCutQuery(bad); got != (graphCutQuery{}) || !errors.Is(err, errLimit) {
			t.Fatal("oversized wire allowance decoded", offset, got, err)
		}
		bad = bytes.Clone(literal)
		clear(bad[offset : offset+4])
		wireGuardReseal(bad)
		if got, err := decodeGraphCutQuery(bad); got != (graphCutQuery{}) || !errors.Is(err, errCorrupt) {
			t.Fatal("partial allowance decoded", offset, got, err)
		}
	}
	oldVersion := bytes.Clone(literal)
	oldVersion[3] = 1
	wireGuardReseal(oldVersion)
	if got, err := decodeGraphCutQuery(oldVersion); got != (graphCutQuery{}) || !errors.Is(err, errCorrupt) {
		t.Fatal("old wire version admitted extended allowance fields", got, err)
	}
}

func TestGraphCutWireCompletedWorkCannotExceedRequestAllowance(t *testing.T) {
	for kind := byte(1); kind <= 4; kind++ {
		p := graphCutWireFixture(kind)
		p.question.sourceRows, p.question.sourceBytes, p.question.outputBytes = p.observation.sourceRows, p.observation.sourceBytes, p.observation.outputBytes
		wire, err := encodeGraphCutReplyBody(p, nil)
		if err != nil {
			t.Fatal("exact spent/requested counterpart refused", kind, err)
		}
		got, err := decodeGraphCutReplyBody(wire)
		if err != nil || got != p {
			t.Fatal("completed native work not conserved", kind, got, err)
		}
		for _, edit := range []func(*graphCutAuthorityBody){
			func(p *graphCutAuthorityBody) { p.question.sourceRows-- },
			func(p *graphCutAuthorityBody) { p.question.sourceBytes-- },
			func(p *graphCutAuthorityBody) { p.question.outputBytes-- },
			func(p *graphCutAuthorityBody) { p.observation.sourceRows = -1 },
			func(p *graphCutAuthorityBody) { p.observation.sourceBytes = -1 },
			func(p *graphCutAuthorityBody) { p.observation.outputBytes = -1 },
			func(p *graphCutAuthorityBody) { p.observation.sourceRows = (1 << 20) + 1 },
			func(p *graphCutAuthorityBody) { p.observation.sourceBytes = (64 << 20) + 1 },
			func(p *graphCutAuthorityBody) { p.observation.outputBytes = (64 << 20) + 1 },
		} {
			bad := p
			edit(&bad)
			if wire, err := encodeGraphCutAuthorityBody(bad); wire != nil || !errors.Is(err, errInvalid) {
				t.Fatal("negative/oversized/one-short completed work admitted", kind, err)
			}
		}
		body, err := encodeGraphCutAuthorityBody(p)
		if err != nil {
			t.Fatal(err)
		}
		for i, offset := range []int{385, 389, 393} {
			bad := bytes.Clone(body)
			binary.BigEndian.PutUint32(bad[offset:offset+4], []uint32{14, 1025, 257}[i])
			wireGuardReseal(bad)
			if got, err := decodeGraphCutAuthorityBody(bad); got != (graphCutAuthorityBody{}) || !errors.Is(err, errCorrupt) {
				t.Fatal("resealed work exceeding authenticated query accepted", kind, offset, got, err)
			}
			binary.BigEndian.PutUint32(bad[offset:offset+4], math.MaxUint32)
			wireGuardReseal(bad)
			if got, err := decodeGraphCutAuthorityBody(bad); got != (graphCutAuthorityBody{}) || !errors.Is(err, errLimit) {
				t.Fatal("oversized completed-work wire count admitted", kind, offset, got, err)
			}
		}
	}
	boundary := graphCutWireFixture(1)
	boundary.observation.sourceRows, boundary.observation.sourceBytes, boundary.observation.outputBytes = 1<<20, 64<<20, 64<<20
	for _, bounded := range []bool{false, true} {
		p := boundary
		if bounded {
			p.question.sourceRows, p.question.sourceBytes, p.question.outputBytes = p.observation.sourceRows, p.observation.sourceBytes, p.observation.outputBytes
		}
		wire, err := encodeGraphCutAuthorityBody(p)
		if err != nil {
			t.Fatal("exact finite native-work limit refused", bounded, err)
		}
		got, err := decodeGraphCutAuthorityBody(wire)
		if err != nil || got != p {
			t.Fatal("legacy-zero/request-bound work limit differs", bounded, got, err)
		}
		clear(wire)
		if got != p {
			t.Fatal("completed work values borrowed caller frame")
		}
		if err := checkGraphCutProof(graphCutProof{id: got.id, question: got.question, observation: got.observation}, got.question); !errors.Is(err, errInvalid) {
			t.Fatal("decoding work minted source proof", err)
		}
	}
}

func TestGraphCutWireRejectsQueryFieldsBorrowedFromAnotherKind(t *testing.T) {
	for kind := byte(1); kind <= 4; kind++ {
		base := graphCutWireFixture(kind).question
		wire, err := encodeGraphCutQuery(base)
		if err != nil || len(wire) != 121 || string(wire[:4]) != "GCQ\x02" {
			t.Fatal("exact query format", kind, len(wire), err)
		}
		got, err := decodeGraphCutQuery(wire)
		if err != nil || got != base {
			t.Fatal("complete query lost a field", kind, got, err)
		}
		clear(wire)
		if got != base {
			t.Fatal("by-value query borrowed caller bytes")
		}
		for _, edit := range []func(*graphCutQuery){
			func(q *graphCutQuery) { q.kind = 0 },
			func(q *graphCutQuery) { q.kind = 5 },
			func(q *graphCutQuery) { q.nonce = [16]byte{} },
			func(q *graphCutQuery) { q.round = math.MaxUint64 },
		} {
			bad := base
			edit(&bad)
			if out, err := encodeGraphCutQuery(bad); out != nil || !errors.Is(err, errInvalid) {
				t.Fatal("invalid query admitted", kind, err)
			}
		}
	}
	for _, bad := range []graphCutQuery{
		{kind: 1, nonce: [16]byte{1}, round: 1},
		{kind: 1, nonce: [16]byte{1}, request: [16]byte{2}},
		{kind: 1, nonce: [16]byte{1}, digest: [32]byte{2}},
		{kind: 2, nonce: [16]byte{1}},
		{kind: 2, nonce: [16]byte{1}, round: 1, request: [16]byte{2}},
		{kind: 2, nonce: [16]byte{1}, round: 1, digest: [32]byte{2}},
		{kind: 3, nonce: [16]byte{1}},
		{kind: 3, nonce: [16]byte{1}, request: [16]byte{2}, round: 1},
		{kind: 3, nonce: [16]byte{1}, request: [16]byte{2}, digest: [32]byte{2}},
		{kind: 4, nonce: [16]byte{1}, request: [16]byte{2}},
		{kind: 4, nonce: [16]byte{1}, digest: [32]byte{2}},
		{kind: 4, nonce: [16]byte{1}, request: [16]byte{2}, digest: [32]byte{2}, round: 1},
	} {
		if out, err := encodeGraphCutQuery(bad); out != nil || !errors.Is(err, errInvalid) {
			t.Fatal("kind-specific query field leaked", bad, err)
		}
	}
}

func TestGraphCutWirePreservesEveryObservationAndRejectsCrossKindResidue(t *testing.T) {
	for kind := byte(1); kind <= 4; kind++ {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			base := graphCutWireFixture(kind)
			wire, err := encodeGraphCutAuthorityBody(base)
			if err != nil || len(wire) > 4096 || string(wire[:4]) != "GCB\x02" {
				t.Fatal("bounded body", len(wire), err)
			}
			got, err := decodeGraphCutAuthorityBody(wire)
			if err != nil || got != base {
				t.Fatal("full observation differs", got, base, err)
			}
			clear(wire)
			if got != base {
				t.Fatal("body retained a frame alias")
			}
			for _, edit := range []func(*graphCutAuthorityBody){
				func(p *graphCutAuthorityBody) { p.scope.partition++ },
				func(p *graphCutAuthorityBody) { p.observation.scope.group[0] ^= 1 },
				func(p *graphCutAuthorityBody) {
					p.scope.semantic = crossPartitionSemanticContractID()
					p.observation.scope = p.scope
				},
				func(p *graphCutAuthorityBody) { p.id.session = [16]byte{} },
				func(p *graphCutAuthorityBody) { p.id.sequence = 0 },
				func(p *graphCutAuthorityBody) { p.observation.configuration = [32]byte{} },
				func(p *graphCutAuthorityBody) { p.observation.index = 0 },
				func(p *graphCutAuthorityBody) { p.question.nonce = [16]byte{} },
			} {
				bad := base
				edit(&bad)
				if out, err := encodeGraphCutAuthorityBody(bad); out != nil || !errors.Is(err, errInvalid) {
					t.Fatal("uncorrelated body admitted", kind, err)
				}
			}
			for other := byte(1); other <= 4; other++ {
				if other == kind {
					continue
				}
				bad, residue := base, graphCutWireFixture(other).observation
				switch other {
				case 1:
					bad.observation.floor = residue.floor
				case 2:
					bad.observation.certificate = residue.certificate
				case 3:
					bad.observation.local = residue.local
				case 4:
					bad.observation.participant = residue.participant
				}
				if out, err := encodeGraphCutAuthorityBody(bad); out != nil || !errors.Is(err, errInvalid) {
					t.Fatal("unused observation silently dropped", kind, other, err)
				}
			}
		})
	}
}

func TestGraphCutWireRejectsKindSpecificMismatches(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind byte
		edit func(*graphCutAuthorityBody)
	}{
		{"pending-without-coordinator", 1, func(p *graphCutAuthorityBody) { p.observation.coordinator = 0 }},
		{"coordinator-without-pending", 1, func(p *graphCutAuthorityBody) { p.observation.pending = [16]byte{} }},
		{"floor-overflow", 1, func(p *graphCutAuthorityBody) { p.observation.floor = math.MaxUint64 }},
		{"certificate-wrong-round", 2, func(p *graphCutAuthorityBody) { p.observation.certificate.round++ }},
		{"certificate-future-index", 2, func(p *graphCutAuthorityBody) { p.observation.certificate.index = 21 }},
		{"certificate-other-scope", 2, func(p *graphCutAuthorityBody) { p.observation.certificate.scope.partition++ }},
		{"certificate-other-config", 2, func(p *graphCutAuthorityBody) { p.observation.certificate.configuration[0] ^= 1 }},
		{"certificate-not-predecessor", 2, func(p *graphCutAuthorityBody) {
			p.observation.certificate.sourceIndex = p.observation.certificate.index
		}},
		{"receipt-other-identity", 3, func(p *graphCutAuthorityBody) { p.observation.local.identity[0] ^= 1 }},
		{"receipt-future-index", 3, func(p *graphCutAuthorityBody) { p.observation.local.index = 21 }},
		{"receipt-zero-index", 3, func(p *graphCutAuthorityBody) { p.observation.local.index = 0 }},
		{"receipt-other-scope", 3, func(p *graphCutAuthorityBody) { p.observation.local.scope.partition++ }},
		{"participant-other-tx", 4, func(p *graphCutAuthorityBody) { p.observation.participant.transaction[0] ^= 1 }},
		{"participant-other-digest", 4, func(p *graphCutAuthorityBody) { p.observation.participant.digest[0] ^= 1 }},
		{"participant-unknown-phase", 4, func(p *graphCutAuthorityBody) { p.observation.participant.phase = crossResolve + 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := graphCutWireFixture(tc.kind)
			tc.edit(&p)
			if out, err := encodeGraphCutAuthorityBody(p); out != nil || !errors.Is(err, errInvalid) {
				t.Fatal("kind-specific contradictory evidence admitted", err)
			}
		})
	}
	for phase := crossPhase(0); phase <= crossResolve; phase++ {
		p := graphCutWireFixture(4)
		p.observation.participant.phase = phase
		wire, err := encodeGraphCutAuthorityBody(p)
		if err != nil {
			t.Fatal("absence or declared participant phase refused", phase, err)
		}
		got, err := decodeGraphCutAuthorityBody(wire)
		if err != nil || got != p {
			t.Fatal("participant phase changed", phase, err)
		}
	}
	base := graphCutWireFixture(1)
	base.observation.pending, base.observation.coordinator, base.observation.floor = [16]byte{}, 0, 0
	if _, err := encodeGraphCutAuthorityBody(base); err != nil {
		t.Fatal("zero floor/no pending counterpart refused", err)
	}
}

func graphCutWireResealBody(b []byte) []byte { return seal(bytes.Clone(b)) }

func TestGraphCutWireDecoderRejectsResealedContradictoryObservations(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind byte
		edit func([]byte)
	}{
		{"zero-session", 1, func(b []byte) { clear(b[164:180]) }},
		{"zero-sequence", 1, func(b []byte) { clear(b[180:188]) }},
		{"zero-config", 1, func(b []byte) { clear(b[313:345]) }},
		{"zero-index", 1, func(b []byte) { clear(b[353:361]) }},
		{"pending-without-coordinator", 1, func(b []byte) { clear(b[377:385]) }},
		{"coordinator-without-pending", 1, func(b []byte) { clear(b[361:377]) }},
		{"certificate-with-collect-floor", 2, func(b []byte) { binary.BigEndian.PutUint64(b[345:353], 1) }},
		{"certificate-other-config", 2, func(b []byte) { b[525] ^= 1; wireGuardReseal(b[401:793]) }},
		{"certificate-other-round", 2, func(b []byte) { binary.BigEndian.PutUint64(b[557:565], 13); wireGuardReseal(b[401:793]) }},
		{"certificate-future-index", 2, func(b []byte) { binary.BigEndian.PutUint64(b[565:573], 21); wireGuardReseal(b[401:793]) }},
		{"receipt-other-identity", 3, func(b []byte) { b[525] ^= 1; wireGuardReseal(b[401:621]) }},
		{"receipt-future-index", 3, func(b []byte) { binary.BigEndian.PutUint64(b[581:589], 21); wireGuardReseal(b[401:621]) }},
		{"participant-other-transaction", 4, func(b []byte) { b[397] ^= 1 }},
		{"participant-other-digest", 4, func(b []byte) { b[413] ^= 1 }},
		{"participant-unknown-phase", 4, func(b []byte) { b[445] = 5 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire, err := encodeGraphCutAuthorityBody(graphCutWireFixture(tc.kind))
			if err != nil {
				t.Fatal(err)
			}
			tc.edit(wire)
			wireGuardReseal(wire)
			got, err := decodeGraphCutAuthorityBody(wire)
			if got != (graphCutAuthorityBody{}) || !errors.Is(err, errCorrupt) {
				t.Fatal("resealed contradictory body retained fields", got, err)
			}
		})
	}
}

func TestGraphCutWireRejectsTruncationsTrailingBytesAndOversizedCounts(t *testing.T) {
	for kind := byte(1); kind <= 4; kind++ {
		p := graphCutWireFixture(kind)
		query, err := encodeGraphCutQuery(p.question)
		if err != nil {
			t.Fatal(err)
		}
		for n := range len(query) {
			got, err := decodeGraphCutQuery(query[:n])
			if got != (graphCutQuery{}) || !errors.Is(err, errCorrupt) {
				t.Fatal("query truncation returned fields", kind, n, err)
			}
		}
		body, err := encodeGraphCutAuthorityBody(p)
		if err != nil {
			t.Fatal(err)
		}
		for n := range len(body) {
			got, err := decodeGraphCutAuthorityBody(body[:n])
			if got != (graphCutAuthorityBody{}) || !errors.Is(err, errCorrupt) {
				t.Fatal("body truncation returned fields", kind, n, err)
			}
		}
		for _, frame := range [][]byte{
			graphCutWireResealBody(append(bytes.Clone(body[:len(body)-32]), 0)),
			graphCutWireResealBody(body[:len(body)-33]),
		} {
			got, err := decodeGraphCutAuthorityBody(frame)
			if got != (graphCutAuthorityBody{}) || !errors.Is(err, errCorrupt) {
				t.Fatal("noncanonical body admitted", kind, err)
			}
		}
		for _, offset := range []int{4, 188} { // scope/query length prefixes, before any payload copy.
			bad := bytes.Clone(body)
			binary.BigEndian.PutUint32(bad[offset:offset+4], math.MaxUint32)
			wireGuardReseal(bad)
			got, err := decodeGraphCutAuthorityBody(bad)
			if got != (graphCutAuthorityBody{}) || !errors.Is(err, errLimit) {
				t.Fatal("oversized nested count admitted", kind, offset, err)
			}
		}
		bad := bytes.Clone(query)
		bad[4] = 5
		wireGuardReseal(bad)
		got, err := decodeGraphCutQuery(bad)
		if got != (graphCutQuery{}) || !errors.Is(err, errCorrupt) {
			t.Fatal("resealed invalid query admitted", err)
		}
	}
	for _, decode := range []func([]byte) error{
		func(b []byte) error { _, err := decodeGraphCutQuery(b); return err },
		func(b []byte) error { _, err := decodeGraphCutAuthorityBody(b); return err },
		func(b []byte) error { _, err := decodeGraphCutReplyBody(b); return err },
	} {
		if err := decode(nil); !errors.Is(err, errCorrupt) {
			t.Fatal("nil frame did not refuse", err)
		}
		if err := decode(make([]byte, 4097)); !errors.Is(err, errLimit) {
			t.Fatal("outer frame bound widened", err)
		}
		if err := decode(make([]byte, 4096)); !errors.Is(err, errCorrupt) || errors.Is(err, errLimit) {
			t.Fatal("exact outer limit is not an over-limit frame", err)
		}
	}
}

func TestGraphCutWireReplyKeepsRefusalsDistinctAndZero(t *testing.T) {
	p := graphCutWireFixture(1)
	wire, err := encodeGraphCutReplyBody(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeGraphCutReplyBody(wire)
	if err != nil || got != p {
		t.Fatal("success reply lost observation", err)
	}
	for i, cause := range []error{errGraphCutPending, errGraphCutScope} {
		wire, err := encodeGraphCutReplyBody(graphCutAuthorityBody{}, fmt.Errorf("wrapped: %w", cause))
		if err != nil || len(wire) != 41 || wire[4] != byte(i+1) {
			t.Fatal("explicit refusal status", i, err)
		}
		got, err := decodeGraphCutReplyBody(wire)
		if got != (graphCutAuthorityBody{}) || !errors.Is(err, cause) || errors.Is(err, []error{errGraphCutScope, errGraphCutPending}[i]) {
			t.Fatal("refusal acquired body or unrelated sentinel", got, err)
		}
		if out, err := encodeGraphCutReplyBody(p, cause); out != nil || !errors.Is(err, errInvalid) {
			t.Fatal("refusal retained observation", err)
		}
		joined := errors.Join(cause, fmt.Errorf("same: %w", cause))
		if _, err := encodeGraphCutReplyBody(graphCutAuthorityBody{}, joined); err != nil {
			t.Fatal("repeated same refusal cause lost", err)
		}
	}
	for _, cause := range []error{errInvalid, errLimit, errors.New("unknown"), errors.Join(errGraphCutPending, errGraphCutScope), errors.Join(errGraphCutPending, errInvalid), errors.Join(errGraphCutScope, errors.New("other"))} {
		if out, err := encodeGraphCutReplyBody(graphCutAuthorityBody{}, cause); out != nil || !errors.Is(err, errInvalid) {
			t.Fatal("unclassified/mixed error became a cut status", cause, err)
		}
	}
	if out, err := encodeGraphCutReplyBody(graphCutAuthorityBody{}, nil); out != nil || !errors.Is(err, errInvalid) {
		t.Fatal("zero body became success", err)
	}
	for n := range len(wire) {
		got, err := decodeGraphCutReplyBody(wire[:n])
		if got != (graphCutAuthorityBody{}) || !errors.Is(err, errCorrupt) {
			t.Fatal("reply truncation retained output", n, err)
		}
	}
	for _, status := range []byte{1, 2, 3, 255} {
		bad := bytes.Clone(wire)
		bad[4] = status
		wireGuardReseal(bad)
		got, err := decodeGraphCutReplyBody(bad)
		if got != (graphCutAuthorityBody{}) || !errors.Is(err, errCorrupt) {
			t.Fatal("unknown/refusal status admitted successful payload", status, err)
		}
	}
	for _, raw := range [][]byte{
		{'G', 'C', 'P', 2, 0, 0, 0, 0, 0},    // Success needs a complete authority body.
		{'G', 'C', 'P', 2, 1, 0, 0, 0, 1, 0}, // Refusal forbids even one byte.
		{'G', 'C', 'P', 2, 2, 0, 0, 0, 0, 0}, // Trailing bytes are not canonical.
	} {
		got, err := decodeGraphCutReplyBody(graphCutWireResealBody(raw))
		if got != (graphCutAuthorityBody{}) || !errors.Is(err, errCorrupt) {
			t.Fatal("noncanonical status payload", got, err)
		}
	}
}

func TestGraphCutWireNativeCompletedReadStillDecodesWithoutProof(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	_, tr := certifiedCoordinator(n)
	q := graphCutQuery{kind: 1, nonce: [16]byte{31}}
	reply, err := tr.Read(t.Context(), n.hosts[0][0], q)
	if err != nil || reply.err != nil || checkGraphCutProof(reply.proof, q) != nil {
		t.Fatal("native completed source read prerequisite", err, reply.err)
	}
	p := graphCutAuthorityBody{scope: reply.proof.observation.scope, id: reply.id, question: q, observation: reply.proof.observation}
	wire, err := encodeGraphCutReplyBody(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGraphCutReplyBody(wire)
	if err != nil || decoded != p {
		t.Fatal("native observation conservation", err)
	}
	unowned := graphCutProof{id: decoded.id, question: decoded.question, observation: decoded.observation}
	if err := checkGraphCutProof(unowned, q); !errors.Is(err, errInvalid) {
		t.Fatal("decoding minted source-owned proof", err)
	}
}
