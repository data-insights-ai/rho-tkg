package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// The seal here is only a representation fixture. No fabricated Host accepts
// it, and no test treats it as distributed transition evidence.
func rangeMoveAuthorityCodecFixture(t *testing.T) rangeMoveAuthority {
	t.Helper()
	intent := rangeMoveCodecIntent()
	wire, err := encodeRangeMoveIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	status := rangeMoveCodecStatus(t)
	image := make([]byte, 172)
	image[0], image[171] = 21, 22
	return sealRangeMoveAuthority(rangeMoveAuthority{
		scope: intent.source, id: allocationReadID{session: [16]byte{23}, sequence: 24},
		question:    rangeMoveQuery{move: status.move, nonce: [16]byte{25}, sourceRows: 64, sourceBytes: 1 << 20, outputBytes: 1 << 20},
		observation: rangeMoveObservation{scope: intent.source, configuration: intent.publication.Configuration, index: 30, floor: 14, root: raftlog.ApplicationRoot{Index: 30, Image: image, ImageHash: sha256.Sum256(image)}, status: status, intent: wire, progress: []byte{26, 27}, sourceRows: 4, sourceBytes: 1024, ownedBytes: 2048},
	})
}

func TestRangeMoveQueryLiteralAndAllowanceBounds(t *testing.T) {
	// Independent GMY2 body: move,nonce,page,three allowances and original selector.
	body := make([]byte, 81)
	copy(body, "GMY\x02")
	body[4], body[20] = 1, 2
	binary.BigEndian.PutUint32(body[44:48], 64)
	binary.BigEndian.PutUint32(body[48:52], 4096)
	binary.BigEndian.PutUint32(body[52:56], 8192)
	literal := seal(body)
	want := rangeMoveQuery{move: [16]byte{1}, nonce: [16]byte{2}, sourceRows: 64, sourceBytes: 4096, outputBytes: 8192}
	got, err := decodeRangeMoveQuery(literal)
	if err != nil || got != want {
		t.Fatal("request literal changed", got, err)
	}
	wire, err := encodeRangeMoveQuery(want)
	if err != nil || !bytes.Equal(wire, literal) {
		t.Fatal("encoder differs from independent request literal", err)
	}
	for _, allowance := range [][3]int{{1, 1, 1}, {1 << 20, 64 << 20, 64 << 20}} {
		q := want
		q.sourceRows, q.sourceBytes, q.outputBytes = allowance[0], allowance[1], allowance[2]
		wire, err := encodeRangeMoveQuery(q)
		if err != nil {
			t.Fatal("finite boundary refused", allowance, err)
		}
		if got, err := decodeRangeMoveQuery(wire); err != nil || got != q {
			t.Fatal("allowance changed", got, err)
		}
	}
	for _, tc := range []struct {
		name string
		edit func(*rangeMoveQuery)
	}{
		{"no-move", func(q *rangeMoveQuery) { q.move = [16]byte{} }},
		{"no-nonce", func(q *rangeMoveQuery) { q.nonce = [16]byte{} }},
		{"excess-page", func(q *rangeMoveQuery) { q.page = 1025 }},
		{"no-rows", func(q *rangeMoveQuery) { q.sourceRows = 0 }},
		{"negative-bytes", func(q *rangeMoveQuery) { q.sourceBytes = -1 }},
		{"no-output", func(q *rangeMoveQuery) { q.outputBytes = 0 }},
		{"excess-rows", func(q *rangeMoveQuery) { q.sourceRows = (1 << 20) + 1 }},
		{"excess-source", func(q *rangeMoveQuery) { q.sourceBytes = (64 << 20) + 1 }},
		{"excess-output", func(q *rangeMoveQuery) { q.outputBytes = (64 << 20) + 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := want
			tc.edit(&bad)
			if bad.valid() {
				t.Fatal("invalid query passed validator")
			}
			if wire, err := encodeRangeMoveQuery(bad); wire != nil || !errors.Is(err, errInvalid) {
				t.Fatal("invalid query encoded", err)
			}
		})
	}
	for n := range len(literal) {
		if got, err := decodeRangeMoveQuery(literal[:n]); got != (rangeMoveQuery{}) || !errors.Is(err, errCorrupt) {
			t.Fatal("truncated query exported partial fields", n, err)
		}
	}
	for _, offset := range []int{44, 48, 52} {
		bad := bytes.Clone(literal)
		binary.BigEndian.PutUint32(bad[offset:offset+4], math.MaxUint32)
		if got, err := decodeRangeMoveQuery(rangeMoveCodecReseal(bad)); got != (rangeMoveQuery{}) || !errors.Is(err, errLimit) {
			t.Fatal("untrusted allowance admitted", offset, err)
		}
		clear(bad[offset : offset+4])
		if got, err := decodeRangeMoveQuery(rangeMoveCodecReseal(bad)); got != (rangeMoveQuery{}) || !errors.Is(err, errCorrupt) {
			t.Fatal("zero allowance admitted", offset, err)
		}
	}
	badSeal := bytes.Clone(literal)
	badSeal[len(badSeal)-1] ^= 1
	trailing := seal(append(bytes.Clone(body), 0))
	for _, bad := range [][]byte{badSeal, trailing} {
		if got, err := decodeRangeMoveQuery(bad); got != (rangeMoveQuery{}) || !errors.Is(err, errCorrupt) {
			t.Fatal("noncanonical query exported fields", err)
		}
	}
}

func TestRangeMoveOuterResponseAcceptsMoveReadKind(t *testing.T) {
	scope := rangeMoveCodecIntent().source
	want := graphWireResponse{peer: graphPeerIdentity{scope: scope, voter: 1}, issuer: [16]byte{34}, kind: sourceMoveRead, sequence: 35, nonce: [16]byte{36}, body: []byte{37, 38}}
	wire, err := encodeGraphWireResponse(want)
	if err != nil {
		t.Fatal("outer response refused newly supported move read", err)
	}
	got, err := decodeGraphWireResponse(wire)
	if err != nil || got.err != nil || got.peer != want.peer || got.issuer != want.issuer || got.kind != want.kind || got.sequence != want.sequence || got.nonce != want.nonce || !bytes.Equal(got.body, want.body) {
		t.Fatal("outer response lost move read kind/body", got, err)
	}
}

func TestRangeMoveAuthorityDecodeIsInert(t *testing.T) {
	for _, present := range []bool{false, true} {
		p := rangeMoveAuthorityCodecFixture(t)
		if !present {
			p.observation.status = rangeMoveStatus{}
			p.observation.intent, p.observation.progress = nil, nil
			p = sealRangeMoveAuthority(p)
		}
		wire, err := encodeRangeMoveAuthorityBody(p)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeRangeMoveAuthorityBody(wire)
		want := p
		want.stamp = nil
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("authority body dropped correlated observation", present, got, err)
		}
		if got.stamp != nil || got.validSeal() {
			t.Fatal("decoder minted completed source authority")
		}
		if wire, err := encodeRangeMoveAuthorityBody(got); wire != nil || !errors.Is(err, errInvalid) {
			t.Fatal("inert decoded body was exportable as authority", err)
		}
		proof := rangeMoveProof{id: got.id, question: got.question, observation: got.observation}
		if err := checkRangeMoveProof(proof); !errors.Is(err, errInvalid) {
			t.Fatal("inert body became a native completed proof", err)
		}
	}
}

func rangeMoveNativeAbsenceAuthority(t *testing.T) (*allocationHost, rangeMoveAuthority) {
	t.Helper()
	n := newCrossPartitionNetworkAgreement(t, nil, rangeMoveSemanticContractID())
	h := n.hosts[0][0]
	q := rangeMoveQuery{move: [16]byte{31}, nonce: [16]byte{32}, sourceRows: 64, sourceBytes: 1 << 20, outputBytes: 1 << 20}
	event, err := h.ReadRangeMove(t.Context(), q)
	if err != nil {
		t.Fatal("issue genuine native move read", err)
	}
	queue, replies := event.packets, event.moveReplies
	for steps := 0; len(queue) > 0; steps++ {
		if steps >= 1024 {
			t.Fatal("native read exceeded finite packet schedule")
		}
		packet := queue[0]
		queue = queue[1:]
		if packet.Snapshot || packet.To < 1 || packet.To > 3 {
			t.Fatal("unexpected native read packet")
		}
		next, err := n.hosts[0][packet.To-1].Step(packet)
		if err != nil {
			t.Fatal(err)
		}
		if len(queue)+len(next.packets) > 128 || len(replies)+len(next.moveReplies) > 16 {
			t.Fatal("native read packet/reply bounds exceeded")
		}
		queue = append(queue, next.packets...)
		replies = append(replies, next.moveReplies...)
	}
	if len(replies) != 1 || replies[0].id != event.readID || replies[0].question != q || replies[0].err != nil {
		t.Fatal("missing matched actual native completion", replies)
	}
	p, err := h.exportRangeMoveAuthority(replies[0].proof)
	if err != nil || !p.validSeal() || p.observation.status != (rangeMoveStatus{}) || len(p.observation.intent)+len(p.observation.progress) != 0 {
		t.Fatal("native absence export counterpart", p, err)
	}
	return h, p
}

func TestRangeMoveAuthoritySealBindsWholeNativeObservation(t *testing.T) {
	_, native := rangeMoveNativeAbsenceAuthority(t)
	for _, tc := range []struct {
		name string
		edit func(*rangeMoveAuthority)
	}{
		{"missing-seal", func(p *rangeMoveAuthority) { p.stamp = nil }},
		{"scope", func(p *rangeMoveAuthority) { p.scope.partition++ }},
		{"read-id", func(p *rangeMoveAuthority) { p.id.sequence++ }},
		{"query-move", func(p *rangeMoveAuthority) { p.question.move[0] ^= 1 }},
		{"query-nonce", func(p *rangeMoveAuthority) { p.question.nonce[0] ^= 1 }},
		{"query-budget", func(p *rangeMoveAuthority) { p.question.sourceBytes++ }},
		{"observed-scope", func(p *rangeMoveAuthority) { p.observation.scope.ownership++ }},
		{"configuration", func(p *rangeMoveAuthority) { p.observation.configuration[0] ^= 1 }},
		{"index", func(p *rangeMoveAuthority) { p.observation.index++ }},
		{"floor", func(p *rangeMoveAuthority) { p.observation.floor++ }},
		{"root-generation", func(p *rangeMoveAuthority) { p.observation.root.Generation++ }},
		{"root-index", func(p *rangeMoveAuthority) { p.observation.root.Index++ }},
		{"root-hash", func(p *rangeMoveAuthority) { p.observation.root.ImageHash[0] ^= 1 }},
		{"root-image", func(p *rangeMoveAuthority) {
			p.observation.root.Image = bytes.Clone(p.observation.root.Image)
			p.observation.root.Image[0] ^= 1
		}},
		{"absent-status-index", func(p *rangeMoveAuthority) { p.observation.status.index++ }},
		{"absent-status-floor", func(p *rangeMoveAuthority) { p.observation.status.floor++ }},
		{"absent-status-namespace", func(p *rangeMoveAuthority) { p.observation.status.ns.partition++ }},
		{"intent", func(p *rangeMoveAuthority) { p.observation.intent = []byte{1} }},
		{"progress", func(p *rangeMoveAuthority) { p.observation.progress = []byte{1} }},
		{"spent-rows", func(p *rangeMoveAuthority) { p.observation.sourceRows++ }},
		{"spent-bytes", func(p *rangeMoveAuthority) { p.observation.sourceBytes++ }},
		{"owned-bytes", func(p *rangeMoveAuthority) { p.observation.ownedBytes++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := native
			if !p.validSeal() {
				t.Fatal("unchanged fixture seal refused")
			}
			tc.edit(&p)
			if p.validSeal() {
				t.Fatal("post-seal mutation retained completed authority")
			}
			if wire, err := encodeRangeMoveAuthorityBody(p); wire != nil || !errors.Is(err, errInvalid) {
				t.Fatal("altered authority exported bytes", err)
			}
		})
	}
}

func TestRangeMoveObservationHashSeparatesIntentAndProgress(t *testing.T) {
	// Pure observation data: a content hash must bind field boundaries without
	// any Host, native proof, or fabricated completed authority.
	before := rangeMoveObservation{intent: []byte{1, 2, 3}, progress: []byte{4, 5}}
	after := before
	after.intent, after.progress = []byte{1, 2}, []byte{3, 4, 5}
	if !bytes.Equal(append(bytes.Clone(before.intent), before.progress...), append(bytes.Clone(after.intent), after.progress...)) {
		t.Fatal("boundary redistribution fixture changed aggregate bytes")
	}
	if rangeMoveObservationHash(before) == rangeMoveObservationHash(after) {
		t.Fatal("intent/progress field redistribution preserved observation hash")
	}
}

func TestRangeMoveDecodedAbsenceCannotPrepareNativeCommand(t *testing.T) {
	// This is an absence refusal, not evidence of an unauthorized accepted move.
	// The decoder's separate seal/export assertions above establish inertness.
	h, native := rangeMoveNativeAbsenceAuthority(t)
	body, err := encodeRangeMoveAuthorityBody(native)
	if err != nil {
		t.Fatal(err)
	}
	inert, err := decodeRangeMoveAuthorityBody(body)
	if err != nil {
		t.Fatal(err)
	}
	intent := rangeMoveCodecIntent()
	intent.source = h.machine.scope()
	intent.target, err = h.target(8)
	if err != nil {
		t.Fatal(err)
	}
	intent.publication.Graph = graphstate.GraphID(intent.source.graph)
	intent.id = requestID{33}
	wire, err := encodeRangeMoveIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := graphstate.NewOutputBudget(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := h.prepareRangeMoveCommand(wire, rangeMoveFence, intent.source.partition, 0, nil, []rangeMoveAuthority{inert}, budget)
	if proposal.target != (allocationScope{}) || proposal.wire != nil || !errors.Is(err, errInvalid) {
		t.Fatal("decoded body granted command authority", proposal, err)
	}
}

func TestRangeMoveAuthorityWireRefusesUncorrelatedBodies(t *testing.T) {
	p := rangeMoveAuthorityCodecFixture(t)
	wire, err := encodeRangeMoveAuthorityBody(p)
	if err != nil {
		t.Fatal(err)
	}
	for n := range len(wire) {
		if got, err := decodeRangeMoveAuthorityBody(wire[:n]); !reflect.DeepEqual(got, rangeMoveAuthority{}) || !errors.Is(err, errCorrupt) {
			t.Fatal("truncated authority exported partial fields", n, err)
		}
	}
	query, err := encodeRangeMoveQuery(p.question)
	if err != nil {
		t.Fatal(err)
	}
	const queryLength = 4 + 120 + 16 + 8
	configuration := queryLength + 4 + len(query)
	index := configuration + 32
	root := index + 16
	rootLength := root + 48
	work := rootLength + 4 + 172
	statusLength := work + 12
	for _, tc := range []struct {
		name string
		edit func([]byte)
		want error
	}{
		{"old-page-body-version", func(b []byte) { b[3] = 2 }, errCorrupt},
		{"native-positive-page", func(b []byte) {
			binary.BigEndian.PutUint64(b[queryLength+4+36:queryLength+4+44], 1)
			nested := b[queryLength+4 : queryLength+4+len(query)]
			copy(nested, rangeMoveCodecReseal(nested))
		}, errCorrupt},
		{"old-binding", func(b []byte) { old := certifiedGraphSemanticContractID(); copy(b[92:124], old[:]) }, errCorrupt},
		{"missing-session", func(b []byte) { clear(b[124:140]) }, errCorrupt},
		{"missing-sequence", func(b []byte) { clear(b[140:148]) }, errCorrupt},
		{"query-length", func(b []byte) { binary.BigEndian.PutUint32(b[queryLength:queryLength+4], math.MaxUint32) }, errLimit},
		{"configuration", func(b []byte) { clear(b[configuration : configuration+32]) }, errCorrupt},
		{"future-root", func(b []byte) { binary.BigEndian.PutUint64(b[root+8:root+16], p.observation.index+1) }, errCorrupt},
		{"image-hash", func(b []byte) { b[root+16] ^= 1 }, errCorrupt},
		{"image-length", func(b []byte) { binary.BigEndian.PutUint32(b[rootLength:rootLength+4], 173) }, errLimit},
		{"spent-over-query", func(b []byte) { binary.BigEndian.PutUint32(b[work:work+4], uint32(p.question.sourceRows+1)) }, errCorrupt},
		{"spent-source-under-root", func(b []byte) { binary.BigEndian.PutUint32(b[work+4:work+8], 171) }, errCorrupt},
		{"owned-over-query", func(b []byte) { binary.BigEndian.PutUint32(b[work+8:work+12], uint32(p.question.outputBytes+1)) }, errCorrupt},
		{"status-length", func(b []byte) { binary.BigEndian.PutUint32(b[statusLength:statusLength+4], math.MaxUint32) }, errLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := bytes.Clone(wire)
			tc.edit(bad)
			if got, err := decodeRangeMoveAuthorityBody(rangeMoveCodecReseal(bad)); !reflect.DeepEqual(got, rangeMoveAuthority{}) || !errors.Is(err, tc.want) {
				t.Fatal("resealed uncorrelated body exported fields", err)
			}
		})
	}
	badSeal := bytes.Clone(wire)
	badSeal[len(badSeal)-1] ^= 1
	for _, bad := range [][]byte{badSeal, seal(append(bytes.Clone(wire[:len(wire)-32]), 0))} {
		if got, err := decodeRangeMoveAuthorityBody(bad); !reflect.DeepEqual(got, rangeMoveAuthority{}) || !errors.Is(err, errCorrupt) {
			t.Fatal("noncanonical authority body admitted", err)
		}
	}
	if got, err := decodeRangeMoveAuthorityBody(make([]byte, graphSourceFrameBytes-511)); !reflect.DeepEqual(got, rangeMoveAuthority{}) || !errors.Is(err, errLimit) {
		t.Fatal("outer authority bound widened", err)
	}
}

func TestRangeMovePointerDoorsRejectNil(t *testing.T) {
	if (*declaredMaterializer)(nil).rangeMoves() {
		t.Fatal("nil materializer claimed range handoff capability")
	}
	if m, err := newRangeMoveMaterializer(nil, namespace{}, graphstore.OwnershipDeclaration{}, defaultMaterializerLimits()); m != nil || !errors.Is(err, errInvalid) {
		t.Fatal("nil store constructed materializer", err)
	}
	if _, err := (*allocationHost)(nil).ReadRangeMove(t.Context(), rangeMoveAuthorityCodecFixture(t).question); !errors.Is(err, errInvalid) {
		t.Fatal("nil read Host", err)
	}
	if got, err := (*allocationHost)(nil).exportRangeMoveAuthority(rangeMoveProof{}); !reflect.DeepEqual(got, rangeMoveAuthority{}) || !errors.Is(err, errInvalid) {
		t.Fatal("nil Host exported authority", err)
	}
	if got, err := (*allocationHost)(nil).prepareRangeMoveCommand(nil, rangeMoveFence, 3, 0, nil, nil, nil); got.wire != nil || got.target != (allocationScope{}) || !errors.Is(err, errInvalid) {
		t.Fatal("nil Host returned command", err)
	}
}
