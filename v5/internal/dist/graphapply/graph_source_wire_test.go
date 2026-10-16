package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Written before principal validation. These are inert codec/budget guards,
// never evidence of Host admission, completed reads or authenticated authority.
// The principal records genuine RED output before any production correction.
func TestGraphSourceWireRequestAdmissionSymmetry(t *testing.T) {
	base := graphSourceRequest{kind: sourceGet, sequence: 7, nonce: [16]byte{3}, maximum: 128, rows: 1}
	for _, tc := range []struct {
		name string
		edit func(*graphSourceRequest)
		want error
	}{
		{"key-64KiB-plus-one", func(r *graphSourceRequest) { r.key = make([]byte, (64<<10)+1) }, errLimit},
		{"upper-64KiB-plus-one", func(r *graphSourceRequest) { r.upper = make([]byte, (64<<10)+1) }, errLimit},
		{"after-64KiB-plus-one", func(r *graphSourceRequest) { r.after = make([]byte, (64<<10)+1) }, errLimit},
		{"command-1MiB-plus-one", func(r *graphSourceRequest) { r.command = make([]byte, (1<<20)+1) }, errLimit},
		{"unknown-phase", func(r *graphSourceRequest) { r.phase = crossPhase(5) }, errInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := base
			tc.edit(&r)
			wire, err := encodeGraphSourceRequest(r)
			if !errors.Is(err, tc.want) || wire != nil {
				t.Fatalf("encoder admitted a request outside independently specified field/phase limits: bytes=%d error=%v want=%v", len(wire), err, tc.want)
			}
		})
	}
	for _, edit := range []func(*graphSourceRequest){
		func(r *graphSourceRequest) { r.key = make([]byte, 64<<10) },
		func(r *graphSourceRequest) { r.upper = make([]byte, 64<<10) },
		func(r *graphSourceRequest) { r.after = make([]byte, 64<<10) },
		func(r *graphSourceRequest) { r.command = make([]byte, 1<<20) },
		func(r *graphSourceRequest) { r.phase = crossPhase(4) },
	} {
		r := base
		edit(&r)
		wire, err := encodeGraphSourceRequest(r)
		if err != nil {
			t.Fatal("exact field boundary refused", err)
		}
		got, err := decodeGraphSourceRequest(wire)
		if err != nil || !bytes.Equal(got.key, r.key) || !bytes.Equal(got.upper, r.upper) || !bytes.Equal(got.after, r.after) || !bytes.Equal(got.command, r.command) || got.phase != r.phase {
			t.Fatal("encoder/decoder exact-boundary disagreement", err)
		}
	}
}

func TestGraphSourceWireInfoReadLimitAdmissionSymmetry(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	_, server, info, _ := sourceSessionFixture(t)
	t.Cleanup(func() { _ = server.close() })
	for _, tc := range []struct {
		name string
		edit func(*graphSourceInfo)
		want error
	}{
		{"rows-4096-plus-one", func(i *graphSourceInfo) { i.limits.Rows = 4097 }, errInvalid},
		// Already refused by the original u32 writer: this is a guard and
		// preserves its native limit classification, not an admission RED.
		{"bytes-64MiB-plus-one", func(i *graphSourceInfo) { i.limits.Bytes = (64 << 20) + 1 }, errLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invalid := info
			tc.edit(&invalid)
			wire, err := encodeGraphSourceInfo(invalid)
			if !errors.Is(err, tc.want) || wire != nil {
				t.Fatalf("encoder admitted read limits beyond decoder's independent immutable ceilings: bytes=%d err=%v", len(wire), err)
			}
		})
	}
}

func TestGraphSourceWirePageMustChargeFutureAndTombstoneVisits(t *testing.T) {
	// Independently written GSP1 header bytes reach the decoder regardless of
	// whether a corrected encoder rejects the same malformed page earlier.
	// Every visited logical key costs at least64 metadata +1 nonempty key byte.
	for _, body := range []string{
		"475350010000000100000000010000000000000000", // Visited1/Bytes0.
		"475350010000000100000040010000000000000000", // Visited1/Bytes64.
		"475350010000000200000081010000000000000000", // Visited2/Bytes129.
	} {
		page, err := decodeGraphSourcePage(wireGuardLiteral(t, body), raftlog.ReadBudget{Rows: 2, Bytes: 256})
		wireGuardPageZero(t, page, err, errCorrupt)
	}
	page, err := decodeGraphSourcePage(wireGuardLiteral(t, "475350010000000200000082010000000000000000"), raftlog.ReadBudget{Rows: 2, Bytes: 130})
	if err != nil || page.Visited != 2 || page.Bytes != 130 || !page.Complete || len(page.Rows) != 0 {
		t.Fatal("valid empty-output native work counterpart", page, err)
	}
}

func TestGraphSourceRemoteBudgetNilReceiveIsInvalid(t *testing.T) {
	// Matches admit's pointer/arena boundary. No fake source or budget arena.
	for _, b := range []*graphRemoteBudget{nil, {}} {
		if err := b.receive(0, 0); !errors.Is(err, errInvalid) {
			t.Fatal("invalid budget receiver/arena became a panic or successful reservation", err)
		}
	}
	arena, err := graphstate.NewOutputBudget(128)
	if err != nil {
		t.Fatal(err)
	}
	b := newGraphRemoteBudget(materializerLimits{sourceBytes: 1, sourceRows: 1}, arena)
	if err := b.receive(0, 0); err != nil || arena.Used() != 128 {
		t.Fatal("valid zero-payload response still reserves its fixed128 bytes", err)
	}
}

func wireGuardLiteral(t *testing.T, body string) []byte {
	t.Helper()
	b, err := hex.DecodeString(body)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	return append(b, h[:]...)
}
func wireGuardReseal(b []byte) {
	h := sha256.Sum256(b[:len(b)-32])
	copy(b[len(b)-32:], h[:])
}
func wireGuardPageZero(t *testing.T, page raftlog.ApplicationPage, err, want error) {
	t.Helper()
	if !errors.Is(err, want) || page.Rows != nil || page.Next != nil || page.Complete || page.Visited != 0 || page.Bytes != 0 {
		t.Fatal("page refusal returned partial output/lost sentinel", page, err, want)
	}
}
func wireGuardRequestZero(t *testing.T, r graphSourceRequest, err, want error) {
	t.Helper()
	if !errors.Is(err, want) || r.kind != 0 || r.sequence != 0 || r.nonce != ([16]byte{}) || r.cookie != (graphSourceCookie{}) || r.maximum != 0 || r.rows != 0 || r.key != nil || r.upper != nil || r.after != nil || r.command != nil || r.tx != ([16]byte{}) || r.phase != 0 {
		t.Fatal("request refusal returned partial fields/lost sentinel", r, err, want)
	}
}

func TestGraphSourceWireRequestLiteralKindsAndBorrowedLifetimeGuard(t *testing.T) {
	// GSQ1: sequence7, nonce3, issuer4/cookie11/secret5, maximum128,
	// rows2, key a-NUL-b, upper z, after a-NUL, command00ff, tx6, phase2.
	const literal = "475351010100000000000000070300000000000000000000000000000004000000000000000000000000000000000000000000000b05000000000000000000000000000000000000800000000200000003610062000000017a0000000261000000000200ff0600000000000000000000000000000002"
	for kind := byte(1); kind <= 8; kind++ {
		wire := wireGuardLiteral(t, literal)
		wire[4] = kind
		wireGuardReseal(wire)
		r, err := decodeGraphSourceRequest(wire)
		if err != nil || r.kind != kind || r.sequence != 7 || r.nonce != ([16]byte{3}) || r.cookie != (graphSourceCookie{issuer: [16]byte{4}, sequence: 11, secret: [16]byte{5}}) || r.maximum != 128 || r.rows != 2 || !bytes.Equal(r.key, []byte{'a', 0, 'b'}) || string(r.upper) != "z" || !bytes.Equal(r.after, []byte{'a', 0}) || !bytes.Equal(r.command, []byte{0, 255}) || r.tx != ([16]byte{6}) || r.phase != crossPhase(2) {
			t.Fatal("literal complete request fields/kind", kind, r, err)
		}
		encoded, err := encodeGraphSourceRequest(r)
		if err != nil || !bytes.Equal(encoded, wire) {
			t.Fatal("encoder changed independent literal wire", kind, err)
		}
		// Request payload spans intentionally borrow a caller-held frame; the
		// scalar cookie/nonce/tx arrays remain by-value. This grants no source.
		wire[81] = 'X'
		wire[13] = 99
		if r.key[0] != 'X' || r.nonce != ([16]byte{3}) {
			t.Fatal("borrowed fields/by-value metadata contract changed")
		}
	}
}

func TestGraphSourceWireRequestMalformedAndCountBeforeAllocationGuard(t *testing.T) {
	const literal = "475351010100000000000000070300000000000000000000000000000004000000000000000000000000000000000000000000000b05000000000000000000000000000000000000800000000200000003610062000000017a0000000261000000000200ff0600000000000000000000000000000002"
	wire := wireGuardLiteral(t, literal)
	for n := range len(wire) {
		got, err := decodeGraphSourceRequest(wire[:n])
		wireGuardRequestZero(t, got, err, errCorrupt)
	}
	for _, tc := range []struct {
		name string
		edit func([]byte)
		want error
	}{
		{"unknown-kind", func(b []byte) { b[4] = sourceReplacementRead + 1 }, errInvalid},
		{"zero-sequence", func(b []byte) { clear(b[5:13]) }, errInvalid},
		{"zero-nonce", func(b []byte) { clear(b[13:29]) }, errInvalid},
		{"maximum-frame-plus-one", func(b []byte) { binary.BigEndian.PutUint32(b[69:73], (2<<20)+1) }, errLimit},
		{"rows-65536-plus-one", func(b []byte) { binary.BigEndian.PutUint32(b[73:77], 65537) }, errLimit},
		{"key-huge-count", func(b []byte) { binary.BigEndian.PutUint32(b[77:81], ^uint32(0)) }, errLimit},
		{"key-count-exceeds-delivered", func(b []byte) { binary.BigEndian.PutUint32(b[77:81], 1000) }, errCorrupt},
		{"unknown-phase", func(b []byte) { b[len(b)-33] = 5 }, errInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			malformed := bytes.Clone(wire)
			tc.edit(malformed)
			wireGuardReseal(malformed)
			r, err := decodeGraphSourceRequest(malformed)
			wireGuardRequestZero(t, r, err, tc.want)
		})
	}
	for _, kind := range []byte{sourceCutRead, sourceCertifiedOpen} {
		known := bytes.Clone(wire)
		known[4] = kind
		wireGuardReseal(known)
		got, e := decodeGraphSourceRequest(known)
		if e != nil || got.kind != kind {
			t.Fatal("new inert request kind rejected", kind, e)
		}
		encoded, e := encodeGraphSourceRequest(got)
		if e != nil || !bytes.Equal(encoded, known) {
			t.Fatal("new kind canonical counterpart", kind, e)
		}
	}
	extra := append(bytes.Clone(wire[:len(wire)-32]), 0)
	h := sha256.Sum256(extra)
	extra = append(extra, h[:]...)
	r, err := decodeGraphSourceRequest(extra)
	wireGuardRequestZero(t, r, err, errInvalid)
	r, err = decodeGraphSourceRequest(make([]byte, (2<<20)+1))
	wireGuardRequestZero(t, r, err, errLimit)
	for _, bad := range []graphSourceRequest{{}, {kind: 1, sequence: 1, nonce: [16]byte{1}, maximum: -1}, {kind: 1, sequence: 1, nonce: [16]byte{1}, rows: -1}} {
		wire, err := encodeGraphSourceRequest(bad)
		if !errors.Is(err, errInvalid) || wire != nil {
			t.Fatal("invalid encoder fields", err)
		}
	}
}

func TestGraphSourceWirePageLiteralOwnershipAndPreciseBoundsGuard(t *testing.T) {
	// Two retained rows a="AA", a-NUL="" and a skipped future/tombstone z:
	// native work67+66+65+one-byte cursor=199. Owned row headers128 plus
	// three key bytes/two value bytes/one cursor byte=134, independently.
	const literal = "4753500100000003000000c700000000017a000000020000000161000000024141000000000261000000000000"
	wire := wireGuardLiteral(t, literal)
	page, err := decodeGraphSourcePage(wire, raftlog.ReadBudget{Rows: 3, Bytes: 199})
	if err != nil || page.Visited != 3 || page.Bytes != 199 || page.Complete || string(page.Next) != "z" || len(page.Rows) != 2 || cap(page.Rows) != 2 || string(page.Rows[0].Key) != "a" || string(page.Rows[0].Value) != "AA" || !bytes.Equal(page.Rows[1].Key, []byte{'a', 0}) || len(page.Rows[1].Value) != 0 {
		t.Fatal("independent full GSP1 page tuple", page, err)
	}
	ownedBytes := 64*cap(page.Rows) + cap(page.Next)
	for _, row := range page.Rows {
		ownedBytes += cap(row.Key) + cap(row.Value)
		if row.Deleted || cap(row.Key) != len(row.Key) || cap(row.Value) != len(row.Value) {
			t.Fatal("owned row capacity/tombstone contract", row)
		}
	}
	if ownedBytes != 134 || cap(page.Next) != 1 {
		t.Fatal("independent conservative owned capacity", ownedBytes)
	}
	encoded, err := encodeGraphSourcePage(page, 128)
	if err != nil || !bytes.Equal(encoded, wire) {
		t.Fatal("canonical page wire changed", err)
	}
	clear(wire)
	if string(page.Rows[0].Value) != "AA" || string(page.Next) != "z" || !bytes.Equal(page.Rows[1].Key, []byte{'a', 0}) {
		t.Fatal("owned decoded page retained frame alias")
	}
	for _, budget := range []raftlog.ReadBudget{{Rows: 2, Bytes: 199}, {Rows: 3, Bytes: 198}} {
		p, err := decodeGraphSourcePage(encoded, budget)
		wireGuardPageZero(t, p, err, errLimit)
	}
	p, err := decodeGraphSourcePage(encoded, raftlog.ReadBudget{})
	wireGuardPageZero(t, p, err, errInvalid)
	for n := range len(encoded) {
		p, err := decodeGraphSourcePage(encoded[:n], raftlog.ReadBudget{Rows: 3, Bytes: 199})
		wireGuardPageZero(t, p, err, errCorrupt)
	}
	if out, err := encodeGraphSourcePage(page, 127); !errors.Is(err, errLimit) || out != nil {
		t.Fatal("explicit encoder frame floor", err)
	}
}

func TestGraphSourceWirePageRejectsPhantomCursorAndMissingRows(t *testing.T) {
	// The byte fixtures are inert and deliberately do not claim knowledge of
	// a source's real key membership or EOF. Only impossible native shapes fail.
	for _, tc := range []struct {
		name, body string
		want       error
	}{
		{"cursor-without-visit", "47535001000000000000000100000000016100000000", errCorrupt},
		{"cursor-before-final-returned-key", "4753500100000003000000c7000000000161000000020000000161000000024141000000000261000000000000", errCorrupt},
		{"complete-with-cursor", "47535001000000010000004201000000016100000000", errCorrupt},
		{"incomplete-no-cursor", "475350010000000100000041000000000000000000", errCorrupt},
		{"phantom-row-with-zero-visits", "4753500100000000000000420100000000000000010000000161000000017800", errCorrupt},
		{"empty-logical-key", "47535001000000010000004101000000000000000100000000000000017800", errCorrupt},
		{"missing-declared-row", "475350010000000100000041010000000000000001", errCorrupt},
		{"huge-row-count", "4753500100000001000000410100000000ffffffff", errLimit},
		{"unknown-complete-flag", "475350010000000100000041020000000000000000", errCorrupt},
		{"returned-tombstone", "4753500100000001000000420100000000000000010000000161000000017801", errCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := decodeGraphSourcePage(wireGuardLiteral(t, tc.body), raftlog.ReadBudget{Rows: 3, Bytes: 256})
			wireGuardPageZero(t, p, err, tc.want)
		})
	}
	// Valid completed empty range consumes zero rows/work and has no cursor.
	p, err := decodeGraphSourcePage(wireGuardLiteral(t, "475350010000000000000000010000000000000000"), raftlog.ReadBudget{Rows: 1, Bytes: 1})
	if err != nil || !p.Complete || p.Visited != 0 || p.Bytes != 0 || len(p.Rows) != 0 || len(p.Next) != 0 {
		t.Fatal("valid empty native range counterpart", p, err)
	}
}

func wireGuardInfoZero(t *testing.T, i graphSourceInfo, err, want error) {
	t.Helper()
	if !errors.Is(err, want) || i.cookie != (graphSourceCookie{}) || i.readID != (allocationReadID{}) || i.nonce != ([16]byte{}) || i.observation != (allocationObservation{}) || i.root.Generation != 0 || i.root.Index != 0 || i.root.Image != nil || i.root.ImageHash != ([32]byte{}) || i.floor != 0 || i.limits != (raftlog.ReadBudget{}) || i.lifetime != 0 {
		t.Fatal("info refusal retained partial metadata/owner bytes", i, err, want)
	}
}

func TestGraphSourceWireInfoNativeFieldsOwnershipAndMismatchGuard(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	_, server, info, _ := sourceSessionFixture(t)
	t.Cleanup(func() { _ = server.close() })
	wire, err := encodeGraphSourceInfo(info)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeGraphSourceInfo(wire)
	if err != nil || got.cookie != info.cookie || got.readID != info.readID || got.nonce != info.nonce || got.observation != info.observation || got.root.Generation != info.root.Generation || got.root.Index != info.root.Index || got.root.ImageHash != info.root.ImageHash || !bytes.Equal(got.root.Image, info.root.Image) || got.floor != info.floor || got.limits != info.limits || got.lifetime != info.lifetime || cap(got.root.Image) != 172 {
		t.Fatal("complete native info and exact image ownership", got, err)
	}
	metadata, err := graphstore.DecodeRoot(got.root.Image)
	if err != nil || metadata.SemanticEpoch() != got.observation.epoch || metadata.EffectDigest() != got.observation.effect || metadata.Namespace().Partition != got.observation.source.scope.partition || [16]byte(metadata.Namespace().Graph) != [16]byte(got.observation.source.scope.graph) || got.observation.config.defaultAxis != info.observation.config.defaultAxis {
		t.Fatal("native root/configuration/default descriptor conservation", err)
	}
	clear(wire)
	if !bytes.Equal(got.root.Image, info.root.Image) || sha256.Sum256(got.root.Image) != got.root.ImageHash {
		t.Fatal("decoded info image borrowed the frame")
	}
	for _, tc := range []struct {
		name string
		edit func(*graphSourceInfo)
	}{
		{"cookie-session-mismatch", func(i *graphSourceInfo) { i.cookie.issuer[0] ^= 1 }},
		{"zero-cookie-sequence", func(i *graphSourceInfo) { i.cookie.sequence = 0 }},
		{"zero-cookie-secret", func(i *graphSourceInfo) { i.cookie.secret = [16]byte{} }},
		{"zero-read-sequence", func(i *graphSourceInfo) { i.readID.sequence = 0 }},
		{"zero-nonce", func(i *graphSourceInfo) { i.nonce = [16]byte{} }},
		{"root-source-index-mismatch", func(i *graphSourceInfo) { i.root.Index++ }},
		{"root-hash-mismatch", func(i *graphSourceInfo) { i.root.ImageHash[0] ^= 1 }},
		{"root-short-image", func(i *graphSourceInfo) { i.root.Image = i.root.Image[:171] }},
		{"configuration-hash-mismatch", func(i *graphSourceInfo) { i.observation.configuration[0] ^= 1 }},
		{"zero-lifetime", func(i *graphSourceInfo) { i.lifetime = 0 }},
		{"minute-plus-one", func(i *graphSourceInfo) { i.lifetime = time.Minute + 1 }},
		{"zero-row-limit", func(i *graphSourceInfo) { i.limits.Rows = 0 }},
		{"zero-byte-limit", func(i *graphSourceInfo) { i.limits.Bytes = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := info
			tc.edit(&bad)
			encoded, err := encodeGraphSourceInfo(bad)
			if !errors.Is(err, errInvalid) || encoded != nil {
				t.Fatal("inconsistent bounded info admitted", err)
			}
		})
	}
	valid, err := encodeGraphSourceInfo(info)
	if err != nil {
		t.Fatal(err)
	}
	for _, length := range []int{0, 35, len(valid) - 1} {
		i, err := decodeGraphSourceInfo(valid[:length])
		wireGuardInfoZero(t, i, err, errCorrupt)
	}
	i, err := decodeGraphSourceInfo(make([]byte, 4097))
	wireGuardInfoZero(t, i, err, errLimit)
	// Independent cookie/session offsets: issuer4..19 and readID session44..59.
	malformed := bytes.Clone(valid)
	malformed[4] ^= 1
	wireGuardReseal(malformed)
	i, err = decodeGraphSourceInfo(malformed)
	wireGuardInfoZero(t, i, err, errCorrupt)
}

func TestGraphSourceWireInfoZeroIssuerCannotBecomeSession(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	_, server, info, _ := sourceSessionFixture(t)
	t.Cleanup(func() { _ = server.close() })
	// Equality of two empty issuer/session arrays is not a session identity.
	info.cookie.issuer = [16]byte{}
	info.readID.session = [16]byte{}
	encoded, err := encodeGraphSourceInfo(info)
	if !errors.Is(err, errInvalid) || encoded != nil {
		t.Fatal("empty issuer/session pair passed the equality check", err)
	}
}

func wireGuardScope() allocationScope {
	// Inert syntactic fields only, not an authenticated endpoint or Host proof.
	return allocationScope{graph: [16]byte{1}, partition: 2, group: [16]byte{3}, ownership: 4, topology: 5, declaration: [32]byte{6}, semantic: [32]byte{7}}
}

func TestGraphSourceWireAuthorityBodyOwnedButNeverSealedGuard(t *testing.T) {
	manifest := []byte("opaque-manifest")
	tx := [16]byte{8}
	status := crossStatus{ns: namespace{graph: [16]byte{1}, partition: 2}, tx: tx, digest: sha256.Sum256(manifest), phase: crossPrepare, index: 9, floor: 3, round: 4, yes: true}
	body := crossAuthorityBody{scope: wireGuardScope(), id: allocationReadID{session: [16]byte{10}, sequence: 11}, question: crossQuery{tx: tx, nonce: [16]byte{12}, phase: crossPrepare}, index: 13, status: status, manifest: manifest}
	wire, err := encodeCrossAuthorityBody(body)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeCrossAuthorityBody(wire)
	if err != nil || got.scope != body.scope || got.id != body.id || got.question != body.question || got.index != 13 || got.status != status || !bytes.Equal(got.manifest, manifest) || cap(got.manifest) != len(manifest) {
		t.Fatal("inert complete authority body/capacity", got, err)
	}
	if (crossAuthority{crossAuthorityBody: got}).validSeal() {
		t.Fatal("decoding caller bytes minted native/authenticated provenance")
	}
	clear(wire)
	if string(got.manifest) != "opaque-manifest" {
		t.Fatal("owned decoded manifest retained input alias")
	}
	for _, edit := range []func(*crossAuthorityBody){
		func(p *crossAuthorityBody) { p.id.session = [16]byte{} },
		func(p *crossAuthorityBody) { p.id.sequence = 0 },
		func(p *crossAuthorityBody) { p.question.nonce = [16]byte{} },
		func(p *crossAuthorityBody) { p.question.tx[0] ^= 1 },
		func(p *crossAuthorityBody) { p.index = 8 },
		func(p *crossAuthorityBody) { p.status.digest[0] ^= 1 },
		func(p *crossAuthorityBody) { p.status.ns.partition++ },
	} {
		bad := body
		edit(&bad)
		encoded, err := encodeCrossAuthorityBody(bad)
		if !errors.Is(err, errInvalid) || encoded != nil {
			t.Fatal("inconsistent inert authority metadata admitted", err)
		}
	}
	valid, err := encodeCrossAuthorityBody(body)
	if err != nil {
		t.Fatal(err)
	}
	for _, length := range []int{0, 35, len(valid) - 1} {
		got, err := decodeCrossAuthorityBody(valid[:length])
		if !errors.Is(err, errCorrupt) || got.manifest != nil || got.scope != (allocationScope{}) || got.status != (crossStatus{}) {
			t.Fatal("authority malformed refusal retained body", got, err)
		}
	}
}

func TestGraphSourceWireResponseCorrelationAndErrorMaskGuard(t *testing.T) {
	base := graphWireResponse{peer: graphPeerIdentity{scope: wireGuardScope(), voter: 9}, issuer: [16]byte{10}, kind: sourceRoot, sequence: 11, nonce: [16]byte{12}, body: []byte("ok")}
	wire, err := encodeGraphWireResponse(base)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeGraphWireResponse(wire)
	if err != nil || got.peer != base.peer || got.issuer != base.issuer || got.kind != base.kind || got.sequence != 11 || got.nonce != base.nonce || string(got.body) != "ok" || got.err != nil {
		t.Fatal("exact inert response correlation fields", got, err)
	}
	// Caller-held outer bodies intentionally borrow the bounded reply frame;
	// nonce/issuer/association fields are values, not decoded authority.
	wire[219] = 'X'
	if string(got.body) != "Xk" || got.issuer != base.issuer {
		t.Fatal("outer response borrowing/by-value metadata contract")
	}
	causes := []error{errInvalid, errLimit, errCorrupt, replica.ErrUnavailable, replica.ErrStopped, raftlog.ErrInvalid, raftlog.ErrLimit, raftlog.ErrCorrupt, raftlog.ErrClosed, raftlog.ErrPoisoned, graphstore.ErrSourceUnavailable, context.Canceled, context.DeadlineExceeded, errRemoteOperationFailure, graphstore.ErrResourceLimit, graphstate.ErrResourceLimit}
	for i, cause := range causes {
		response := base
		response.body = nil
		response.err = errors.Join(errors.New("local detail"), cause)
		encoded, err := encodeGraphWireResponse(response)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeGraphWireResponse(encoded)
		if err != nil || len(decoded.body) != 0 || !errors.Is(decoded.err, cause) {
			t.Fatal("native error mask lost sentinel/retained body", cause, decoded.err, err)
		}
		for j, other := range causes {
			if errors.Is(decoded.err, other) != (i == j) {
				t.Fatal("single native mask acquired unrelated sentinel", i, j)
			}
		}
	}
	response := base
	response.body = nil
	response.err = errors.Join(errInvalid, raftlog.ErrClosed, context.Canceled)
	encoded, err := encodeGraphWireResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGraphWireResponse(encoded)
	if err != nil || !errors.Is(decoded.err, errInvalid) || !errors.Is(decoded.err, raftlog.ErrClosed) || !errors.Is(decoded.err, context.Canceled) || errors.Is(decoded.err, errCorrupt) {
		t.Fatal("joined native error mask parity", decoded.err, err)
	}
	response.err = errors.New("opaque operational cause")
	encoded, err = encodeGraphWireResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err = decodeGraphWireResponse(encoded)
	if err != nil || !errors.Is(decoded.err, errRemoteOperationFailure) || errors.Is(decoded.err, graphstore.ErrSourceUnavailable) || errors.Is(decoded.err, errCorrupt) {
		t.Fatal("unknown operational error fallback acquired corruption", decoded.err, err)
	}
	response = base
	response.err = errLimit
	if encoded, err := encodeGraphWireResponse(response); !errors.Is(err, errInvalid) || encoded != nil {
		t.Fatal("nonzero error admitted nonzero body", err)
	}
	valid, err := encodeGraphWireResponse(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func([]byte)
	}{
		{"error-with-body", func(b []byte) { binary.BigEndian.PutUint16(b[213:215], 1) }},
		{"zero-voter", func(b []byte) { clear(b[164:172]) }},
		{"zero-issuer", func(b []byte) { clear(b[172:188]) }},
		{"zero-sequence", func(b []byte) { clear(b[189:197]) }},
		{"zero-nonce", func(b []byte) { clear(b[197:213]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := bytes.Clone(valid)
			tc.edit(bad)
			wireGuardReseal(bad)
			r, err := decodeGraphWireResponse(bad)
			if !errors.Is(err, errCorrupt) || r.body != nil || r.err != nil || r.sequence != 0 || r.peer != (graphPeerIdentity{}) {
				t.Fatal("malformed response retained partial body/correlation", r, err)
			}
		})
	}
}

func TestGraphSourceRemoteBudgetCumulativeTwoEndpointsGuard(t *testing.T) {
	// Independently: each request2 owns4*2+512=520; each response3 owns
	// 4*3+128=140. Two endpoints consume1320 output,10 source bytes,3 rows.
	for _, allowance := range []int{1319, 1320} {
		arena, err := graphstate.NewOutputBudget(allowance)
		if err != nil {
			t.Fatal(err)
		}
		budget := newGraphRemoteBudget(materializerLimits{sourceBytes: 10, sourceRows: 3}, arena)
		endpointA, endpointB := budget, budget // One request ledger, never per-endpoint reset.
		if err := endpointA.admit(2, 3); err != nil {
			t.Fatal(err)
		}
		if err := endpointA.receive(3, 1); err != nil {
			t.Fatal(err)
		}
		if arena.Used() != 660 || budget.calls != 1 || budget.bytes != 5 || budget.rows != 1 {
			t.Fatal("independent first endpoint arithmetic", arena.Used())
		}
		if err := endpointB.admit(2, 3); err != nil {
			t.Fatal(err)
		}
		err = endpointB.receive(3, 2)
		if allowance == 1319 {
			if !errors.Is(err, errLimit) || !errors.Is(err, graphstate.ErrResourceLimit) || arena.Used() != 1180 || budget.calls != 2 || budget.bytes != 7 || budget.rows != 1 {
				t.Fatal("one-short reply did not preserve spent request/no refund", err, arena.Used())
			}
			continue
		}
		if err != nil || arena.Used() != 1320 || budget.calls != 2 || budget.bytes != 10 || budget.rows != 3 {
			t.Fatal("exact cumulative endpoints arithmetic", err, arena.Used())
		}
		for _, attempt := range []func() error{func() error { return endpointA.admit(1, 0) }, func() error { return endpointB.receive(1, 0) }, func() error { return endpointB.receive(0, 1) }} {
			if err := attempt(); !errors.Is(err, errLimit) || arena.Used() != 1320 || budget.calls != 2 || budget.bytes != 10 || budget.rows != 3 {
				t.Fatal("exhausted endpoints reset/refunded a shared ledger", err)
			}
		}
	}
}

func TestGraphSourceRemoteBudgetIndependentBoundsGuard(t *testing.T) {
	arena, err := graphstate.NewOutputBudget(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	budget := newGraphRemoteBudget(materializerLimits{sourceBytes: 1, sourceRows: 1}, arena)
	if budget.maxCalls != 1024 || budget.maxBytes != 1 || budget.maxRows != 1 || budget.output != arena {
		t.Fatal("configured independent remote ledger widened")
	}
	for _, attempt := range []func() error{func() error { return budget.admit(-1, 0) }, func() error { return budget.admit(0, -1) }, func() error { return budget.admit(0, (2<<20)+1) }, func() error { return (*graphRemoteBudget)(nil).admit(0, 0) }} {
		if err := attempt(); !errors.Is(err, errInvalid) || arena.Used() != 0 || budget.calls != 0 || budget.bytes != 0 || budget.rows != 0 {
			t.Fatal("invalid admission changed consumptive ledgers", err)
		}
	}
	if err := budget.receive(1, 1); err != nil || arena.Used() != 132 {
		t.Fatal("independent response bytes/rows", err)
	}
	for _, attempt := range []func() error{func() error { return budget.receive(-1, 0) }, func() error { return budget.receive(0, -1) }, func() error { return budget.receive(1, 0) }, func() error { return budget.receive(0, 1) }} {
		if err := attempt(); !errors.Is(err, errLimit) || arena.Used() != 132 || budget.bytes != 1 || budget.rows != 1 {
			t.Fatal("source/row cap bypass or failure refund", err)
		}
	}
	callsArena, err := graphstate.NewOutputBudget(1025 * 512)
	if err != nil {
		t.Fatal(err)
	}
	calls := newGraphRemoteBudget(materializerLimits{sourceBytes: 1, sourceRows: 1}, callsArena)
	for range 1024 {
		if err := calls.admit(0, 0); err != nil {
			t.Fatal("finite calls counterpart", err)
		}
	}
	if err := calls.admit(0, 0); !errors.Is(err, errLimit) || calls.calls != 1024 || callsArena.Used() != 1024*512 {
		t.Fatal("call ceiling reset by endpoint continuation", err)
	}
}

func TestGraphSourceWireResourceAndUnknownErrorsRemainDistinct(t *testing.T) {
	base := graphWireResponse{peer: graphPeerIdentity{scope: wireGuardScope(), voter: 9}, issuer: [16]byte{10}, kind: sourceGet, sequence: 11, nonce: [16]byte{12}}
	for _, cause := range []error{graphstore.ErrResourceLimit, graphstate.ErrResourceLimit, errors.New("unclassified backend operation")} {
		response := base
		response.err = cause
		wire, err := encodeGraphWireResponse(response)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeGraphWireResponse(wire)
		if err != nil || got.err == nil || errors.Is(got.err, graphstore.ErrSourceUnavailable) {
			t.Fatal("non-availability error claimed known source availability refusal", cause, got.err, err)
		}
		if (errors.Is(cause, graphstore.ErrResourceLimit) || errors.Is(cause, graphstate.ErrResourceLimit)) && !errors.Is(got.err, cause) {
			t.Fatal("native resource sentinel lost at wire boundary", cause, got.err)
		}
	}
}

func TestGraphSourceWireUnknownOperationIsNotAvailability(t *testing.T) {
	response := graphWireResponse{peer: graphPeerIdentity{scope: wireGuardScope(), voter: 9}, issuer: [16]byte{10}, kind: sourceGet, sequence: 11, nonce: [16]byte{12}, err: errors.New("unclassified backend operation")}
	wire, err := encodeGraphWireResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeGraphWireResponse(wire)
	if err != nil || got.err == nil || errors.Is(got.err, graphstore.ErrSourceUnavailable) || errors.Is(got.err, errLimit) || errors.Is(got.err, errCorrupt) {
		t.Fatal("unknown cause impersonated known native sentinel", got.err, err)
	}
}
