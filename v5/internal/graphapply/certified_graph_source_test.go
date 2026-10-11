package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func certifiedSourceInfoFixture(t *testing.T) (*allocationTestNetwork, *graphSourceServer, graphCertifiedSourceInfo) {
	t.Helper()
	n := newCertifiedGraphNetwork(t)
	crossEdgeFixture(t, n)
	c, _ := certifiedCoordinator(n)
	read := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	cut, err := read.Cut()
	if err != nil || len(cut.certificates) != 2 {
		t.Fatal("genuine persisted cut prerequisite", err)
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	h := n.hosts[0][0]
	server, err := newGraphSourceServer(t.Context(), h, defaultGraphSourceLimits(h.machine.limits))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.close(); err != nil {
			t.Error(err)
		}
	})
	info, err := server.openCertified(t.Context(), cut.certificates[0], [16]byte{19})
	if err != nil {
		t.Fatal("native persisted certificate capture prerequisite", err)
	}
	return n, server, info
}

func assertCertifiedSourceInfoZero(t *testing.T, info graphCertifiedSourceInfo, err, want error) {
	t.Helper()
	if !errors.Is(err, want) || info.cookie != (graphSourceCookie{}) || info.nonce != ([16]byte{}) || info.certificate != (graphCertificate{}) || info.configuration != (genesisAllocationConfig{}) || info.root.Generation != 0 || info.root.Index != 0 || info.root.ImageHash != ([32]byte{}) || info.root.Image != nil || info.limits != (raftlog.ReadBudget{}) || info.lifetime != 0 {
		t.Fatal("certified source refusal retained partial metadata or lost sentinel", info, err)
	}
}

func TestCertifiedGraphSourceInfoCodecOwnsNativeHistoricalMetadataAndRejectsInvalidFields(t *testing.T) {
	_, _, info := certifiedSourceInfoFixture(t)
	wire, err := encodeGraphCertifiedSourceInfo(info)
	if err != nil || len(wire) > 4096 {
		t.Fatal("bounded genuine historical info", err)
	}
	decoded, err := decodeGraphCertifiedSourceInfo(wire)
	if err != nil || decoded.cookie != info.cookie || decoded.nonce != info.nonce || decoded.certificate != info.certificate || decoded.configuration != info.configuration || decoded.root.Generation != info.root.Generation || decoded.root.Index != info.root.Index || decoded.root.ImageHash != info.root.ImageHash || !bytes.Equal(decoded.root.Image, info.root.Image) || cap(decoded.root.Image) != 172 || decoded.limits != info.limits || decoded.lifetime != info.lifetime {
		t.Fatal("complete native capture metadata not conserved", err)
	}
	clear(wire)
	if !bytes.Equal(decoded.root.Image, info.root.Image) || sha256.Sum256(decoded.root.Image) != decoded.root.ImageHash {
		t.Fatal("decoded historical image borrowed frame storage")
	}
	for _, tc := range []struct {
		name string
		edit func(*graphCertifiedSourceInfo)
	}{
		{"zero-certificate-round", func(i *graphCertifiedSourceInfo) { i.certificate.round = 0 }},
		{"empty-configuration", func(i *graphCertifiedSourceInfo) { i.configuration = genesisAllocationConfig{} }},
		{"configuration-digest-mismatch", func(i *graphCertifiedSourceInfo) { i.configuration.maxBlock++ }},
		{"zero-cookie-issuer", func(i *graphCertifiedSourceInfo) { i.cookie.issuer = [16]byte{} }},
		{"zero-cookie-sequence", func(i *graphCertifiedSourceInfo) { i.cookie.sequence = 0 }},
		{"zero-cookie-secret", func(i *graphCertifiedSourceInfo) { i.cookie.secret = [16]byte{} }},
		{"zero-nonce", func(i *graphCertifiedSourceInfo) { i.nonce = [16]byte{} }},
		{"zero-generation", func(i *graphCertifiedSourceInfo) { i.root.Generation = 0 }},
		{"wrong-source-index", func(i *graphCertifiedSourceInfo) { i.root.Index++ }},
		{"short-image", func(i *graphCertifiedSourceInfo) { i.root.Image = i.root.Image[:171] }},
		{"wrong-image", func(i *graphCertifiedSourceInfo) {
			i.root.Image[0] ^= 1
			i.root.ImageHash = sha256.Sum256(i.root.Image)
		}},
		{"wrong-image-hash", func(i *graphCertifiedSourceInfo) { i.root.ImageHash[0] ^= 1 }},
		{"zero-row-limit", func(i *graphCertifiedSourceInfo) { i.limits.Rows = 0 }},
		{"excess-row-limit", func(i *graphCertifiedSourceInfo) { i.limits.Rows = 4097 }},
		{"zero-byte-limit", func(i *graphCertifiedSourceInfo) { i.limits.Bytes = 0 }},
		{"excess-byte-limit", func(i *graphCertifiedSourceInfo) { i.limits.Bytes = (64 << 20) + 1 }},
		{"zero-lifetime", func(i *graphCertifiedSourceInfo) { i.lifetime = 0 }},
		{"negative-lifetime", func(i *graphCertifiedSourceInfo) { i.lifetime = -1 }},
		{"excess-lifetime", func(i *graphCertifiedSourceInfo) { i.lifetime = time.Minute + 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := info
			bad.root.Image = bytes.Clone(info.root.Image)
			tc.edit(&bad)
			if wire, err := encodeGraphCertifiedSourceInfo(bad); wire != nil || !errors.Is(err, errInvalid) {
				t.Fatal("inert invalid capture metadata admitted", err)
			}
		})
	}
	boundary := info
	boundary.limits, boundary.lifetime = raftlog.ReadBudget{Rows: 4096, Bytes: 64 << 20}, time.Minute
	if _, err := encodeGraphCertifiedSourceInfo(boundary); err != nil {
		t.Fatal("exact codec ceilings refused", err)
	}
}

func TestCertifiedGraphSourceInfoDecoderRejectsTruncatedResealedAndOversizedMetadata(t *testing.T) {
	_, _, info := certifiedSourceInfoFixture(t)
	wire, err := encodeGraphCertifiedSourceInfo(info)
	if err != nil {
		t.Fatal(err)
	}
	for length := range len(wire) {
		got, err := decodeGraphCertifiedSourceInfo(wire[:length])
		assertCertifiedSourceInfoZero(t, got, err, errCorrupt)
	}
	got, err := decodeGraphCertifiedSourceInfo(make([]byte, 4097))
	assertCertifiedSourceInfoZero(t, got, err, errLimit)
	cert, err := encodeGraphCertificate(info.certificate)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := encodeGraphGenesisConfig(info.configuration)
	if err != nil {
		t.Fatal(err)
	}
	configCount := 64 + len(cert)
	root := configCount + 4 + len(cfg)
	rows := root + 8 + 8 + 32 + 4 + 172
	for _, tc := range []struct {
		name string
		edit func([]byte)
		want error
	}{
		{"zero-issuer", func(b []byte) { clear(b[4:20]) }, errCorrupt},
		{"zero-sequence", func(b []byte) { clear(b[20:28]) }, errCorrupt},
		{"zero-secret", func(b []byte) { clear(b[28:44]) }, errCorrupt},
		{"zero-nonce", func(b []byte) { clear(b[44:60]) }, errCorrupt},
		{"huge-certificate-count", func(b []byte) { binary.BigEndian.PutUint32(b[60:64], math.MaxUint32) }, errLimit},
		{"huge-config-count", func(b []byte) { binary.BigEndian.PutUint32(b[configCount:configCount+4], math.MaxUint32) }, errLimit},
		{"bad-certificate-checksum", func(b []byte) { b[64+len(cert)-1] ^= 1 }, errCorrupt},
		{"bad-configuration-checksum", func(b []byte) { b[configCount+4+len(cfg)-1] ^= 1 }, errCorrupt},
		{"zero-generation", func(b []byte) { clear(b[root : root+8]) }, errCorrupt},
		{"wrong-index", func(b []byte) { binary.BigEndian.PutUint64(b[root+8:root+16], info.root.Index+1) }, errCorrupt},
		{"wrong-hash", func(b []byte) { b[root+16] ^= 1 }, errCorrupt},
		{"rows-over-limit", func(b []byte) { binary.BigEndian.PutUint32(b[rows:rows+4], 4097) }, errLimit},
		{"bytes-over-limit", func(b []byte) { binary.BigEndian.PutUint32(b[rows+4:rows+8], (64<<20)+1) }, errLimit},
		{"zero-lifetime", func(b []byte) { clear(b[rows+8 : rows+16]) }, errCorrupt},
		{"excess-lifetime", func(b []byte) { binary.BigEndian.PutUint64(b[rows+8:rows+16], uint64(time.Minute)+1) }, errCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := bytes.Clone(wire)
			tc.edit(bad)
			wireGuardReseal(bad)
			got, err := decodeGraphCertifiedSourceInfo(bad)
			assertCertifiedSourceInfoZero(t, got, err, tc.want)
		})
	}
	extra := seal(append(bytes.Clone(wire[:len(wire)-32]), 0))
	got, err = decodeGraphCertifiedSourceInfo(extra)
	assertCertifiedSourceInfoZero(t, got, err, errCorrupt)
}

func TestCertifiedGraphSourceNativeCaptureRetainsExactHistoricalRootAndOwnsSessionLifetime(t *testing.T) {
	n, server, first := certifiedSourceInfoFixture(t)
	h := n.hosts[0][0]
	lease, err := server.lease(first.cookie)
	if err != nil {
		t.Fatal(err)
	}
	native, err := lease.view.RootBounded(t.Context(), 172)
	if err != nil || first.root.Generation != native.Generation || first.root.Index != first.certificate.sourceIndex || native.Index != first.root.Index || native.ImageHash != first.root.ImageHash || !bytes.Equal(native.Image, first.certificate.image[:]) {
		t.Fatal("capture substituted current or different-generation root", first.root, native, err)
	}
	all, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "after-capture", Scope: all}}, 27, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, proposal, reasonNone)
	second, err := server.openCertified(t.Context(), first.certificate, [16]byte{20})
	if err != nil || second.cookie == first.cookie || second.root.Generation != native.Generation || second.root.Index != native.Index || second.root.ImageHash != native.ImageHash || !bytes.Equal(second.root.Image, native.Image) {
		t.Fatal("later native installation retargeted certified historical capture", second.root, err)
	}
	clear(second.root.Image)
	root, err := server.root(t.Context(), second.cookie, 172)
	if err != nil || root.ImageHash != native.ImageHash || !bytes.Equal(root.Image, native.Image) {
		t.Fatal("returned capture metadata aliased owned lease root", root, err)
	}
	third, err := server.openCertified(t.Context(), first.certificate, [16]byte{21})
	assertCertifiedSourceInfoZero(t, third, err, errLimit)
	if err := server.closeSource(first.cookie); err != nil {
		t.Fatal(err)
	}
	if _, err := lease.view.Root(); !errors.Is(err, raftlog.ErrClosed) || !lease.released.Load() {
		t.Fatal("cookie close did not release genuine retained native view", err)
	}
	third, err = server.openCertified(t.Context(), first.certificate, [16]byte{22})
	if err != nil || third.cookie.sequence <= second.cookie.sequence || !bytes.Equal(third.root.Image, native.Image) {
		t.Fatal("released certified slot was not reusable", err)
	}
	if err := server.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Tick(); err != nil {
		t.Fatal("source owner close also closed borrowed Host", err)
	}
}

func TestCertifiedGraphSourceNativeCaptureRefusesWrongMissingCancelledAndClosedAuthority(t *testing.T) {
	n, server, info := certifiedSourceInfoFixture(t)
	if err := server.closeSource(info.cookie); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	var nilServer *graphSourceServer
	got, err := nilServer.openCertified(t.Context(), info.certificate, [16]byte{1})
	assertCertifiedSourceInfoZero(t, got, err, errInvalid)
	//nolint:staticcheck // Nil context is an explicit input-refusal contract.
	got, err = server.openCertified(nil, info.certificate, [16]byte{1})
	assertCertifiedSourceInfoZero(t, got, err, errInvalid)
	got, err = server.openCertified(t.Context(), info.certificate, [16]byte{})
	assertCertifiedSourceInfoZero(t, got, err, errInvalid)
	got, err = server.openCertified(cancelled, info.certificate, [16]byte{1})
	assertCertifiedSourceInfoZero(t, got, err, context.Canceled)
	for _, tc := range []struct {
		name string
		edit func(*graphCertificate)
		want error
	}{
		{"foreign-scope", func(c *graphCertificate) { c.scope.partition = 8 }, errInvalid},
		{"changed-persisted-image", func(c *graphCertificate) { c.image[0] ^= 1 }, errInvalid},
		{"missing-certificate", func(c *graphCertificate) { c.round++ }, replica.ErrUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cert := info.certificate
			tc.edit(&cert)
			got, err := server.openCertified(t.Context(), cert, [16]byte{2})
			assertCertifiedSourceInfoZero(t, got, err, tc.want)
			if server.pending != 0 {
				t.Fatal("failed capture leaked an admitted pending session")
			}
		})
	}
	// Capture itself owns an8192-byte representation. Refusal after retaining
	// the genuine historical view must close it and leave the slot available.
	server.limits.bytes = 8191
	got, err = server.openCertified(t.Context(), info.certificate, [16]byte{3})
	assertCertifiedSourceInfoZero(t, got, err, errLimit)
	server.limits.bytes = defaultGraphSourceLimits(n.hosts[0][0].machine.limits).bytes
	valid, err := server.openCertified(t.Context(), info.certificate, [16]byte{4})
	if err != nil {
		t.Fatal("failed retained capture prevented funded counterpart", err)
	}
	if err := server.closeSource(valid.cookie); err != nil {
		t.Fatal(err)
	}
	if err := server.close(); err != nil {
		t.Fatal(err)
	}
	got, err = server.openCertified(t.Context(), info.certificate, [16]byte{5})
	assertCertifiedSourceInfoZero(t, got, err, replica.ErrStopped)
	if _, err := n.hosts[0][0].Tick(); err != nil {
		t.Fatal("failed capture changed borrowed native Host lifetime", err)
	}
	if _, err := server.lease(info.cookie); !errors.Is(err, replica.ErrStopped) {
		t.Fatal("closed certified source remained queryable", err)
	}
	borrowed, err := n.hosts[0][0].machine.store.ApplicationView(info.root.Index)
	if err != nil {
		t.Fatal("session owner closed borrowed underlying store", err)
	}
	if err := borrowed.Close(); err != nil {
		t.Fatal("borrowed underlying store retained view cleanup", err)
	}
}

func TestCertifiedGraphSourceAuthenticatedCaptureChecksBindingBudgetAndCancellationLifetime(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	crossEdgeFixture(t, n)
	native, _ := certifiedCoordinator(n)
	read := certifiedOpen(t, native, graphReadRequest{mode: graphReadFresh}, nil)
	cut, err := read.Cut()
	if err != nil {
		t.Fatal(err)
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	cert := cut.certificates[0]
	c := remoteCertifiedFixture(t, n)
	backend := remoteCertifiedReceiptBackend(t, c)
	endpoint := backend.endpoints[3]
	assertRefusal := func(source *remoteGraphSource, cfg genesisAllocationConfig, err, want error) {
		t.Helper()
		if source != nil || cfg != (genesisAllocationConfig{}) || !errors.Is(err, want) {
			t.Fatal("failed authenticated capture retained source/configuration", source, cfg, err)
		}
	}
	var nilEndpoint *graphAuthenticatedEndpoint
	source, cfg, err := nilEndpoint.openCertifiedSource(t.Context(), cert)
	assertRefusal(source, cfg, err, errInvalid)
	//nolint:staticcheck // Nil context is an explicit input-refusal contract.
	source, cfg, err = endpoint.openCertifiedSource(nil, cert)
	assertRefusal(source, cfg, err, errInvalid)
	foreign := cert
	foreign.scope.partition = 8
	source, cfg, err = endpoint.openCertifiedSource(t.Context(), foreign)
	assertRefusal(source, cfg, err, errInvalid)
	invalid := cert
	invalid.round = 0
	source, cfg, err = endpoint.openCertifiedSource(t.Context(), invalid)
	assertRefusal(source, cfg, err, errInvalid)
	tiny := remoteCertifiedReceiptBackend(t, c)
	tiny.budget.maxBytes = 2047 // The original cleanup ledger requires2048.
	source, cfg, err = tiny.endpoints[3].openCertifiedSource(t.Context(), cert)
	assertRefusal(source, cfg, err, errLimit)
	missing := cert
	missing.round++
	source, cfg, err = endpoint.openCertifiedSource(t.Context(), missing)
	assertRefusal(source, cfg, err, replica.ErrUnavailable)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	source, cfg, err = endpoint.openCertifiedSource(cancelled, cert)
	assertRefusal(source, cfg, err, context.Canceled)
	source, cfg, err = endpoint.openCertifiedSource(t.Context(), cert)
	if err != nil || source == nil || source.certified == nil || source.certified.certificate != cert || source.info.root.Index != cert.sourceIndex || !bytes.Equal(source.info.root.Image, cert.image[:]) || cfg != source.certified.configuration {
		t.Fatal("genuine authenticated certificate capture failed", cfg, err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Error(err)
		}
	})
	lease, err := n.hosts[0][0].sourceServer.lease(source.info.cookie)
	if err != nil {
		t.Fatal("authenticated capture lacks genuine server-owned session", err)
	}
	root, err := source.RootBounded(t.Context(), 172)
	if err != nil || root.Index != cert.sourceIndex || root.Generation != source.info.root.Generation || !bytes.Equal(root.Image, cert.image[:]) {
		t.Fatal("authenticated retained root mismatch", root, err)
	}
	clear(root.Image)
	if !bytes.Equal(source.info.root.Image, cert.image[:]) {
		t.Fatal("metadata accessor exposed remote retained image backing")
	}
	var nilSource *remoteGraphSource
	nilSource.cancelCertifiedReads()
	source.cancelCertifiedReads()
	if root, err := source.RootBounded(t.Context(), 172); !errors.Is(err, context.Canceled) || root.Image != nil {
		t.Fatal("cancelled certified owner returned metadata", root, err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if !lease.released.Load() {
		t.Fatal("remote close did not release genuine certified native session")
	}
	if _, err := lease.view.Root(); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("remote close left historical native view open", err)
	}
	if _, err := n.hosts[0][0].Tick(); err != nil {
		t.Fatal("remote source cleanup closed borrowed native Host", err)
	}
}
