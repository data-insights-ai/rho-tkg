package graphapply

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// The new marker/format selects only the fresh admitted fixture. Wire recipes
// never supply a native pin, a source lease, a collector cursor or a GC grant.
func nativeFeedRetainedInputWire(q nativeFeedInput) ([]byte, error) {
	wire, err := nativeFeedInputWire(q)
	if err != nil {
		return nil, err
	}
	return append([]byte(nativeFeedRetainedMarker), wire[len(nativeFeedMarker):]...), nil
}

func nativeFeedRetainedInputDecode(r processRequest) (nativeFeedInput, error) {
	return nativeScalarFeedInputDecode(r, nativeFeedRetainedMarker, true)
}

func nativeFeedRetainedCutDecode(wire []byte) (graphCut, error) {
	cut, err := nativeScalarWorkloadCutDecode(wire, retainedOnlyFeedSemanticContractID(), 2, 2)
	if err != nil || cut.certificates[0].scope.partition != 3 || cut.certificates[1].scope.partition != 8 {
		return graphCut{}, errors.Join(errInvalid, err)
	}
	return cut, nil
}

func nativeFeedRetainedTerminalControl(r processRequest) bool {
	q, err := nativeFeedRetainedInputDecode(r)
	return err == nil && (q.Action == "close" || q.Action == "release" || q.Action == "snapshot-close")
}

func nativeFeedRetainedResponseCode(r processRequest, cause error) byte {
	if _, err := nativeFeedRetainedInputDecode(r); err != nil {
		return 0
	}
	return nativeColumnProcessErrorCode(cause)
}

type nativeFeedRetainedSessions struct {
	mu                  sync.Mutex
	core                *nativeFeedSessions
	ctx                 context.Context
	cleanup             context.Context
	cancel              context.CancelFunc
	spec                nativeFeedRetainedSpec
	admit               nativeFeedRetainedAdmission
	calls, cleanupCalls uint64
	closed              bool
}

func newNativeFeedRetainedSessions(ctx context.Context, c *crossProcessCoordinator, cost *nativeGraphCostSpec) (*nativeFeedRetainedSessions, error) {
	if cost == nil || cost.Version != 7 {
		return nil, nil
	}
	if ctx == nil || c == nil || c.host == nil || c.host.machine == nil || c.host.machine.SemanticContractID() != retainedOnlyFeedSemanticContractID() {
		return nil, errInvalid
	}
	spec, err := nativeFeedRetainedSpecFromEnv()
	if err != nil || spec.cost() != *cost {
		return nil, errors.Join(errInvalid, err)
	}
	a, _, err := nativeFeedRetainedAdmissionFromEnv(spec)
	deadline, bounded := ctx.Deadline()
	if err != nil || !bounded || deadline.UnixNano() != a.DeadlineUnixNanos || [16]byte(c.host.machine.ns.graph) != a.Graph || c.host.machine.declaration.TopologyEpoch() != a.Topology || c.host.machine.declaration.Digest() != a.Declaration {
		return nil, errors.Join(errInvalid, err)
	}
	cleanup, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline.Add(30*time.Second))
	return &nativeFeedRetainedSessions{core: &nativeFeedSessions{ctx: ctx, coordinator: c}, ctx: ctx, cleanup: cleanup, cancel: cancel, spec: spec, admit: a}, nil
}

func (s *nativeFeedRetainedSessions) serve(ctx context.Context, r processRequest) ([]byte, error) {
	if s == nil || ctx == nil {
		return nil, errInvalid
	}
	q, err := nativeFeedRetainedInputDecode(r)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errRetainedFeedClosed
	}
	terminal := nativeFeedRetainedTerminalControl(r)
	parent := s.ctx
	if terminal {
		// Two owners (snapshot+feed), each up to eight Close/Release exchanges.
		// No source or normal operation can spend or renew this cleanup lane.
		if s.cleanupCalls >= 32 {
			return nil, errLimit
		}
		s.cleanupCalls++
		parent = s.cleanup
	} else {
		ceiling := 8*nativeFeedRetainedPages(s.spec.cost()) + s.spec.Facts/4 + s.admit.BudgetSeconds/60 + 2048
		if s.calls >= ceiling {
			return nil, errLimit
		}
		s.calls++ // failed, cancelled and unknown exchanges never refund
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	operation, cancel := context.WithCancel(parent)
	stop := func() bool { return false }
	if !terminal {
		stop = context.AfterFunc(ctx, cancel)
	}
	defer func() { stop(); cancel() }()
	if err := operation.Err(); err != nil {
		return nil, err
	}
	if q.Action == "records" {
		return nativeFeedRetainedRecords(operation, s.core.coordinator.host, q)
	}
	// The supplied policy, exact decoder and cost coordinates come solely from
	// this immutable fresh admission. The old Cost5 wrapper passes its own exact
	// values and never accepts this marker or the retained-only action.
	return s.core.serveDecoded(operation, r, q, nativeFeedRetainedPolicy(s.spec.cost(), s.admit.DeadlineUnixNanos), nativeFeedRetainedCutDecode, parent, s.spec.cost(), s.admit.BudgetSeconds)
}

func (s *nativeFeedRetainedSessions) close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	defer s.cancel()
	return s.core.close()
}

// The native event owner has no diagnostic/VFS impersonation. It shares the
// actual Host→Driver→Stage→Store algorithm; only the fresh checked accounting
// profile differs. One successful physical page advances at least one retained
// row or one of five sections, so the admitted record ceiling+six bounds the
// complete scan even when each page processes only one actual row.
func newNativeFeedRetainedGC(ctx context.Context, h *allocationHost, member uint64, spec nativeFeedRetainedSpec, a nativeFeedRetainedAdmission) (*nativeRetentionOwner, error) {
	if ctx == nil || h == nil || h.machine == nil || h.driver == nil || nativeFeedRetainedValidate(spec.cost()) != nil || a.Spec != spec || h.machine.SemanticContractID() != retainedOnlyFeedSemanticContractID() || h.machine.store.ApplicationRetention().Version != 1 || member < 1 || member > 6 {
		return nil, errInvalid
	}
	deadline, bounded := ctx.Deadline()
	if !bounded || deadline.UnixNano() != a.DeadlineUnixNanos || [16]byte(h.machine.ns.graph) != a.Graph || h.machine.declaration.Digest() != a.Declaration || h.machine.declaration.TopologyEpoch() != a.Topology || h.machine.ns.partition != 3+5*((member-1)/3) {
		return nil, errInvalid
	}
	expected, _, err := nativeFeedRetainedAdmissionFromEnv(spec)
	if err != nil || expected != a {
		return nil, errors.Join(errInvalid, err)
	}
	p := h.machine.store.ApplicationLimits()
	if p.RetainedApplicationRecords > ^uint64(0)-134 {
		return nil, errLimit
	}
	pages := p.RetainedApplicationRecords + 6
	var run [16]byte
	decoded, err := hex.DecodeString(a.Run)
	if err != nil || len(decoded) != len(run) || hex.EncodeToString(decoded) != a.Run || bytes.Equal(decoded, make([]byte, 16)) {
		return nil, errInvalid
	}
	copy(run[:], decoded)
	base := nativeRetentionReceipt{Version: 1, Profile: nativeFeedRetainedFormat, Run: run, Member: member, PID: uint64(os.Getpid()), DeadlineUnixNanos: a.DeadlineUnixNanos}
	return newNativeRetentionOwnerFor(ctx, h, base, pages+128, pages, min(512, p.MaxPageRows), min(1<<20, p.MaxInstallBytes), true)
}

// BeforeACK labels only an original proven by the current actual native GFC
// cursor and immutable original. It never means that historical CDC was empty.
// The independent oracle joins the first-install bytes in its pre-GC capture.
type nativeFeedRetainedRecordPage struct {
	Page      nativeFeedRecordPage
	BeforeACK []bool
}

func nativeFeedRetainedRecords(ctx context.Context, h *allocationHost, input nativeFeedInput) (wire []byte, err error) {
	if ctx == nil || h == nil || h.machine == nil || h.driver == nil || h.machine.SemanticContractID() != retainedOnlyFeedSemanticContractID() || len(input.Original) == 0 || len(input.Original) > graphPinManifestBytes+128 {
		return nil, errInvalid
	}
	boundary, err := nativeFeedRetainedCutDecode(input.Boundary)
	if err != nil {
		return nil, err
	}
	var cert graphCertificate
	for _, c := range boundary.certificates {
		if c.scope == h.machine.scope() {
			cert = c
		}
	}
	if cert.index == 0 || input.From > input.Through || input.Through < cert.index || input.Through > h.driver.Applied() {
		return nil, errInvalid
	}
	proof, err := h.AtGraphCertificate(ctx, cert)
	if err != nil {
		return nil, err
	}
	if err = proof.Close(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	q, err := graphRetentionReader(h, ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, q.view.Close())
		if err != nil {
			wire = nil
		}
	}()
	if err = reserveComposer(q, 8192+8*len(input.Original)); err != nil {
		return nil, err
	}
	definition, err := decodeRetainedFeedDefinition(input.Original)
	if err != nil {
		return nil, err
	}
	manifest, err := decodeGraphPinManifest(definition.manifest)
	if err != nil || manifest.publisher.semantic != retainedOnlyFeedSemanticContractID() {
		return nil, errors.Join(errInvalid, err)
	}
	stored, found, err := crossControl(q, retainedFeedDefinitionKey(h.machine.ns, manifest.id()), graphPinManifestBytes+128)
	if err != nil || !found || !bytes.Equal(stored.Value, input.Original) {
		return nil, errors.Join(errInvalid, err)
	}
	cursor, found, err := readFeedCursor(q, manifest.id())
	if err != nil || !found {
		return nil, errors.Join(errCorrupt, err)
	}
	acknowledged, ok := feedLocalPosition(manifest, h.machine.ns.partition, cursor.positions)
	if !ok {
		return nil, errCorrupt
	}
	view, err := h.machine.store.ApplicationView(input.Through)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, view.Close())
		if err != nil {
			wire = nil
		}
	}()
	root, err := view.RootBounded(ctx, 172)
	if err != nil {
		return nil, err
	}
	cfg, err := h.currentConfiguration()
	if err != nil {
		return nil, err
	}
	source := nativeColumnSource{binding: nativeColumnBinding{Version: 2, Policy: nativeColumnPolicy{FrameBytes: 32 << 10, ChunkRows: 16}}}
	source.hosts[0], source.views[0], source.roots[0], source.final[0], source.configurations[0] = h, view, root, cert, cfg
	page := nativeFeedRetainedRecordPage{Page: nativeFeedRecordPage{Partition: h.machine.ns.partition, After: input.From, Next: input.From, Through: input.Through, Root: root.Image, Frames: []nativeColumnFrame{}}, BeforeACK: []bool{}}
	for len(page.Page.Frames) < 4 && page.Page.Next < input.Through {
		index := page.Page.Next + 1
		actual, e := h.machine.feedNativeFrame(q, index, 32<<10, acknowledged)
		if e != nil {
			return nil, e
		}
		frame, e := source.frame(ctx, 0, index)
		if e != nil {
			// Only the exact authenticated below-ACK replay may omit collected
			// sidecars. All other missing native history remains a real refusal.
			if !actual.originalBeforeCut || !errors.Is(e, raftlog.ErrApplicationExpired) || len(actual.changes) != 0 || len(actual.originalOutcome) == 0 {
				return nil, e
			}
			frame = nativeColumnFrame{Partition: actual.partition, Index: actual.index, Root: actual.root, Outcome: bytes.Clone(actual.outcome), CDC: bytes.Clone(actual.changes), Registration: bytes.Clone(actual.registration), OriginalOutcome: bytes.Clone(actual.originalOutcome)}
		}
		if frame.Root != actual.root || !bytes.Equal(frame.Outcome, actual.outcome) || !bytes.Equal(frame.CDC, actual.changes) {
			return nil, errCorrupt
		}
		page.Page.Frames = append(page.Page.Frames, frame)
		page.BeforeACK = append(page.BeforeACK, actual.originalBeforeCut)
		page.Page.Next++
	}
	page.Page.Complete = page.Page.Next == input.Through
	wire, err = json.Marshal(page)
	if len(wire) > 1<<20 {
		return nil, errLimit
	}
	return wire, err
}
