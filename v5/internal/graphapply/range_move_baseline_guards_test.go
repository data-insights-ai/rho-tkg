package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func rangeMoveBaselineGuardReader(t *testing.T, m *declaredMaterializer, index uint64) (*reader, genesisAllocationConfig) {
	t.Helper()
	view, err := m.store.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := view.Close(); err != nil {
			t.Error(err)
		}
	})
	base, err := view.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	output, err := graphstate.NewOutputBudget(m.limits.outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	q := &reader{ctx: t.Context(), view: view, ns: m.ns, base: base, limits: m.limits.allocation, arena: output, bytes: cap(base.Image)}
	cfg, err := m.readGenesisConfig(q)
	if err != nil {
		t.Fatal("actual native genesis configuration", err)
	}
	return q, cfg
}

func rangeMoveBaselineGuardUsage(t *testing.T, s *raftlog.Store) raftlog.ApplicationUsage {
	t.Helper()
	usage, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	return usage
}

// This helper writes deliberately malformed APPLICATION DATA into a disposable
// copy of a closed native voter's store, using the store's checked public APIs.
// The synthetic entry is not a Host command, quorum decision, certificate or
// snapshot proof, and is never submitted to a native Driver or exported as one.
func rangeMoveBaselineGuardInstallMalformed(t *testing.T, s *raftlog.Store, base raftlog.ApplicationRoot, writes []raftlog.KV, controls []raftlog.ApplicationControlPut) uint64 {
	t.Helper()
	last, err := s.LastIndex()
	if err != nil || last != base.Index {
		t.Fatal("malformed-data fixture has a pending native log suffix", last, base.Index, err)
	}
	hard, _, err := s.InitialState()
	if err != nil || hard.GetTerm() == 0 || hard.GetCommit() != last {
		t.Fatal("not the original native persisted log coordinate", err)
	}
	next := last + 1
	data := []byte("E17 explicit malformed stored-data guard; no Host or quorum authority")
	if err := s.AdmitApplication(len(data)); err != nil {
		t.Fatal("native admission refused malformed-data fixture", err)
	}
	hard.Commit = new(next)
	if err := s.Persist(raft.Ready{HardState: hard, Entries: []*pb.Entry{{Index: new(next), Term: new(hard.GetTerm()), Type: pb.EntryNormal.Enum(), Data: data}}}); err != nil {
		t.Fatal("native storage refused synthetic malformed-data entry", err)
	}
	if err := sortPartitionWrites(writes); err != nil {
		t.Fatal(err)
	}
	batch := batchAt(base)
	batch.Writes, batch.ControlPuts = writes, controls
	if err := s.InstallApplication(next, batch); err != nil {
		t.Fatal("checked storage refused malformed application data", err)
	}
	return next
}

func TestRangeMoveCatalogBaselineOwnsExactRetainedCaptureAndRefusesMalformedImportData(t *testing.T) {
	n, _, intent, _, transports := rangeMoveCoordinatorFixture(t, 64)
	gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: transports[8], sequence: 3}
	transports[8] = gate
	coordinator, err := newRangeMoveCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := runRangeMoveToCompletion(t, coordinator, intent)
	if !gate.stopped || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || pending.committed {
		t.Fatal("baseline guard requires genuine nonzero target staging", pending, err)
	}
	move := pending.resume.move
	donor := n.hosts[1][2]
	m := donor.machine
	index := donor.driver.Applied()
	q, cfg := rangeMoveBaselineGuardReader(t, m, index)
	status, found, err := readRangeMoveStatus(q, move)
	if err != nil || !found || status.phase != rangeMoveImport || status.sequence != 2 || status.result != rangeMoveApplied {
		t.Fatal("not the actual staged native target", status, found, err)
	}
	progressWire, found, err := q.getBounded(rangeMoveProgressKey(m.ns, move), 1<<20)
	if err != nil || !found {
		t.Fatal(err)
	}
	progress, err := decodeRangeMoveProgress(progressWire, q.arena)
	if err != nil || progress.imported == nil || !progress.imported.PreparationComplete || progress.imported.Sequence == 0 || progress.imported.Rows == 0 || progress.imported.Complete || progress.imported.Sealed {
		t.Fatal("baseline fixture lacks actual imported future-tree state", progress.imported, err)
	}
	capture, found, err := crossControl(q, rangeMoveControlKey(m.ns, move, 'C'), 1024)
	if err != nil || !found {
		t.Fatal(err)
	}
	captured, captureFloor, err := moveCaptureRoot(capture.Value, intent.target)
	if err != nil || captured.Index >= q.base.Index || progress.descriptor.Target.Index != captured.Index {
		t.Fatal("no genuine retained C predecessor for current import", captured.Index, q.base.Index, err)
	}
	before := rangeMoveBaselineGuardUsage(t, m.store)
	baseline, err := m.rangeMoveCatalogBaseline(q, cfg)
	if err != nil || baseline == nil || baseline == q.view {
		t.Fatal("genuine import did not acquire a separately owned native baseline", err)
	}
	root, err := baseline.RootBounded(t.Context(), 172)
	binding, bindingErr := baseline.ApplicationBinding(t.Context())
	if err != nil || bindingErr != nil || root.Index != captured.Index || root.Index == q.base.Index || root.ImageHash != captured.ImageHash || !bytes.Equal(root.Image, captured.Image) || binding != m.binding {
		_ = baseline.Close()
		t.Fatal("baseline substituted current root or a foreign native binding", root.Index, q.base.Index, err, bindingErr)
	}
	withBaseline := rangeMoveBaselineGuardUsage(t, m.store)
	if withBaseline.Views != before.Views+1 || withBaseline.ViewBytes != before.ViewBytes+172 {
		_ = baseline.Close()
		t.Fatal("baseline did not own exactly one bounded native root handle", before, withBaseline)
	}
	budget := graphstore.OwnershipBudget{SourceRows: m.limits.sourceRows, SourceBytes: m.limits.sourceBytes, OutputBytes: m.limits.outputBytes}
	catalog, _, err := graphstore.OpenPartitionCatalogWithDefaultAxis(t.Context(), baseline, graphstore.Namespace{Graph: graphstate.GraphID(m.ns.graph), Partition: m.ns.partition}, m.owner, cfg.defaultAxis, m.limits.catalog, m.limits.graph, budget)
	if err != nil {
		_ = baseline.Close()
		t.Fatal("retained C baseline did not validate its complete native catalog", err)
	}
	if entity, found, err := catalog.Entity(t.Context(), graphstore.EntityRef{Graph: graphstate.GraphID(m.ns.graph), ID: 17}); err != nil || !found || entity.ID != 17 {
		_ = baseline.Close()
		t.Fatal("baseline lost unrelated target identity", found, err)
	}
	if _, found, err := catalog.Entity(t.Context(), graphstore.EntityRef{Graph: graphstate.GraphID(m.ns.graph), ID: 1}); err != nil || found {
		_ = baseline.Close()
		t.Fatal("baseline exposed privately staged moved identity as captured state", found, err)
	}
	if err := baseline.Close(); err != nil {
		t.Fatal(err)
	}
	if after := rangeMoveBaselineGuardUsage(t, m.store); after != before {
		t.Fatal("closing baseline leaked native view ownership", before, after)
	}
	if root, err := baseline.RootBounded(t.Context(), 172); root.Image != nil || !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("closed baseline retained native data access", err)
	}
	if root, err := q.view.RootBounded(t.Context(), 172); err != nil || root.Index != index {
		t.Fatal("closing owned baseline closed the caller's borrowed current view", err)
	}
	// Both production callers must discharge their own temporary baseline.
	// Restore additionally opens/closes its primary view; the caller's owner is
	// still live, so equality of all usage fields detects either kind of leak.
	q2, cfg2 := rangeMoveBaselineGuardReader(t, m, index)
	before = rangeMoveBaselineGuardUsage(t, m.store)
	if actual, err := m.initialized(q2); err != nil || actual != cfg2 {
		t.Fatal("initialized refused genuine import baseline", err)
	}
	if after := rangeMoveBaselineGuardUsage(t, m.store); after != before {
		t.Fatal("initialized retained its temporary baseline", before, after)
	}
	if err := m.Restore(index, q2.base.Image); err != nil {
		t.Fatal("Restore refused genuine staged import", err)
	}
	if after := rangeMoveBaselineGuardUsage(t, m.store); after != before {
		t.Fatal("Restore retained primary or baseline view", before, after)
	}
	// This is a well-formed native source manifest with no imported target state.
	// Substitution at the target must fail the role check, not merely its codec.
	source := n.hosts[0][0]
	sourceQ, _ := rangeMoveBaselineGuardReader(t, source.machine, source.driver.Applied())
	sourceManifest, found, err := sourceQ.getBounded(rangeMoveProgressKey(source.machine.ns, move), 1<<20)
	if err != nil || !found {
		t.Fatal(err)
	}
	sourceProgress, err := decodeRangeMoveProgress(sourceManifest, sourceQ.arena)
	if err != nil || sourceProgress.imported != nil || !sourceProgress.transferComplete() {
		t.Fatal("source counterpart is not a genuine complete native manifest", err)
	}
	if err := sourceQ.view.Close(); err != nil {
		t.Fatal(err)
	}
	if err := q2.view.Close(); err != nil {
		t.Fatal(err)
	}
	if err := q.view.Close(); err != nil {
		t.Fatal(err)
	}
	if err := donor.Close(); err != nil {
		t.Fatal("close native donor before disposable data copies", err)
	}
	originalConfig := n.configs[1][2]
	for _, kind := range []string{"deleted-progress", "corrupt-progress", "source-manifest-without-import", "missing-capture", "corrupt-capture", "capture-index-mismatch"} {
		t.Run(kind, func(t *testing.T) {
			configuration := originalConfig
			configuration.Dir, configuration.Create = t.TempDir(), false
			if err := os.CopyFS(configuration.Dir, os.DirFS(originalConfig.Dir)); err != nil {
				t.Fatal("copy closed genuine native store for explicit malformed data", err)
			}
			store, err := raftlog.Open(configuration)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			machine, err := newRangeMoveMaterializer(store, m.ns, n.d, m.limits)
			if err != nil {
				t.Fatal("unmodified native copy failed positive Restore", err)
			}
			current, nativeCfg := rangeMoveBaselineGuardReader(t, machine, index)
			var writes []raftlog.KV
			var controls []raftlog.ApplicationControlPut
			testMove := move
			switch kind {
			case "deleted-progress":
				writes = []raftlog.KV{{Key: rangeMoveProgressKey(m.ns, move), Deleted: true}}
			case "corrupt-progress":
				bad := bytes.Clone(progressWire)
				bad[len(bad)-1] ^= 1
				writes = []raftlog.KV{{Key: rangeMoveProgressKey(m.ns, move), Value: bad}}
			case "source-manifest-without-import":
				writes = []raftlog.KV{{Key: rangeMoveProgressKey(m.ns, move), Value: bytes.Clone(sourceManifest)}}
			default:
				// Existing native controls are immutable and are not overwritten.
				// An explicitly synthetic local identity permits a missing or bad
				// C row to reach baseline validation through normal store APIs.
				badIntent := intent
				badIntent.id = requestID{231}
				// rangeMoveID is SOURCE qualified, even on target protocol rows.
				testMove = rangeMoveID(source.machine.ns, badIntent.id)
				intentWire, err := encodeRangeMoveIntent(badIntent)
				if err != nil {
					t.Fatal(err)
				}
				badStatus := status
				badStatus.move, badStatus.intent, badStatus.index = testMove, sha256.Sum256(intentWire), index+1
				statusWire, err := encodeRangeMoveStatus(badStatus)
				if err != nil {
					t.Fatal(err)
				}
				writes = []raftlog.KV{{Key: rangeMoveActiveKey(m.ns), Value: testMove[:]}, {Key: rangeMoveStatusKey(m.ns, testMove), Value: statusWire}, {Key: rangeMoveProgressKey(m.ns, testMove), Value: bytes.Clone(progressWire)}}
				controls = []raftlog.ApplicationControlPut{{Key: rangeMoveControlKey(m.ns, testMove, 'I'), Value: intentWire}}
				if kind != "missing-capture" {
					badCapture := bytes.Clone(capture.Value)
					if kind == "corrupt-capture" {
						badCapture[len(badCapture)-1] ^= 1
					} else {
						wrong := captured
						wrong.Index++
						badCapture, err = boundedEncoding(1024, func(w *boundedWriter) {
							w.add([]byte{'G', 'M', 'C', 1})
							w.add(appendAllocationScope(nil, intent.target))
							w.u64(captureFloor)
							emitSourceRoot(w, wrong)
						})
						if err != nil {
							t.Fatal(err)
						}
					}
					// GP controls are sorted by their complete native key. C precedes I.
					controls = append([]raftlog.ApplicationControlPut{{Key: rangeMoveControlKey(m.ns, testMove, 'C'), Value: badCapture}}, controls...)
				}
			}
			next := rangeMoveBaselineGuardInstallMalformed(t, store, current.base, writes, controls)
			if err := current.view.Close(); err != nil {
				t.Fatal(err)
			}
			adversarial, actualCfg := rangeMoveBaselineGuardReader(t, machine, next)
			if actualCfg != nativeCfg || !bytes.Equal(adversarial.base.Image, current.base.Image) {
				t.Fatal("malformed-data fixture changed native configuration or published graph image")
			}
			before := rangeMoveBaselineGuardUsage(t, store)
			baseline, err := machine.rangeMoveCatalogBaseline(adversarial, actualCfg)
			if baseline != nil {
				_ = baseline.Close()
				t.Fatal("malformed import/capture substituted a current or retained baseline", kind, testMove)
			}
			if !errors.Is(err, errCorrupt) {
				t.Fatal("malformed imported state lost corruption sentinel", kind, err)
			}
			if after := rangeMoveBaselineGuardUsage(t, store); after != before {
				t.Fatal("refused baseline leaked native ownership", kind, before, after)
			}
			initialized, _ := rangeMoveBaselineGuardReader(t, machine, next)
			before = rangeMoveBaselineGuardUsage(t, store)
			if _, err := machine.initialized(initialized); !errors.Is(err, errCorrupt) {
				t.Fatal("initialized fell back from malformed import/capture to current catalog", kind, err)
			}
			if err := machine.Restore(next, adversarial.base.Image); !errors.Is(err, errCorrupt) {
				t.Fatal("Restore fell back from malformed imported state to current catalog", kind, err)
			}
			if after := rangeMoveBaselineGuardUsage(t, store); after != before {
				t.Fatal("failed initialized/Restore leaked temporary native views", kind, before, after)
			}
			if root, err := adversarial.view.RootBounded(t.Context(), 172); err != nil || root.Index != next {
				t.Fatal("application-data refusal closed or poisoned caller's borrowed view", kind, err)
			}
		})
	}
}
