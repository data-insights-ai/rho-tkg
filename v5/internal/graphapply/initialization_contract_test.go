package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

// Fixed pre-change GCD1 fixtures. The reserved slot is historical byte data,
// not a descriptor format or proof of graph readiness.
func TestGCD1InitializationPreservesLegacyBytesAndEffectDigests(t *testing.T) {
	for _, tc := range []struct {
		name, wire, digest string
		schemas            []graphstate.PropertyDefinition
	}{
		{"empty", "4743440101000000000000000000000000000000000000000000000701000000000000000100000000000000010000000000000002000000000000000000000000000000000000000000000000bbab6c9b05a7e09d1dd18e905ae2eedfa95a0889d11d0ef6cfa9a0c4b55b3b00", "e4c520b5ea577bb3887b13e91904a8d4042d53b181edab064942a279d2a8c25b", nil},
		{"schema", "47434401010000000000000000000000000000000000000000000007010000000000000001000000000000000100000000000000020000000100000006616e737765720106010000000000000000000000000000000000000000005d7302d08ff714058bdbba2824bb29c2e9d6fae606c2729558066119c817d3d1", "f86be483ddaa9730d60d3df17defa8c7d1ee2a2557e17ececc0a4fc489e1f60f", []graphstate.PropertyDefinition{{Name: "answer", Owner: graphstate.Node, Type: graphstate.ScalarScope, Cardinality: graphstate.ScalarCardinality}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire, err := hex.DecodeString(tc.wire)
			if err != nil {
				t.Fatal(err)
			}
			l := defaultMaterializerLimits()
			g := graphChanges{ns: codecNamespace(), initialized: true, topology: 1, schema: 1, schemas: tc.schemas}
			encoded, err := encodeGraphChanges(g, l)
			if err != nil || !bytes.Equal(encoded, wire) {
				t.Fatal("legacy bytes changed", err)
			}
			decoded, err := decodeGraphChanges(wire, g.ns, l)
			if err != nil || decoded.ns != g.ns || !decoded.initialized || decoded.topology != 1 || decoded.schema != 1 || !slices.Equal(decoded.schemas, g.schemas) || len(decoded.entities)+len(decoded.lives)+len(decoded.values)+len(decoded.groups) != 0 {
				t.Fatal(decoded, err)
			}
			again, err := encodeGraphChanges(decoded, l)
			if err != nil || !bytes.Equal(again, wire) {
				t.Fatal("retained re-encode changed", err)
			}
			root, err := graphstore.NewRoot(graphstore.Namespace{Graph: [16]byte(g.ns.graph), Partition: g.ns.partition}, 1)
			if err != nil {
				t.Fatal(err)
			}
			digest := graphEffectDigest(root.EffectDigest(), wire)
			if hex.EncodeToString(digest[:]) != tc.digest {
				t.Fatal("legacy digest changed", digest)
			}
			for _, slot := range []uint64{0, 1, 3} {
				bad := bytes.Clone(wire[:len(wire)-32])
				binary.BigEndian.PutUint64(bad[45:53], slot)
				if _, err := decodeGraphChanges(seal(bad), g.ns, l); !errors.Is(err, errCorrupt) {
					t.Fatal("changed reserved slot accepted", slot, err)
				}
			}
		})
	}
	plain := graphChanges{ns: codecNamespace()}
	wire, err := encodeGraphChanges(plain, defaultMaterializerLimits())
	if err != nil || binary.BigEndian.Uint64(wire[45:53]) != 0 {
		t.Fatal("non-init legacy slot changed", err)
	}
}

func initializedReader(t *testing.T, f *graphFixture) (*reader, *graphstore.Catalog, graphstore.Root) {
	t.Helper()
	index, _, err := f.s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	v, err := f.s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	})
	base, err := v.Root()
	if err != nil {
		t.Fatal(err)
	}
	c, root, err := f.m.catalog(v)
	if err != nil {
		t.Fatal(err)
	}
	l := f.m.limits.allocation
	l.readRows = min(l.readRows, f.m.limits.sourceRows)
	l.readBytes = min(l.readBytes, f.m.limits.sourceBytes)
	return &reader{ctx: t.Context(), view: v, ns: f.n, base: base, limits: l, bytes: cap(base.Image), stageBytes: 128}, c, root
}

func TestInitializedGuardChargesActualWorkAndPreflightsOwnedMetadata(t *testing.T) {
	f := readyGraphFixture(t)
	f.m.limits.catalog.MaxStages = 1
	q, c, root := initializedReader(t, f)
	view, err := graphstore.OpenReadView(t.Context(), c, f.m.limits.graph)
	if err != nil {
		t.Fatal(err)
	}
	fullWork := view.Work()
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	before := q.bytes
	if err := f.m.checkInitialized(q, c, root); err != nil {
		t.Fatal(err)
	}
	rows, readBytes := q.rows, q.bytes-before
	// Full reader checks exactly descriptor + four physical roots; one allocator
	// point read checks co-init. No catalog scan or per-fact work is hidden.
	if rows != fullWork.Records+1 || readBytes <= fullWork.Bytes {
		t.Fatal("unexpected fixed readiness work", rows, readBytes)
	}
	// Include visible fixed typed state, conservative two empty cursor maps and
	// separate descriptor/fingerprint scratch; root image copies are variable.
	fixed := unsafe.Sizeof(reader{}) + unsafe.Sizeof(request{}) + unsafe.Sizeof(graphRequest{}) + unsafe.Sizeof(outcome{}) + unsafe.Sizeof(graphstore.ReadView{}) + unsafe.Sizeof(graphstore.PageReader{}) + unsafe.Sizeof(graphstore.Catalog{}) + unsafe.Sizeof(graphstore.GraphLimits{}) + 2*128 + 1024
	if fixed > readinessMetadataBytes {
		t.Fatal("readiness fixed allowance too small", fixed)
	}
	short, c, root := initializedReader(t, f)
	m := *f.m
	m.limits.outputBytes = graphResultMetadataBytes + readinessMetadataBytes + 4*cap(short.base.Image)
	beforeRows, beforeBytes := short.rows, short.bytes
	if err := m.checkInitialized(short, c, root); !errors.Is(err, errLimit) || short.rows != beforeRows || short.bytes != beforeBytes {
		t.Fatal("fixed metadata refusal performed source work", err)
	}
	for _, dimension := range []string{"rows", "bytes", "output"} {
		for _, delta := range []int{-1, 0, 1} {
			t.Run(fmt.Sprintf("%s/%d", dimension, delta), func(t *testing.T) {
				q, c, root := initializedReader(t, f)
				m := *f.m
				switch dimension {
				case "rows":
					q.limits.readRows = rows + delta
				case "bytes":
					q.limits.readBytes = q.bytes + readBytes + delta
				case "output":
					m.limits.outputBytes = graphResultMetadataBytes + readinessMetadataBytes + 4*cap(q.base.Image) + fullWork.Bytes + delta
				}
				err := m.checkInitialized(q, c, root)
				if delta < 0 {
					if !errors.Is(err, errLimit) && !errors.Is(err, graphstore.ErrResourceLimit) {
						t.Fatal("wrong bounded refusal", err)
					}
				} else if err != nil {
					t.Fatal("exact readiness budget refused", err)
				}
				view, err := graphstore.OpenReadView(t.Context(), c, f.m.limits.graph)
				if err != nil {
					t.Fatal("guard leaked catalog handle allowance", err)
				}
				if err := view.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestPhysicalFullFormatDoesNotAuthorizeControlsOrInitializationOutcomes(t *testing.T) {
	for _, corruption := range []string{"missing", "keys-only", "unknown-coverage"} {
		t.Run(corruption, func(t *testing.T) {
			f := openGraphFixture(t)
			init := graphInit(t)
			original, installed := f.commit(t, init)
			var descriptor raftlog.KV
			for _, w := range installed.Writes {
				if len(w.Value) > 28 && bytes.Equal(w.Value[:4], []byte{'G', 'C', 1, 0x11}) {
					descriptor = raftlog.KV{Key: bytes.Clone(w.Key), Value: bytes.Clone(w.Value)}
				}
			}
			if len(descriptor.Key) == 0 {
				t.Fatal("real initializer descriptor absent")
			}
			switch corruption {
			case "missing":
				descriptor.Value, descriptor.Deleted = nil, true
			case "keys-only":
				descriptor.Value[28], descriptor.Value[29] = 1, 1
			case "unknown-coverage":
				descriptor.Value[29] = 255
			}
			q, _, root := initializedReader(t, f)
			b := batchAt(q.base)
			b.Writes = []raftlog.KV{descriptor}
			installIncompleteFixture(t, f, b)
			if declaration, err := root.SinglePartition(); err != nil || declaration.IndexVersion != 3 || isBootstrapRoot(root) {
				t.Fatal("not physical format3 fixture", declaration, err)
			}
			if _, err := newMaterializer(f.s, f.n, 1, f.m.limits); !errors.Is(err, graphstore.ErrCorrupt) {
				t.Fatal("Restore skipped actual Full proof", err)
			}
			fresh := graphInit(t)
			fresh.attempt[0] = 9
			initWire, err := encodeGraphRequest(fresh, f.m.limits)
			if err != nil {
				t.Fatal(err)
			}
			control := codecRequests(t)[2]
			controlWire, err := encodeRequest(control, f.m.limits.allocation)
			if err != nil {
				t.Fatal(err)
			}
			operations := graphCodecRequest(t)
			opsWire, err := encodeGraphRequest(operations, f.m.limits)
			if err != nil {
				t.Fatal(err)
			}
			for _, wire := range [][]byte{controlWire, initWire, initWire[:requestHeaderBytes+1], opsWire} {
				q, _, _ := initializedReader(t, f)
				before, found, err := q.view.Get(t.Context(), allocatorKey(f.n), 4096)
				if err != nil || !found {
					t.Fatal(before, found, err)
				}
				index, image, err := f.s.Checkpoint()
				if err != nil {
					t.Fatal(err)
				}
				staged, err := f.stage(t, wire)
				if !errors.Is(err, graphstore.ErrCorrupt) || !reflect.DeepEqual(staged, raftlog.ApplicationBatch{}) {
					t.Fatal("false readiness produced outcome", staged, err)
				}
				afterIndex, afterImage, err := f.s.Checkpoint()
				if err != nil || afterIndex != index || !bytes.Equal(image, afterImage) {
					t.Fatal("refusal changed root", err)
				}
				after, found, err := q.view.Get(t.Context(), allocatorKey(f.n), 4096)
				if err != nil || !found || !bytes.Equal(before.Value, after.Value) {
					t.Fatal("refusal changed allocator", err)
				}
				identity, _, err := commandIdentity(wire)
				if err != nil {
					t.Fatal(err)
				}
				if row, found, err := q.view.Get(t.Context(), outcomeKey(identity), 4096); err != nil || found {
					t.Fatal("refusal wrote dedup mapping", row, found, err)
				}
			}
			oldWire, err := encodeGraphRequest(init, f.m.limits)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := f.stage(t, oldWire)
			if err != nil || len(replayed.Writes)+len(replayed.Changes) != 0 {
				t.Fatal("retry acquired new coverage dependence", replayed, err)
			}
			o, err := decodeAnyOutcome(replayed.Outcome, f.n)
			if err != nil || o.index != original.index || o.reason != original.reason || o.hash != original.hash || o.disposition != requestReplay {
				t.Fatal(o, err)
			}
			// Changed payload using that same identity remains mismatch before the
			// descriptor guard, and cannot overwrite the original mapping.
			changed := init
			changed.maxBlock--
			wire, err := encodeGraphRequest(changed, f.m.limits)
			if err != nil {
				t.Fatal(err)
			}
			mismatch, err := f.stage(t, wire)
			if err != nil || len(mismatch.Writes) != 0 {
				t.Fatal(mismatch, err)
			}
			o, err = decodeAnyOutcome(mismatch.Outcome, f.n)
			if err != nil || o.reason != reasonMismatch {
				t.Fatal(o, err)
			}
		})
	}
}

func TestInitializedGuardSharesOuterBudgetsAndRetriesAfterRelief(t *testing.T) {
	f := readyGraphFixture(t)
	r := codecRequests(t)[3]
	r.id[0], r.sequence, r.count = 10, 2, 1
	wire, err := encodeRequest(r, f.m.limits.allocation)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := f.stage(t, wire)
	if err != nil {
		t.Fatal(err)
	}
	q, c, root := initializedReader(t, f)
	view, err := graphstore.OpenReadView(t.Context(), c, f.m.limits.graph)
	if err != nil {
		t.Fatal(err)
	}
	fullWork := view.Work()
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := q.get(outcomeKey(r)); err != nil || found {
		t.Fatal(found, err)
	}
	if err := f.m.checkInitialized(q, c, root); err != nil {
		t.Fatal(err)
	}
	if o, _, err := q.transition(r, q.base.Index+1); err != nil || o.reason != reasonNone {
		t.Fatal(o, err)
	}
	rows, readBytes := q.rows, q.bytes
	if rows != fullWork.Records+5 {
		t.Fatal("unexpected composed control source work", rows, readBytes)
	}
	limits := f.m.limits
	for _, dimension := range []string{"rows", "bytes", "output"} {
		for _, delta := range []int{-1, 0, 1} {
			t.Run(fmt.Sprintf("%s/%d", dimension, delta), func(t *testing.T) {
				f.m.limits = limits
				switch dimension {
				case "rows":
					f.m.limits.sourceRows = rows + delta
				case "bytes":
					f.m.limits.sourceBytes = readBytes + delta
				case "output":
					f.m.limits.outputBytes = graphResultMetadataBytes + readinessMetadataBytes + 4*cap(q.base.Image) + fullWork.Bytes + delta
				}
				beforeIndex, beforeRoot, err := f.s.Checkpoint()
				if err != nil {
					t.Fatal(err)
				}
				b, err := f.stage(t, wire)
				if delta < 0 {
					wanted := errLimit
					if dimension == "output" {
						wanted = graphstore.ErrResourceLimit
					}
					if !errors.Is(err, wanted) || !reflect.DeepEqual(b, raftlog.ApplicationBatch{}) {
						t.Fatal("wrong operational refusal", b, err)
					}
				} else if err != nil || !reflect.DeepEqual(b, baseline) {
					t.Fatal("relief changed control effects", b, err)
				}
				afterIndex, afterRoot, err := f.s.Checkpoint()
				if err != nil || beforeIndex != afterIndex || !bytes.Equal(beforeRoot, afterRoot) {
					t.Fatal("staging/refusal changed persisted root", err)
				}
				if row, found, err := q.view.Get(t.Context(), outcomeKey(r), 4096); err != nil || found {
					t.Fatal("operational staging wrote mapping", row, found, err)
				}
			})
		}
	}
	f.m.limits = limits
	q, c, root = initializedReader(t, f)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	q.ctx = ctx
	if err := f.m.checkInitialized(q, c, root); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	q, c, root = initializedReader(t, f)
	if err := q.view.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.m.checkInitialized(q, c, root); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
}

func TestInitializationLegacyEvidenceSurvivesMutationAndReopen(t *testing.T) {
	f := openGraphFixture(t)
	init := graphInit(t)
	init.schemas = []graphstate.PropertyDefinition{{Name: "answer", Owner: graphstate.Node, Type: graphstate.ScalarScope, Cardinality: graphstate.ScalarCardinality}}
	o, b := f.commit(t, init)
	root, err := graphstore.DecodeRoot(b.Image)
	if err != nil {
		t.Fatal(err)
	}
	if digest := root.EffectDigest(); hex.EncodeToString(digest[:]) != "f86be483ddaa9730d60d3df17defa8c7d1ee2a2557e17ececc0a4fc489e1f60f" {
		t.Fatal("initial digest diverged from legacy", digest)
	}
	f.control(t, codecRequests(t)[2])
	reserve := codecRequests(t)[3]
	reserve.count, reserve.sequence = 16, 1
	if granted, _ := f.control(t, reserve); granted.reason != reasonNone {
		t.Fatal(granted)
	}
	if changed, _ := f.commit(t, graphCodecRequest(t)); changed.reason != reasonNone {
		t.Fatal(changed)
	}
	index, image, err := f.s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(image, b.Image) {
		t.Fatal("fixture did not mutate after initialization")
	}
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	f.cfg.Create = false
	reopened, err := raftlog.Open(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := newMaterializer(reopened, f.n, 1, f.m.limits); err != nil {
		t.Fatal(err)
	}
	old, err := reopened.ApplicationRecord(t.Context(), o.index, false, 1<<20)
	if err != nil || !bytes.Equal(old, b.Changes) {
		t.Fatal("retained initialization CDC rewritten", err)
	}
	if _, err := decodeChangeEnvelope(old, o, f.m.limits); err != nil {
		t.Fatal(err)
	}
	v, err := reopened.ApplicationView(o.index)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	base, err := v.Root()
	if err != nil || !bytes.Equal(base.Image, b.Image) || base.ImageHash != sha256.Sum256(b.Image) {
		t.Fatal("old initialization root replaced by current", base, err)
	}
	oldRoot, err := graphstore.DecodeRoot(base.Image)
	if err != nil || oldRoot.EffectDigest() != root.EffectDigest() {
		t.Fatal("old digest changed", oldRoot, err)
	}
	current, currentImage, err := reopened.Checkpoint()
	if err != nil || current != index || !bytes.Equal(currentImage, image) {
		t.Fatal("current root changed on reopen", err)
	}
}

func TestInitializedGuardVariableRootBackingUsesOuterOutputHeadroom(t *testing.T) {
	f := readyGraphFixture(t)
	q, c, _ := initializedReader(t, f)
	v, err := graphstore.OpenReadView(t.Context(), c, f.m.limits.graph)
	if err != nil {
		t.Fatal(err)
	}
	smallWork := v.Work()
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if o, _ := f.commit(t, graphCodecRequest(t)); o.reason != reasonNone {
		t.Fatal(o)
	}
	r := graphCodecRequest(t)
	r.id[0], r.claims = 9, nil
	r.revision, err = state.NewRevision(99, 7)
	if err != nil {
		t.Fatal(err)
	}
	scope := r.operations[0].Scope
	r.operations = make([]graphstate.Operation, 24)
	for j := range r.operations {
		r.operations[j] = graphstate.Operation{Kind: graphstate.AddLabel, Owner: 11, Life: 12, Scope: scope, Name: fmt.Sprintf("%03d", j) + strings.Repeat("x", 197)}
	}
	if o, _ := f.commit(t, r); o.reason != reasonNone {
		t.Fatal(o)
	}
	_, c, _ = initializedReader(t, f)
	v, err = graphstore.OpenReadView(t.Context(), c, f.m.limits.graph)
	if err != nil {
		t.Fatal(err)
	}
	largeWork := v.Work()
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if largeWork.Bytes <= smallWork.Bytes+4096 {
		t.Fatal("fixture did not enlarge physical root backing", smallWork, largeWork)
	}
	control := codecRequests(t)[3]
	control.id[0], control.sequence, control.count = 10, 2, 1
	wire, err := encodeRequest(control, f.m.limits.allocation)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := f.stage(t, wire)
	if err != nil {
		t.Fatal(err)
	}
	beforeIndex, beforeRoot, err := f.s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	limits := f.m.limits
	f.m.limits.outputBytes = graphResultMetadataBytes + readinessMetadataBytes + 4*cap(q.base.Image) + smallWork.Bytes
	b, err := f.stage(t, wire)
	if !errors.Is(err, graphstore.ErrResourceLimit) || !reflect.DeepEqual(b, raftlog.ApplicationBatch{}) {
		t.Fatal("large root bypassed outer output bound", b, err)
	}
	afterIndex, afterRoot, err := f.s.Checkpoint()
	if err != nil || afterIndex != beforeIndex || !bytes.Equal(beforeRoot, afterRoot) {
		t.Fatal("local layout refusal changed root", err)
	}
	f.m.limits = limits
	relieved, err := f.stage(t, wire)
	if err != nil || !reflect.DeepEqual(relieved, baseline) {
		t.Fatal("resource relief changed logical control effects", relieved, err)
	}
}

func TestCorruptAllocatorCoInitNeverBecomesGraphBusinessRejection(t *testing.T) {
	f := readyGraphFixture(t)
	q, _, _ := initializedReader(t, f)
	b := batchAt(q.base)
	b.Writes = []raftlog.KV{{Key: allocatorKey(f.n), Value: []byte{1}}}
	installIncompleteFixture(t, f, b)
	if _, err := newMaterializer(f.s, f.n, 1, f.m.limits); !errors.Is(err, errCorrupt) {
		t.Fatal("Restore ignored corrupt co-init", err)
	}
	control := codecRequests(t)[2]
	control.id[0] = 10
	controlWire, err := encodeRequest(control, f.m.limits.allocation)
	if err != nil {
		t.Fatal(err)
	}
	init := graphInit(t)
	init.attempt[0] = 9
	initWire, err := encodeGraphRequest(init, f.m.limits)
	if err != nil {
		t.Fatal(err)
	}
	operation := graphCodecRequest(t)
	operation.operations = operation.operations[1:]
	operation.operations[0].Owner = 999 // would be an ordinary NotFound on valid co-init
	opsWire, err := encodeGraphRequest(operation, f.m.limits)
	if err != nil {
		t.Fatal(err)
	}
	index, image, err := f.s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	for _, wire := range [][]byte{controlWire, initWire, opsWire} {
		b, err := f.stage(t, wire)
		if !errors.Is(err, errCorrupt) || !reflect.DeepEqual(b, raftlog.ApplicationBatch{}) {
			t.Fatal("corruption became business outcome", b, err)
		}
	}
	current, currentImage, err := f.s.Checkpoint()
	if err != nil || current != index || !bytes.Equal(currentImage, image) {
		t.Fatal("corrupt co-init refusal changed persisted root", err)
	}
}

func TestRestoreAndConstructorHonorSharedSourceBoundaries(t *testing.T) {
	for _, initialized := range []bool{false, true} {
		t.Run(fmt.Sprintf("initialized=%t", initialized), func(t *testing.T) {
			f := openGraphFixture(t)
			if initialized {
				f.commit(t, graphInit(t))
			}
			q, c, root := initializedReader(t, f)
			if initialized {
				if err := f.m.checkInitialized(q, c, root); err != nil {
					t.Fatal(err)
				}
			} else if err := q.emptyProof(); err != nil {
				t.Fatal(err)
			}
			rows, readBytes := q.rows, q.bytes
			index, image, err := f.s.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			for _, dimension := range []string{"bytes", "rows"} {
				if !initialized && dimension == "rows" {
					continue
				} // metadata proof has no point-KV reads
				for _, delta := range []int{-1, 0, 1} {
					t.Run(fmt.Sprintf("%s/%d", dimension, delta), func(t *testing.T) {
						l := f.m.limits
						if dimension == "bytes" {
							l.sourceBytes = readBytes + delta
						} else {
							l.sourceRows = rows + delta
						}
						m := *f.m
						m.limits = l
						err := m.Restore(index, image)
						constructed, ctorErr := newMaterializer(f.s, f.n, 1, l)
						if delta < 0 {
							if !errors.Is(err, errLimit) || !errors.Is(ctorErr, errLimit) || constructed != nil {
								t.Fatal("outer source cap ignored", err, ctorErr)
							}
						} else if err != nil || ctorErr != nil || constructed == nil {
							t.Fatal("exact Restore/constructor cap refused", err, ctorErr)
						}
						afterIndex, afterImage, err := f.s.Checkpoint()
						if err != nil || afterIndex != index || !bytes.Equal(afterImage, image) {
							t.Fatal("Restore changed durable root", err)
						}
					})
				}
			}
		})
	}
}
