package graphstore

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func c0OwnershipInstall(t *testing.T, s *raftlog.Store, base raftlog.ApplicationRoot, image []byte, writes []raftlog.KV, key string) (uint64, error) {
	t.Helper()
	index, _, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	next := index + 1
	if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(next)}, Entries: []*pb.Entry{{Index: new(next), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: []byte("C0 initializer")}}}); err != nil {
		t.Fatal(err)
	}
	batch := raftlog.ApplicationBatch{BaseGeneration: base.Generation, BaseIndex: base.Index, BaseImageHash: base.ImageHash, Image: image, Writes: writes, ControlPuts: []raftlog.ApplicationControlPut{{Key: []byte(key), Value: []byte("registered")}}}
	return next, s.InstallApplication(next, batch)
}

func c0OwnershipRoot(t *testing.T, s *raftlog.Store) raftlog.ApplicationRoot {
	t.Helper()
	index, _, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	}()
	base, err := v.Root()
	if err != nil {
		t.Fatal(err)
	}
	return base
}

func TestC0DeclaredPartitionSeedAndPhysicalInitializationWithControls(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "published"}[published], func(t *testing.T) {
			d := partitionInitDeclaration(t)
			local := Namespace{d.Graph(), 1}
			cfg := ownershipConfig(vfs.NewMem(), local, [16]byte{9}, 1)
			cfg.Controls = raftlog.ApplicationControlConfig{Version: 1}
			contract, err := raftlog.ApplicationContractForPolicyWithControls(cfg.Application, cfg.Controls)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Transfer.Contract = contract
			s, err := raftlog.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := BootstrapBoundOwnership(s, s.ApplicationBinding(), d, [3]uint64{1, 2, 3}); err != nil {
				t.Fatal(err)
			}
			base := c0OwnershipRoot(t, s)
			_, err = c0OwnershipInstall(t, s, base, base.Image, nil, "control-a")
			if err != nil {
				t.Fatal(err)
			}
			base = c0OwnershipRoot(t, s)
			at, err := c0OwnershipInstall(t, s, base, base.Image, nil, "control-b")
			if err != nil {
				t.Fatal(err)
			}
			pending := ownershipView(t, s, at)
			if published {
				effects, _, err := StageOwnershipDeclaration(t.Context(), pending, d, Limits{}, ownershipBudget())
				if err != nil {
					t.Fatal(err)
				}
				image, err := EncodeRoot(effects.Root)
				if err != nil {
					t.Fatal(err)
				}
				at, err = c0OwnershipInstall(t, s, effects.Base, image, effects.Writes, "control-publish")
				if err != nil {
					t.Fatal(err)
				}
			}
			v := ownershipView(t, s, at)
			original, err := v.Root()
			if err != nil {
				t.Fatal(err)
			}
			before, err := s.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			controls, err := s.ApplicationControlUsage()
			if err != nil || controls.Records != map[bool]uint64{false: 2, true: 3}[published] || before.GraphRecords != 3*at+map[bool]uint64{false: 0, true: 1}[published] {
				t.Fatal(before, controls, err)
			}
			proof, seed, seedWork, err := CheckDeclaredPartitionSeed(t.Context(), v, d, Limits{}, ownershipBudget())
			if err != nil || !reflect.DeepEqual(proof, original) || seed.next != 1 || seed.epoch != 0 || seed.topology.index != 0 {
				t.Fatal(proof, seed, seedWork, err)
			}

			// Independent wire counts: fixed metadata/root 1536+172; scratch
			// 512+3*32; declaration lookup 57+64; expected wire 57+160+64.
			seedBytes, seedRows, seedOutput := 2890, 1, 1708
			if published {
				key := ownershipKey(d.Graph(), d.TopologyEpoch(), d.Digest())
				// Found declaration value/entries 160+96, then complete sole-KV
				// proof: 172+117+(60+zeros)+64+(68+zeros)+196+172.
				seedBytes = 3823 + 2*bytes.Count(key, []byte{0})
				seedRows = 2
			}
			if seedWork != (PageWork{Records: seedRows, Bytes: seedBytes}) {
				t.Fatal("independent seed costs", seedWork, seedBytes)
			}
			for _, dimension := range []string{"source", "rows", "output"} {
				for _, delta := range []int{-1, 0, 1} {
					budget := ownershipBudget()
					switch dimension {
					case "source":
						budget.SourceBytes = seedBytes + delta
					case "rows":
						budget.SourceRows = seedRows + delta
					case "output":
						budget.OutputBytes = seedOutput + delta
					}
					if budget.SourceRows == 0 {
						continue
					}
					got, root, work, err := CheckDeclaredPartitionSeed(t.Context(), v, d, Limits{}, budget)
					if delta < 0 {
						if !errors.Is(err, ErrResourceLimit) || got.Index != 0 || root != (Root{}) || work.Bytes > budget.SourceBytes || work.Records > budget.SourceRows {
							t.Fatal(dimension, got, root, work, err)
						}
					} else if err != nil || !reflect.DeepEqual(got, original) || root != seed || work != seedWork {
						t.Fatal(dimension, got, root, work, err)
					}
				}
			}
			schema := graphstate.PropertyDefinition{Name: "name", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}
			effects, work, err := InitializeDeclaredPartition(t.Context(), v, d, []graphstate.PropertyDefinition{schema}, Limits{}, GraphLimits{}, ownershipBudget())
			if err != nil || effects.Root.ownershipMode != ownershipInitialized || effects.Root.topology != (topologyDeclaration{7, 1, 3}) || effects.Root.epoch != 0 || effects.Root.next != 6 || effects.Root.effect != seed.effect || !reflect.DeepEqual(effects.Base, original) || effects.Work != work {
				t.Fatal(effects, work, err)
			}
			wantWrites, wantDecl := 8, 1
			if published {
				wantWrites, wantDecl = 7, 0
			}
			if len(effects.Writes) != wantWrites || work.Records != seedRows+1 {
				t.Fatal("physical root set", effects.Writes, work)
			}
			declWrites, output := 0, 512+172+64*wantWrites
			for _, row := range effects.Writes {
				if bytes.Equal(row.Key, ownershipKey(d.Graph(), d.TopologyEpoch(), d.Digest())) {
					declWrites++
				}
				if cap(row.Key) != len(row.Key) || cap(row.Value) != len(row.Value) {
					t.Fatal("output ownership")
				}
				output += len(row.Key) + len(row.Value)
			}
			if declWrites != wantDecl || effects.OwnedBytes != output {
				t.Fatal("declaration/output set", declWrites, effects.OwnedBytes, output)
			}
			// Static source-byte oracle, independent of returned work: descriptor
			// absence 172+25+64; stage image/metadata/schema 172+512+136;
			// empty presence 4480+256+384+4096+43+512+384+640+173.
			// seedBytes is independently counted above from declaration wire/Z.
			initializerBytes := seedBytes + 12049
			wantWork := PageWork{Records: seedRows + 1, Bytes: initializerBytes}
			wantWire, err := EncodeRoot(effects.Root)
			if err != nil {
				t.Fatal(err)
			}
			for _, delta := range []int{-1, 0, 1} {
				budget := ownershipBudget()
				budget.SourceBytes = initializerBytes + delta
				out, used, err := InitializeDeclaredPartition(t.Context(), v, d, []graphstate.PropertyDefinition{schema}, Limits{}, GraphLimits{}, budget)
				if delta < 0 {
					// The final pending presence key/wire/header charge is 173;
					// it cannot fit and is not added to consumed source work.
					wantUsed := PageWork{Records: seedRows + 1, Bytes: initializerBytes - 173}
					if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(out, GraphEffects{}) || used != wantUsed {
						t.Fatal("literal initializer one-short source boundary", out, used, err, wantUsed)
					}
				} else {
					wire, encodeErr := EncodeRoot(out.Root)
					if err != nil || encodeErr != nil || used != wantWork || out.Work != wantWork || out.Root != effects.Root || !reflect.DeepEqual(out.Base, original) || !reflect.DeepEqual(out.Writes, effects.Writes) || len(out.Writes) != wantWrites || out.OwnedBytes != output || !bytes.Equal(wire, wantWire) {
						t.Fatal("literal initializer exact source boundary", out, used, err, encodeErr, wantWork)
					}
				}
				if after, err := s.ApplicationUsage(); err != nil || after != before {
					t.Fatal("source boundary changed durable accounting/poisoned store", after, err)
				}
				if borrowed, err := v.Root(); err != nil || !reflect.DeepEqual(borrowed, original) {
					t.Fatal("source boundary invalidated borrowed seed", borrowed, err)
				}
			}
			for _, delta := range []int{-1, 0, 1} {
				budget := ownershipBudget()
				budget.SourceRows = seedRows + 1 + delta
				out, used, err := InitializeDeclaredPartition(t.Context(), v, d, []graphstate.PropertyDefinition{schema}, Limits{}, GraphLimits{}, budget)
				if delta < 0 {
					if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(out, GraphEffects{}) || used.Records > budget.SourceRows {
						t.Fatal(out, used, err)
					}
				} else if err != nil || out.Root != effects.Root || used != work {
					t.Fatal(out, used, err)
				}
				budget = ownershipBudget()
				budget.OutputBytes = max(seedOutput, output) + delta
				out, used, err = InitializeDeclaredPartition(t.Context(), v, d, []graphstate.PropertyDefinition{schema}, Limits{}, GraphLimits{}, budget)
				if delta < 0 {
					if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(out, GraphEffects{}) {
						t.Fatal(out, used, err)
					}
				} else if err != nil || out.Root != effects.Root {
					t.Fatal(out, used, err)
				}
			}
			if after, err := s.ApplicationUsage(); err != nil || after != before {
				t.Fatal("staging mutated ledgers", after, err)
			}
			if current := c0OwnershipRoot(t, s); !reflect.DeepEqual(current, original) {
				t.Fatal("staging advanced base", current)
			}
			wire, err := EncodeRoot(effects.Root)
			if err != nil || len(wire) != 172 || wire[3] != 5 {
				t.Fatal(wire, err)
			}

			// A real intervening control-only commit invalidates the captured
			// installation base even when the graph image remains identical.
			at, err = c0OwnershipInstall(t, s, original, original.Image, nil, "control-intervening")
			if err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := CheckDeclaredPartitionSeed(t.Context(), v, d, Limits{}, ownershipBudget()); !errors.Is(err, raftlog.ErrInvalid) {
				t.Fatal("old seed borrowed current authority", err)
			}
			installed, err := effects.Root.AdvanceEffects(sha256.Sum256([]byte("C0 co-composed initialization")))
			if err != nil {
				t.Fatal(err)
			}
			image, err := EncodeRoot(installed)
			if err != nil {
				t.Fatal(err)
			}
			staleUsage, err := s.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			_, err = c0OwnershipInstall(t, s, effects.Base, image, effects.Writes, "control-stale-refused")
			if !errors.Is(err, raftlog.ErrInvalid) {
				t.Fatal("stale base installed", err)
			}
			if after, err := s.ApplicationUsage(); err != nil || after.GraphBytes != staleUsage.GraphBytes || after.GraphRecords != staleUsage.GraphRecords || after.ControlBytes != staleUsage.ControlBytes || after.ControlRecords != staleUsage.ControlRecords || after.CheckpointBytes != staleUsage.CheckpointBytes || after.TailEntries != staleUsage.TailEntries+1 {
				t.Fatal("refusal changed application ledgers or lost real persisted tail", after, err)
			}
			fresh := ownershipView(t, s, at)
			effects, _, err = InitializeDeclaredPartition(t.Context(), fresh, d, []graphstate.PropertyDefinition{schema}, Limits{}, GraphLimits{}, ownershipBudget())
			if err != nil {
				t.Fatal(err)
			}
			// The refused install already persisted its next entry. Install the
			// fresh effects at that real committed index, without a duplicate Persist.
			next := at + 1
			batch := raftlog.ApplicationBatch{BaseGeneration: effects.Base.Generation, BaseIndex: effects.Base.Index, BaseImageHash: effects.Base.ImageHash, Image: image, Writes: effects.Writes, ControlPuts: []raftlog.ApplicationControlPut{{Key: []byte("control-initialized"), Value: []byte("registered")}}}
			if err := s.InstallApplication(next, batch); err != nil {
				t.Fatal(err)
			}
			check := func(view *raftlog.ApplicationView) {
				t.Helper()
				catalog, work, err := OpenPartitionCatalog(t.Context(), view, local, seed.owner, Limits{}, GraphLimits{}, ownershipBudget())
				if err != nil || catalog == nil || catalog.localFull == nil || work.Records != 7 {
					t.Fatal(catalog, work, err)
				}
				got, found, err := catalog.Property(t.Context(), graphstate.Node, "name")
				if err != nil || !found || got != schema {
					t.Fatal(got, found, err)
				}
				if _, found, err := catalog.Property(t.Context(), graphstate.Node, "phantom"); err != nil || found {
					t.Fatal(found, err)
				}
				if _, found, err := catalog.Entity(t.Context(), EntityRef{d.Graph(), 999}); err != nil || found {
					t.Fatal(found, err)
				}
				if _, err := OpenReadView(t.Context(), catalog, GraphLimits{}); !errors.Is(err, ErrTopologyUnsupported) {
					t.Fatal(err)
				}
				revision, err := state.NewRevision(1, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := StageOperations(t.Context(), catalog, nil, revision, GraphLimits{}); !errors.Is(err, ErrTopologyUnsupported) {
					t.Fatal(err)
				}
				if _, err := NewPageReader(catalog, PageLimits{}); !errors.Is(err, ErrTopologyUnsupported) {
					t.Fatal(err)
				}
				for _, key := range []string{"control-a", "control-b", "control-intervening", "control-initialized"} {
					row, found, _, err := view.GetControl(t.Context(), []byte(key), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
					if err != nil || !found || string(row.Value) != "registered" {
						t.Fatal(key, row, found, err)
					}
				}
				if _, found, _, err := view.GetControl(t.Context(), []byte("control-stale-refused"), raftlog.ReadBudget{Rows: 2, Bytes: 4096}); err != nil || found {
					t.Fatal("refused control row appeared", found, err)
				}
			}
			check(ownershipView(t, s, next))
			if got, _, err := OpenPartitionCatalog(t.Context(), v, local, seed.owner, Limits{}, GraphLimits{}, ownershipBudget()); !errors.Is(err, ErrTopologyUnsupported) || got != nil {
				t.Fatal("old seed gained roots", got, err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			cfg.Create = false
			reopened, err := raftlog.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			check(ownershipView(t, reopened, next))
			if got, _, err := OpenPartitionCatalog(t.Context(), ownershipView(t, reopened, original.Index), local, seed.owner, Limits{}, GraphLimits{}, ownershipBudget()); !errors.Is(err, ErrTopologyUnsupported) || got != nil {
				t.Fatal("reopened old seed gained readiness", got, err)
			}
		})
	}
}
