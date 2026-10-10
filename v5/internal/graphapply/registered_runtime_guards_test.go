package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"go.etcd.io/raft/v3"
)

var registeredGuardLimits = registeredOperationLimits{8 << 20, 8 << 20}

// Only public configuration and Initialize/bootstrap; no backend mutation hook.
func registeredGuardStore(t *testing.T, graph, voter byte, edit func(*raftlog.Config), image []byte) (*raftlog.Store, func()) {
	t.Helper()
	dir, err := os.MkdirTemp(os.Getenv("RHO_REGISTERED_GENESIS_EVIDENCE_ROOT"), "registered-guard-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("retained guard Graph%d voter%d: %s", graph, voter, dir)
	cfg := raftlog.Config{Dir: dir, Create: true, Application: raftlog.DefaultApplicationPolicy(uint64(voter)), Controls: raftlog.ApplicationControlConfig{Version: 1},
		Transfer:      raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{graph}, Partition: 1, Group: [16]byte{graph + 1}}, Limits: raftlog.DefaultApplicationTransferLimits()},
		Generations:   raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2000000},
		PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 10000},
		Replication:   raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: registeredSemanticContractID()}
	if edit != nil {
		edit(&cfg)
	}
	cfg.Transfer.Contract, err = raftlog.ApplicationContractForPolicyWithControls(cfg.Application, cfg.Controls)
	if err != nil {
		t.Fatal(err)
	}
	s, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	driverOwned := false
	t.Cleanup(func() {
		if !driverOwned {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if image == nil {
		err = graphstore.BootstrapBoundSinglePartition(s, s.ApplicationBinding(), 1, [3]uint64{1, 2, 3})
	} else {
		err = s.Initialize([]uint64{1, 2, 3}, image)
	}
	if err != nil {
		t.Fatal("public initialization setup", err)
	}
	return s, func() { driverOwned = true }
}

func registeredGuardGroup(t *testing.T, graph byte) (*registeredStageTestGroup, [3]*registeredMaterializer) {
	t.Helper()
	g := &registeredStageTestGroup{t: t}
	var machines [3]*registeredMaterializer
	t.Cleanup(func() {
		for _, d := range g.drivers {
			if d != nil {
				if err := d.Close(); err != nil {
					t.Error(err)
				}
			}
		}
	})
	for j := range 3 {
		s, handoff := registeredGuardStore(t, graph, byte(j+1), nil, nil)
		m, err := newRegisteredMaterializer(s, registeredGuardLimits)
		if err != nil {
			t.Fatal(err)
		}
		d, err := replica.Open(replica.Config{ID: uint64(j + 1), Store: s, ApplicationMachine: m, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		if err != nil {
			t.Fatal(err)
		}
		handoff()
		g.stores[j], g.drivers[j], machines[j] = s, d, m
	}
	out, err := g.drivers[0].Campaign()
	if err != nil {
		t.Fatal(err)
	}
	g.pump(out)
	g.roles(2, 2)
	return g, machines
}

func registeredGuardZeroState(t *testing.T, s *raftlog.Store, limits registeredOperationLimits, want, constructorWant error) {
	t.Helper()
	base, root, origin, err := registeredCurrentState(s, limits, 0)
	if !errors.Is(err, want) || !reflect.DeepEqual(base, raftlog.ApplicationRoot{}) || root != (graphstore.Root{}) || origin != (registeredOrigin{}) {
		t.Fatal("current-state error must be zero", err)
	}
	base, root, err = registeredCurrentRoot(s, limits, 0)
	if !errors.Is(err, want) || !reflect.DeepEqual(base, raftlog.ApplicationRoot{}) || root != (graphstore.Root{}) {
		t.Fatal("current-root error must be zero", err)
	}
	source, err := captureRegisteredGenesisSource(s, limits)
	if !errors.Is(err, want) || source != (registeredGenesisSource{}) {
		t.Fatal("capture error must be zero", err)
	}
	if constructorWant == nil {
		constructorWant = want
	}
	machine, err := newRegisteredMaterializer(s, limits)
	if !errors.Is(err, constructorWant) || machine != nil {
		t.Fatal("constructor error must be zero", err)
	}
}

func TestRegisteredRuntimeGuardPublicInitializeRootAndReadCaps(t *testing.T) {
	// Independent GR2 Graph23/bootstrap bytes, supplied through actual Initialize.
	seed := registeredStageHex(t, "47520202170000000000000000000000000000000000000000000001000000000000000100000000000000000000000000000001a9ff65b3a3e3456152bb49ccdb79a67ad6ee6b0b518f726b9b6caf4807e5cc020000000000000001000000000000000100000000000000004d821b4f0204d4230b1c7148412ecd78fdcee36289e36c547800e5fad9630807")
	for _, tc := range []struct {
		name            string
		edit            func(*raftlog.Config)
		image           func([]byte) []byte
		want            error
		constructorWant error
	}{
		{"short image", nil, func(b []byte) []byte { return b[:139] }, errCorrupt, nil},
		{"wrong header", nil, func(b []byte) []byte { b[2] = 3; return b }, errCorrupt, nil},
		{"invalid graph checksum", nil, func(b []byte) []byte { b[139] ^= 1; return b }, graphstore.ErrCorrupt, nil},
		{"binding namespace", func(c *raftlog.Config) { c.Transfer.Identity.Graph = [16]byte{99} }, func(b []byte) []byte { return b }, errCorrupt, nil},
		{"not bootstrap topology", nil, func(b []byte) []byte {
			binary.BigEndian.PutUint64(b[100:108], 1)
			sum := sha256.Sum256(b[:108])
			copy(b[108:], sum[:])
			return b
		}, errCorrupt, nil},
		{"wrong machine meaning", func(c *raftlog.Config) { c.SemanticContractID = SemanticContractID() }, nil, errInvalid, nil},
		{"controls absent", func(c *raftlog.Config) { c.Controls = raftlog.ApplicationControlConfig{} }, nil, errInvalid, raftlog.ErrInvalid},
		{"read row cap", func(c *raftlog.Config) { c.Application.MaxPageRows = 1 }, nil, raftlog.ErrLimit, nil},
		{"too small image policy", func(c *raftlog.Config) { c.Application.MaxImageBytes = 139 }, func(b []byte) []byte { return b[:0] }, errInvalid, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var image []byte
			if tc.image != nil {
				image = tc.image(bytes.Clone(seed))
			}
			s, _ := registeredGuardStore(t, 23, 1, tc.edit, image)
			registeredGuardZeroState(t, s, registeredGuardLimits, tc.want, tc.constructorWant)
		})
	}
}

func TestRegisteredRuntimeGuardOperationAndClosedStore(t *testing.T) {
	s, _ := registeredGuardStore(t, 25, 1, nil, nil)
	for _, tc := range []struct {
		limits registeredOperationLimits
		extra  int
		want   error
	}{
		{registeredOperationLimits{}, 0, errInvalid},
		{registeredGuardLimits, -1, errInvalid},
		{registeredOperationLimits{(8 << 20) + 1, 8 << 20}, 0, errInvalid},
		{registeredOperationLimits{8 << 20, (8 << 20) + 1}, 0, errInvalid},
		{registeredGuardLimits, (8 << 20) + 1, errLimit},
		{registeredOperationLimits{1, 8 << 20}, 0, errLimit},
		{registeredOperationLimits{8 << 20, 1}, 0, errLimit},
	} {
		if err := tc.limits.reserve(s, tc.extra); !errors.Is(err, tc.want) {
			t.Fatal("operation reservation", err)
		}
	}
	registeredGuardZeroState(t, nil, registeredGuardLimits, errInvalid, nil)
	registeredGuardZeroState(t, s, registeredOperationLimits{1, 1}, errLimit, nil)
	m, err := newRegisteredMaterializer(s, registeredGuardLimits)
	if err != nil {
		t.Fatal(err)
	}
	base, _, _, err := registeredCurrentState(s, registeredGuardLimits, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := registeredKnownTerm(s, 1, 2); !errors.Is(err, errCorrupt) {
		t.Fatal("known term mismatch", err)
	}
	if err := registeredKnownTerm(s, 2, 2); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatal("unavailable is not compacted", err)
	}
	for _, index := range []uint64{0, 2} {
		if err := m.Restore(index, base.Image); !errors.Is(err, errInvalid) {
			t.Fatal("Restore index guard", err)
		}
	}
	wrong := bytes.Clone(base.Image)
	wrong[0] ^= 1
	if err := m.Restore(base.Index, wrong); !errors.Is(err, errInvalid) {
		t.Fatal("Restore argument mismatch", err)
	}
	bad := *m
	bad.source.ImageHash[0] ^= 1
	if err := bad.Restore(base.Index, base.Image); !errors.Is(err, errInvalid) {
		t.Fatal("cached source mismatch", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	registeredGuardZeroState(t, s, registeredGuardLimits, raftlog.ErrClosed, nil)
	if err := m.Restore(base.Index, base.Image); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("closed Restore", err)
	}
	batch, err := m.Stage(replica.Entry{Term: 1}, raftlog.ApplicationBudget{Writes: 1, Bytes: 4096, ImageBytes: 140, ChangeBytes: 1, OutcomeBytes: 145})
	if !errors.Is(err, raftlog.ErrClosed) || !reflect.DeepEqual(batch, raftlog.ApplicationBatch{}) {
		t.Fatal("closed Stage must be zero", err)
	}
}

func TestRegisteredRuntimeGuardActualCommittedCommandsAndFences(t *testing.T) {
	for j, tc := range []struct {
		name   string
		change func([]byte) []byte
		want   error
	}{
		{"short command", func(b []byte) []byte { return b[:4] }, errInvalid},
		{"version", func(b []byte) []byte { b[5] = 2; return b }, errInvalid},
		{"flags", func(b []byte) []byte { b[6] = 1; return b }, errInvalid},
		{"declared length", func(b []byte) []byte { b[10] ^= 1; return b }, errInvalid},
		{"unknown magic", func(b []byte) []byte { copy(b[:4], "BAD1"); return b }, errInvalid},
		{"source kind", func(b []byte) []byte { b[57] = 0; return b }, errInvalid},
		{"same-key different owner", func(b []byte) []byte { b[117] = 2; return b }, errInvalid},
		{"unsupported Register", func(b []byte) []byte { copy(b[:4], "RJQ1"); return b }, errRegisteredStageUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, machines := registeredGuardGroup(t, byte(27+2*j))
			m := machines[0]
			command, err := encodeGenesisCommand(m.source, registeredJournalBudget{3314})
			if err != nil {
				t.Fatal(err)
			}
			submitted := tc.change(bytes.Clone(command))
			out, err := g.drivers[0].Propose(submitted)
			if err == nil {
				out, _, err = registeredStagePump(g.drivers, out)
			}
			if !errors.Is(err, replica.ErrStopped) || !errors.Is(err, tc.want) || (out.Applied != 0 || out.Packets != nil || out.Reads != nil || out.SnapshotSends != nil) {
				t.Fatal("actual committed refusal must stop with zero output", err)
			}
			cp, image, err := g.stores[0].Checkpoint()
			if err != nil || cp != 2 {
				t.Fatal("refusal advanced checkpoint", cp, err)
			}
			stored, err := g.stores[0].Entries(3, 4, uint64(g.stores[0].Limits().MaxReadBytes))
			hard, _, stateErr := g.stores[0].InitialState()
			if err != nil || stateErr != nil || len(stored) != 1 || hard.GetCommit() != 3 || stored[0].GetIndex() != 3 || stored[0].GetTerm() != 2 || !bytes.Equal(stored[0].GetData(), submitted) {
				t.Fatal("refusal not actual quorum entry3/term2", err, stateErr)
			}
			usage, err := g.stores[0].ApplicationControlUsage()
			if err != nil || usage.Bytes != 0 || usage.Records != 0 || usage.TotalBytes != 550 || usage.TotalRecords != 6 {
				t.Fatal("refusal installed effects", usage, err)
			}
			// Direct invalid inputs never claim successful admission; coordinates/data
			// start from the observed committed entry above, not fabricated log state.
			base, _, _, err := registeredCurrentState(g.stores[0], registeredGuardLimits, 0)
			if err != nil {
				t.Fatal(err)
			}
			actual := replica.Entry{Index: stored[0].GetIndex(), Term: stored[0].GetTerm(), Data: stored[0].GetData(), Generation: base.Generation}
			for _, change := range []func(*replica.Entry){func(e *replica.Entry) { e.Index-- }, func(e *replica.Entry) { e.Generation++ }, func(e *replica.Entry) { e.Term++ }} {
				invalid := actual
				change(&invalid)
				batch, err := m.Stage(invalid, g.stores[0].ApplicationBudget())
				if !errors.Is(err, errInvalid) || !reflect.DeepEqual(batch, raftlog.ApplicationBatch{}) {
					t.Fatal("invalid direct fence must be zero", err)
				}
			}
			batch, err := m.Stage(actual, raftlog.ApplicationBudget{})
			if !errors.Is(err, errInvalid) || !reflect.DeepEqual(batch, raftlog.ApplicationBatch{}) {
				t.Fatal("empty direct budget must be zero", err)
			}
			limited := g.stores[0].ApplicationBudget()
			limited.ImageBytes = 139
			batch, err = m.Stage(actual, limited)
			if !errors.Is(err, errLimit) || !reflect.DeepEqual(batch, raftlog.ApplicationBatch{}) {
				t.Fatal("image budget before command output", err)
			}
			oversize := actual
			oversize.Data = make([]byte, (4<<20)+12)
			batch, err = m.Stage(oversize, g.stores[0].ApplicationBudget())
			if !errors.Is(err, errLimit) || !reflect.DeepEqual(batch, raftlog.ApplicationBatch{}) {
				t.Fatal("oversize invalid input must be zero", err)
			}
			if !bytes.Equal(image, base.Image) {
				t.Fatal("unit refusals changed current image")
			}
		})
	}
}

func TestRegisteredRuntimeGuardOriginCurrentViewAndPinLimit(t *testing.T) {
	g, machines := registeredGuardGroup(t, 45)
	command, err := encodeGenesisCommand(machines[0].source, registeredJournalBudget{3314})
	if err != nil {
		t.Fatal(err)
	}
	out, err := g.drivers[0].Propose(command)
	if err != nil {
		t.Fatal(err)
	}
	g.pump(out)
	g.roles(3, 2)
	s := g.stores[0]
	base, root, origin, err := registeredCurrentState(s, registeredGuardLimits, 0)
	if err != nil || origin.index != 3 || origin.term != 2 {
		t.Fatal("actual Origin counterpart", err)
	}
	fresh, empty, err := registeredCurrentRoot(s, registeredGuardLimits, 0)
	if !errors.Is(err, errInvalid) || !reflect.DeepEqual(fresh, raftlog.ApplicationRoot{}) || empty != (graphstore.Root{}) {
		t.Fatal("fresh root must reject present Origin", err)
	}
	v, err := s.ApplicationView(base.Index)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*raftlog.ApplicationRoot, *raftlog.ApplicationBinding){
		func(b *raftlog.ApplicationRoot, id *raftlog.ApplicationBinding) { b.Index = 2 },
		func(b *raftlog.ApplicationRoot, id *raftlog.ApplicationBinding) { b.ImageHash[0] ^= 1 },
		func(b *raftlog.ApplicationRoot, id *raftlog.ApplicationBinding) { id.SemanticContractID[0] ^= 1 },
		func(b *raftlog.ApplicationRoot, id *raftlog.ApplicationBinding) { id.Identity.Graph[0] ^= 1 },
	} {
		invalid, binding := base, s.ApplicationBinding()
		change(&invalid, &binding)
		got, err := readRegisteredOrigin(v, s, invalid, root, binding)
		if !errors.Is(err, errCorrupt) || got != (registeredOrigin{}) {
			t.Fatal("invalid same-view input must be zero", err)
		}
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := readRegisteredOrigin(v, s, base, root, s.ApplicationBinding())
	if !errors.Is(err, raftlog.ErrClosed) || got != (registeredOrigin{}) {
		t.Fatal("closed control view must be zero", err)
	}
	var held []*raftlog.ApplicationView
	defer func() {
		for _, view := range held {
			if err := view.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	for range s.ApplicationLimits().MaxViews - 1 {
		view, err := s.ApplicationView(base.Index)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, view)
	}
	registeredGuardZeroState(t, s, registeredGuardLimits, raftlog.ErrLimit, nil)
	for _, view := range held {
		if err := view.Close(); err != nil {
			t.Fatal(err)
		}
	}
	held = nil
	if _, _, _, err := registeredCurrentState(s, registeredGuardLimits, 0); err != nil {
		t.Fatal("released shared pins must restore usability", err)
	}
}
