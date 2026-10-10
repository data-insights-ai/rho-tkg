package graphstore

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// Declaration-only fixture agreement/machine, never graphapply/Host support.
type ownershipFixtureMachine struct {
	store       *raftlog.Store
	declaration OwnershipDeclaration
	command     []byte
	semantic    raftlog.ApplicationSemanticContractID
}

func (m *ownershipFixtureMachine) SemanticContractID() raftlog.ApplicationSemanticContractID {
	return m.semantic
}
func (m *ownershipFixtureMachine) Restore(index uint64, image []byte) (err error) {
	v, err := m.store.ApplicationView(index)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, v.Close()) }()
	base, err := v.RootBounded(context.Background(), ownershipRootBytes)
	if err != nil {
		return err
	}
	if !bytes.Equal(base.Image, image) {
		return ErrInvalid
	}
	read, _, err := ReadOwnershipDeclaration(context.Background(), v, Limits{}, ownershipBudget())
	if err != nil {
		return err
	}
	if read.Published() && read.Declaration().Digest() != m.declaration.Digest() {
		return ErrCorrupt
	}
	return nil
}
func (m *ownershipFixtureMachine) Stage(entry replica.Entry, budget raftlog.ApplicationBudget) (batch raftlog.ApplicationBatch, err error) {
	index, _, err := m.store.Checkpoint()
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	v, err := m.store.ApplicationView(index)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	defer func() {
		if closeErr := v.Close(); closeErr != nil {
			batch = raftlog.ApplicationBatch{}
			err = errors.Join(err, closeErr)
		}
	}()
	base, err := v.RootBounded(context.Background(), ownershipRootBytes)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if entry.Generation != base.Generation || entry.Index != base.Index+1 {
		return raftlog.ApplicationBatch{}, ErrInvalid
	}
	batch = raftlog.ApplicationBatch{BaseGeneration: base.Generation, BaseIndex: base.Index, BaseImageHash: base.ImageHash, Image: base.Image}
	if len(entry.Data) != 0 {
		if !bytes.Equal(entry.Data, m.command) {
			return raftlog.ApplicationBatch{}, ErrInvalid
		}
		effects, _, err := StageOwnershipDeclaration(context.Background(), v, m.declaration, Limits{}, ownershipBudget())
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		batch.Image, err = EncodeRoot(effects.Root)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		batch.Writes = effects.Writes
		digest := m.declaration.Digest()
		batch.Outcome = exactCopy(digest[:]) // Opaque metadata fixture outcome only.
	}
	policy := m.store.ApplicationLimits()
	policy.MaxInstallWrites = min(policy.MaxInstallWrites, budget.Writes)
	policy.MaxInstallBytes = min(policy.MaxInstallBytes, budget.Bytes)
	policy.MaxImageBytes = min(policy.MaxImageBytes, budget.ImageBytes)
	policy.MaxChangeBytes = min(policy.MaxChangeBytes, budget.ChangeBytes)
	policy.MaxOutcomeBytes = min(policy.MaxOutcomeBytes, budget.OutcomeBytes)
	if _, err := policy.Preflight(batch, m.store.Limits()); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	return batch, nil
}

func TestOwnershipDeclarationActualThreeVoterPublicationRetainedAndReopen(t *testing.T) {
	declaration := ownershipDecl(t, 4)
	command, err := encodeOwnershipDeclaration(declaration, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	stores := [3]*raftlog.Store{}
	drivers := [3]*replica.Driver{}
	configs := [3]raftlog.Config{}
	filesystems := [3]*vfs.MemFS{}
	old := [3]*raftlog.ApplicationView{}
	for i := range 3 {
		fs := vfs.NewCrashableMem()
		filesystems[i] = fs
		// Exactly three actual stores/RawNodes: no temporary replica fixture.
		local := Namespace{Graph: declaration.Graph(), Partition: 1}
		root, err := NewOwnershipRoot(local, declaration)
		if err != nil {
			t.Fatal(err)
		}
		cfg := ownershipConfig(fs, local, [16]byte{9}, uint64(i+1))
		image, err := EncodeRoot(root)
		if err != nil {
			t.Fatal(err)
		}
		s, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Initialize([]uint64{1, 2, 3}, image); err != nil {
			t.Fatal(err)
		}
		stores[i], configs[i] = s, cfg
		old[i] = ownershipView(t, s, 1)
		machine := &ownershipFixtureMachine{s, declaration, command, cfg.SemanticContractID}
		driver, err := replica.Open(replica.Config{ID: uint64(i + 1), Store: s, ApplicationMachine: machine, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		if err != nil {
			t.Fatal(err)
		}
		drivers[i] = driver
		t.Cleanup(func() {
			if err := driver.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	reads := []replica.ReadResult{}
	packets := 0
	deliver := func(output replica.Output) {
		t.Helper()
		queue := output.Packets
		reads = append(reads, output.Reads...)
		for len(queue) != 0 {
			if packets >= 4096 || len(queue) > 128 {
				t.Fatal("bounded metadata transport exhausted", packets, len(queue))
			}
			packet := queue[0]
			queue = queue[1:]
			packets++
			if packet.Snapshot || packet.To < 1 || packet.To > 3 || len(packet.Payload) > 2<<20 {
				t.Fatal("unexpected metadata packet", packet.From, packet.To)
			}
			out, err := drivers[packet.To-1].Step(packet)
			if err != nil {
				t.Fatal(err)
			}
			if len(out.SnapshotSends) != 0 {
				t.Fatal("metadata fixture unexpectedly needs a snapshot")
			}
			reads = append(reads, out.Reads...)
			queue = append(queue, out.Packets...)
		}
	}
	out, err := drivers[0].Campaign()
	if err != nil {
		t.Fatal(err)
	}
	deliver(out)
	out, err = drivers[0].Propose(command)
	if err != nil {
		t.Fatal(err)
	}
	deliver(out)
	for range 2 {
		for _, driver := range drivers {
			out, err := driver.Tick()
			if err != nil {
				t.Fatal(err)
			}
			deliver(out)
		}
	}
	token := []byte("metadata-declaration-publication")
	out, err = drivers[0].ReadIndex(token)
	if err != nil {
		t.Fatal(err)
	}
	deliver(out)
	barrier := uint64(0)
	for _, read := range reads {
		if bytes.Equal(read.Context, token) {
			barrier = read.Index
		}
	}
	if barrier < 3 {
		t.Fatal("request-bound quorum barrier missing", reads)
	}
	// Independent expected qualified map, not declaration accessor output.
	want := map[uint64]PartitionOwnership{1: {1, 10, [16]byte{9}}, 2: {2, 11, [16]byte{9}}, 3: {3, 12, [16]byte{9}}, 4: {4, 13, [16]byte{9}}}
	for i, driver := range drivers {
		if driver.Applied() < barrier {
			t.Fatal("follower behind barrier", i, driver.Applied(), barrier)
		}
		read, _, err := ReadOwnershipDeclaration(t.Context(), ownershipView(t, stores[i], barrier), Limits{}, ownershipBudget())
		if err != nil || !read.Published() || read.Root().epoch != 0 || read.Root().next != 1 || read.Binding().Identity.Partition != 1 || read.Declaration().Len() != len(want) {
			t.Fatal(read, err)
		}
		for n := range read.Declaration().Len() {
			entry, _ := read.Declaration().PartitionAt(n)
			if entry != want[entry.Partition] {
				t.Fatal("qualified declaration differs from independent map", entry)
			}
		}
		pending, _, err := ReadOwnershipDeclaration(t.Context(), old[i], Limits{}, ownershipBudget())
		if err != nil || pending.Published() || pending.Declaration().Len() != 0 {
			t.Fatal("held pending view advanced with publication", pending, err)
		}
		if _, err := OpenCatalog(ownershipView(t, stores[i], barrier), read.Root().namespace, read.Root().owner, Limits{}); !errors.Is(err, ErrTopologyUnsupported) {
			t.Fatal("metadata enabled graph operations", err)
		}
		usage, err := stores[i].ApplicationUsage()
		if err != nil || usage.RetainedRecords != 3*driver.Applied()+1 {
			t.Fatal("declaration not written exactly once", usage, err)
		}
	}
	out, err = drivers[0].Propose(command)
	if err != nil {
		t.Fatal(err)
	}
	deliver(out)
	for _, driver := range drivers {
		out, err := driver.Tick()
		if err != nil {
			t.Fatal(err)
		}
		deliver(out)
	}
	for i, driver := range drivers {
		usage, err := stores[i].ApplicationUsage()
		if err != nil || usage.RetainedRecords != 3*driver.Applied()+1 {
			t.Fatal("identical retry wrote another declaration version", usage, err)
		}
		crash := filesystems[i].CrashClone(vfs.CrashCloneCfg{})
		if err := driver.Close(); err != nil {
			t.Fatal(err)
		}
		cfg := configs[i]
		cfg.FS, cfg.Create = crash, false
		reopened, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		read, _, err := ReadOwnershipDeclaration(t.Context(), ownershipView(t, reopened, barrier), Limits{}, ownershipBudget())
		if err != nil || !read.Published() || read.Declaration().Digest() != declaration.Digest() {
			t.Fatal("reopen lost metadata declaration", read, err)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
