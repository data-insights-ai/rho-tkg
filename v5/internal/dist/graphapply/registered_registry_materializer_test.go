package graphapply

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"
	"unsafe"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Breaks a materializer that fails to create both immutable registration rows.
// Public Store unit evidence is not a quorum, Driver, factory or Host admission.
func TestRegisteredRegistryStageFirstCreatedAtActualEntry(t *testing.T) {
	literal := func(text string) []byte {
		t.Helper()
		b, err := hex.DecodeString(text)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	image := literal("47520202010000000000000000000000000000000000000000000001000000000000000100000000000000000000000000000001945c9793df894a15367965dd4283c91b220042f2e9988b6c554ea59e5864334c00000000000000010000000000000001000000000000000026c51b3182da8ee986af5a09019cd9574cebbd2008953bbdcf8388e55d17e3f5")
	hki := literal("484b49310201000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010103000000000000000000000000000000")
	hkv := literal("484b56310301000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f000100000000000000040000")
	lookup := literal("484352310001020001000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010000000000000004000000b7010300000000000000000000000000000001000000000000000226a28dedef197296c095d19f0334e2d51406b22efc1a4eed50cd6d38b13b53ca47dbf9fa532d24438f384b551cf473352be09e20531df0d23f56f076dc3411f2484b56310301000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f000100000000000000040000000001029f790fb77ec398a44d94a9d61ad0cccda1d750af8fe3a08958ace6392b847e75")
	stored := literal("484352310001030001000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f0001000000000000000400000160010300000000000000000000000000000001000000000000000226a28dedef197296c095d19f0334e2d51406b22efc1a4eed50cd6d38b13b53ca47dbf9fa532d24438f384b551cf473352be09e20531df0d23f56f076dc3411f20000010250494a3100010001000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010103000000000000000000000000000000011587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc60da51c70000007a424453310001000000004c4447503100010000010a0000000000000000000000000000000100010000000c63616d6572612d636c6f636b0000000b6d696c6c697365636f6e640a00000000000000000000000000000000000001040000000000000000000000000000000000000000000001000000000000004000e3691336de4646185cb1c7c71778f6a45df24e59c9a7b79a71b98e56412e2bba")
	created := literal("4a4f5231000101000000000000000400a1484b49310201000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f000101030000000000000000000000000000000126a28dedef197296c095d19f0334e2d51406b22efc1a4eed50cd6d38b13b53ca47dbf9fa532d24438f384b551cf473352be09e20531df0d23f56f076dc3411f202484b49310201000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f000101030000000000000000000000000000000126a28dedef197296c095d19f0334e2d51406b22efc1a4eed50cd6d38b13b53ca47dbf9fa532d24438f384b551cf473352be09e20531df0d23f56f076dc3411f2484b56310301000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010000000000000004000000000000000000027c6df5b1706823114c51f041dce0039deced64069f457128fffa182356055b31")
	var imageHash [32]byte
	copy(imageHash[:], literal("12cf5d257654d0fa3779b2b4026d838440ec14e65c1ad4abda97080ecc01f86a"))
	s := inventoryTestStore(t)
	inventoryGenesis(t, s, inventoryLiteral(t, "OriginHCR378"))
	audit, err := newRegisteredCurrentInventory(t.Context(), s, registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960})
	if err != nil {
		t.Fatal("real full-policy current audit constructor", err)
	}
	t.Cleanup(func() {
		if err := audit.Close(); err != nil {
			t.Error("actual audit owner cleanup", err)
		}
	})
	proof := inventoryComplete(t, audit)
	if proof.base.Index != 3 || proof.base.Generation != 1 || proof.base.ImageHash != imageHash || !bytes.Equal(proof.base.Image, image) || proof.controlBytes != 510 || proof.controlRecords != 1 || proof.graphBytes != 970 || proof.graphRecords != 9 || proof.pairs != 0 || proof.origin.index != 3 || proof.origin.term != 2 {
		t.Fatal("complete actual Source1/S3/term/census baseline", proof)
	}
	before, err := s.ApplicationControlUsage()
	if err != nil || before != (raftlog.ApplicationControlUsage{Bytes: 510, Records: 1, TotalBytes: 1480, TotalRecords: 10}) {
		t.Fatal("actual whole captured bank", before, err)
	}
	if _, found, _, err := audit.view.GetControl(t.Context(), hki, raftlog.ReadBudget{Rows: 2, Bytes: 1819}); err != nil || found {
		t.Fatal("fresh first key absent before registration", found, err)
	}
	want := raftlog.ApplicationBatch{BaseGeneration: 1, BaseIndex: 3, BaseImageHash: imageHash, Image: image, Outcome: created, ControlPuts: []raftlog.ApplicationControlPut{{Key: hki, Value: lookup}, {Key: hkv, Value: stored}}}
	usage, err := s.ApplicationLimits().PreflightControlBatch(want, s.Limits())
	if err != nil || usage.ControlBytes != 1166 || usage.ControlRecords != 2 || usage.RetainedBytes != 1910 || usage.RetainedRecords != 5 {
		t.Fatal("independent expected full Created tuple fits actual profile", usage, err)
	}
	wire := inventoryLiteral(t, "RJQ269")
	entry := &pb.Entry{Index: new(uint64(4)), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: wire}
	if err := s.Persist(raft.Ready{Entries: []*pb.Entry{entry}, HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(4))}}); err != nil {
		t.Fatal("public unit Ready commits actual R4/term2 input", err)
	}
	term, err := s.Term(4)
	if err != nil || term != 2 {
		t.Fatal("actual registration entry term", term, err)
	}
	entries, err := s.Entries(4, 5, uint64(s.Limits().MaxReadBytes))
	if err != nil || len(entries) != 1 || entries[0].GetIndex() != 4 || entries[0].GetTerm() != 2 || !bytes.Equal(entries[0].GetData(), wire) {
		t.Fatal("actual durable Entry Data before Stage", entries, err)
	}
	index, checkpoint, err := s.Checkpoint()
	if err != nil || index != 3 || !bytes.Equal(checkpoint, image) || s.ApplicationGeneration() != 1 {
		t.Fatal("Ready cannot install registry evidence before Stage", index, err)
	}
	t.Log("REGISTER_STAGE_PUBLIC_BASELINE_PASS cut3/gen1 R4/term2 controls510/1 total1480/10")
	// The initial scaffold only holds these real unit inputs; it admits nothing.
	m, err := newRegisteredRegistryMaterializer(s, audit, registeredRegistryStageLimits{workBytes: 134869620, ownedBytes: 17411056})
	if err != nil {
		t.Fatal("inert Stage scaffold setup", err)
	}
	got, stageErr := m.Stage(replica.Entry{Generation: 1, Index: 4, Term: 2, Data: wire}, s.ApplicationBudget())
	if after, err := s.ApplicationControlUsage(); err != nil || after != before {
		t.Fatal("Stage alone has no durable effect even when unavailable", after, err)
	}
	if stageErr != nil && (!errors.Is(stageErr, errRegisteredRegistryStageUnavailable) || got.BaseGeneration != 0 || got.BaseIndex != 0 || got.BaseImageHash != ([32]byte{}) || got.Image != nil || got.Writes != nil || got.Changes != nil || got.Outcome != nil || got.ControlPuts != nil) {
		t.Fatal("only honest unavailable whole-zero Stage is the intended RED", got, stageErr)
	}
	if stageErr != nil || got.BaseGeneration != 1 || got.BaseIndex != 3 || got.BaseImageHash != imageHash || !bytes.Equal(got.Image, image) || got.Writes != nil || got.Changes != nil || !bytes.Equal(got.Outcome, created) || len(got.ControlPuts) != 2 {
		t.Fatalf("want independent exact Created registration at actual R4/term2 with2controls, got Stage error:%v", stageErr)
	}
	for i, expected := range want.ControlPuts {
		if !bytes.Equal(got.ControlPuts[i].Key, expected.Key) || !bytes.Equal(got.ControlPuts[i].Value, expected.Value) {
			t.Fatal("complete sorted immutable HKI/HKV tuple", i)
		}
	}
	if after, err := s.ApplicationControlUsage(); err != nil || after != before {
		t.Fatal("Stage alone has no durable effect", after, err)
	}
}

func registryUnitLimits() registeredRegistryStageLimits {
	return registeredRegistryStageLimits{workBytes: 134869620, ownedBytes: 17411056}
}
func registryUnitMachine(t *testing.T, s *raftlog.Store, l registeredRegistryStageLimits) *registeredRegistryMaterializer {
	t.Helper()
	a, err := newRegisteredCurrentInventory(t.Context(), s, registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960})
	if err != nil {
		t.Fatal(err)
	}
	inventoryComplete(t, a)
	m, err := newRegisteredRegistryMaterializer(s, a, l)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	return m
}
func registryUnitEntry(t *testing.T, s *raftlog.Store, index, term uint64, data []byte) replica.Entry {
	t.Helper()
	e := &pb.Entry{Index: new(index), Term: new(term), Type: pb.EntryNormal.Enum(), Data: data}
	if err := s.Persist(raft.Ready{Entries: []*pb.Entry{e}, HardState: &pb.HardState{Term: new(term), Commit: new(index)}}); err != nil {
		t.Fatal(err)
	}
	return replica.Entry{Generation: s.ApplicationGeneration(), Index: index, Term: term, Data: data}
}
func registryUnitInstall(t *testing.T, s *raftlog.Store, m *registeredRegistryMaterializer, e replica.Entry, b raftlog.ApplicationBatch) {
	t.Helper()
	if err := s.InstallApplication(e.Index, b); err != nil {
		t.Fatal(err)
	}
	if err := m.Restore(e.Index, b.Image); err != nil {
		t.Fatal("verify actual synced adjacent delta", err)
	}
}
func registryUnitZero(t *testing.T, b raftlog.ApplicationBatch, err, want error) {
	t.Helper()
	if !errors.Is(err, want) || b.BaseGeneration != 0 || b.BaseIndex != 0 || b.BaseImageHash != ([32]byte{}) || b.Image != nil || b.Changes != nil || b.Outcome != nil || b.Writes != nil || b.ControlPuts != nil {
		t.Fatal("wholezero typed refusal", b, err, want)
	}
}

// Breaks new-input admission before durable original resolution, including
// Original with one allowed write and an install allowance too small for Created.
func TestRegisteredRegistryOriginalConflictAndNoop(t *testing.T) {
	vectors, _ := registryVectors(t)
	s := inventorySmall(t)
	l := registryUnitLimits()
	l.newPreparedBytes = 257
	m := registryUnitMachine(t, s, l)
	e := registryUnitEntry(t, s, 5, 3, inventoryLiteral(t, "RJQ269"))
	budget := s.ApplicationBudget()
	budget.Writes = 1
	budget.Bytes = 2652
	b, err := m.Stage(e, budget)
	if err != nil || b.ControlPuts != nil || b.Writes != nil || b.Changes != nil || !bytes.Equal(b.Outcome, registryBytes(t, vectors["base"].OriginalE5JOR)) {
		t.Fatal("fresh complete original precedes257-byte cap/onewrite/2652install", b, err)
	}
	registryUnitInstall(t, s, m, e, b)
	if m.base.Index != 5 || m.usage.Bytes != 1676 || m.usage.Records != 3 || m.pending != nil {
		t.Fatal("Original retains immutable R4, advances evaluation5 only")
	}
	e = registryUnitEntry(t, s, 6, 3, registryBytes(t, vectors["changed-reference"].RJQ))
	b, err = m.Stage(e, budget)
	if err != nil || b.ControlPuts != nil || !bytes.Equal(b.Outcome, registryBytes(t, vectors["changed-reference"].ConflictAgainstBaseE6JOR)) {
		t.Fatal("changed canonical payload tag3 Conflict without effects", b, err)
	}
	registryUnitInstall(t, s, m, e, b)
	out, err := decodeRegistryOutcome(b.Outcome, registryTestBudget())
	if err != nil || out.status != 3 || out.evaluationIndex != 6 || out.original.position != 4 || out.original.term != 2 {
		t.Fatal("actual evaluation and immutable original coordinates separate", out, err)
	}
	e = registryUnitEntry(t, s, 7, 3, registryBytes(t, vectors["declared-POSIX-identity-ms"].RJQ))
	b, err = m.Stage(e, budget)
	if err != nil || b.ControlPuts != nil {
		t.Fatal("changed kind is same HKI Conflict", err)
	}
	out, err = decodeRegistryOutcome(b.Outcome, registryTestBudget())
	if err != nil || out.status != 3 || out.submitted.kind != 2 || out.original.submitted.kind != 1 {
		t.Fatal("kind is value, not lookup identity", out, err)
	}
	registryUnitInstall(t, s, m, e, b)
	e = registryUnitEntry(t, s, 8, 3, nil)
	budget.Bytes = 1714
	b, err = m.Stage(e, budget)
	if err != nil || b.Outcome != nil || b.ControlPuts != nil || b.Writes != nil || b.Changes != nil {
		t.Fatal("adjacent noop exact empty output envelopes", b, err)
	}
	registryUnitInstall(t, s, m, e, b)
	if err := m.Restore(8, b.Image); err != nil {
		t.Fatal("samebase opening restore freshly revalidated", err)
	}
}
func TestRegisteredRegistrySuppliedJointBudgetBeforeOutput(t *testing.T) {
	vectors, _ := registryVectors(t)
	for _, tc := range []struct {
		name     string
		existing bool
		wire     []byte
		exact    int
	}{{"Created", false, inventoryLiteral(t, "RJQ269"), 6596}, {"Original", true, inventoryLiteral(t, "RJQ269"), 2652}, {"Conflict", true, registryBytes(t, vectors["changed-reference"].RJQ), 2652}, {"noop", false, nil, 1714}} {
		t.Run(tc.name, func(t *testing.T) {
			s := inventoryTestStore(t)
			inventoryGenesis(t, s, inventoryLiteral(t, "OriginHCR378"))
			index := uint64(4)
			if tc.existing {
				inventoryInstall(t, s, 4, inventoryLiteral(t, "RJQ269"), inventoryLiteral(t, "RegistrationCreatedJOR469"), []raftlog.ApplicationControlPut{{Key: inventoryLiteral(t, "HKI96"), Value: inventoryLiteral(t, "LookupHCR309")}, {Key: inventoryLiteral(t, "HKV89"), Value: inventoryLiteral(t, "StoredHCR478")}})
				index = 5
			}
			m := registryUnitMachine(t, s, registryUnitLimits())
			e := registryUnitEntry(t, s, index, 2, tc.wire)
			before, err := s.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			budget := s.ApplicationBudget()
			for _, bytes := range []int{1, tc.exact - 1} {
				budget.Bytes = bytes
				b, err := m.Stage(e, budget)
				registryUnitZero(t, b, err, errLimit)
				after, err := s.ApplicationUsage()
				if err != nil || after != before || m.pending != nil || m.base.Index != index-1 {
					t.Fatal("joint quota refusal changes no owner/prefix/durable effects", after, err)
				}
			}
			budget.Bytes = tc.exact
			b, err := m.Stage(e, budget)
			if err != nil {
				t.Fatal("exact source-derived joint installation admission", tc.exact, err)
			}
			registryUnitInstall(t, s, m, e, b)
		})
	}
}
func TestRegisteredRegistryAdoptionAndFreshness(t *testing.T) {
	s := inventorySmall(t)
	a, err := newRegisteredCurrentInventory(t.Context(), s, registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960})
	if err != nil {
		t.Fatal(err)
	}
	returned := inventoryComplete(t, a)
	returned.base.Image[0] ^= 1
	returned.controlBytes = 0
	returned.origin = registeredOrigin{}
	copied := registeredInventoryAudit{self: a.self, store: s}
	if m, err := newRegisteredRegistryMaterializer(s, &copied, registryUnitLimits()); m != nil || !errors.Is(err, errInvalid) {
		t.Fatal("copied audit cannot consume original", m, err)
	}
	m, err := newRegisteredRegistryMaterializer(s, a, registryUnitLimits())
	if err != nil {
		t.Fatal("actual retained audit fields, not caller mutable summary", err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	if !bytes.Equal(m.base.Image, inventoryLiteral(t, "GR2")) || m.usage.Bytes != 1676 || m.origin.index != 3 {
		t.Fatal("mutated summary never becomes machine evidence")
	}
	if second, err := newRegisteredRegistryMaterializer(s, a, registryUnitLimits()); second != nil || !errors.Is(err, errInvalid) {
		t.Fatal("genuine audit consumed only once", second, err)
	}
	if usage, err := s.ApplicationUsage(); err != nil || usage.Views != 0 {
		t.Fatal("adoption closes actual audit view", usage, err)
	}
	// Public advancement is genuine and legal; a retained pin/cached summary
	// never lets Stage treat the old cut as current.
	inventoryInstall(t, s, 5, nil, nil, nil)
	e := registryUnitEntry(t, s, 6, 2, inventoryLiteral(t, "RJQ269"))
	b, err := m.Stage(e, s.ApplicationBudget())
	registryUnitZero(t, b, err, errRegisteredRegistryStageUnavailable)
	if m.base.Index != 4 || m.pending != nil {
		t.Fatal("drift cannot advance private verified base")
	}
	if err := m.Restore(5, inventoryLiteral(t, "GR2")); !errors.Is(err, errRegisteredRegistryStageUnavailable) {
		t.Fatal("unknown adjacent install requires new complete audit", err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal("repeat cleanup keeps cause", err)
	}
	b, err = m.Stage(e, s.ApplicationBudget())
	registryUnitZero(t, b, err, raftlog.ErrClosed)
	if err := m.Restore(4, inventoryLiteral(t, "GR2")); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("closed restore", err)
	}
	fresh := registryUnitMachine(t, s, registryUnitLimits())
	if err := fresh.Restore(5, inventoryLiteral(t, "GR2")); err != nil {
		t.Fatal("new full audit permits reopened current restore", err)
	}
}
func TestRegisteredRegistryPendingAndOwnedOutputs(t *testing.T) {
	s := inventoryTestStore(t)
	inventoryGenesis(t, s, inventoryLiteral(t, "OriginHCR378"))
	m := registryUnitMachine(t, s, registryUnitLimits())
	wire := inventoryLiteral(t, "RJQ269")
	e := registryUnitEntry(t, s, 4, 2, wire)
	b, err := m.Stage(e, s.ApplicationBudget())
	if err != nil {
		t.Fatal(err)
	}
	if cap(b.Image) != 140 || cap(b.Outcome) != 469 || len(b.ControlPuts) != 2 || cap(b.ControlPuts) != 2 || cap(b.ControlPuts[0].Key) != 96 || cap(b.ControlPuts[1].Key) != 89 || cap(b.ControlPuts[0].Value) != 309 || cap(b.ControlPuts[1].Value) != 478 {
		t.Fatal("exact owned complete output tuple", b)
	}
	wire[11] ^= 1 // Input ownership ended; emitted data and pending refs remain immutable.
	if !bytes.Equal(b.ControlPuts[1].Value, inventoryLiteral(t, "StoredHCR478")) {
		t.Fatal("owned Stored borrows no caller PIJ")
	}
	b.Image[0] ^= 1
	if !bytes.Equal(m.base.Image, inventoryLiteral(t, "GR2")) {
		t.Fatal("owned batch image cannot mutate machine base")
	}
	b.Image[0] ^= 1
	again, err := m.Stage(e, s.ApplicationBudget())
	registryUnitZero(t, again, err, errInvalid)
	if err := m.Restore(4, b.Image); !errors.Is(err, errRegisteredRegistryStageUnavailable) {
		t.Fatal("pending Stage cannot pretend uninstalled durability", err)
	}
	if err := m.Restore(3, b.Image); !errors.Is(err, errRegisteredRegistryStageUnavailable) {
		t.Fatal("wrong pending coordinate", err)
	}
	registryUnitInstall(t, s, m, e, b)
	if m.base.Index != 4 || m.usage.Bytes != 1676 || m.pending != nil {
		t.Fatal("commit only actual verified delta")
	}
	if err := m.Restore(6, b.Image); !errors.Is(err, errRegisteredRegistryStageUnavailable) {
		t.Fatal("nonadjacent requires fresh inventory", err)
	}
	if err := m.Restore(4, []byte{1}); !errors.Is(err, errInvalid) {
		t.Fatal("bounded incoming restore image", err)
	}
	copied := registeredRegistryMaterializer{self: m.self, store: s}
	if err := copied.Close(); !errors.Is(err, errInvalid) {
		t.Fatal("copied machine never closes original", err)
	}
	if err := new(registeredRegistryMaterializer).Close(); !errors.Is(err, errInvalid) {
		t.Fatal("forged zero machine", err)
	}
}

func TestRegisteredRegistryLargeOriginalJournalAndReopen(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	s, cfg := inventoryTestStoreConfig(t)
	inventoryGenesis(t, s, inventoryLiteral(t, "OriginHCR378"))
	m := registryUnitMachine(t, s, registryUnitLimits())
	large := inventoryLargePIJ(t) // Existing independent legal1024-axis grammar, no padding/second producer.
	var firstWire []byte
	for j := range 3 {
		pij := bytes.Clone(large)
		pij[82] = byte(5 + j)
		wire, err := encodeRegistryCommand(pij, registryTestFormat(), registryTestBudget())
		if err != nil {
			t.Fatal(err)
		}
		if j == 0 {
			firstWire = wire
		}
		e := registryUnitEntry(t, s, uint64(4+j), 2, wire)
		b, err := m.Stage(e, s.ApplicationBudget())
		if err != nil {
			t.Fatal("full legal Created", j, err)
		}
		if len(b.ControlPuts) != 2 || len(b.ControlPuts[1].Value) != 4192684 || cap(b.ControlPuts[1].Value) != 4192684 {
			t.Fatal("full Stored exact owned bytes")
		}
		registryUnitInstall(t, s, m, e, b)
	}
	usage, err := s.ApplicationControlUsage()
	if err != nil || usage.Bytes <= 8<<20 || usage.Records != 7 {
		t.Fatal("journal exceeds8MiB without cumulative cap", usage, err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	recovered, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal("actual Close/Open", err)
	}
	t.Cleanup(func() {
		if err := recovered.Close(); err != nil {
			t.Error(err)
		}
	})
	l := registryUnitLimits()
	l.newPreparedBytes = 257
	opened := registryUnitMachine(t, recovered, l)
	e := registryUnitEntry(t, recovered, 7, 3, firstWire)
	budget := recovered.ApplicationBudget()
	budget.Writes = 1
	budget.Bytes = 2652
	b, err := opened.Stage(e, budget)
	if err != nil || b.ControlPuts != nil {
		t.Fatal("full4MiB original before new257cap and tiny Created-ineligible outputbudget", err)
	}
	out, err := decodeRegistryOutcome(b.Outcome, registryTestBudget())
	if err != nil || out.status != 2 || out.evaluationIndex != 7 || out.original.position != 4 || out.original.term != 2 {
		t.Fatal("original retained addressing after compaction/reopen", out, err)
	}
	registryUnitInstall(t, recovered, opened, e, b)
	if after, err := recovered.ApplicationControlUsage(); err != nil || after.Bytes != usage.Bytes || after.Records != 7 {
		t.Fatal("large Original adds no controls", after, err)
	}
	fresh := bytes.Clone(firstWire)
	fresh[11+82] = 9
	e = registryUnitEntry(t, recovered, 8, 3, fresh)
	b, err = opened.Stage(e, recovered.ApplicationBudget())
	registryUnitZero(t, b, err, errLimit)
	if opened.base.Index != 7 || opened.pending != nil {
		t.Fatal("newcap rejection does not advance verified original prefix")
	}
}
func TestRegisteredRegistryMalformedAndCoordinateRefusals(t *testing.T) {
	for _, name := range []string{"magic", "length", "scope", "meaning", "union", "index", "generation", "term", "emptybudget"} {
		t.Run(name, func(t *testing.T) {
			s := inventoryTestStore(t)
			inventoryGenesis(t, s, inventoryLiteral(t, "OriginHCR378"))
			m := registryUnitMachine(t, s, registryUnitLimits())
			wire := inventoryLiteral(t, "RJQ269")
			e := registryUnitEntry(t, s, 4, 2, wire)
			want := errInvalid
			switch name {
			case "magic":
				e.Data = bytes.Clone(wire)
				e.Data[0] ^= 1
			case "length":
				e.Data = wire[:len(wire)-1]
			case "scope":
				e.Data = bytes.Clone(wire)
				e.Data[11+8] ^= 1
			case "meaning":
				e.Data = bytes.Clone(wire)
				e.Data[11+99] ^= 1
			case "union":
				e.Data = bytes.Clone(wire)
				e.Data[len(wire)-1] = 1
			case "index":
				e.Index = 5
			case "generation":
				e.Generation = 2
			case "term":
				e.Term = 3
			}
			before, err := s.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			budget := s.ApplicationBudget()
			if name == "emptybudget" {
				budget.Bytes = 0
			}
			b, err := m.Stage(e, budget)
			registryUnitZero(t, b, err, want)
			after, err := s.ApplicationUsage()
			if err != nil || after != before || m.pending != nil || m.base.Index != 3 {
				t.Fatal("refusal has no view/pending/durable effects", after, err)
			}
		})
	}
	// Immutable format overflow is a static bounded slice, never an I/O fault.
	s := inventorySmall(t)
	m := registryUnitMachine(t, s, registryUnitLimits())
	b, err := m.Stage(replica.Entry{Data: make([]byte, registryPreparedMax+12)}, s.ApplicationBudget())
	registryUnitZero(t, b, err, errLimit)
	if nilMachine := (*registeredRegistryMaterializer)(nil); nilMachine.SemanticContractID() != ([32]byte{}) {
		t.Fatal("nil agreement is wholezero")
	}
	b, err = (*registeredRegistryMaterializer)(nil).Stage(replica.Entry{}, s.ApplicationBudget())
	registryUnitZero(t, b, err, errInvalid)
	if err := (*registeredRegistryMaterializer)(nil).Restore(1, nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil restore", err)
	}
	if out, err := newRegisteredRegistryMaterializer(nil, nil, registryUnitLimits()); out != nil || !errors.Is(err, errInvalid) {
		t.Fatal("nil constructor", err)
	}
}
func TestRegisteredRegistryQuotaAndPureJointGuards(t *testing.T) {
	s := inventoryTestStore(t)
	inventoryGenesis(t, s, inventoryLiteral(t, "OriginHCR378"))
	// The independently enumerated first small Stage debits include actual
	// 2E+128 term admission, BOTH full-policy preflight passes, and extra JOR hash.
	const work = 629374 + 1724 + 8388906 + 8271 + 269 + 9517 + 1819 + 4195950 + 5503 + 280 + 16792160 + 469
	const peak = 629374 + 269 + 140 + 140 + 2*4195950
	for _, tc := range []struct {
		name string
		w, o uint64
		ok   bool
	}{{"workshort", work - 1, peak, false}, {"ownershort", work, peak - 1, false}, {"exact", work, peak, true}} {
		t.Run(tc.name, func(t *testing.T) {
			store := inventoryTestStore(t)
			inventoryGenesis(t, store, inventoryLiteral(t, "OriginHCR378"))
			l := registryUnitLimits()
			l.workBytes, l.ownedBytes = tc.w, tc.o
			m := registryUnitMachine(t, store, l)
			e := registryUnitEntry(t, store, 4, 2, inventoryLiteral(t, "RJQ269"))
			before, err := store.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			b, err := m.Stage(e, store.ApplicationBudget())
			if tc.ok {
				if err != nil {
					t.Fatal("exact source/peak model", err)
				}
			} else {
				registryUnitZero(t, b, err, errLimit)
				after, err := store.ApplicationUsage()
				if err != nil || after != before || m.pending != nil {
					t.Fatal("quota refusal changes no owner state", err)
				}
			}
		})
	}
	p := s.ApplicationLimits()
	b := s.ApplicationBudget()
	ref := registryExpectedRef(t, func() registryVector { v, _ := registryVectors(t); return v["base"] }())
	for _, which := range []string{"image", "outcome", "writes", "key", "value", "bytes", "n"} {
		p1, b1, n := p, b, 258
		switch which {
		case "image":
			b1.ImageBytes = 139
		case "outcome":
			b1.OutcomeBytes = 468
		case "writes":
			b1.Writes = 1
		case "key":
			p1.MaxKeyBytes = 95
		case "value":
			p1.MaxValueBytes = 477
		case "bytes":
			b1.Bytes = 6595
		case "n":
			n = 135
		}
		want := errLimit
		if which == "n" {
			want = errInvalid
		}
		if err := registryJointAdmission(p1, b1, 1, n, ref); !errors.Is(err, want) {
			t.Fatal("pure branch-specific preallocation admission", which, err)
		}
	}
	for _, bad := range []registeredRegistryStageLimits{{}, {workBytes: 1, ownedBytes: 1}, {workBytes: 134869620, ownedBytes: 17411056, newPreparedBytes: registryPreparedMax + 1}} {
		a, err := newRegisteredCurrentInventory(t.Context(), s, registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960})
		if err != nil {
			t.Fatal(err)
		}
		inventoryComplete(t, a)
		out, err := newRegisteredRegistryMaterializer(s, a, bad)
		if out != nil || (!errors.Is(err, errInvalid) && !errors.Is(err, errLimit)) {
			t.Fatal("constructor quota/format wholezero", err)
		}
		if err := a.Close(); err != nil {
			t.Fatal("failed adoption preserves caller owner", err)
		}
	}
}
func TestRegisteredRegistrySharedEmissionSameInput(t *testing.T) {
	pij := inventoryLiteral(t, "PIJ258")
	plan, err := prepareRegistryRecordEmission(pij, 4, 2, registryTestFormat(), registryTestBudget())
	if err != nil || plan.self != plan || &plan.prepared[0] != &pij[0] || cap(plan.prepared) != cap(pij) || plan.work != 9517 {
		t.Fatal("single checked canonical inspection of exact same immutable span", plan, err)
	}
	copied := *plan // No mutex; a copied private emitter cannot spend original reservation.
	k, l, v, stored, err := emitRegistryRecords(&copied)
	if !errors.Is(err, errInvalid) || k != ([96]byte{}) || v != ([89]byte{}) || l != nil || stored != nil {
		t.Fatal("copied emitter wholezero")
	}
	k, l, v, stored, err = emitRegistryRecords(plan)
	if err != nil || !bytes.Equal(k[:], inventoryLiteral(t, "HKI96")) || !bytes.Equal(v[:], inventoryLiteral(t, "HKV89")) || !bytes.Equal(l, inventoryLiteral(t, "LookupHCR309")) || !bytes.Equal(stored, inventoryLiteral(t, "StoredHCR478")) {
		t.Fatal("unchanged full codec golden tuple", err)
	}
	k, l, v, stored, err = emitRegistryRecords(plan)
	if !errors.Is(err, errInvalid) || k != ([96]byte{}) || v != ([89]byte{}) || l != nil || stored != nil {
		t.Fatal("single reservation is not emitted twice")
	}
	_, l, _, stored, err = encodeRegistryRecords(pij, 4, 2, registryTestFormat(), registryTestBudget())
	if err != nil || !bytes.Equal(l, inventoryLiteral(t, "LookupHCR309")) || !bytes.Equal(stored, inventoryLiteral(t, "StoredHCR478")) {
		t.Fatal("old API still delegates exact admitted emission", err)
	}
	if k, l, v, stored, err := encodeRegistryRecords(pij, 0, 2, registryTestFormat(), registryTestBudget()); !errors.Is(err, errInvalid) || k != ([96]byte{}) || v != ([89]byte{}) || l != nil || stored != nil {
		t.Fatal("old API preserves wholezero errors")
	}
	if k, l, v, stored, err := emitRegistryRecords(nil); !errors.Is(err, errInvalid) || k != ([96]byte{}) || v != ([89]byte{}) || l != nil || stored != nil {
		t.Fatal("no emitter from an absent inspection")
	}
	for _, short := range []registeredRegistryCodecBudget{{workBytes: 9516, ownedBytes: 4883}, {workBytes: 9517, ownedBytes: 4882}} {
		if plan, err := prepareRegistryRecordEmission(pij, 4, 2, registryTestFormat(), short); plan != nil || !errors.Is(err, errLimit) {
			t.Fatal("unchanged exact/oneshort pre-inspection output reservation", plan, err)
		}
	}
	if plan, err := prepareRegistryRecordEmission(pij, 0, 2, registryTestFormat(), registryTestBudget()); plan != nil || !errors.Is(err, errInvalid) {
		t.Fatal("no position supplied by arbitrary inspection", err)
	}
	if plan, err := prepareRegistryRecordEmission(pij, 4, 2, registeredFormatBudget{}, registryTestBudget()); plan != nil || !errors.Is(err, errInvalid) {
		t.Fatal("immutable format before any emission", err)
	}
	if plan, err := prepareRegistryRecordEmission([]byte{1}, 4, 2, registryTestFormat(), registryTestBudget()); plan != nil || !errors.Is(err, errInvalid) {
		t.Fatal("short PIJ never mints an emitter", err)
	}
}

func TestRegisteredRegistryRestoreRejectsDifferentInstalledBytes(t *testing.T) {
	for _, name := range []string{"lookup", "stored", "outcome", "changes"} {
		t.Run(name, func(t *testing.T) {
			s := inventoryTestStore(t)
			inventoryGenesis(t, s, inventoryLiteral(t, "OriginHCR378"))
			m := registryUnitMachine(t, s, registryUnitLimits())
			e := registryUnitEntry(t, s, 4, 2, inventoryLiteral(t, "RJQ269"))
			b, err := m.Stage(e, s.ApplicationBudget())
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "lookup":
				b.ControlPuts[0].Value[120] ^= 1
				inventoryReseal(b.ControlPuts[0].Value)
			case "stored":
				b.ControlPuts[1].Value[152] ^= 1
				inventoryReseal(b.ControlPuts[1].Value)
			case "outcome":
				b.Outcome[17+96] = 2
				inventoryReseal(b.Outcome)
			case "changes":
				b.Changes = []byte{1}
			}
			if err := s.InstallApplication(4, b); err != nil {
				t.Fatal("ordinary unit installs caller supplied static wrong semantic bytes", err)
			}
			err = m.Restore(4, b.Image)
			if !errors.Is(err, errCorrupt) && !errors.Is(err, errRegisteredRegistryStageUnavailable) {
				t.Fatal("actual installed bytes not pending expectation", err)
			}
			if m.base.Index != 3 || m.pending == nil {
				t.Fatal("bad restore advances no private verified state")
			}
		})
	}
}
func TestRegisteredRegistrySnapshotGenerationRequiresNewAudit(t *testing.T) {
	donor := inventorySmall(t)
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	receiver, p, manifest := inventoryPrepared(t, donor)
	inventoryGenesis(t, receiver, inventoryLiteral(t, "OriginHCR378"))
	old := registryUnitMachine(t, receiver, registryUnitLimits())
	portable, err := newRegisteredPreparedInventory(t.Context(), p, registeredInventoryPreparedContext{index: 4, term: manifest.Term, voters: [3]uint64{1, 2, 3}}, registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960})
	if err != nil {
		t.Fatal(err)
	}
	evidence := inventoryComplete(t, portable)
	if evidence.prepared != p || evidence.base.Generation != 2 || evidence.controlRecords != 3 {
		t.Fatal("inert exact prepared bank evidence")
	}
	if err := portable.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := raftlog.EncodeApplicationSnapshotDescriptor(manifest)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(manifest.Index), Term: new(manifest.Term), ConfState: manifest.ConfState}, Data: data}
	claim, err := receiver.ClaimPreparedApplicationSnapshot(p, snapshot)
	if err != nil {
		t.Fatal("actual sameStore Claim after read owners close", err)
	}
	t.Cleanup(func() {
		if err := claim.Close(); err != nil {
			t.Error("actual claim owner cleanup", err)
		}
	})
	root, err := receiver.PersistApplicationReady(raft.Ready{Snapshot: snapshot, HardState: &pb.HardState{Term: new(manifest.Term), Commit: new(manifest.Index)}}, claim)
	if err != nil || root.Generation != 2 || root.Index != 4 {
		t.Fatal("actual public AS4 generation activation, nonquorum unit", root, err)
	}
	if err := old.Restore(root.Index, root.Image); !errors.Is(err, errRegisteredRegistryStageUnavailable) {
		t.Fatal("old machine cannot promote portable/cached proof", err)
	}
	fresh := registryUnitMachine(t, receiver, registryUnitLimits())
	if fresh.base.Generation != 2 || fresh.base.Index != 4 || fresh.usage.Bytes != 1676 || fresh.usage.Records != 3 {
		t.Fatal("full current new-generation bank audit required")
	}
	if err := fresh.Restore(4, root.Image); err != nil {
		t.Fatal("same new-generation exact current evidence", err)
	}
}
func TestRegisteredRegistryLogicalFixedPhases(t *testing.T) {
	// Names enumerate declared source-level caller/return slots. No assumption
	// that ABI return copies/registers, heap objects or hash workspaces vanish.
	core := unsafe.Sizeof(registeredParsed{}) + unsafe.Sizeof(registeredPreparedInspection{}) + unsafe.Sizeof(registeredLedger{}) + unsafe.Sizeof(registeredFormatBudget{}) + 2*unsafe.Sizeof(registeredCursor{}) + unsafe.Sizeof(registeredAxisView{}) + 2*256 + 2*32 + 96 + 128
	type inspectBridge struct {
		Receiver    *registryLedger
		Input       []byte
		Format      registeredFormatBudget
		Result      registeredPreparedInspection
		Err         error
		Child, Work int
	}
	type mintCaller struct {
		Input          []byte
		Format         registeredFormatBudget
		Budget         registeredRegistryCodecBudget
		Position, Term uint64
		Plan           *registryRecordEmission
		ReturnPlan     *registryRecordEmission
		Q              *registryLedger
		LiveLedger     registryLedger
		N              int
		Err, ReturnErr error
	}
	type wrapperCaller struct {
		Input           []byte
		Format          registeredFormatBudget
		Budget          registeredRegistryCodecBudget
		Position, Term  uint64
		Plan            *registryRecordEmission
		Err             error
		ReturnedHKI     [96]byte
		ReturnedHKV     [89]byte
		ReturnedBuffers [2][]byte
		ReturnedErr     error
	}
	inspection := core + unsafe.Sizeof(inspectBridge{}) + unsafe.Sizeof(mintCaller{}) + unsafe.Sizeof(wrapperCaller{}) + unsafe.Sizeof(registryRecordEmission{})
	type emitCaller struct {
		Plan                     *registryRecordEmission
		Ref                      registeredRegistryRef
		Sizes                    [2]uint32
		ReturnedHKI              [96]byte
		ReturnedHKV              [89]byte
		Buffers, ReturnedBuffers [2][]byte
		Err, ReturnErr           error
		Zero                     func(error)
	}
	type refCaller struct {
		Argument       *registeredPreparedInspection
		Position, Term uint64
		Return         registeredRegistryRef
		HKI            [96]byte
		HKV            [89]byte
	}
	type keysCaller struct {
		Origin   registeredOriginScope
		Key      [16]byte
		Position uint64
		HKI      [96]byte
		HKV      [89]byte
	}
	emission := unsafe.Sizeof(registryRecordEmission{}) + unsafe.Sizeof(wrapperCaller{}) + unsafe.Sizeof(emitCaller{}) + unsafe.Sizeof(refCaller{}) + unsafe.Sizeof(keysCaller{}) + 2*256 + 2*32 + 128
	if core > 2048 || inspection > 4096 || emission > 4096 {
		t.Fatal("existing codec4096 includes actualheap plan/caller/result/core/ref/key/hash slots", core, inspection, emission)
	}
	type stageCaller struct {
		Receiver        *registeredRegistryMaterializer
		Entry           replica.Entry
		Budget          raftlog.ApplicationBudget
		Ledger          *registryStageLedger
		View            *raftlog.ApplicationView
		Batch, Returned raftlog.ApplicationBatch
		Delta           registryInstalledDelta
		Plan            *registryRecordEmission
		Ref, Incoming   registeredRegistryRef
		Record          raftlog.ApplicationControlRecord
		Err, CloseErr   error
	}
	type originalCaller struct {
		Receiver              *registeredRegistryMaterializer
		Ledger                *registryStageLedger
		View                  *raftlog.ApplicationView
		Lookup, Stored        raftlog.ApplicationControlRecord
		Ref, Actual, Returned registeredRegistryRef
		Pij                   []byte
		N                     int
		Err, ReturnErr        error
	}
	type restoreCaller struct {
		Receiver        *registeredRegistryMaterializer
		Input           []byte
		Target          raftlog.ApplicationRoot
		Usage           raftlog.ApplicationControlUsage
		PendingArgument registryInstalledDelta
		PendingOwner    *registryInstalledDelta
		ReturnedErr     error
		View            *raftlog.ApplicationView
		Ledger          *registryStageLedger
		Err, CloseErr   error
	}
	// Additional4096 covers the NEW materializer, pending heap delta, operation
	// ledger, and outer caller/nested pair/restore frames; strict4096 is a
	// DISTINCT existing leaf allowance, not borrowed again for new objects.
	type pointCaller struct {
		Receiver        *registeredRegistryMaterializer
		Ledger          *registryStageLedger
		View            *raftlog.ApplicationView
		Key             []byte
		Limit           int
		Returned, Local raftlog.ApplicationControlRecord
		Found           bool
		Work            raftlog.ApplicationControlReadWork
		Err             error
	}
	type historyCaller struct {
		Receiver       *registeredRegistryMaterializer
		Ledger         *registryStageLedger
		View           *raftlog.ApplicationView
		Ref            registeredRegistryRef
		Err, ReturnErr error
	}
	type historyRootCaller struct {
		Receiver       *registeredRegistryMaterializer
		Ledger         *registryStageLedger
		View           *raftlog.ApplicationView
		Ref            *registeredRegistryRef
		Root           raftlog.ApplicationRoot
		Err, ReturnErr error
	}
	type historyOutcomeCaller struct {
		Receiver       *registeredRegistryMaterializer
		Ledger         *registryStageLedger
		View           *raftlog.ApplicationView
		Ref            *registeredRegistryRef
		Wire           []byte
		Outcome        registeredRegistryOutcome
		Err, ReturnErr error
	}
	type installedOutcomeCaller struct {
		Receiver       *registeredRegistryMaterializer
		Ledger         *registryStageLedger
		View           *raftlog.ApplicationView
		Delta          *registryInstalledDelta
		Maximum        int
		Wire           []byte
		Outcome        registeredRegistryOutcome
		HashWorkspace  [256]byte
		Digest         [32]byte
		Err, ReturnErr error
	}
	type installedChangeCaller struct {
		Receiver       *registeredRegistryMaterializer
		Ledger         *registryStageLedger
		View           *raftlog.ApplicationView
		Index          uint64
		Change         []byte
		Err, ReturnErr error
	}
	type jointCaller struct {
		Policy raftlog.ApplicationPolicy
		Budget raftlog.ApplicationBudget
		Status byte
		N      int
		Ref    registeredRegistryRef
		Widths [2]int
		Work   uint64
		Err    error
	}
	type preflightCaller struct {
		Receiver *registeredRegistryMaterializer
		Ledger   *registryStageLedger
		Batch    raftlog.ApplicationBatch
		Budget   raftlog.ApplicationBudget
		Policy   raftlog.ApplicationPolicy
		Result   raftlog.ApplicationControlBatchUsage
		Err      error
	}
	type countCaller struct {
		Span, Separator []byte
		Count           int
	}
	type termCaller struct {
		Store                      *raftlog.Store
		Index, Term, Actual, First uint64
		Err, ReturnErr             error
	}
	type verifyPairCaller struct {
		Receiver       *registeredRegistryMaterializer
		Ledger         *registryStageLedger
		View           *raftlog.ApplicationView
		Argument       registryInstalledDelta
		Lookup         raftlog.ApplicationControlRecord
		Found          bool
		Ref            registeredRegistryRef
		Err, ReturnErr error
	}
	stage := unsafe.Sizeof(registeredRegistryMaterializer{}) + unsafe.Sizeof(registryInstalledDelta{}) + unsafe.Sizeof(registryStageLedger{}) + unsafe.Sizeof(stageCaller{}) + unsafe.Sizeof(originalCaller{})
	restore := unsafe.Sizeof(registeredRegistryMaterializer{}) + unsafe.Sizeof(registryInstalledDelta{}) + unsafe.Sizeof(registryStageLedger{}) + unsafe.Sizeof(restoreCaller{}) + unsafe.Sizeof(originalCaller{})
	stage += unsafe.Sizeof(pointCaller{})
	stageHistory := unsafe.Sizeof(registeredRegistryMaterializer{}) + unsafe.Sizeof(registryInstalledDelta{}) + unsafe.Sizeof(registryStageLedger{}) + unsafe.Sizeof(stageCaller{}) + unsafe.Sizeof(historyCaller{}) + max(unsafe.Sizeof(historyRootCaller{}), unsafe.Sizeof(historyOutcomeCaller{}), unsafe.Sizeof(termCaller{}))
	stageOutput := unsafe.Sizeof(registeredRegistryMaterializer{}) + unsafe.Sizeof(registryInstalledDelta{}) + unsafe.Sizeof(registryStageLedger{}) + unsafe.Sizeof(stageCaller{}) + max(unsafe.Sizeof(jointCaller{})+unsafe.Sizeof(countCaller{}), unsafe.Sizeof(preflightCaller{}))
	restoreBase := restore - unsafe.Sizeof(originalCaller{})
	restore += unsafe.Sizeof(verifyPairCaller{}) + unsafe.Sizeof(pointCaller{})
	// Root/JOR/change callees end before pair/term calls. Keep the actual
	// verify argument slot; nested helpers borrow its pointer, not a copy.
	restoreHistory := restoreBase + unsafe.Sizeof(verifyPairCaller{}) + unsafe.Sizeof(historyCaller{}) + max(unsafe.Sizeof(historyRootCaller{}), unsafe.Sizeof(historyOutcomeCaller{}), unsafe.Sizeof(termCaller{}))
	restoreOutcome := restoreBase + unsafe.Sizeof(verifyPairCaller{}) + unsafe.Sizeof(installedOutcomeCaller{})
	restoreChange := restoreBase + unsafe.Sizeof(verifyPairCaller{}) + max(unsafe.Sizeof(installedChangeCaller{}), unsafe.Sizeof(termCaller{}))
	restore = max(restore, restoreHistory, restoreOutcome, restoreChange)
	if stage > 4096 || stageHistory > 4096 || stageOutput > 4096 || restore > 4096 {
		t.Fatal("new additional4096 has no implicit leaf overlap", stage, restore)
	}
	t.Log("registry logical fixed source phases", core, inspection, emission, stage, stageHistory, stageOutput, restore)
	// Explicit capacities—not sizeof—fund variable source owners. Constructor
	// conservatively carries WHOLE a.held+newStageF simultaneously; Stage keeps
	// incomingcap+machineimage+capturedview, point(raw+owned), or emittedbatch.
	if registryStageFixed != 625278+4096 || 629374+4194315+140+140+2*4195950 > 17411056 {
		t.Fatal("full immutable caller+largest paired source phase funded")
	}
}

func registryGuardLedger(t *testing.T, extra uint64) *registryStageLedger {
	t.Helper()
	l := registryUnitLimits()
	l.workBytes = registryStageFixed + extra
	q, err := registryStageStart(l, 0)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

// Breaks validator helpers that turn an admitted malformed row or a finite
// source-work refusal into authority. Every view is a genuine public Store
// owner; these checks claim semantic data validation, not foreign ownership.
func TestRegisteredRegistryFiniteValidatorBoundaries(t *testing.T) {
	s := inventorySmall(t)
	m := registryUnitMachine(t, s, registryUnitLimits())
	if m.SemanticContractID() != registeredInventorySemanticContractID() {
		t.Fatal("nonnull compiled agreement", m.SemanticContractID())
	}
	v, err := s.ApplicationView(4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	})
	lookup, found, _, err := v.GetControl(t.Context(), inventoryLiteral(t, "HKI96"), raftlog.ReadBudget{Rows: 2, Bytes: 1819})
	if err != nil || !found {
		t.Fatal("real lookup baseline", err)
	}
	vectors, _ := registryVectors(t)
	ref := registryExpectedRef(t, vectors["base"])
	if got, err := m.capture(registryGuardLedger(t, 1723), m.base, m.usage); got != nil || !errors.Is(err, errLimit) {
		t.Fatal("capture before configured image call", got, err)
	}
	if err := m.originAt(registryGuardLedger(t, 8270), v); !errors.Is(err, errLimit) {
		t.Fatal("origin exact source one short", err)
	}
	if err := m.term(registryGuardLedger(t, 8388905), 4, 2); !errors.Is(err, errLimit) {
		t.Fatal("each actual term call prepaid", err)
	}
	if got, err := m.original(registryGuardLedger(t, 4713), v, lookup, 4); got != (registeredRegistryRef{}) || !errors.Is(err, errLimit) {
		t.Fatal("lookup codec before quota", got, err)
	}
	bad := lookup
	bad.Value = bytes.Clone(lookup.Value)
	bad.Value[0] ^= 1
	if got, err := m.original(registryGuardLedger(t, 134000000), v, bad, 4); got != (registeredRegistryRef{}) || !errors.Is(err, errCorrupt) {
		t.Fatal("malformed actual-shaped lookup cannot return partial ref", got, err)
	}
	if got, err := m.original(registryGuardLedger(t, 134000000), v, lookup, 3); got != (registeredRegistryRef{}) || !errors.Is(err, errCorrupt) {
		t.Fatal("actual original R4 is beyond allowed through3", got, err)
	}
	wrongOriginal := ref
	wrongOriginal.term++ // Static expected addressing; the genuine JOR stays unchanged.
	if err := m.historyOutcome(registryGuardLedger(t, 134000000), v, &wrongOriginal); !errors.Is(err, errCorrupt) {
		t.Fatal("actual Created JOR must match complete original ref", err)
	}
	if err := m.installedChange(registryGuardLedger(t, 1084), v, 4); !errors.Is(err, errLimit) {
		t.Fatal("actual change policy1 requires1082+3 work before read", err)
	}
	if err := m.installedChange(registryGuardLedger(t, 134000000), v, 99); !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal("change coordinate exceeds genuine view cut", err)
	}
	if err := m.history(registryGuardLedger(t, 1641), v, ref); !errors.Is(err, errLimit) {
		t.Fatal("RootAt policy before call", err)
	}
	if err := m.history(registryGuardLedger(t, 1642+7523-1), v, ref); !errors.Is(err, errLimit) {
		t.Fatal("outcome policy before call", err)
	}
	missing := ref
	missing.position = 99
	if err := m.history(registryGuardLedger(t, 134000000), v, missing); !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal("original coordinate exceeds genuine view cut", err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.originAt(registryGuardLedger(t, 134000000), v); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("genuine closed view", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := m.capture(registryGuardLedger(t, 134000000), m.base, m.usage); got != nil || !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("genuine closed store capture wholezero", got, err)
	}
}
func TestRegisteredRegistryOriginStaticRows(t *testing.T) {
	known := inventorySmall(t)
	m := registryUnitMachine(t, known, registryUnitLimits())
	for _, name := range []string{"absent", "checksum", "different_term"} {
		t.Run(name, func(t *testing.T) {
			s := inventoryTestStore(t)
			index := uint64(1)
			if name != "absent" {
				origin := inventoryLiteral(t, "OriginHCR378")
				if name == "checksum" {
					origin[0] ^= 1
				} else {
					binary.BigEndian.PutUint64(origin[len(origin)-40:len(origin)-32], 3)
					inventoryReseal(origin)
				}
				inventoryGenesis(t, s, origin)
				index = 3
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
			want := errCorrupt
			if name == "absent" {
				want = errRegisteredInventoryUnavailable
			}
			if err := m.originAt(registryGuardLedger(t, 134000000), v); !errors.Is(err, want) {
				t.Fatal("typed malformed/different source data, no owner readiness claim", name, err)
			}
		})
	}
}
func TestRegisteredRegistryStageEntryAndTermQuotas(t *testing.T) {
	s := inventoryTestStore(t)
	inventoryGenesis(t, s, inventoryLiteral(t, "OriginHCR378"))
	m := registryUnitMachine(t, s, registryUnitLimits())
	// There is no actual R4 yet. A caller-supplied coordinate/term cannot stand
	// in for the persisted entry; this is ordinary absence, not an I/O failure.
	e := replica.Entry{Generation: 1, Index: 4, Term: 2, Data: inventoryLiteral(t, "RJQ269")}
	b, err := m.Stage(e, s.ApplicationBudget())
	registryUnitZero(t, b, err, raft.ErrUnavailable)
	e = registryUnitEntry(t, s, 4, 2, e.Data)
	v, err := s.ApplicationView(3)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	}()
	b, delta, err := m.stage(registryGuardLedger(t, 8388905), v, e, s.ApplicationBudget())
	registryUnitZero(t, b, err, errLimit)
	if delta != (registryInstalledDelta{}) || m.pending != nil || m.base.Index != 3 {
		t.Fatal("term source quota leaves no staged delta")
	}
}
func TestRegisteredRegistryInstalledVerificationQuotas(t *testing.T) {
	s := inventoryTestStore(t)
	inventoryGenesis(t, s, inventoryLiteral(t, "OriginHCR378"))
	m := registryUnitMachine(t, s, registryUnitLimits())
	e := registryUnitEntry(t, s, 4, 2, inventoryLiteral(t, "RJQ269"))
	b, err := m.Stage(e, s.ApplicationBudget())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.InstallApplication(4, b); err != nil {
		t.Fatal(err)
	}
	v, err := s.ApplicationView(4)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	}()
	before := m.base
	for _, extra := range []uint64{8270, 8271 + 7523 - 1, 8271 + 7523 + 469 - 1} {
		if err := m.verifyInstalled(registryGuardLedger(t, extra), v, *m.pending); !errors.Is(err, errLimit) {
			t.Fatal("each admitted origin/outcome/extra hash step one short", extra, err)
		}
		if !sameBase(m.base, before) || m.pending == nil {
			t.Fatal("validator quota cannot publish verified prefix")
		}
	}
}

// Imported unit tests above are guards; no saved external original RED exists.
// These barriers start real competing clients. They do not force overlap at the
// native-close window; lock detachment remains a separately reviewed source fact.
func TestRegisteredRegistryConcurrentOwnershipGuard(t *testing.T) {
	t.Run("double-adoption", func(t *testing.T) {
		s := inventorySmall(t)
		a, err := newRegisteredCurrentInventory(t.Context(), s, registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960})
		if err != nil {
			t.Fatal(err)
		}
		inventoryComplete(t, a)
		t.Cleanup(func() { _ = a.Close() })
		type adoption struct {
			m   *registeredRegistryMaterializer
			err error
		}
		start := make(chan struct{})
		release := sync.OnceFunc(func() { close(start) })
		defer release()
		results := make(chan adoption, 2)
		var wg sync.WaitGroup
		for range 2 {
			wg.Go(func() {
				<-start
				m, err := newRegisteredRegistryMaterializer(s, a, registryUnitLimits())
				results <- adoption{m, err}
			})
		}
		release()
		var winner *registeredRegistryMaterializer
		for range 2 {
			select {
			case result := <-results:
				if result.err == nil {
					if result.m == nil || winner != nil {
						t.Fatal("audit granted more than one owner", result.m)
					}
					winner = result.m
				} else if result.m != nil || !errors.Is(result.err, errInvalid) && !errors.Is(result.err, errLimit) && !errors.Is(result.err, raftlog.ErrLimit) {
					t.Fatal("unexpected competing adoption", result.m, result.err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("competing constructors did not finish")
			}
		}
		wg.Wait()
		if winner == nil {
			t.Fatal("finite roomy policy admitted no constructor")
		}
		t.Cleanup(func() { _ = winner.Close() })
		a.mu.Lock()
		consumed := a.closed && a.view == nil
		a.mu.Unlock()
		if !consumed {
			t.Fatal("winning constructor did not detach audit owner")
		}
		select {
		case <-a.closeDone:
		default:
			t.Fatal("winning constructor did not complete native owner cleanup")
		}
		if err := a.Close(); err != nil {
			t.Fatal("repeat close of consumed audit", err)
		}
		usage, err := s.ApplicationUsage()
		if err != nil || usage.Views != 0 || usage.ViewBytes != 0 {
			t.Fatal("concurrent adoption leaked native views", usage, err)
		}
		transfer, err := s.ApplicationTransferUsage()
		if err != nil || transfer.PinnedLogicalBytes != 0 {
			t.Fatal("concurrent adoption leaked native pins", transfer, err)
		}
	})

	t.Run("stage-restore-close", func(t *testing.T) {
		s := inventoryTestStore(t)
		inventoryGenesis(t, s, inventoryLiteral(t, "OriginHCR378"))
		m := registryUnitMachine(t, s, registryUnitLimits())
		wire := inventoryLiteral(t, "RJQ269")
		e := registryUnitEntry(t, s, 4, 2, wire)
		image := inventoryLiteral(t, "GR2")
		before, err := s.ApplicationUsage()
		if err != nil {
			t.Fatal(err)
		}
		type call struct {
			kind  byte
			batch raftlog.ApplicationBatch
			err   error
		}
		start, closeStart := make(chan struct{}), make(chan struct{})
		release := sync.OnceFunc(func() { close(start) })
		releaseClose := sync.OnceFunc(func() { close(closeStart) })
		defer release()
		defer releaseClose()
		results := make(chan call, 17)
		created := make(chan struct{}, 8)
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				<-start
				b, err := m.Stage(e, s.ApplicationBudget())
				if err == nil {
					created <- struct{}{}
				}
				results <- call{kind: 1, batch: b, err: err}
			})
			wg.Go(func() {
				<-start
				results <- call{kind: 2, err: m.Restore(3, image)}
			})
		}
		wg.Go(func() {
			<-closeStart
			results <- call{kind: 3, err: m.Close()}
		})
		release()
		// Close follows one actual successful Stage, so the successful owned
		// result assertion cannot disappear because Close won the first race.
		select {
		case <-created:
		case <-time.After(10 * time.Second):
			t.Fatal("no actual competing Stage completed")
		}
		releaseClose()
		var staged raftlog.ApplicationBatch
		successes := 0
		for range 17 {
			select {
			case result := <-results:
				switch result.kind {
				case 1:
					if result.err == nil {
						successes++
						staged = result.batch
					} else {
						if !errors.Is(result.err, errInvalid) && !errors.Is(result.err, raftlog.ErrClosed) {
							t.Fatal("unexpected competing Stage error", result.err)
						}
						want := errInvalid
						if errors.Is(result.err, raftlog.ErrClosed) {
							want = raftlog.ErrClosed
						}
						registryUnitZero(t, result.batch, result.err, want)
					}
				case 2:
					if result.err != nil && !errors.Is(result.err, errInvalid) && !errors.Is(result.err, errRegisteredRegistryStageUnavailable) && !errors.Is(result.err, raftlog.ErrClosed) {
						t.Fatal("unexpected competing Restore error", result.err)
					}
				case 3:
					if result.err != nil && !errors.Is(result.err, errLimit) {
						t.Fatal("unexpected competing Close error", result.err)
					}
				default:
					t.Fatal("unknown concurrent client")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("competing clients did not finish")
			}
		}
		wg.Wait()
		if successes != 1 || len(staged.ControlPuts) != 2 || staged.BaseIndex != 3 {
			t.Fatal("pending adjacent operation admitted more than once", successes)
		}
		m.mu.Lock()
		unchanged := m.base.Index == 3 && !m.busy
		m.mu.Unlock()
		if !unchanged {
			t.Fatal("concurrent callbacks advanced without an actual install")
		}
		if err := m.Close(); err != nil {
			t.Fatal("final detached cleanup", err)
		}
		wire[11] ^= 1 // The borrowed immutable input lifetime has ended.
		if !bytes.Equal(staged.Image, image) || !bytes.Equal(staged.Outcome, inventoryLiteral(t, "RegistrationCreatedJOR469")) || !bytes.Equal(staged.ControlPuts[1].Value, inventoryLiteral(t, "StoredHCR478")) {
			t.Fatal("successful output borrowed input or closed machine ownership")
		}
		after, err := s.ApplicationUsage()
		if err != nil || after != before || after.Views != 0 || after.ViewBytes != 0 {
			t.Fatal("callbacks leaked owners or changed durable state", before, after, err)
		}
		transfer, err := s.ApplicationTransferUsage()
		if err != nil || transfer.PinnedLogicalBytes != 0 {
			t.Fatal("callbacks leaked native pins", transfer, err)
		}
	})

	t.Run("synthetic-busy-refusal", func(t *testing.T) {
		// Direct branch guard for a test-controlled phase. This does not claim
		// that the start barrier observes an actual native-close window.
		s := inventorySmall(t)
		m := registryUnitMachine(t, s, registryUnitLimits())
		m.mu.Lock()
		m.busy = true
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			m.busy = false
			m.mu.Unlock()
		}()
		b, err := m.Stage(replica.Entry{}, s.ApplicationBudget())
		registryUnitZero(t, b, err, errInvalid)
		if err := m.Restore(4, inventoryLiteral(t, "GR2")); !errors.Is(err, errInvalid) {
			t.Fatal("Restore during controlled busy phase", err)
		}
		if err := m.Close(); !errors.Is(err, errLimit) {
			t.Fatal("Close during controlled busy phase", err)
		}
		m.mu.Lock()
		preserved := !m.closed && m.pending == nil && m.base.Index == 4 && m.busy
		m.busy = false
		m.mu.Unlock()
		if !preserved {
			t.Fatal("busy refusal consumed or changed the owner")
		}
	})
}
