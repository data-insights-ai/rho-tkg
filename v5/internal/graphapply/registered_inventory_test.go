package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"testing"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Breaks an audit that accepts one-sided pairs/census. Public Store unit inputs
// prove storage setup only, never quorum, Genesis creation or Register authority.
func TestRegisteredInventoryFirstCompleteCapturedBank(t *testing.T) {
	literal := map[string]string{
		"Lineage":                   "cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f",
		"GR2":                       "47520202010000000000000000000000000000000000000000000001000000000000000100000000000000000000000000000001945c9793df894a15367965dd4283c91b220042f2e9988b6c554ea59e5864334c00000000000000010000000000000001000000000000000026c51b3182da8ee986af5a09019cd9574cebbd2008953bbdcf8388e55d17e3f5",
		"source244":                 "4a47533100010100000000000000000000000000000000000000000000010200000000000000000000000000000001000000000000000100000000000000010000008c12cf5d257654d0fa3779b2b4026d838440ec14e65c1ad4abda97080ecc01f86a0000000000000001030000000000000001000000000000000200000000000000031587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc60da51c70000000000000002000000000000006000000000004000dc000000000000008c00000000000000020000000001001172000000000000000200000000008019be000000000000000100000000000001d5",
		"GJQ255":                    "474a5131000100000000f44a47533100010100000000000000000000000000000000000000000000010200000000000000000000000000000001000000000000000100000000000000010000008c12cf5d257654d0fa3779b2b4026d838440ec14e65c1ad4abda97080ecc01f86a0000000000000001030000000000000001000000000000000200000000000000031587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc60da51c70000000000000002000000000000006000000000004000dc000000000000008c00000000000000020000000001001172000000000000000200000000008019be000000000000000100000000000001d5",
		"HKS47":                     "484b533101010000000000000000000000000000000000000000000001020000000000000000000000000000000001",
		"OriginHCR378":              "484352310001010001000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010000000000000003000000fc4a47533100010100000000000000000000000000000000000000000000010200000000000000000000000000000001000000000000000100000000000000010000008c12cf5d257654d0fa3779b2b4026d838440ec14e65c1ad4abda97080ecc01f86a0000000000000001030000000000000001000000000000000200000000000000031587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc60da51c70000000000000002000000000000006000000000004000dc000000000000008c00000000000000020000000001001172000000000000000200000000008019be000000000000000100000000000001d50000000000000002c1f43d27c82b0228fb43eeec9944d6dc54943ce127561ca36fcbd5ed596a455f",
		"GenesisCreatedJOR145":      "4a4f52310001040000000000000003000001484b53310101000000000000000000000000000000000000000000000102000000000000000000000000000000000100000000000000030000000000000002cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f85d901e3bfaf8ca663c544619a3969ab2f751ca9f691b2bf5c24d3ba8b573bc1",
		"PIJ258":                    "50494a3100010001000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010103000000000000000000000000000000011587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc60da51c70000007a424453310001000000004c4447503100010000010a0000000000000000000000000000000100010000000c63616d6572612d636c6f636b0000000b6d696c6c697365636f6e640a00000000000000000000000000000000000001040000000000000000000000000000000000000000000001000000000000004000",
		"RJQ269":                    "524a51310001000000010250494a3100010001000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010103000000000000000000000000000000011587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc60da51c70000007a424453310001000000004c4447503100010000010a0000000000000000000000000000000100010000000c63616d6572612d636c6f636b0000000b6d696c6c697365636f6e640a00000000000000000000000000000000000001040000000000000000000000000000000000000000000001000000000000004000",
		"HKI96":                     "484b49310201000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010103000000000000000000000000000000",
		"HKV89":                     "484b56310301000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f000100000000000000040000",
		"LookupHCR309":              "484352310001020001000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010000000000000004000000b7010300000000000000000000000000000001000000000000000226a28dedef197296c095d19f0334e2d51406b22efc1a4eed50cd6d38b13b53ca47dbf9fa532d24438f384b551cf473352be09e20531df0d23f56f076dc3411f2484b56310301000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f000100000000000000040000000001029f790fb77ec398a44d94a9d61ad0cccda1d750af8fe3a08958ace6392b847e75",
		"StoredHCR478":              "484352310001030001000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f0001000000000000000400000160010300000000000000000000000000000001000000000000000226a28dedef197296c095d19f0334e2d51406b22efc1a4eed50cd6d38b13b53ca47dbf9fa532d24438f384b551cf473352be09e20531df0d23f56f076dc3411f20000010250494a3100010001000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010103000000000000000000000000000000011587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc60da51c70000007a424453310001000000004c4447503100010000010a0000000000000000000000000000000100010000000c63616d6572612d636c6f636b0000000b6d696c6c697365636f6e640a00000000000000000000000000000000000001040000000000000000000000000000000000000000000001000000000000004000e3691336de4646185cb1c7c71778f6a45df24e59c9a7b79a71b98e56412e2bba",
		"RegistrationCreatedJOR469": "4a4f5231000101000000000000000400a1484b49310201000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f000101030000000000000000000000000000000126a28dedef197296c095d19f0334e2d51406b22efc1a4eed50cd6d38b13b53ca47dbf9fa532d24438f384b551cf473352be09e20531df0d23f56f076dc3411f202484b49310201000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f000101030000000000000000000000000000000126a28dedef197296c095d19f0334e2d51406b22efc1a4eed50cd6d38b13b53ca47dbf9fa532d24438f384b551cf473352be09e20531df0d23f56f076dc3411f2484b56310301000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010000000000000004000000000000000000027c6df5b1706823114c51f041dce0039deced64069f457128fffa182356055b31",
		"SubmissionHash":            "26a28dedef197296c095d19f0334e2d51406b22efc1a4eed50cd6d38b13b53ca",
		"PreparedHash":              "47dbf9fa532d24438f384b551cf473352be09e20531df0d23f56f076dc3411f2",
	}
	decode := func(name string) []byte {
		t.Helper()
		b, err := hex.DecodeString(literal[name])
		if err != nil || len(b) == 0 {
			t.Fatal("independent frozen literal", name, err)
		}
		return b
	}
	for _, tc := range []struct {
		name           string
		lookup, stored bool
		bytes, records uint64
	}{{"complete_pair", true, true, 1676, 3}, {"missing_stored", true, false, 1015, 2}, {"orphan_stored", false, true, 1171, 2}} {
		t.Run(tc.name, func(t *testing.T) {
			policy := raftlog.DefaultApplicationPolicy(1)
			policy.MaxKeyBytes, policy.MaxValueBytes, policy.MaxImageBytes = 96, 4194524, 140
			policy.MaxPageRows, policy.MaxPageBytes = 2, 16781682
			policy.MaxInstallWrites, policy.MaxInstallBytes, policy.MaxChangeBytes, policy.MaxOutcomeBytes = 2, 8395198, 1, 469
			limits := raftlog.DefaultLimits()
			limits.MaxEntryBytes, limits.MaxReadBytes, limits.MaxReadyBytes = 4194389, 4194421, 4194901
			if err := policy.Validate(limits); err != nil {
				t.Fatal("full supporting policy invalid", err)
			}
			controls := raftlog.ApplicationControlConfig{Version: 1}
			contract, err := raftlog.ApplicationContractForPolicyWithControls(policy, controls)
			if err != nil {
				t.Fatal(err)
			}
			identity := raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}
			cfg := raftlog.Config{Dir: "db", FS: vfs.NewMem(), Create: true, Limits: limits, Application: policy, Controls: controls,
				Transfer:    raftlog.ApplicationTransferConfig{Identity: identity, Contract: contract, Limits: raftlog.DefaultApplicationTransferLimits()},
				Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 10000},
				Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: registeredInventorySemanticContractID()}
			s, err := raftlog.Open(cfg)
			if err != nil {
				t.Fatal("actual full policy Open", err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			image := decode("GR2")
			if err := s.Initialize([]uint64{1, 2, 3}, image); err != nil {
				t.Fatal(err)
			}
			install := func(index uint64, command, outcome []byte, puts []raftlog.ApplicationControlPut) {
				t.Helper()
				at, current, err := s.Checkpoint()
				if err != nil || at != index-1 || !bytes.Equal(current, image) {
					t.Fatal("actual contiguous unit base", err)
				}
				view, err := s.ApplicationView(at)
				if err != nil {
					t.Fatal(err)
				}
				root, readErr := view.RootBounded(t.Context(), 140)
				if err := errors.Join(readErr, view.Close()); err != nil {
					t.Fatal(err)
				}
				entry := &pb.Entry{Index: new(index), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: command}
				if err := s.Persist(raft.Ready{Entries: []*pb.Entry{entry}, HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(index)}}); err != nil {
					t.Fatal("actual public unit Ready", err)
				}
				batch := raftlog.ApplicationBatch{BaseGeneration: root.Generation, BaseIndex: root.Index, BaseImageHash: root.ImageHash, Image: image, Outcome: outcome, ControlPuts: puts}
				if _, err := policy.PreflightControlBatch(batch, limits); err != nil {
					t.Fatal("actual proposed unit install staging", err)
				}
				if err := s.InstallApplication(index, batch); err != nil {
					t.Fatal("actual public unit Install", err)
				}
			}
			install(2, nil, nil, nil)
			install(3, decode("GJQ255"), decode("GenesisCreatedJOR145"), []raftlog.ApplicationControlPut{{Key: decode("HKS47"), Value: decode("OriginHCR378")}})
			var puts []raftlog.ApplicationControlPut
			if tc.lookup {
				puts = append(puts, raftlog.ApplicationControlPut{Key: decode("HKI96"), Value: decode("LookupHCR309")})
			}
			if tc.stored {
				puts = append(puts, raftlog.ApplicationControlPut{Key: decode("HKV89"), Value: decode("StoredHCR478")})
			}
			install(4, decode("RJQ269"), decode("RegistrationCreatedJOR469"), puts)
			view, err := s.ApplicationView(4)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := view.Close(); err != nil {
					t.Error(err)
				}
			}()
			root, err := view.RootBounded(t.Context(), 140)
			if err != nil || root.Index != 4 || root.Generation != 1 || !bytes.Equal(root.Image, image) {
				t.Fatal("same captured actual root", err)
			}
			proof, err := view.ProveNoApplicationData(t.Context())
			if err != nil || !sameBase(root, proof) {
				t.Fatal("actual empty graph proof", err)
			}
			usage, err := view.ControlInventoryUsage(t.Context())
			if err != nil || usage.Bytes != tc.bytes || usage.Records != tc.records || usage.TotalBytes != 1714+tc.bytes || usage.TotalRecords != 12+tc.records {
				t.Fatal("independent complete per-view storage census", usage, err)
			}
			for _, record := range []struct {
				key, value string
				index      uint64
				found      bool
			}{{"HKS47", "OriginHCR378", 3, true}, {"HKI96", "LookupHCR309", 4, tc.lookup}, {"HKV89", "StoredHCR478", 4, tc.stored}} {
				row, found, _, err := view.GetControl(t.Context(), decode(record.key), raftlog.ReadBudget{Rows: 2, Bytes: 4195950})
				if err != nil || found != record.found || found && (row.Index != record.index || !bytes.Equal(row.Key, decode(record.key)) || !bytes.Equal(row.Value, decode(record.value))) {
					t.Fatal("complete literal/index baseline", record.key, err)
				}
			}
			origin, err := decodeInventoryRegisteredOrigin(decode("OriginHCR378"), registeredJournalBudget{walkBytes: 6971})
			if err != nil || origin.index != 3 || origin.term != 2 || origin.source.Scope != identity || origin.source.SourcePosition != 1 || origin.source.SourceTerm != 1 || origin.source.SharedContract != ([10]uint64{2, 96, 4194524, 140, 2, 16781682, 2, 8395198, 1, 469}) {
				t.Fatal("NEW full policy Source1/S3 baseline", err)
			}
			var ref registeredRegistryRef
			ref.position, ref.term, ref.submitted.kind = 4, 2, 1
			copy(ref.submitted.lookupKey[:], decode("HKI96"))
			copy(ref.storedKey[:], decode("HKV89"))
			copy(ref.submitted.submissionHash[:], decode("SubmissionHash"))
			copy(ref.submitted.preparedHash[:], decode("PreparedHash"))
			scope := registeredOriginScope{graph: identity.Graph, partition: 1, group: identity.Group, protocol: 1}
			copy(scope.lineage[:], decode("Lineage"))
			parsed, n, err := decodeRegistryLookup(decode("HKI96"), decode("LookupHCR309"), 4, scope, registeredRegistryCodecBudget{workBytes: 4714, ownedBytes: 4096})
			if err != nil || parsed != ref || n != 258 {
				t.Fatal("independent full reciprocal ref/hash/length baseline", err)
			}
			storedRef, prepared, err := decodeRegistryStored(decode("HKV89"), decode("StoredHCR478"), 4, scope, registryTestFormat(), registeredRegistryCodecBudget{workBytes: 8144, ownedBytes: 4096})
			if err != nil || storedRef != parsed || !bytes.Equal(prepared, decode("PIJ258")) {
				t.Fatal("complete independent Stored PIJ/twohash baseline", err)
			}
			for _, at := range []struct {
				index, term uint64
				outcome     []byte
			}{{1, 1, nil}, {3, 2, decode("GenesisCreatedJOR145")}, {4, 2, decode("RegistrationCreatedJOR469")}} {
				retained, err := view.RootAt(t.Context(), at.index, 140)
				if err != nil || retained.Generation != root.Generation || !bytes.Equal(retained.Image, image) {
					t.Fatal("same-bank Source1/S/R history", err)
				}
				term, err := s.Term(at.index)
				if err != nil || term != at.term {
					t.Fatal("actual unit stored original term", err)
				}
				outcome, err := view.ApplicationRecord(t.Context(), at.index, true, 469)
				if err != nil || !bytes.Equal(outcome, at.outcome) {
					t.Fatal("full Source1/CreatedS/CreatedR outcomes", err)
				}
			}
			if err := view.Close(); err != nil {
				t.Fatal(err)
			}
			t.Logf("INVENTORY_UNIT_FULL_POLICY_LITERAL_CENSUS_BASELINE_PASS %s", tc.name)
			budget := registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960}
			audit, err := newRegisteredCurrentInventory(t.Context(), s, budget)
			if err != nil {
				if (!tc.lookup || !tc.stored) && errors.Is(err, errCorrupt) && audit == nil {
					return
				}
				t.Fatalf("INVENTORY_VALID_CURRENT_AUDIT_UNAVAILABLE: %v", err)
			}
			defer func() {
				if err := audit.Close(); err != nil {
					t.Error(err)
				}
			}()
			done := false
			for phase := 0; !done && phase < 128; phase++ {
				done, err = audit.Advance(t.Context(), budget)
				if err != nil {
					break
				}
			}
			if !tc.lookup || !tc.stored {
				if !errors.Is(err, errCorrupt) {
					t.Fatal("one-sided pair must refuse actual complete bank", err)
				}
				if summary, err := audit.Finish(t.Context(), budget); summary != nil || !errors.Is(err, errCorrupt) {
					t.Fatal("corrupt inventory cannot publish partial census", err)
				}
				return
			}
			summary, err := audit.Finish(t.Context(), budget)
			if err != nil || !done || summary == nil || summary.owner != audit || summary.base.Index != 4 || summary.base.Generation != 1 || summary.base.ImageHash != root.ImageHash || summary.origin != origin || summary.controlBytes != 1676 || summary.controlRecords != 3 || summary.graphBytes != 1714 || summary.graphRecords != 12 || summary.pairs != 1 || summary.maxOriginalTerm != 2 {
				t.Fatal("exact pointer-bound complete inventory summary", summary, err)
			}
		})
	}
}

func inventoryLiteral(t *testing.T, name string) []byte {
	t.Helper()
	literals := map[string]string{
		"GR2":                       "47520202010000000000000000000000000000000000000000000001000000000000000100000000000000000000000000000001945c9793df894a15367965dd4283c91b220042f2e9988b6c554ea59e5864334c00000000000000010000000000000001000000000000000026c51b3182da8ee986af5a09019cd9574cebbd2008953bbdcf8388e55d17e3f5",
		"GJQ255":                    "474a5131000100000000f44a47533100010100000000000000000000000000000000000000000000010200000000000000000000000000000001000000000000000100000000000000010000008c12cf5d257654d0fa3779b2b4026d838440ec14e65c1ad4abda97080ecc01f86a0000000000000001030000000000000001000000000000000200000000000000031587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc60da51c70000000000000002000000000000006000000000004000dc000000000000008c00000000000000020000000001001172000000000000000200000000008019be000000000000000100000000000001d5",
		"HKS47":                     "484b533101010000000000000000000000000000000000000000000001020000000000000000000000000000000001",
		"OriginHCR378":              "484352310001010001000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010000000000000003000000fc4a47533100010100000000000000000000000000000000000000000000010200000000000000000000000000000001000000000000000100000000000000010000008c12cf5d257654d0fa3779b2b4026d838440ec14e65c1ad4abda97080ecc01f86a0000000000000001030000000000000001000000000000000200000000000000031587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc60da51c70000000000000002000000000000006000000000004000dc000000000000008c00000000000000020000000001001172000000000000000200000000008019be000000000000000100000000000001d50000000000000002c1f43d27c82b0228fb43eeec9944d6dc54943ce127561ca36fcbd5ed596a455f",
		"GenesisCreatedJOR145":      "4a4f52310001040000000000000003000001484b53310101000000000000000000000000000000000000000000000102000000000000000000000000000000000100000000000000030000000000000002cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f85d901e3bfaf8ca663c544619a3969ab2f751ca9f691b2bf5c24d3ba8b573bc1",
		"PIJ258":                    "50494a3100010001000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010103000000000000000000000000000000011587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc60da51c70000007a424453310001000000004c4447503100010000010a0000000000000000000000000000000100010000000c63616d6572612d636c6f636b0000000b6d696c6c697365636f6e640a00000000000000000000000000000000000001040000000000000000000000000000000000000000000001000000000000004000",
		"RJQ269":                    "524a51310001000000010250494a3100010001000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010103000000000000000000000000000000011587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc60da51c70000007a424453310001000000004c4447503100010000010a0000000000000000000000000000000100010000000c63616d6572612d636c6f636b0000000b6d696c6c697365636f6e640a00000000000000000000000000000000000001040000000000000000000000000000000000000000000001000000000000004000",
		"HKI96":                     "484b49310201000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010103000000000000000000000000000000",
		"HKV89":                     "484b56310301000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f000100000000000000040000",
		"LookupHCR309":              "484352310001020001000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010000000000000004000000b7010300000000000000000000000000000001000000000000000226a28dedef197296c095d19f0334e2d51406b22efc1a4eed50cd6d38b13b53ca47dbf9fa532d24438f384b551cf473352be09e20531df0d23f56f076dc3411f2484b56310301000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f000100000000000000040000000001029f790fb77ec398a44d94a9d61ad0cccda1d750af8fe3a08958ace6392b847e75",
		"StoredHCR478":              "484352310001030001000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f0001000000000000000400000160010300000000000000000000000000000001000000000000000226a28dedef197296c095d19f0334e2d51406b22efc1a4eed50cd6d38b13b53ca47dbf9fa532d24438f384b551cf473352be09e20531df0d23f56f076dc3411f20000010250494a3100010001000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010103000000000000000000000000000000011587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc60da51c70000007a424453310001000000004c4447503100010000010a0000000000000000000000000000000100010000000c63616d6572612d636c6f636b0000000b6d696c6c697365636f6e640a00000000000000000000000000000000000001040000000000000000000000000000000000000000000001000000000000004000e3691336de4646185cb1c7c71778f6a45df24e59c9a7b79a71b98e56412e2bba",
		"RegistrationCreatedJOR469": "4a4f5231000101000000000000000400a1484b49310201000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f000101030000000000000000000000000000000126a28dedef197296c095d19f0334e2d51406b22efc1a4eed50cd6d38b13b53ca47dbf9fa532d24438f384b551cf473352be09e20531df0d23f56f076dc3411f202484b49310201000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f000101030000000000000000000000000000000126a28dedef197296c095d19f0334e2d51406b22efc1a4eed50cd6d38b13b53ca47dbf9fa532d24438f384b551cf473352be09e20531df0d23f56f076dc3411f2484b56310301000000000000000000000000000000000000000000000102000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f00010000000000000004000000000000000000027c6df5b1706823114c51f041dce0039deced64069f457128fffa182356055b31",
	}
	b, err := hex.DecodeString(literals[name])
	if err != nil || len(b) == 0 {
		t.Fatal("qualified independent inventory literal", name, err)
	}
	return b
}
func inventoryTestStoreConfig(t *testing.T) (*raftlog.Store, raftlog.Config) {
	t.Helper()
	p := raftlog.DefaultApplicationPolicy(1)
	p.MaxKeyBytes, p.MaxValueBytes, p.MaxImageBytes = 96, 4194524, 140
	p.MaxPageRows, p.MaxPageBytes = 2, 16781682
	p.MaxInstallWrites, p.MaxInstallBytes, p.MaxChangeBytes, p.MaxOutcomeBytes = 2, 8395198, 1, 469
	l := raftlog.DefaultLimits()
	l.MaxEntryBytes, l.MaxReadBytes, l.MaxReadyBytes = 4194389, 4194421, 4194901
	c, err := raftlog.ApplicationContractForPolicyWithControls(p, raftlog.ApplicationControlConfig{Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	cfg := raftlog.Config{Dir: "db", FS: vfs.NewMem(), Create: true, Limits: l, Application: p, Controls: raftlog.ApplicationControlConfig{Version: 1}, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, Contract: c, Limits: raftlog.DefaultApplicationTransferLimits()}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 10000}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: registeredInventorySemanticContractID()}
	cfg.Transfer.Limits.MaxChunkRows, cfg.Transfer.Limits.MaxChunkBytes = 1, 8390892
	s, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Initialize([]uint64{1, 2, 3}, inventoryLiteral(t, "GR2")); err != nil {
		t.Fatal(err)
	}
	return s, cfg
}
func inventoryTestStore(t *testing.T) *raftlog.Store {
	t.Helper()
	s, _ := inventoryTestStoreConfig(t)
	return s
}

func inventoryInstall(t *testing.T, s *raftlog.Store, index uint64, command, outcome []byte, puts []raftlog.ApplicationControlPut) {
	t.Helper()
	v, err := s.ApplicationView(index - 1)
	if err != nil {
		t.Fatal(err)
	}
	root, err := v.RootBounded(t.Context(), 140)
	if closeErr := v.Close(); err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	e := &pb.Entry{Index: new(index), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: command}
	if err := s.Persist(raft.Ready{Entries: []*pb.Entry{e}, HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(index)}}); err != nil {
		t.Fatal("unit Ready", err)
	}
	batch := raftlog.ApplicationBatch{BaseGeneration: root.Generation, BaseIndex: root.Index, BaseImageHash: root.ImageHash, Image: inventoryLiteral(t, "GR2"), Outcome: outcome, ControlPuts: puts}
	if err := s.InstallApplication(index, batch); err != nil {
		t.Fatal("unit Install", err)
	}
}
func inventoryGenesis(t *testing.T, s *raftlog.Store, origin []byte) {
	t.Helper()
	inventoryInstall(t, s, 2, nil, nil, nil)
	inventoryInstall(t, s, 3, inventoryLiteral(t, "GJQ255"), inventoryLiteral(t, "GenesisCreatedJOR145"), []raftlog.ApplicationControlPut{{Key: inventoryLiteral(t, "HKS47"), Value: origin}})
}
func inventorySmall(t *testing.T) *raftlog.Store {
	t.Helper()
	s := inventoryTestStore(t)
	inventoryGenesis(t, s, inventoryLiteral(t, "OriginHCR378"))
	inventoryInstall(t, s, 4, inventoryLiteral(t, "RJQ269"), inventoryLiteral(t, "RegistrationCreatedJOR469"), []raftlog.ApplicationControlPut{{Key: inventoryLiteral(t, "HKI96"), Value: inventoryLiteral(t, "LookupHCR309")}, {Key: inventoryLiteral(t, "HKV89"), Value: inventoryLiteral(t, "StoredHCR478")}})
	return s
}
func inventoryComplete(t *testing.T, a *registeredInventoryAudit) *registeredInventorySummary {
	t.Helper()
	b := registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960}
	done := false
	for steps := 0; !done && steps < 128; steps++ {
		var err error
		done, err = a.Advance(t.Context(), b)
		if err != nil {
			t.Fatal("bounded fixture phase", a.phase, err)
		}
	}
	summary, err := a.Finish(t.Context(), b)
	if !done || err != nil || summary == nil {
		t.Fatal("complete fixture", err)
	}
	return summary
}
func inventoryReseal(wire []byte) {
	sum := sha256.Sum256(wire[:len(wire)-32])
	copy(wire[len(wire)-32:], sum[:])
}

func TestRegisteredInventoryQuotaCancellationAndOwners(t *testing.T) {
	s := inventorySmall(t)
	b := registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960}
	for _, bad := range []registeredInventoryBudget{{}, {sourceBytes: 1023, ownedBytes: 1024}, {sourceBytes: 1024, ownedBytes: 1023}} {
		if a, err := newRegisteredCurrentInventory(t.Context(), s, bad); a != nil || (!errors.Is(err, errLimit) && !errors.Is(err, errInvalid)) {
			t.Fatal("constructor metadata admitted before API", err)
		}
	}
	a, err := newRegisteredCurrentInventory(t.Context(), s, b)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	before, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	captured := a.usage
	caller := registeredInventoryBudget{sourceBytes: a.maximum.sourceBytes + 1, ownedBytes: a.maximum.ownedBytes + 1}
	for _, request := range [][2]uint64{{a.maximum.sourceBytes - inventoryFixed + 1, 0}, {0, a.maximum.ownedBytes - a.held() + 1}} {
		if err := a.need(caller, request[0], request[1]); !errors.Is(err, errLimit) {
			t.Fatal("caller quota cannot override derived contract maximum", request, err)
		}
		after, err := s.ApplicationUsage()
		if err != nil || after != before || a.usage != captured || a.phase != inventoryOrigin || a.cursor != nil || a.pendingKey != nil || a.controlBytes != 0 || a.controlRecords != 0 || a.lookups != 0 || a.stored != 0 || a.seeds != 0 {
			t.Fatal("intrinsic maximum refusal leaves complete prefix and owner usage unchanged", after, err)
		}
	}
	copied := registeredInventoryAudit{self: a.self, view: a.view, closeDone: a.closeDone}
	if err := copied.Close(); !errors.Is(err, errInvalid) {
		t.Fatal("copied audit cannot release genuine view", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if done, err := a.Advance(ctx, b); done || !errors.Is(err, context.Canceled) || a.phase != inventoryOrigin || a.controlRecords != 0 || a.cursor != nil {
		t.Fatal("real cancellation is resumable with unchanged prefix", err)
	}
	// Origin exact work/owned independently follows F-840+2*140+1300.
	for _, short := range []registeredInventoryBudget{{sourceBytes: 633548, ownedBytes: 626018}, {sourceBytes: 633549, ownedBytes: 626017}} {
		if done, err := a.Advance(t.Context(), short); done || !errors.Is(err, errLimit) || a.phase != inventoryOrigin || a.cursor != nil || a.controlRecords != 0 {
			t.Fatal("origin one-short before action", err)
		}
	}
	if _, err := a.Advance(t.Context(), registeredInventoryBudget{sourceBytes: 633549, ownedBytes: 626018}); err != nil {
		t.Fatal("exact Origin phase", err)
	}
	for a.phase != inventorySourceOutcome {
		if _, err := a.Advance(t.Context(), b); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.Advance(t.Context(), registeredInventoryBudget{sourceBytes: 626363, ownedBytes: 17406960}); !errors.Is(err, errLimit) || a.phase != inventorySourceOutcome || a.cursor != nil || a.controlRecords != 0 {
		t.Fatal("outcome cannot borrow change policy T1 instead of Q469", err)
	}
	if _, err := a.Advance(t.Context(), registeredInventoryBudget{sourceBytes: 627767 - 1, ownedBytes: 17406960}); !errors.Is(err, errLimit) || a.phase != inventorySourceOutcome {
		t.Fatal("actual outcome policy one-short", err)
	}
	if _, err := a.Advance(t.Context(), registeredInventoryBudget{sourceBytes: 627767, ownedBytes: 17406960}); err != nil {
		t.Fatal("actual outcome policy exact", err)
	}
	summary := inventoryComplete(t, a)
	summary.base.Image[0] ^= 1
	if !bytes.Equal(a.base.Image, inventoryLiteral(t, "GR2")) {
		t.Fatal("summary output aliases retained audit/view image")
	}
	if err := a.Close(); err != nil || a.Close() != err {
		t.Fatal("repeated actual owner release", err)
	}
	if result, err := a.Finish(t.Context(), b); result != nil || !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("closed owner cannot expose live completion", err)
	}
	usage, err := s.ApplicationUsage()
	if err != nil || usage.Views != 0 || usage.ViewBytes != 0 {
		t.Fatal("all genuine inventory views released", usage, err)
	}
}

func inventoryLargePIJ(t *testing.T) []byte {
	t.Helper()
	domain := []byte("DGP1\x00\x01\x00\x04\x00")
	ref := bytes.Repeat([]byte{'a'}, 4066)
	for ordinal := uint16(1); ordinal <= 1024; ordinal++ {
		var id [16]byte
		binary.BigEndian.PutUint16(id[14:], ordinal)
		domain = append(domain, id[:]...)
		domain = append(domain, 1, 0, 1)
		domain = binary.BigEndian.AppendUint32(domain, 4066)
		domain = append(domain, ref...)
		domain = binary.BigEndian.AppendUint32(domain, 1)
		domain = append(domain, 'u')
	}
	var last [16]byte
	binary.BigEndian.PutUint16(last[14:], 1024)
	domain = append(domain, last[:]...)
	domain = append(domain, 0)
	body := []byte("BDS1\x00\x01\x00")
	body = binary.BigEndian.AppendUint32(body, 4192282)
	body = append(body, domain...)
	body = append(body, 0, 0, 1, 4)
	body = append(body, make([]byte, 15)...)
	body = binary.BigEndian.AppendUint64(body, 1)
	body = binary.BigEndian.AppendUint64(body, 64)
	pij := bytes.Clone(inventoryLiteral(t, "PIJ258")[:131])
	pij = binary.BigEndian.AppendUint32(pij, 4192328)
	pij = append(pij, body...)
	pij = append(pij, 0)
	if len(domain) != 4192282 || len(body) != 4192328 || len(pij) != 4192464 {
		t.Fatal("independent legal full-format wire")
	}
	return pij
}
func TestRegisteredInventoryFullFormatBeyondEightMiB(t *testing.T) {
	s, cfg := inventoryTestStoreConfig(t)
	inventoryGenesis(t, s, inventoryLiteral(t, "OriginHCR378"))
	large := inventoryLargePIJ(t)
	for j := range 3 {
		pij := bytes.Clone(large)
		pij[82] = byte(5 + j)
		index := uint64(4 + j)
		hki, lookup, hkv, stored, err := encodeRegistryRecords(pij, index, 2, registeredInventoryFormat(), registryTestBudget())
		if err != nil {
			t.Fatal(err)
		}
		ref, borrowed, err := decodeRegistryStored(hkv[:], stored, index, inventoryScope(t), registeredInventoryFormat(), registryTestBudget())
		if err != nil || len(borrowed) != 4192464 || &borrowed[0] != &stored[188] {
			t.Fatal("legal full-format baseline", err)
		}
		outcome, err := encodeRegistryOutcome(1, index, ref.submitted, ref, registryTestBudget())
		if err != nil {
			t.Fatal(err)
		}
		command, err := encodeRegistryCommand(pij, registeredInventoryFormat(), registryTestBudget())
		if err != nil {
			t.Fatal(err)
		}
		inventoryInstall(t, s, index, command, outcome, []raftlog.ApplicationControlPut{{Key: hki[:], Value: lookup}, {Key: hkv[:], Value: stored}})
	}
	usage, err := s.ApplicationControlUsage()
	if err != nil || usage.Bytes <= 8<<20 || usage.Records != 7 {
		t.Fatal("actual retained journal exceeds8MiB, not oneoperation quota", usage, err)
	}
	a, err := newRegisteredCurrentInventory(t.Context(), s, registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	summary := inventoryComplete(t, a)
	if summary.pairs != 3 || summary.controlBytes != usage.Bytes || summary.controlRecords != usage.Records || summary.base.Index != 6 || summary.maxOriginalTerm != 2 {
		t.Fatal("complete large original journal under finite eachphase", summary)
	}
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	// Actual compaction retains source/root/outcomes; knownTerm accepts only
	// original indices below FirstIndex, never substitutes a current term.
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	recovered, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal("actual durable Store reopen", err)
	}
	t.Cleanup(func() {
		if err := recovered.Close(); err != nil {
			t.Error(err)
		}
	})
	reopened, err := newRegisteredCurrentInventory(t.Context(), recovered, registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if inventoryComplete(t, reopened).pairs != 3 {
		t.Fatal("actual reopened compacted full originals lost")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	receiver, prepared, manifest := inventoryPrepared(t, recovered)
	portable, err := newRegisteredPreparedInventory(t.Context(), prepared, registeredInventoryPreparedContext{index: 6, term: manifest.Term, voters: [3]uint64{1, 2, 3}}, registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960})
	if err != nil {
		t.Fatal("actual prepared largeStored bank", err)
	}
	defer portable.Close()
	proof := inventoryComplete(t, portable)
	if proof.prepared != prepared || proof.controlBytes != usage.Bytes || proof.controlRecords != 7 || proof.pairs != 3 || proof.base.Generation != 2 || proof.base.Index != 6 {
		t.Fatal("complete actual largeAS4 samebank audit", proof)
	}
	if err := portable.Close(); err != nil {
		t.Fatal(err)
	}
	current, err := receiver.ApplicationUsage()
	if err != nil || current.Views != 0 || current.ControlRecords != 0 {
		t.Fatal("largeprepared readclose leaves receiver dormant", current, err)
	}
}

func inventoryScope(t *testing.T) registeredOriginScope {
	t.Helper()
	o := registeredOriginScope{graph: [16]byte{1}, partition: 1, group: [16]byte{2}, protocol: 1}
	copy(o.lineage[:], inventoryLiteral(t, "OriginHCR378")[48:80])
	return o
}
func TestRegisteredInventoryCommittedSemanticRefusals(t *testing.T) {
	for _, name := range []string{"origin_scope", "origin_contract", "lookup_scope", "lookup_hash", "lookup_length", "stored_scope", "stored_hash", "stored_length", "stored_term", "stored_index", "stored_slot", "created_JOR", "unknown_family"} {
		t.Run(name, func(t *testing.T) {
			s := inventoryTestStore(t)
			origin := inventoryLiteral(t, "OriginHCR378")
			if name == "origin_scope" {
				origin[8] ^= 1
				inventoryReseal(origin)
			}
			if name == "origin_contract" {
				origin[94+164+7] ^= 1
				inventoryReseal(origin)
			}
			inventoryGenesis(t, s, origin)
			lookup, stored := inventoryLiteral(t, "LookupHCR309"), inventoryLiteral(t, "StoredHCR478")
			key := inventoryLiteral(t, "HKV89")
			outcome := inventoryLiteral(t, "RegistrationCreatedJOR469")
			switch name {
			case "lookup_scope":
				lookup[8] ^= 1
			case "lookup_hash":
				lookup[120] ^= 1
			case "lookup_length":
				binary.BigEndian.PutUint32(lookup[273:277], 259)
			case "stored_scope":
				stored[8] ^= 1
			case "stored_hash":
				stored[152] ^= 1
			case "stored_length":
				binary.BigEndian.PutUint32(stored[184:188], 259)
			case "stored_term":
				binary.BigEndian.PutUint64(stored[112:120], 1)
			case "stored_index":
				binary.BigEndian.PutUint64(stored[82:90], 5)
			case "stored_slot":
				key[88] = 1
			case "created_JOR":
				outcome[6] = 2
			}
			inventoryReseal(lookup)
			inventoryReseal(stored)
			inventoryReseal(outcome)
			inventoryInstall(t, s, 4, inventoryLiteral(t, "RJQ269"), outcome, []raftlog.ApplicationControlPut{{Key: inventoryLiteral(t, "HKI96"), Value: lookup}, {Key: key, Value: stored}})
			if name == "unknown_family" {
				inventoryInstall(t, s, 5, nil, nil, []raftlog.ApplicationControlPut{{Key: []byte("X"), Value: []byte{0}}})
			}
			b := registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960}
			a, err := newRegisteredCurrentInventory(t.Context(), s, b)
			if err != nil {
				if !errors.Is(err, errCorrupt) || a != nil {
					t.Fatal("refused constructor whole-zero", err)
				}
				return
			}
			defer a.Close()
			done := false
			for steps := 0; !done && err == nil && steps < 128; steps++ {
				done, err = a.Advance(t.Context(), b)
			}
			if !errors.Is(err, errCorrupt) {
				t.Fatal("stored semantic input must fail", name, err)
			}
			if summary, err := a.Finish(t.Context(), b); summary != nil || !errors.Is(err, errCorrupt) {
				t.Fatal("no partial summary", name, err)
			}
		})
	}
}
func inventoryPrepared(t *testing.T, donor *raftlog.Store) (*raftlog.Store, *raftlog.PreparedApplicationSnapshot, raftlog.ApplicationSnapshotManifest) {
	t.Helper()
	cut, err := donor.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	export, err := donor.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := export.Close(); err != nil {
			t.Error(err)
		}
	}()
	complete := false
	for steps := 0; !complete && steps < 64; steps++ {
		complete, err = export.BuildManifest(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 8390892})
		if err != nil {
			t.Fatal(err)
		}
	}
	if !complete {
		t.Fatal("bounded unit export build")
	}
	manifest, err := export.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	receiver := inventoryTestStore(t)
	imported, err := receiver.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := imported.Abort(); err != nil {
			t.Error(err)
		}
	})
	final := false
	for steps := 0; !final && steps < 64; steps++ {
		chunk, err := export.Next(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 8390892})
		if err != nil {
			t.Fatal(err)
		}
		if err := imported.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		final = chunk.Final
	}
	if !final {
		t.Fatal("bounded unit full AS4 stream")
	}
	if err := imported.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	p, err := imported.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	return receiver, p, manifest
}
func TestRegisteredInventoryPreparedEvidenceAndClaimOwners(t *testing.T) {
	donor := inventorySmall(t)
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	receiver, p, manifest := inventoryPrepared(t, donor)
	b := registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960}
	expected := registeredInventoryPreparedContext{index: 4, term: 2, voters: [3]uint64{1, 2, 3}}
	// Receiver remains at1: any active receiver Term(4) proof would be false.
	if at, _, err := receiver.Checkpoint(); err != nil || at != 1 {
		t.Fatal("dormant receiver changed", at, err)
	}
	if _, err := receiver.Term(4); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatal("receiver has no source4 term", err)
	}
	a, err := newRegisteredPreparedInventory(t.Context(), p, expected, b)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	expected.term, expected.voters[0] = 99, 99
	summary := inventoryComplete(t, a)
	if summary.prepared != p || summary.base.Generation != 2 || summary.origin.index != 3 || summary.base.Index != 4 || summary.controlBytes != 1676 || summary.graphRecords != 12 || summary.maxOriginalTerm != 2 || summary.expected.term != 2 || summary.expected.voters != ([3]uint64{1, 2, 3}) {
		t.Fatal("exact p/context/samebank inert evidence", summary)
	}
	data, err := raftlog.EncodeApplicationSnapshotDescriptor(manifest)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(manifest.Index), Term: new(manifest.Term), ConfState: manifest.ConfState}, Data: data}
	if claim, err := receiver.ClaimPreparedApplicationSnapshot(p, snapshot); claim != nil || !errors.Is(err, raftlog.ErrLimit) {
		t.Fatal("actual audit reader blocks Claim", err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	claim, err := receiver.ClaimPreparedApplicationSnapshot(p, snapshot)
	if err != nil {
		t.Fatal("actual reader owner released before Claim", err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, limit := range []registeredInventoryBudget{{sourceBytes: 1023, ownedBytes: 1024}, {sourceBytes: 627943, ownedBytes: b.ownedBytes}} {
		if audit, err := newRegisteredPreparedInventory(t.Context(), p, summary.expected, limit); audit != nil || !errors.Is(err, errLimit) {
			t.Fatal("prepared actual metadata/image charge precedes view", audit, err)
		}
	}
	if audit, err := newRegisteredPreparedInventory(canceled, p, summary.expected, b); audit != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("prepared real cancellation before owner metadata", audit, err)
	}
	bad, err := newRegisteredPreparedInventory(t.Context(), p, registeredInventoryPreparedContext{index: 4, term: 1, voters: [3]uint64{1, 2, 3}}, b)
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	if _, err := bad.Advance(t.Context(), b); !errors.Is(err, errCorrupt) {
		t.Fatal("portable maxterm is validated as inert context", err)
	}
	if summary, err := bad.Finish(t.Context(), b); summary != nil || !errors.Is(err, errCorrupt) {
		t.Fatal("bad context no summary", err)
	}
}
func TestRegisteredInventoryLogicalPhaseLayoutAndRefusals(t *testing.T) {
	c := raftlog.ApplicationContract{Version: 2, MaxKeyBytes: 96, MaxValueBytes: 4194524, MaxImageBytes: 140, MaxPageRows: 2, MaxPageBytes: 16781682, MaxInstallWrites: 2, MaxInstallBytes: 8395198, MaxChangeBytes: 1, MaxOutcomeBytes: 469}
	maximum, err := inventoryRequirements(c, 4194389)
	if err != nil || maximum != (registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960}) {
		t.Fatal("checked MAX of independent phase laws", maximum, err)
	}
	if _, err := inventoryRequirements(c, (64<<20)+1); !errors.Is(err, errInvalid) {
		t.Fatal("entry max before arithmetic", err)
	}
	bad := c
	bad.MaxPageBytes--
	if _, err := inventoryRequirements(bad, 4194389); !errors.Is(err, errLimit) {
		t.Fatal("fullformat capability floor", err)
	}
	// Actual caller+owner+summary+codec/nested/value-copy models are logical,
	// never compiler stack/ABI elision/heap/RSS assertions.
	type StrictBridge struct {
		Receiver    *registryLedger
		Input       []byte
		Format      registeredFormatBudget
		NamedResult registeredPreparedInspection
		Error       error
		Child, Work int
	}
	type StoredCaller struct {
		Key, Value  []byte
		Insertion   uint64
		Origin      registeredOriginScope
		Format      registeredFormatBudget
		Budget      registeredRegistryCodecBudget
		ReturnRef   registeredRegistryRef
		ReturnSpan  []byte
		ReturnError error
		Ledger      *registryLedger
		Payload     []byte
		LocalRef    registeredRegistryRef
		N           int
		PIJ         []byte
		Inspection  registeredPreparedInspection
		LiveLedger  registryLedger
		LocalError  error
	}
	type AuditCaller struct {
		Receiver      *registeredInventoryAudit
		Context       context.Context
		Budget        registeredInventoryBudget
		ReturnSummary *registeredInventorySummary
		Error         error
		WorkOwned     [4]uint64
		Contract      raftlog.ApplicationContract
		RootSlots     [3]raftlog.ApplicationRoot
		OriginSlots   [2]registeredOrigin
		RefSlots      [2]registeredRegistryRef
		KeyHeaders    [3][]byte
		RecordReturns [2]raftlog.ApplicationControlRecord
	}
	type KeyScopeCallee struct {
		SourceArguments [2]registeredGenesisSource
		Keys            [2][47]byte
		LineageInputs   [2][86]byte
		Digests         [3][32]byte
		HashState       [256]byte
		ScopeReturns    [2]registeredOriginScope
		Error           error
	}
	type CloseCaller struct {
		Receiver                  *registeredInventoryAudit
		DetachedView              *raftlog.ApplicationView
		Done                      chan struct{}
		LocalError, ReturnedError error
	}
	core := unsafe.Sizeof(registeredParsed{}) + unsafe.Sizeof(registeredPreparedInspection{}) + unsafe.Sizeof(registeredLedger{}) + unsafe.Sizeof(registeredFormatBudget{}) + 2*unsafe.Sizeof(registeredCursor{}) + unsafe.Sizeof(registeredAxisView{}) + 2*256 + 2*32 + 96 + 128
	strict := core + unsafe.Sizeof(StrictBridge{}) + unsafe.Sizeof(StoredCaller{})
	// Each source-level owner/argument/result is distinct; retain caller and
	// close slots even though the expensive codec phase completes before Close.
	fixed := 65536*9 + unsafe.Sizeof(registeredInventoryAudit{}) + unsafe.Sizeof(registeredInventorySummary{}) + unsafe.Sizeof(AuditCaller{}) + unsafe.Sizeof(KeyScopeCallee{}) + unsafe.Sizeof(CloseCaller{}) + unsafe.Sizeof(raftlog.ApplicationView{}) + unsafe.Sizeof(raftlog.PreparedApplicationSnapshot{}) + strict + 2048 + 2*256 + 8192
	if core > 2048 || strict > 4096 {
		t.Fatal("all actual nested canonical/result slots", core, strict)
	}
	if fixed > uintptr(inventoryFixed)-840 {
		t.Fatal("audit-specific simultaneous fixed metadata", fixed, inventoryFixed)
	}
	t.Log("audit logical metadata sources sizeof", fixed, inventoryFixed)
	s := inventorySmall(t)
	b := registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960}
	//nolint:staticcheck // SA1012: exercise the intentional nil-context rejection contract.
	if a, err := newRegisteredCurrentInventory(nil, s, b); a != nil || !errors.Is(err, errInvalid) {
		t.Fatal("nil context", err)
	}
	if a, err := newRegisteredCurrentInventory(t.Context(), nil, b); a != nil || !errors.Is(err, errInvalid) {
		t.Fatal("nil store", err)
	}
	if a, err := newRegisteredPreparedInventory(t.Context(), nil, registeredInventoryPreparedContext{index: 4, term: 2, voters: [3]uint64{1, 2, 3}}, b); a != nil || !errors.Is(err, errInvalid) {
		t.Fatal("nil prepared", err)
	}
	a, err := newRegisteredCurrentInventory(t.Context(), s, b)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if summary, err := a.Finish(t.Context(), b); summary != nil || !errors.Is(err, errInvalid) {
		t.Fatal("pending is not completion", err)
	}
	//nolint:staticcheck // SA1012: exercise the intentional nil-context rejection contract.
	if _, err := a.Advance(nil, b); !errors.Is(err, errInvalid) {
		t.Fatal("advance nil context", err)
	}
	if err := new(registeredInventoryAudit).Close(); !errors.Is(err, errInvalid) {
		t.Fatal("forged owner zero cannot close", err)
	}
}

// Breaks constructors/finish that lose genuine-owner error or quota boundaries.
// These are ordinary public lifecycle inputs, not altered audit state or faults.
func TestRegisteredInventoryCaptureLifecycleRefusals(t *testing.T) {
	b := registeredInventoryBudget{sourceBytes: 42572854, ownedBytes: 17406960}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	s := inventorySmall(t)
	if a, err := newRegisteredCurrentInventory(ctx, s, b); a != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("current canceled before metadata", a, err)
	}
	if a, err := newRegisteredCurrentInventory(t.Context(), s, registeredInventoryBudget{sourceBytes: 627001, ownedBytes: b.ownedBytes}); a != nil || !errors.Is(err, errLimit) {
		t.Fatal("current configured image charge one short", a, err)
	}
	v, err := s.ApplicationView(4)
	if err != nil {
		t.Fatal(err)
	}
	contract, err := raftlog.ApplicationContractForPolicyWithControls(s.ApplicationLimits(), s.ApplicationControls())
	if err != nil {
		t.Fatal(err)
	}
	//nolint:staticcheck // SA1012: exercise the intentional nil-context rejection contract.
	if a, err := finishInventoryCapture(nil, v, contract, registeredInventoryPreparedContext{}, s, nil); a != nil || !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal("capture genuine view API refusal is whole zero", a, err)
	}
	if usage, err := s.ApplicationUsage(); err != nil || usage.Views != 0 {
		t.Fatal("failed capture releases genuine view", usage, err)
	}
	a, err := newRegisteredCurrentInventory(t.Context(), s, b)
	if err != nil {
		t.Fatal(err)
	}
	inventoryComplete(t, a)
	for _, tc := range []struct {
		ctx  context.Context
		want error
	}{{nil, errInvalid}, {ctx, context.Canceled}} {
		if out, err := a.Finish(tc.ctx, b); out != nil || !errors.Is(err, tc.want) {
			t.Fatal("finish nil/canceled whole zero", out, err)
		}
	}
	if out, err := a.Finish(t.Context(), registeredInventoryBudget{sourceBytes: 625278 + 16781682 + 1304 - 1, ownedBytes: b.ownedBytes}); out != nil || !errors.Is(err, errLimit) {
		t.Fatal("finish fresh census precharge one short", out, err)
	}
	if err := a.view.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.rootAt(t.Context(), 1); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("history preserves genuine closed view refusal", err)
	}
	if out, err := a.Finish(t.Context(), b); out != nil || !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("finish closed view cannot authorize summary", out, err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	fresh := inventoryTestStore(t)
	empty, err := newRegisteredCurrentInventory(t.Context(), fresh, b)
	if err != nil {
		t.Fatal(err)
	}
	if done, err := empty.Advance(t.Context(), b); done || !errors.Is(err, errRegisteredInventoryUnavailable) || empty.cursor != nil || empty.controlRecords != 0 {
		t.Fatal("fresh empty bank never creates missing origin", done, err)
	}
	if out, err := empty.Finish(t.Context(), b); out != nil || !errors.Is(err, errRegisteredInventoryUnavailable) {
		t.Fatal("missing origin has no partial evidence", out, err)
	}
	if err := empty.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Close(); err != nil {
		t.Fatal(err)
	}
	if out, err := newRegisteredCurrentInventory(t.Context(), fresh, b); out != nil || !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("closed store has no captured owner", out, err)
	}
	_, cfg := inventoryTestStoreConfig(t)
	for _, small := range []bool{true, false} {
		cfg.FS = vfs.NewMem()
		if small {
			cfg.Application.MaxPageRows = 1
		} else {
			cfg.Application.MaxPageRows = 2
		}
		cfg.Transfer.Contract, err = raftlog.ApplicationContractForPolicyWithControls(cfg.Application, cfg.Controls)
		if err != nil {
			t.Fatal(err)
		}
		owner, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal("matched actual policy config", err)
		}
		seed := inventoryLiteral(t, "GR2")
		if !small {
			seed = []byte{1}
		}
		if err := owner.Initialize([]uint64{1, 2, 3}, seed); err != nil {
			t.Fatal(err)
		}
		want := errCorrupt
		if small {
			want = errLimit
		}
		out, err := newRegisteredCurrentInventory(t.Context(), owner, b)
		if out != nil || !errors.Is(err, want) {
			t.Fatal("actual small policy/wrong-size seed refusal", small, out, err)
		}
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
	}
	binding := raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 1}}
	for _, seed := range [][]byte{nil, make([]byte, 140), append([]byte{'G', 'R', 2, 2}, make([]byte, 136)...), inventoryLiteral(t, "GR2")} {
		binding.Identity.Partition = 2 // Valid wire is rejected for actual namespace mismatch.
		if err := inventorySeed(seed, binding); !errors.Is(err, errCorrupt) {
			t.Fatal("seed framing/canonical/namespace refusal", err)
		}
	}
	if err := inventoryDataError(errors.Join(errLimit, context.Canceled)); !errors.Is(err, errCorrupt) || !errors.Is(err, errLimit) || !errors.Is(err, context.Canceled) {
		t.Fatal("data-limit classification preserves joined cause", err)
	}
}
