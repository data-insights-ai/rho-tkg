package raftlog

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	pb "go.etcd.io/raft/v3/raftpb"
)

func fuzzSnapshotManifest() ApplicationSnapshotManifest {
	p, tc := transferConfig(1)
	m := ApplicationSnapshotManifest{Identity: tc.Identity, Contract: ApplicationContractForPolicy(p), Index: 1, Term: 1, ConfState: &pb.ConfState{Voters: []uint64{1}}, Image: []byte("initial"), ImageHash: sha256.Sum256([]byte("initial"))}
	state := snapshotVerifier{digest: snapshotSeed(m)}
	for j := 1; j < 4; j++ {
		m.NamespaceRecords[j] = 1
		m.NamespaceBytes[j] = 45
		if j == 1 {
			m.NamespaceBytes[j] += 7
		}
	}
	for _, tag := range []byte{appRootTag, appChangeTag, appOutcomeTag} {
		k := appIndexKey(tag, 1)
		var value []byte
		if tag == appRootTag {
			value = m.Image
		}
		state, _ = state.accept(m, k, appFrame(k, value, false))
	}
	m.RecordsHash = state.digest
	return m
}
func FuzzApplicationSnapshotManifest(f *testing.F) {
	m := fuzzSnapshotManifest()
	wire, err := EncodeApplicationSnapshotManifest(m)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(wire)
	f.Add([]byte("AS"))
	f.Add(wire[:len(wire)-1])
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 1<<20 {
			t.Skip()
		}
		m, err := DecodeApplicationSnapshotManifest(input, 64<<10)
		if err != nil {
			return
		}
		encoded, err := EncodeApplicationSnapshotManifest(m)
		if err != nil {
			t.Fatal(err)
		}
		again, err := DecodeApplicationSnapshotManifest(encoded, 64<<10)
		if err != nil {
			t.Fatal(err)
		}
		a, _ := manifestID(m)
		b, _ := manifestID(again)
		if a != b {
			t.Fatal("manifest roundtrip changed binding")
		}
	})
}
func FuzzApplicationSnapshotImportNeverChangesActive(f *testing.F) {
	m := fuzzSnapshotManifest()
	var data []byte
	for _, tag := range []byte{appRootTag, appChangeTag, appOutcomeTag} {
		k := appIndexKey(tag, 1)
		var value []byte
		if tag == appRootTag {
			value = m.Image
		}
		data = appendSnapshotFrame(data, k, appFrame(k, value, false))
	}
	f.Add(data)
	f.Add([]byte{255, 255, 255, 255, 255, 255, 255, 255})
	f.Add(data[:len(data)-1])
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 64<<10 {
			t.Skip()
		}
		s := transferStore(t, "fuzz", vfs.NewMem(), 3)
		before, _ := encodeMeta(s.meta)
		i, err := s.BeginApplicationImport(t.Context(), m)
		if err != nil {
			t.Fatal(err)
		}
		if err := i.Append(t.Context(), ApplicationSnapshotChunk{Data: input, Final: true}); err == nil {
			if err := i.Verify(t.Context()); err != nil {
				t.Fatal("accepted stream failed verification", err)
			}
		}
		after, _ := encodeMeta(s.meta)
		if !bytes.Equal(before, after) {
			t.Fatal("network bytes changed active state")
		}
		if err := i.Abort(); err != nil {
			t.Fatal(err)
		}
		if err := s.ScrubApplication(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}
