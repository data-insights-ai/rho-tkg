package raftlog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io/fs"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestApplicationPublicationGetterAndMetadataAreFailClosed(t *testing.T) {
	var nilStore *Store
	if _, err := nilStore.PublishedApplicationCut(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	legacy := generationStore(t, vfs.NewMem())
	if _, err := legacy.PublishedApplicationCut(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	s := publicationStore(t, vfs.NewMem())
	raw, err := encodeMeta(s.meta)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw[:4]) != "RLM5" || metadataBytes(s.meta) != uint64(len(raw)) {
		t.Fatal("publication format/size drift")
	}
	trailer := appendGenerationMeta(nil, s.meta.Gen)
	if !bytes.Equal(trailer[:4], []byte{'A', 'G', 2, 0}) {
		t.Fatal("old reader could silently open new publication state")
	}
	for j := range len(trailer) {
		if _, _, err := decodeGenerationMeta(trailer[:j]); !errors.Is(err, ErrCorrupt) {
			t.Fatal("truncated AG2 accepted", j, err)
		}
	}
	p := appendPublicationMeta(nil, s.meta.Gen.Publication)
	for _, offset := range []int{0, 4, 12, 20} {
		bad := bytes.Clone(p)
		for j := 0; j < 8 && offset+j < len(bad); j++ {
			bad[offset+j] = 255
		}
		if _, _, err := decodePublicationMeta(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatal("invalid publication fields accepted", offset, err)
		}
	}
	if err := s.PublishSnapshot(); !errors.Is(err, raft.ErrSnapOutOfDate) {
		t.Fatal("synthetic base republished", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PublishedApplicationCut(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func TestApplicationPublicationMismatchRefusesBeforeBankCleanup(t *testing.T) {
	for _, kind := range []string{"ID", "bank", "generation", "free", "staging", "bytes", "records", "base"} {
		t.Run(kind, func(t *testing.T) {
			mem := vfs.NewCrashableMem()
			s := publicationStore(t, mem)
			m := s.meta
			switch kind {
			case "ID":
				m.Gen.Publication.ID[0] ^= 1
			case "bank":
				m.Gen.Publication.Bank = 2
			case "generation":
				m.Gen.Publication.Generation++
			case "free":
				m.Gen.Publication.Bank = 1
				m.Gen.Publication.Generation = 2
				m.Gen.HighWater = 2
				m.Gen.Banks[1] = applicationBank{Generation: 2, State: bankFree}
			case "staging":
				m.Gen.Publication.Bank = 1
				m.Gen.Publication.Generation = 2
				m.Gen.HighWater = 2
				m.Gen.Banks[1] = applicationBank{Generation: 2, Through: 1, State: bankStaging, ReservedBytes: 142, ReservedRecords: 3}
			case "bytes":
				m.Gen.Publication.Bytes++
			case "records":
				m.Gen.Publication.Records++
			case "base":
				m.Gen.Publication.Valid = false
			}
			raw, err := encodeMeta(m)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.db.Set(metaKey, raw, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			marker := bankIndexKey(1, appRootTag, 1)
			if err := s.db.Set(marker, appFrame(marker, []byte("preserve on refusal"), false), pebble.Sync); err != nil {
				t.Fatal(err)
			}
			clone := mem.CrashClone(vfs.CrashCloneCfg{})
			if opened, err := Open(Config{Dir: "db", FS: clone, Application: s.meta.App.Policy, Transfer: s.meta.Transfer, Generations: s.meta.Gen.Limits, PublishedCuts: s.meta.Gen.Publication.Limits}); opened != nil || !errors.Is(err, ErrCorrupt) {
				if opened != nil {
					_ = opened.Close()
				}
				t.Fatal("bad publication reference opened", err)
			}
			db, err := pebble.Open("db", &pebble.Options{FS: clone})
			if err != nil {
				t.Fatal(err)
			}
			value, closer, err := db.Get(marker)
			if err != nil || !bytes.Contains(value, []byte("preserve on refusal")) {
				t.Fatal("failed open erased a bank", err)
			}
			if err := closer.Close(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestApplicationPublicationHeadroomIsSpecificAndLegacyCannotAcquireIt(t *testing.T) {
	p, tc := transferConfig(1)
	g := ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}
	pub := ApplicationPublishedCutLimits{MaxTransferChunks: 1_000_000}
	l := DefaultLimits()
	l.MaxEntryBytes = frameOverhead
	l.MaxReadBytes = frameOverhead + 32
	initial := metadata{Hard: &pb.HardState{}, Conf: &pb.ConfState{}, Snap: &pb.Snapshot{}, App: applicationMetadata{Policy: p}, Transfer: tc, Gen: generationMetadata{Limits: g, HighWater: 1, Banks: [2]applicationBank{{Generation: 1, State: bankActive}, {}}, Publication: publicationMetadata{Limits: pub}}}
	worst := initial
	worst.Hard = &pb.HardState{Term: new(^uint64(0)), Vote: new(p.LocalVoter), Commit: new(^uint64(0))}
	cs := &pb.ConfState{Voters: []uint64{p.LocalVoter}}
	worst.Conf = cs
	worst.Snap = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(^uint64(0)), Term: new(^uint64(0)), ConfState: cs}}
	wire, err := encodeMeta(worst)
	if err != nil {
		t.Fatal(err)
	}
	need := len(wire) + readyEnvelopeBytes
	for _, delta := range []int{-1, 0} {
		mem := vfs.NewMem()
		l.MaxReadyBytes = need + delta
		s, err := Open(Config{Dir: "limited", FS: mem, Create: true, Limits: l, Application: p, Transfer: tc, Generations: g, PublishedCuts: pub})
		if delta < 0 {
			if s != nil || !errors.Is(err, ErrLimit) {
				if s != nil {
					_ = s.Close()
				}
				t.Fatal("publication admitted unprogressable metadata envelope", err)
			}
			if _, err := mem.Stat("limited"); !errors.Is(err, fs.ErrNotExist) {
				t.Fatal("headroom refusal created a database", err)
			}
		} else {
			if err != nil {
				t.Fatal("exact supported-width headroom refused", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	mem := vfs.NewCrashableMem()
	old := generationStore(t, mem)
	c := Config{Dir: "db", FS: mem.CrashClone(vfs.CrashCloneCfg{}), Application: old.meta.App.Policy, Transfer: old.meta.Transfer, Generations: old.meta.Gen.Limits, PublishedCuts: pub}
	if opened, err := Open(c); opened != nil || !errors.Is(err, ErrInvalid) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatal("AG1 acquired publication mode silently", err)
	}
	c.PublishedCuts = ApplicationPublishedCutLimits{}
	recovered, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if readyMetadataFingerprint(t, recovered) != readyMetadataFingerprint(t, old) {
		t.Fatal("publication-mode refusal changed legacy state")
	}
}
func TestApplicationPublicationDescriptorBindingCoversAllPortableFields(t *testing.T) {
	s := publicationStore(t, vfs.NewMem())
	c, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	for _, alter := range []func(*ApplicationCutReference){func(c *ApplicationCutReference) { c.Identity.Group[0]++ }, func(c *ApplicationCutReference) { c.Contract.MaxValueBytes++ }, func(c *ApplicationCutReference) { c.Term++ }, func(c *ApplicationCutReference) { c.ImageHash = sha256.Sum256([]byte("other")) }, func(c *ApplicationCutReference) { c.RetainedBytes++ }, func(c *ApplicationCutReference) { c.RetainedRecords++ }} {
		changed := c
		alter(&changed)
		id, err := cutID(changed)
		if err != nil || id == c.ID {
			t.Fatal("descriptor field not bound", err)
		}
	}
}

func TestApplicationPublicationRejectsInvalidPortableBindingsAndConfiguration(t *testing.T) {
	s := publicationStore(t, vfs.NewMem())
	c, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	for _, alter := range []func(*ApplicationCutReference){func(c *ApplicationCutReference) { c.Identity.Graph = [16]byte{} }, func(c *ApplicationCutReference) { c.Contract.Version = 0 }, func(c *ApplicationCutReference) { c.ConfState = nil }} {
		bad := c
		alter(&bad)
		if _, err := cutID(bad); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid portable binding accepted", err)
		}
	}
	p, tc := transferConfig(1)
	for _, config := range []Config{{Dir: "bad", FS: vfs.NewMem(), Create: true, Application: p, Transfer: tc, PublishedCuts: ApplicationPublishedCutLimits{MaxTransferChunks: 1}}, {Dir: "bad", FS: vfs.NewMem(), Create: true, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{MaxBytes: 1 << 30, MaxRecords: 1_000_000}, PublishedCuts: ApplicationPublishedCutLimits{MaxTransferChunks: 1<<32 + 1}}} {
		if opened, err := Open(config); opened != nil || !errors.Is(err, ErrInvalid) {
			if opened != nil {
				_ = opened.Close()
			}
			t.Fatal("invalid publication config opened", err)
		}
	}
}
