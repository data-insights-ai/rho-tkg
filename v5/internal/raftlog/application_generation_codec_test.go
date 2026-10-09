package raftlog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestApplicationGenerationReportsLocalConfigurationWithoutLease(t *testing.T) {
	var nilStore *Store
	if nilStore.ApplicationGeneration() != 0 {
		t.Fatal("nil generation")
	}
	legacy := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	if legacy.ApplicationGeneration() != 0 {
		t.Fatal("legacy generation changed")
	}
	s := generationStore(t, vfs.NewMem())
	if s.ApplicationGeneration() != 1 {
		t.Fatal("initial local generation")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.ApplicationGeneration() != 1 {
		t.Fatal("closed getter changed historical configuration")
	}
}
func TestApplicationGenerationConfigRejectsMissingPrerequisitesAndBudgets(t *testing.T) {
	p, tc := transferConfig(1)
	for _, c := range []Config{
		{Dir: "db", Create: true, Generations: ApplicationGenerationLimits{MaxBytes: 1, MaxRecords: 1}},
		{Dir: "db", Create: true, Application: p, Generations: ApplicationGenerationLimits{MaxBytes: 1, MaxRecords: 1}},
		{Dir: "db", Create: true, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{MaxBytes: 0, MaxRecords: 1}},
		{Dir: "db", Create: true, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{MaxBytes: 1, MaxRecords: 0}},
		{Dir: "db", Create: true, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{MaxBytes: 1<<40 + 1, MaxRecords: 1}},
		{Dir: "db", Create: true, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{MaxBytes: 1, MaxRecords: 1<<40 + 1}},
	} {
		c.FS = vfs.NewMem()
		if opened, err := Open(c); opened != nil || !errors.Is(err, ErrInvalid) {
			if opened != nil {
				_ = opened.Close()
			}
			t.Fatal("invalid generation mode admitted", err)
		}
	}
	for _, limit := range []ApplicationGenerationLimits{{MaxBytes: 1, MaxRecords: 100}, {MaxBytes: 1000, MaxRecords: 2}} {
		mem := vfs.NewCrashableMem()
		c := Config{Dir: "tiny", FS: mem, Create: true, Application: p, Transfer: tc, Generations: limit}
		s, err := Open(c)
		if err != nil {
			t.Fatal(err)
		}
		before := readyMetadataFingerprint(t, s)
		if err := s.Initialize([]uint64{1}, []byte("initial")); !errors.Is(err, ErrLimit) {
			t.Fatal("initial rows escaped global limit", err)
		}
		if readyMetadataFingerprint(t, s) != before || !s.canInitialize || s.poison != nil {
			t.Fatal("initial admission mutated store")
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
func TestApplicationGenerationCodecAndMetadataBoundaries(t *testing.T) {
	s := generationStore(t, vfs.NewMem())
	m := s.meta
	encoded, err := encodeMeta(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded[:4]) != "RLM5" || metadataBytes(m) != uint64(len(encoded)) {
		t.Fatal("RLM5 size/format mismatch", len(encoded), metadataBytes(m))
	}
	l := s.limits
	l.MaxReadyBytes = len(encoded) + readyEnvelopeBytes
	decoded, err := decodeMeta(encoded, l)
	if err != nil || decoded.Gen != m.Gen {
		t.Fatal("generation round-trip changed binding", decoded.Gen, err)
	}
	l.MaxReadyBytes--
	if _, err := decodeMeta(encoded, l); !errors.Is(err, ErrLimit) {
		t.Fatal("RLM5 one-byte metadata overflow ignored", err)
	}
	raw := appendGenerationMeta(nil, m.Gen)
	for j := range len(raw) {
		if _, _, err := decodeGenerationMeta(raw[:j]); !errors.Is(err, ErrCorrupt) {
			t.Fatal("truncated generation trailer admitted", j, err)
		}
	}
	for _, offset := range []int{0, 4, 12, 28, 4 + 10*8, 4 + 17*8} {
		bad := bytes.Clone(raw)
		for j := 0; j < 8 && offset+j < len(bad); j++ {
			bad[offset+j] = 255
		}
		if _, _, err := decodeGenerationMeta(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatal("malformed generation trailer admitted", offset, err)
		}
	}
	bad := bytes.Clone(encoded)
	copy(bad[:4], []byte("RLM6"))
	if _, err := decodeMeta(bad, s.limits); !errors.Is(err, ErrCorrupt) {
		t.Fatal("future format opened", err)
	}
}
func TestApplicationGenerationRejectsCorruptDescriptorsBeforeCleanup(t *testing.T) {
	for _, kind := range []string{"duplicate", "high-water", "zero-high-water", "active-slot", "active-generation", "active-ledger", "free-ledger", "staging-through", "staging-reservation", "retired-through", "retired-reservation"} {
		t.Run(kind, func(t *testing.T) {
			mem := vfs.NewCrashableMem()
			s := generationStore(t, mem)
			m := s.meta
			switch kind {
			case "duplicate":
				m.Gen.Banks[1].Generation = m.Gen.Banks[0].Generation
			case "high-water":
				m.Gen.HighWater++
			case "zero-high-water":
				m.Gen.HighWater = 0
			case "active-slot":
				m.Gen.Active = 2
			case "active-generation":
				m.Gen.Banks[0].Generation = 0
			case "active-ledger":
				m.Gen.Banks[0].Bytes++
			case "free-ledger":
				m.Gen.Banks[1].Bytes = 1
			case "staging-through":
				m.Gen.HighWater = 2
				m.Gen.Banks[1] = applicationBank{Generation: 2, State: bankStaging, ReservedBytes: 135, ReservedRecords: 3}
			case "staging-reservation":
				m.Gen.HighWater = 2
				m.Gen.Banks[1] = applicationBank{Generation: 2, Through: 2, State: bankStaging, ReservedBytes: 135, ReservedRecords: 3}
			case "retired-through":
				m.Gen.HighWater = 2
				m.Gen.Banks[1] = applicationBank{Generation: 2, State: bankRetired}
			case "retired-reservation":
				m.Gen.HighWater = 2
				m.Gen.Banks[1] = applicationBank{Generation: 2, Through: 1, State: bankRetired, ReservedRecords: 3}
			}
			if err := s.validateGenerationMeta(m); !errors.Is(err, ErrInvalid) {
				t.Fatal("malformed descriptor not refused", err)
			}
			raw, err := encodeMeta(m)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.db.Set(metaKey, raw, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			key := bankIndexKey(1, appRootTag, 1)
			if err := s.db.Set(key, appFrame(key, []byte("must survive refusal"), false), pebble.Sync); err != nil {
				t.Fatal(err)
			}
			clone := mem.CrashClone(vfs.CrashCloneCfg{})
			if opened, err := Open(Config{Dir: "db", FS: clone, Application: s.meta.App.Policy, Transfer: s.meta.Transfer, Generations: s.meta.Gen.Limits}); opened != nil || !errors.Is(err, ErrCorrupt) {
				if opened != nil {
					_ = opened.Close()
				}
				t.Fatal("malformed generation metadata opened", err)
			}
			db, err := pebble.Open("db", &pebble.Options{FS: clone})
			if err != nil {
				t.Fatal(err)
			}
			value, closer, err := db.Get(key)
			if err != nil || !bytes.Contains(value, []byte("must survive refusal")) {
				t.Fatal("failed open ran bank cleanup", err)
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
func TestApplicationGenerationExhaustionAndLegacyReopenAreNonmutating(t *testing.T) {
	s := generationStore(t, vfs.NewMem())
	m := s.meta
	m.Gen.HighWater = math.MaxUint64
	m.Gen.Banks[1].Generation = math.MaxUint64
	if err := s.commit(m, nil); err != nil {
		t.Fatal(err)
	}
	e, err := s.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	manifest, err := e.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	before := readyMetadataFingerprint(t, s)
	if _, err := s.BeginApplicationImport(t.Context(), manifest); !errors.Is(err, ErrLimit) {
		t.Fatal("generation counter wrapped", err)
	}
	if readyMetadataFingerprint(t, s) != before {
		t.Fatal("exhaustion changed rows")
	}
	mem := vfs.NewCrashableMem()
	legacy := transferStore(t, "legacy", mem, 1)
	c := Config{Dir: "legacy", FS: mem.CrashClone(vfs.CrashCloneCfg{}), Application: legacy.meta.App.Policy, Transfer: legacy.meta.Transfer, Generations: ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}}
	if opened, err := Open(c); opened != nil || !errors.Is(err, ErrInvalid) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatal("RLM4 silently acquired generations", err)
	}
	c.Generations = ApplicationGenerationLimits{}
	r, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if readyMetadataFingerprint(t, r) != readyMetadataFingerprint(t, legacy) {
		t.Fatal("failed generation reopen changed legacy rows")
	}
}

func TestApplicationGenerationLegacyBatchesRejectNonzeroBinding(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	persist(t, s, 2, 2, ent(2, 2, "pending"))
	before := readyMetadataFingerprint(t, s)
	if err := s.InstallApplication(2, ApplicationBatch{BaseGeneration: 1, BaseIndex: 1, BaseImageHash: sha256.Sum256([]byte("initial"))}); !errors.Is(err, ErrInvalid) {
		t.Fatal("legacy nonzero generation silently accepted", err)
	}
	if readyMetadataFingerprint(t, s) != before {
		t.Fatal("legacy generation refusal changed keys")
	}
}

func TestApplicationGenerationCleanupRejectsActiveStaleAndPinnedBanks(t *testing.T) {
	s := generationStore(t, vfs.NewMem())
	before := readyMetadataFingerprint(t, s)
	for _, request := range []struct {
		bank       byte
		generation uint64
	}{{0, 1}, {2, 1}, {1, 99}} {
		if err := s.clearGenerationBank(request.bank, request.generation); !errors.Is(err, ErrInvalid) {
			t.Fatal("unsafe cleanup admitted", request, err)
		}
	}
	if _, _, err := decodeBankAppKey(1, nil, 1024); !errors.Is(err, ErrCorrupt) {
		t.Fatal("nil bank key admitted", err)
	}
	if _, _, err := decodeBankAppKey(1, appVersionKey([]byte("wrong bank"), 2), 1024); !errors.Is(err, ErrCorrupt) {
		t.Fatal("bank key mismatch admitted", err)
	}
	if _, err := s.pinGeneration(2); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid bank pin admitted", err)
	}
	if err := s.retireImport(&ApplicationImport{s: s, bank: 0, generation: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatal("active bank retired", err)
	}
	if readyMetadataFingerprint(t, s) != before {
		t.Fatal("unsafe cleanup changed keys")
	}
	e, err := s.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	manifest, err := e.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	i, err := s.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Abort()
	before = readyMetadataFingerprint(t, s)
	if err := s.clearGenerationBank(i.bank, i.generation); !errors.Is(err, ErrLimit) {
		t.Fatal("live import pin ignored", err)
	}
	if err := s.clearGenerationBank(i.bank, i.generation-1); !errors.Is(err, ErrInvalid) {
		t.Fatal("stale cleanup nonce admitted", err)
	}
	if readyMetadataFingerprint(t, s) != before {
		t.Fatal("pinned bank cleanup changed keys")
	}
	m := s.meta
	m.Gen.Limits.MaxBytes = 200
	if err := s.validateGenerationMeta(m); !errors.Is(err, ErrLimit) {
		t.Fatal("two individually fitting banks escaped aggregate cap", err)
	}
	m = s.meta
	m.App.Policy.MaxInstallBytes = 0
	if err := generationCharge(m, 1); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid reservation denominator admitted", err)
	}
}
func TestApplicationGenerationReopensIntermediateRetiredBankSafely(t *testing.T) {
	mem := vfs.NewCrashableMem()
	s := generationStore(t, mem)
	e, err := s.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	manifest, err := e.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	i, err := s.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	err = s.retireImport(i)
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	r, err := Open(Config{Dir: "db", FS: mem.CrashClone(vfs.CrashCloneCfg{}), Application: s.meta.App.Policy, Transfer: s.meta.Transfer, Generations: s.meta.Gen.Limits})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.meta.Gen.Banks[1].State != bankFree || r.meta.Gen.Banks[1].Generation != 2 || r.ApplicationGeneration() != 1 {
		t.Fatal("retired partial bank recovered incorrectly", r.meta.Gen)
	}
	root, err := viewApplication(t, r, 1).Root()
	if err != nil || string(root.Image) != "initial" || root.Generation != 1 {
		t.Fatal("retired cleanup changed active root", root, err)
	}
}
