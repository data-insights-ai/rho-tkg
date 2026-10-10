package raftlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestControlIndependentKeyAndFrameVectors(t *testing.T) {
	for _, tc := range []struct {
		bank       byte
		key, frame string
	}{
		{0, "124100ff0000fffffffffffffffd", "415601009490873a568582310112663cd01f89ca2ca26b527175feb83f572468f8276463726567"},
		{1, "134100ff0000fffffffffffffffd", "415601001f471528ef4f5a52bd3061b129f677f4c4f91cbaaed8ee2a76e5b817138681f6726567"},
	} {
		key, _ := hex.DecodeString(tc.key)
		frame, _ := hex.DecodeString(tc.frame)
		got := controlVersionKey(tc.bank, []byte{'A', 0}, 2)
		if !bytes.Equal(got, key) || !bytes.Equal(appFrame(got, []byte("reg"), false), frame) {
			t.Fatalf("bank %d canonical vector changed", tc.bank)
		}
		logical, index, err := decodeControlKey(tc.bank, key, 2)
		if err != nil || !bytes.Equal(logical, []byte{'A', 0}) || index != 2 || len(key)+len(frame) != 53 {
			t.Fatal(logical, index, err)
		}
		for _, bad := range [][]byte{key[:len(key)-1], append(bytes.Clone(key), 0), bytes.Clone(key)} {
			if len(bad) == len(key) {
				bad[0] = 16
			}
			if _, _, err := decodeControlKey(tc.bank, bad, 2); !errors.Is(err, ErrCorrupt) {
				t.Fatal("bad key accepted", bad, err)
			}
		}
	}
}

func TestControlIndependentInstallGrowthAndOneShort(t *testing.T) {
	p := DefaultApplicationPolicy(1)
	b := ApplicationBatch{Image: []byte("seed"), Changes: []byte("c"), Outcome: []byte("o"), ControlPuts: []ApplicationControlPut{{Key: []byte{'A', 0}, Value: []byte("reg")}}}
	for _, delta := range []int{-1, 0, 1} {
		q := p
		q.MaxInstallBytes = 1664 + delta
		u, err := q.PreflightControlBatch(b, DefaultLimits())
		if delta < 0 {
			if !errors.Is(err, ErrLimit) || u != (ApplicationControlBatchUsage{}) {
				t.Fatal(u, err)
			}
			continue
		}
		if err != nil || u.RetainedBytes != 194 || u.RetainedRecords != 4 || u.ControlBytes != 53 || u.ControlRecords != 1 {
			t.Fatal(u, err)
		}
	}
	if b.Image[0] != 's' || b.ControlPuts[0].Value[0] != 'r' {
		t.Fatal("preflight mutated input")
	}
	b.Writes = []KV{{Key: []byte("x"), Value: []byte("y")}}
	p.MaxInstallWrites = 1
	if _, err := p.Preflight(b, DefaultLimits()); !errors.Is(err, ErrLimit) {
		t.Fatal("separate put count evaded joint cap", err)
	}
}

func TestControlOptInFormatsAndLegacyBytes(t *testing.T) {
	s, cfg := newControlStore(t, nil, 1)
	raw, err := encodeMeta(s.meta)
	if err != nil || string(raw[:4]) != "RLM7" || uint64(len(raw)) != metadataBytes(s.meta) {
		t.Fatal(raw, err)
	}
	decoded, err := decodeMeta(raw, s.limits)
	if err != nil || decoded.Controls != s.meta.Controls || decoded.Gen != s.meta.Gen {
		t.Fatal(decoded.Controls, err)
	}
	if cfg.Transfer.Contract.Version != 2 || s.ApplicationControls() != (ApplicationControlConfig{Version: 1}) {
		t.Fatal(cfg)
	}
	for n := 0; n < len(raw); n++ {
		if _, err := decodeMeta(raw[:n], s.limits); !errors.Is(err, ErrCorrupt) {
			t.Fatal(n, err)
		}
	}
	changed := bytes.Clone(raw)
	changed[len(changed)-1] ^= 1
	if _, err := decodeMeta(changed, s.limits); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	legacy := semanticStore(t, cfg.FS, ApplicationSemanticContractID{8}, 2)
	before, _ := encodeMeta(legacy.meta)
	if string(before[:4]) != "RLM6" || legacy.ApplicationControls() != (ApplicationControlConfig{}) {
		t.Fatal("legacy format changed")
	}
	after, _ := encodeMeta(legacy.meta)
	if sha256.Sum256(before) != sha256.Sum256(after) {
		t.Fatal("legacy nondeterminism")
	}
}

func TestControlMetadataRejectsInvalidGraphSubtotalBeforeAggregateArithmetic(t *testing.T) {
	s, _ := newControlStore(t, nil, 1)
	installControl(t, s, "seed", ApplicationControlPut{Key: []byte{'A', 0}, Value: []byte("reg")})
	if s.meta.App.Bytes != 280 || s.meta.App.Records != 6 || s.meta.Controls.Bytes != 53 || s.meta.Controls.Records != 1 {
		t.Fatal("actual independently counted baseline", s.meta.App, s.meta.Controls)
	}
	before, err := encodeMeta(s.meta)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name           string
		bytes, records uint64
	}{
		{"graph-bytes-max", math.MaxUint64, 6},
		{"graph-bytes-max-minus-one", math.MaxUint64 - 1, 6},
		{"graph-records-max", 280, math.MaxUint64},
		{"graph-records-max-minus-one", 280, math.MaxUint64 - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Pure validation of an actual metadata copy isolates ordering;
			// ultimate Open rejection would not prove this validator safe.
			m := s.meta
			m.App.Bytes, m.App.Records = tc.bytes, tc.records
			if err := validateControlMeta(m); !errors.Is(err, ErrLimit) {
				t.Fatal("invalid graph subtotal reached aggregate arithmetic", err)
			}
		})
	}
	after, err := encodeMeta(s.meta)
	if err != nil || !bytes.Equal(before, after) || s.poison != nil {
		t.Fatal("pure validation changed actual store", err)
	}
}

func TestControlLegacyRLM6AG2AP1FrozenLiteralBytes(t *testing.T) {
	// Independently enumerated BE64/proto2 fields, frozen before this first
	// candidate run; this is the unchanged committed legacy semanticStore.
	s := semanticStore(t, vfs.NewMem(), ApplicationSemanticContractID{8}, 2)
	const metadataHex = "524c4d36cbec2531db50baf746f8890a03b64a00b07e037b2b125ba8172e8934e8ba5535000000000000000100000000000000010000000000000001" +
		"000000000000000100000000000000000000000000000000000000000000000700000000000000070000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ac1b5c0961a7269b6a053ee64276ed0e" +
		"20a7f48aefb9f67519539d23aaf10149ac1b5c0961a7269b6a053ee64276ed0e20a7f48aefb9f67519539d23aaf10149000000000000000408011801" +
		"0000000000000006080108020803000000000000000e120c0a0608010802080310011801414d01000000000000000002000000000000040000000000" +
		"001000000000000000010000000000000000100000000000002000000000000000001000000000000040000000000000001000000000000000010000" +
		"00000000000000100000000000100000000000004000000000000000000f424000000000000000400000000001000000000000000000002000000000" +
		"0000008e0000000000000003000000000000000100000000000000004154010001000000000000000000000000000000000000000000000703000000" +
		"000000000000000000000000000000000000000100000000000004000000000000100000000000000001000000000000000010000000000000200000" +
		"000000000000100000000000004000000000000000100000000000000001000000000000000000020000000000001000000000000040000000000000" +
		"4000000000000000000f424000000000c000000041470200000000008000000000000000001e84800000000000000001000000000000000000000000" +
		"000000010000000000000001000000000000008e00000000000000030000000000000000000000000000000000000000000000010000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004150010000000000000f4240" +
		"000000000000000100000000000000000000000000000001000000000000008e00000000000000031b30ed7b19c35bab991793d3bd63f71bd0f05266" +
		"ba996840d97b091c0a1d78b2415202000000000000000001000000000000000200000000000000030000000000000000000000000000000000000000" +
		"0000000000000000000000000800000000000000000000000000000000000000000000000000000000000000"
	want, err := hex.DecodeString(metadataHex)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := encodeMeta(s.meta)
	if err != nil || !bytes.Equal(actual, want) {
		t.Fatalf("legacy metadata bytes changed: len=%d expected=944 SHA=%x expected=f60e616780242f2cda1095c0d97d41a272dd8f277dd071c75e3597bf40bea3c6 err=%v", len(actual), sha256.Sum256(actual), err)
	}
	const generationHex = "41470200000000008000000000000000001e84800000000000000001000000000000000000000000000000010000000000000001000000000000008e" +
		"000000000000000300000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000" +
		"000000000000000000000000000000000000000000000000000000004150010000000000000f42400000000000000001000000000000000000000000" +
		"00000001000000000000008e00000000000000031b30ed7b19c35bab991793d3bd63f71bd0f05266ba996840d97b091c0a1d78b2"
	want, err = hex.DecodeString(generationHex)
	if err != nil || !bytes.Equal(appendGenerationMeta(nil, s.meta.Gen), want) {
		t.Fatal("legacy AG2/AP1 changed", err)
	}
	const cutHex = "1b30ed7b19c35bab991793d3bd63f71bd0f05266ba996840d97b091c0a1d78b2"
	want, err = hex.DecodeString(cutHex)
	if err != nil || !bytes.Equal(s.meta.Gen.Publication.ID[:], want) {
		t.Fatal("legacy cut changed", err)
	}
	decoded, err := decodeMeta(actual, s.limits)
	if err != nil || decoded.Controls != (controlMetadata{}) {
		t.Fatal("legacy decode acquired controls", err)
	}
}

func TestControlPublicPreflightAndContractErrorsReturnZero(t *testing.T) {
	valid := DefaultApplicationPolicy(1)
	for _, tc := range []struct {
		name string
		edit func(*ApplicationPolicy, *ApplicationBatch, *Limits)
		want error
	}{
		{"disabled", func(p *ApplicationPolicy, _ *ApplicationBatch, _ *Limits) { *p = ApplicationPolicy{} }, ErrInvalid},
		{"bad-limits", func(_ *ApplicationPolicy, _ *ApplicationBatch, l *Limits) { *l = Limits{} }, ErrInvalid},
		{"invalid-policy", func(p *ApplicationPolicy, _ *ApplicationBatch, _ *Limits) { p.LocalVoter = 0 }, ErrInvalid},
		{"empty-key", func(_ *ApplicationPolicy, b *ApplicationBatch, _ *Limits) { b.ControlPuts[0].Key = nil }, ErrInvalid},
		{"duplicate", func(_ *ApplicationPolicy, b *ApplicationBatch, _ *Limits) {
			b.ControlPuts = append(b.ControlPuts, b.ControlPuts[0])
		}, ErrInvalid},
		{"key-limit", func(p *ApplicationPolicy, b *ApplicationBatch, _ *Limits) {
			b.ControlPuts[0].Key = make([]byte, p.MaxKeyBytes+1)
		}, ErrLimit},
		{"value-limit", func(p *ApplicationPolicy, b *ApplicationBatch, _ *Limits) {
			b.ControlPuts[0].Value = make([]byte, p.MaxValueBytes+1)
		}, ErrLimit},
		{"joint-write-limit", func(p *ApplicationPolicy, b *ApplicationBatch, _ *Limits) {
			p.MaxInstallWrites = 1
			b.Writes = []KV{{Key: []byte("g")}}
		}, ErrLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, l := valid, DefaultLimits()
			b := ApplicationBatch{Image: []byte("seed"), ControlPuts: []ApplicationControlPut{{Key: []byte("r"), Value: []byte("reg")}}}
			tc.edit(&p, &b, &l)
			if out, err := p.PreflightControlBatch(b, l); out != (ApplicationControlBatchUsage{}) || !errors.Is(err, tc.want) {
				t.Fatal(out, err)
			}
			if out, err := p.Preflight(b, l); out != (ApplicationBatchUsage{}) || !errors.Is(err, tc.want) {
				t.Fatal("aggregate companion diverged", out, err)
			}
		})
	}
	for _, version := range []uint32{0, 1} {
		out, err := ApplicationContractForPolicyWithControls(valid, ApplicationControlConfig{version})
		if err != nil || out.Version != version+1 {
			t.Fatal(out, err)
		}
	}
	for _, p := range []ApplicationPolicy{{}, {LocalVoter: 1}} {
		if out, err := ApplicationContractForPolicyWithControls(p, ApplicationControlConfig{1}); out != (ApplicationContract{}) || !errors.Is(err, ErrInvalid) {
			t.Fatal(out, err)
		}
	}
}

func TestControlMetadataFiniteCapsAndControlShape(t *testing.T) {
	s, _ := newControlStore(t, nil, 1)
	installControl(t, s, "seed", ApplicationControlPut{Key: []byte{'A', 0}, Value: []byte("reg")})
	for _, records := range []bool{false, true} {
		for _, limit := range []uint64{0, 1<<40 + 1} {
			m := s.meta
			if records {
				m.App.Policy.RetainedApplicationRecords = limit
			} else {
				m.App.Policy.RetainedApplicationBytes = limit
			}
			if err := validateControlMeta(m); !errors.Is(err, ErrInvalid) {
				t.Fatal("invalid configuration cap", limit, err)
			}
		}
	}
	m := s.meta
	m.Controls.Records = 2
	if err := validateControlMeta(m); !errors.Is(err, ErrCorrupt) {
		t.Fatal("minimum-frame shape lost", err)
	}
}
