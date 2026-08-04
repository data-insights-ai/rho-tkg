package core

// Fuzz harness for the replica-apply / delta-merge decode path — BACKLOG 12g.
//
// import_fuzz_test.go's FuzzImport already fuzzes the WHOLE import pipeline
// end-to-end; this harness attacks the sibling untrusted-stream boundary
// applyChangeRecordLocked (reached via ReplOps.ApplyChange) never got direct
// fuzz coverage: a primary's change-log feed record (LSN, Tag, Payload) is,
// from a replica's point of view, exactly as untrusted as an import stream —
// it arrives over the wire from a peer and is decoded via
// storeutil.SafeUnmarshal per CLAUDE.md's "decode untrusted msgpack only
// through SafeUnmarshal" rule — but unlike Import, nothing had ever driven
// arbitrary/adversarial (Tag, Payload) pairs through it directly. Same
// contract as lesson 47/FuzzImport: apply of ANY (tag, payload) must NEVER
// panic or crash the process — it returns an error (rejected) or applies
// cleanly.

import (
	"bytes"
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// fuzzApplyChangeSeedRecords captures REAL committed change-log records from
// a change-log-enabled graph exercising a varied mutation sequence (create,
// update x2 for history, delete, label add/remove, Reset for Clear) — giving
// the fuzzer structurally realistic starting points for each easily-reachable
// tag, mirroring fuzzImportSeedStreams' captured-export-stream approach.
func fuzzApplyChangeSeedRecords(tb testing.TB) []storepkg.ChangeRecord {
	tb.Helper()
	ctx := context.Background()

	bs, err := badger.New(badger.Config{InMemory: true, ChangeLog: true})
	if err != nil {
		tb.Fatalf("badger.New: %v", err)
	}
	g, err := New(Config{Store: bs})
	if err != nil {
		tb.Fatalf("New: %v", err)
	}
	defer func() { _ = g.Close() }()

	n1, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		tb.Fatalf("Add n1: %v", err)
	}
	n2, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		tb.Fatalf("Add n2: %v", err)
	}
	rel, err := g.Rels.Add(ctx, "KNOWS", n1, n2, map[string]any{"since": int64(2020)})
	if err != nil {
		tb.Fatalf("Add rel: %v", err)
	}
	if _, err := g.Nodes.Update(ctx, n1.ID(), map[string]any{"name": "Ada2"}); err != nil {
		tb.Fatalf("Update n1 (1): %v", err)
	}
	if _, err := g.Nodes.Update(ctx, n1.ID(), map[string]any{"name": "Ada3"}); err != nil {
		tb.Fatalf("Update n1 (2): %v", err)
	}
	if err := g.Nodes.AddLabel(ctx, n2.ID(), "VIP"); err != nil {
		tb.Fatalf("AddLabel: %v", err)
	}
	if err := g.Nodes.RemoveLabel(ctx, n2.ID(), "VIP"); err != nil {
		tb.Fatalf("RemoveLabel: %v", err)
	}
	if err := g.Rels.Delete(ctx, rel.ID()); err != nil {
		tb.Fatalf("Delete rel: %v", err)
	}
	if err := g.Nodes.Delete(ctx, n2.ID()); err != nil {
		tb.Fatalf("Delete n2: %v", err)
	}

	recs, err := g.Repl.ChangeFeed(0, 0)
	if err != nil {
		tb.Fatalf("ChangeFeed: %v", err)
	}
	return recs
}

func FuzzApplyChange(f *testing.F) {
	for _, rec := range fuzzApplyChangeSeedRecords(f) {
		f.Add(byte(rec.Tag), rec.Payload)
	}
	// Raw adversarial seeds: every valid tag byte with an empty/garbage
	// payload, plus a couple invalid tag bytes — mirrors FuzzImport's
	// truncated/garbage framing seeds.
	for tag := byte(1); tag <= 13; tag++ {
		f.Add(tag, []byte(nil))
		f.Add(tag, []byte{0xc0}) // a single msgpack nil byte
		f.Add(tag, []byte{0xff, 0xff, 0xff, 0xff})
	}
	f.Add(byte(0), []byte(nil))  // invalid tag (0 is reserved/unused)
	f.Add(byte(99), []byte(nil)) // invalid tag (out of range)
	f.Add(byte(255), []byte{0x01})

	f.Fuzz(func(t *testing.T, tag byte, payload []byte) {
		bs, err := badger.New(badger.Config{InMemory: true, ChangeLog: true})
		if err != nil {
			t.Fatalf("badger.New: %v", err)
		}
		g, err := New(Config{Store: bs})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer func() { _ = g.Close() }()

		rec := storepkg.ChangeRecord{LSN: 1, Tag: storepkg.ChangeTag(tag), Payload: append([]byte(nil), payload...)}

		// Must never panic / crash, regardless of how hostile (tag, payload) is.
		if err := g.Repl.ApplyChange(rec); err != nil {
			return // rejected — a correct outcome for corrupt/adversarial input
		}

		// Applied successfully -> the graph must be self-consistent enough to
		// export without error (no read-but-cannot-persist asymmetry, mirroring
		// FuzzImport's post-success invariant).
		var buf bytes.Buffer
		if err := g.IO.Export(&buf); err != nil {
			t.Fatalf("export after a successfully applied change record failed: %v", err)
		}
	})
}
