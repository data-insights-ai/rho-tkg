package graph_test

// Backlog 73, review round: the fixes the Opus review of 75f86596 asked for,
// each a red test first.

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	constraintspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestUntimedUnboundedEndpointConstraint (review 1): an untimed relationship
// under UntimedUnbounded starts at -inf; an endpoint whose FIRST version is
// unbounded is valid there too, also after it was updated (its current row
// then starts at the update's TxFrom). Every relationship create door must
// accept it. Break case: an endpoint whose first version is timed still
// refuses the -inf relationship.
func TestUntimedUnboundedEndpointConstraint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backends := allStoreBackendsWith(func(c *graphpkg.Config) { c.UntimedValidStart = graphpkg.UntimedUnbounded })
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			t.Parallel()
			g := b.open(t)
			if err := g.Constraints().Add(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}); err != nil {
				t.Fatal(err)
			}
			a, err := g.Nodes().Add(ctx, []string{"N"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			c, err := g.Nodes().Add(ctx, []string{"N"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"v": int64(1)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(ctx, c.ID(), map[string]any{"v": int64(1)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Add(ctx, "R", a, c, nil); err != nil {
				t.Errorf("Rels().Add untimed between updated unbounded endpoints: %v", err)
			}
			bb, err := g.Batch().New()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := bb.AddRelationship("R", c, a, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := bb.Execute(); err != nil {
				t.Errorf("batch AddRelationship: %v", err)
			}
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				_, err := tx.AddRelationship("R", a, c, nil)
				return err
			}); err != nil {
				t.Errorf("tx AddRelationship: %v", err)
			}
			for _, conc := range []bool{false, true} {
				s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true, Concurrent: conc})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.AddRelationship("R", a, c, nil); err != nil {
					t.Errorf("ingest AddRelationship (concurrent=%v): %v", conc, err)
				}
				if err := s.Close(); err != nil {
					t.Errorf("ingest close (concurrent=%v): %v", conc, err)
				}
			}
			viol, err := g.Constraints().DryRunValidate(ctx, constraintspkg.DryRunFacts{Rels: []constraintspkg.DryRunRel{
				{Ref: "untimed", TypeName: "R", StartID: a.ID(), EndID: c.ID()},
			}})
			if err != nil || len(viol) != 0 {
				t.Errorf("DryRunValidate untimed rel: %+v (%v), want no violation", viol, err)
			}

			// Break case: a first version that is timed bounds the -inf relationship.
			timed, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{"tkg_valid_from": types.Instant(1_000_000)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Add(ctx, "R", timed, c, nil); !errors.Is(err, temporalpkg.ErrRelBeforeStartNode) {
				t.Errorf("untimed rel from a timed endpoint: err = %v, want ErrRelBeforeStartNode", err)
			}
		})
	}
}

// TestResetKeepsBitemporalMigrationDone (review 3): Admin().Reset reaped the
// one-shot bitemporal migration's schema_version, so the next open ran the
// migration over rows written after the Reset and cleared the start a close
// row copies from its version (equal starts read as an inherited legacy
// ValidFrom): an unbounded closed first version, and a timed one, lost their
// start. Both modes, badger on disk.
func TestResetKeepsBitemporalMigrationDone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, mode := range []graphpkg.UntimedValidStart{graphpkg.UntimedFromTxFrom, graphpkg.UntimedUnbounded} {
		t.Run(uvsModeName(mode), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			open := func() *graphpkg.Graph {
				t.Helper()
				g, err := graphpkg.New(graphpkg.Config{BadgerDir: dir, AllowReset: true, UntimedValidStart: mode})
				if err != nil {
					t.Fatal(err)
				}
				return g
			}
			g := open()
			if _, err := g.Nodes().Add(ctx, []string{"N"}, nil); err != nil {
				t.Fatal(err)
			}
			if err := g.Admin().Reset(); err != nil {
				t.Fatal(err)
			}
			untimed, err := g.Nodes().Add(ctx, []string{"N"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			timed, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{"tkg_valid_from": types.Instant(100_000)})
			if err != nil {
				t.Fatal(err)
			}
			end := untimed.Temporal().TxFrom + 3_600_000
			for _, id := range []types.NodeID{untimed.ID(), timed.ID()} {
				if _, err := g.Nodes().Update(ctx, id, map[string]any{"tkg_valid_to": end}); err != nil {
					t.Fatal(err)
				}
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g = open()
			defer g.Close()
			// The close row (version 1) answers over its version's whole span;
			// with its start cleared, version 0 answers before the close's TxFrom.
			if n, err := g.Temporal().NodeAt(timed.ID(), 150_000); err != nil || n.Version() != 1 {
				t.Errorf("closed timed version at 150000 after Reset + reopen: %v (%v), want the close row (version 1)", n, err)
			}
			n, err := g.Temporal().NodeAt(untimed.ID(), uvsFar)
			if mode == graphpkg.UntimedUnbounded && (err != nil || n.Version() != 1) {
				t.Errorf("closed unbounded first version at far after Reset + reopen: %v (%v), want the close row (version 1)", n, err)
			}
			if mode == graphpkg.UntimedFromTxFrom && err == nil {
				t.Errorf("closed untimed TxFrom version valid at far after Reset + reopen")
			}
		})
	}
}

// TestUntimedConversionCrashSafe (review 2): the conversion wrote its final
// mode key durably while the rewritten rows still sat in the store's write
// buffer, so a crash right after the call left a store that claims
// UntimedUnbounded but holds first versions still starting at TxFrom. The
// crash is played by copying the live directory right after the call (the
// write buffer is not on disk; FlushInterval holds it). The copy must open
// under UntimedUnbounded with every converted row in place, or refuse a
// TxFrom open as an unfinished conversion — never a silent mix.
func TestUntimedConversionCrashSafe(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	hold := time.Hour
	open := map[string]func(dir string) (storepkg.MandatoryStore, error){
		"badger": func(dir string) (storepkg.MandatoryStore, error) {
			return badger.New(badger.Config{Dir: dir, FlushInterval: hold})
		},
		"sharded": func(dir string) (storepkg.MandatoryStore, error) {
			return sharded.New(sharded.Config{Dir: dir, BaseSlot: 0, SlotCount: 2, FlushInterval: hold})
		},
		// Tiered keeps the mode key on the reference shard and these rows on
		// an event shard: the key's own flush does not cover them.
		"tiered": func(dir string) (storepkg.MandatoryStore, error) {
			return tiered.New(tiered.Config{DataDir: dir, ShardWindow: 7 * 24 * time.Hour, RefLabels: []string{"Ref"}, FlushInterval: hold})
		},
	}
	for _, name := range []string{"badger", "sharded", "tiered"} {
		openStore := open[name]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			newGraph := func(dir string, mode graphpkg.UntimedValidStart) *graphpkg.Graph {
				t.Helper()
				st, err := openStore(dir)
				if err != nil {
					t.Fatal(err)
				}
				g, err := graphpkg.New(graphpkg.Config{Store: st, UntimedValidStart: mode})
				if err != nil {
					_ = st.Close()
					t.Fatal(err)
				}
				return g
			}
			dir := t.TempDir()
			g := newGraph(dir, graphpkg.UntimedFromTxFrom)
			var ids []types.NodeID
			for i := 0; i < 8; i++ {
				n, err := g.Nodes().Add(ctx, []string{"U"}, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, n.ID())
			}
			if err := g.Close(); err != nil { // the rows are on disk
				t.Fatal(err)
			}
			g = newGraph(dir, graphpkg.UntimedFromTxFrom)
			if err := g.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedUnbounded); err != nil {
				t.Fatal(err)
			}
			crashed := t.TempDir()
			uvsCopyDir(t, dir, crashed)
			_ = g.Close()

			st, err := openStore(crashed)
			if err != nil {
				t.Fatal(err)
			}
			cg, err := graphpkg.New(graphpkg.Config{Store: st, UntimedValidStart: graphpkg.UntimedUnbounded})
			if err != nil {
				_ = st.Close()
				t.Fatalf("crash copy opened unbounded: %v", err)
			}
			defer cg.Close()
			for _, id := range ids {
				if _, err := cg.Temporal().NodeAt(id, uvsFar); err != nil {
					t.Errorf("crash copy: node %d not valid at far (%v): the store claims UntimedUnbounded with an unconverted row", id, err)
				}
			}
		})
	}
}

// uvsCopyDir copies src to dst recursively (a crash image of a live store).
func uvsCopyDir(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o750)
		}
		data, err := os.ReadFile(p) // #nosec G304 -- test-owned temp directory
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o600)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestUntimedModeReplicates (review 5): the mode key is MetaKV, which the
// change feed did not carry, so a replica of a converted primary kept
// TxFrom as its persisted mode. The primary's conversion now emits ChangeMeta
// records; the replica applies and persists them: it reopens under
// UntimedUnbounded, refuses TxFrom, and answers the converted rows.
func TestUntimedModeReplicates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pdir, rdir := t.TempDir(), t.TempDir()
	p, err := graphpkg.New(graphpkg.Config{BadgerDir: pdir, ChangeLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	n, err := p.Nodes().Add(ctx, []string{"U"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var snap bytes.Buffer
	if err := p.IO().Export(&snap); err != nil {
		t.Fatal(err)
	}
	lsn0, err := p.Replication().LastCommittedLSN()
	if err != nil {
		t.Fatal(err)
	}
	openReplica := func(mode graphpkg.UntimedValidStart) (*graphpkg.Graph, error) {
		return graphpkg.New(graphpkg.Config{BadgerDir: rdir, SnowflakeNodeID: 6, ReadOnlyReplica: true, UntimedValidStart: mode})
	}
	r, err := openReplica(graphpkg.UntimedFromTxFrom)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.IO().Import(bytes.NewReader(snap.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := r.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatal(err)
	}

	if err := p.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedUnbounded); err != nil {
		t.Fatal(err)
	}
	var recs []storepkg.ChangeRecord
	if err := p.Replication().ForEachChange(lsn0, func(rec storepkg.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("replica applies the conversion: %v", err)
	}
	if _, err := r.Temporal().NodeAt(n.ID(), uvsFar); err != nil {
		t.Errorf("replica: converted node at far: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openReplica(graphpkg.UntimedFromTxFrom); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
		t.Fatalf("replica reopened txfrom after the primary converted: err = %v, want ErrUntimedModeMismatch", err)
	}
	r, err = openReplica(graphpkg.UntimedUnbounded)
	if err != nil {
		t.Fatalf("replica reopened unbounded: %v", err)
	}
	defer r.Close()
	if _, err := r.Temporal().NodeAt(n.ID(), uvsFar); err != nil {
		t.Errorf("replica reopened: converted node at far: %v", err)
	}
}

// TestImportCarriesUntimedMode (review 4): an export did not say which mode
// its untimed rows were written under, so an import mixed the rules silently.
// The export header now carries it: an empty target adopts it (and persists
// it), a target with rows under the other mode refuses the import with
// ErrUntimedModeMismatch and holds nothing of it.
func TestImportCarriesUntimedMode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	export := func(mode graphpkg.UntimedValidStart) ([]byte, types.NodeID) {
		t.Helper()
		g, err := graphpkg.New(graphpkg.Config{UntimedValidStart: mode})
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
		n, err := g.Nodes().Add(ctx, []string{"U"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		if err := g.IO().Export(&buf); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes(), n.ID()
	}
	unbExport, unbID := export(graphpkg.UntimedUnbounded)
	txExport, _ := export(graphpkg.UntimedFromTxFrom)

	// An empty TxFrom-configured badger store adopts the export's mode.
	dir := t.TempDir()
	g, err := graphpkg.New(graphpkg.Config{BadgerDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.IO().Import(bytes.NewReader(unbExport), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("import an unbounded export into an empty store: %v", err)
	}
	if _, err := g.Temporal().NodeAt(unbID, uvsFar); err != nil {
		t.Errorf("imported unbounded row at far: %v", err)
	}
	created, err := g.Nodes().Add(ctx, []string{"U"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if vf := created.Temporal().ValidFrom; vf != types.UnboundedValidFrom {
		t.Errorf("create after adopting the unbounded export: ValidFrom = %d, want UnboundedValidFrom", vf)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := graphpkg.New(graphpkg.Config{BadgerDir: dir}); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
		t.Fatalf("reopen txfrom after adopting an unbounded export: err = %v, want ErrUntimedModeMismatch", err)
	}

	// A target with rows under the other mode refuses, both directions.
	cases := []uvsImportCase{
		{"unbounded-into-txfrom", graphpkg.UntimedFromTxFrom, unbExport},
		{"txfrom-into-unbounded", graphpkg.UntimedUnbounded, txExport},
	}
	for _, c := range cases {
		tg, err := graphpkg.New(graphpkg.Config{UntimedValidStart: c.target})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tg.Nodes().Add(ctx, []string{"Local"}, nil); err != nil {
			t.Fatal(err)
		}
		if err := tg.IO().Import(bytes.NewReader(c.data), tkgio.ImportOptions{}); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
			t.Errorf("%s: import err = %v, want ErrUntimedModeMismatch", c.name, err)
		}
		if ns, err := tg.Nodes().ByLabel("U", storepkg.QueryOpts{}); err != nil || len(ns) != 0 {
			t.Errorf("%s: refused import left %d rows (%v)", c.name, len(ns), err)
		}
		_ = tg.Close()
	}
}

type uvsImportCase struct {
	name   string
	target graphpkg.UntimedValidStart
	data   []byte
}

// TestImportMergeRefusesOtherUntimedMode (review 4, the delta door): a delta
// whose source writes under the other mode than the merge target is refused
// with ErrUntimedModeMismatch (a source converted after the base export: the
// target re-bootstraps from a full export); a delta of the same mode merges.
func TestImportMergeRefusesOtherUntimedMode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	src, err := graphpkg.New(graphpkg.Config{BadgerInMemory: true, ChangeLog: true, UntimedValidStart: graphpkg.UntimedUnbounded})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if _, err := src.Nodes().Add(ctx, []string{"U"}, nil); err != nil {
		t.Fatal(err)
	}
	var full bytes.Buffer
	if err := src.IO().Export(&full); err != nil {
		t.Fatal(err)
	}
	hdr, err := tkgio.HeaderOf(bytes.NewReader(full.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	later, err := src.Nodes().Add(ctx, []string{"U"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var delta bytes.Buffer
	if err := src.IO().ExportSince(&delta, hdr.To); err != nil {
		t.Fatal(err)
	}

	same, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer same.Close()
	if err := same.IO().Import(bytes.NewReader(full.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := same.IO().ImportMerge(bytes.NewReader(delta.Bytes()), tkgio.MergeOptions{}); err != nil {
		t.Fatalf("merge a delta of the adopted mode: %v", err)
	}
	if _, err := same.Temporal().NodeAt(later.ID(), uvsFar); err != nil {
		t.Errorf("merged unbounded row at far: %v", err)
	}

	other, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Nodes().Add(ctx, []string{"U"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := other.IO().ImportMerge(bytes.NewReader(delta.Bytes()), tkgio.MergeOptions{}); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
		t.Fatalf("merge an unbounded delta into a TxFrom graph: err = %v, want ErrUntimedModeMismatch", err)
	}
}

// TestUntimedConversionStampsForeignIncomingStubs (review, cross-machine
// edges): a sharded END machine holds a Model-A incoming stub of an edge whose
// authoritative row lives on the START machine; the stub carries that row's
// temporal block verbatim. Converting the END machine must stamp a stub of an
// untimed first version like the START machine's conversion stamps the edge,
// or the END machine's incoming reads start the edge at its TxFrom while the
// START machine reads it from -inf. A stub of a timed edge is kept.
func TestUntimedConversionStampsForeignIncomingStubs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, ChangeLog: true})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0, Store: st})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	end, err := g.Nodes().Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	x, err := g.Nodes().Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels().AddByID(ctx, "KNOWS", x.ID(), end.ID(), nil); err != nil {
		t.Fatal(err)
	}
	untimed := storepkg.ForeignIncomingEdge{
		RelID: types.RelID(snowflake.ID(700003)), TypeName: "KNOWS",
		StartID: types.NodeID(snowflake.ID(700001)), EndID: end.ID(),
		FromHash: "aa11", ToHash: "bb22", TxFrom: 1234, Version: 0, AttestTx: 1,
	}
	timed := untimed
	timed.RelID = types.RelID(snowflake.ID(700005))
	timed.ValidFrom = 500
	// A replica of the END machine, bootstrapped before the conversion.
	var snap bytes.Buffer
	if err := g.IO().Export(&snap); err != nil {
		t.Fatal(err)
	}
	lsn0, err := g.Replication().LastCommittedLSN()
	if err != nil {
		t.Fatal(err)
	}
	repStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	rep, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 6, Store: repStore, ReadOnlyReplica: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rep.Close()
	if err := rep.IO().Import(bytes.NewReader(snap.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := rep.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatal(err)
	}
	for _, e := range []storepkg.ForeignIncomingEdge{untimed, timed} {
		if err := g.Rels().RecordForeignIncoming(ctx, e); err != nil {
			t.Fatal(err)
		}
	}

	if err := g.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedUnbounded); err != nil {
		t.Fatal(err)
	}
	var recs []storepkg.ChangeRecord
	if err := g.Replication().ForEachChange(lsn0, func(rec storepkg.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := rep.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("replica applies the conversion: %v", err)
	}
	rin, err := rep.Rels().Incoming(end.ID(), "KNOWS")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rin {
		if r.ID() == untimed.RelID && r.Temporal().ValidFrom != types.UnboundedValidFrom {
			t.Errorf("replica: untimed stub ValidFrom = %d, want UnboundedValidFrom", r.Temporal().ValidFrom)
		}
	}
	in, err := g.Rels().Incoming(end.ID(), "KNOWS")
	if err != nil {
		t.Fatal(err)
	}
	got := map[types.RelID]types.Instant{}
	for _, r := range in {
		got[r.ID()] = r.Temporal().ValidFrom
	}
	if vf, ok := got[untimed.RelID]; !ok || vf != types.UnboundedValidFrom {
		t.Errorf("untimed stub after conversion: ValidFrom = %d (present %v), want UnboundedValidFrom", vf, ok)
	}
	if vf := got[timed.RelID]; vf != 500 {
		t.Errorf("timed stub after conversion: ValidFrom = %d, want 500 kept", vf)
	}
}
