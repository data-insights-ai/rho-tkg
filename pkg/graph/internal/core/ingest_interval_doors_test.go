package core

import (
	"context"
	"errors"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Door equivalence for the Session interval doors: the ingest Session must run
// the same cascade as the standalone Temporal().Set*VersionInterval, so the
// stored history (row count, per-row valid interval, props, and which rows are
// superseded) is identical whichever door grew the interval. A Session door
// that forwarded to a different operation (a plain update, an in-place row
// rewrite) produces a different history shape and fails here.

type ivRow struct {
	from, to types.Instant
	cnt      int64
}

func ivRowsOfNodes(t *testing.T, hist []*types.Node) []ivRow {
	t.Helper()
	rows := make([]ivRow, 0, len(hist))
	for _, h := range hist {
		v, _ := h.GetProperty("cnt")
		c, _ := v.(int64)
		rows = append(rows, ivRow{h.Temporal().ValidFrom, h.Temporal().ValidTo, c})
	}
	return rows
}

func ivRowsOfRels(t *testing.T, hist []*types.Relationship) []ivRow {
	t.Helper()
	rows := make([]ivRow, 0, len(hist))
	for _, h := range hist {
		v, _ := h.GetProperty("cnt")
		c, _ := v.(int64)
		rows = append(rows, ivRow{h.Temporal().ValidFrom, h.Temporal().ValidTo, c})
	}
	return rows
}

func ivSeed(t *testing.T, g *Core) (*types.Node, *types.Relationship) {
	t.Helper()
	ctx := context.Background()
	n, err := g.Nodes.Add(ctx, []string{"N"}, map[string]any{"tkg_valid_from": types.Instant(1000), "tkg_valid_to": types.Instant(2000), "cnt": int64(1)})
	if err != nil {
		t.Fatalf("Nodes.Add: %v", err)
	}
	m, err := g.Nodes.Add(ctx, []string{"N"}, nil)
	if err != nil {
		t.Fatalf("Nodes.Add: %v", err)
	}
	r, err := g.Rels.Add(ctx, "T", n, m, map[string]any{"tkg_valid_from": types.Instant(1000), "tkg_valid_to": types.Instant(2000), "cnt": int64(1)})
	if err != nil {
		t.Fatalf("Rels.Add: %v", err)
	}
	return n, r
}

func TestSessionIntervalDoors_MatchStandaloneAndBatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	props := map[string]any{"cnt": int64(2)}

	type door struct {
		name string
		do   func(g *Core, n types.NodeID, r types.RelID) error
	}
	session := func(opts IngestOptions) func(g *Core, n types.NodeID, r types.RelID) error {
		return func(g *Core, n types.NodeID, r types.RelID) error {
			s, err := g.Ingest.NewSession(opts)
			if err != nil {
				return err
			}
			if err := s.SetNodeVersionInterval(n, 1000, 5000, props); err != nil {
				return err
			}
			if err := s.SetRelVersionInterval(r, 1000, 5000, props); err != nil {
				return err
			}
			tok, err := s.Submit()
			if err != nil {
				return err
			}
			if err := g.Ingest.WaitApplied(tok); err != nil {
				return err
			}
			return s.Close()
		}
	}
	doors := []door{
		{"standalone", func(g *Core, n types.NodeID, r types.RelID) error {
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, n, 1000, 5000, props); err != nil {
				return err
			}
			_, err := g.Temporal.SetRelVersionInterval(ctx, r, 1000, 5000, props)
			return err
		}},
		{"batch", func(g *Core, n types.NodeID, r types.RelID) error {
			b, err := NewBatchBuilder(g)
			if err != nil {
				return err
			}
			if err := b.SetNodeVersionInterval(n, 1000, 5000, props); err != nil {
				return err
			}
			if err := b.SetRelVersionInterval(r, 1000, 5000, props); err != nil {
				return err
			}
			_, err = b.Execute()
			return err
		}},
		{"session-strong-sync", session(IngestOptions{Sync: true})},
		{"session-strong-async", session(IngestOptions{})},
		{"session-concurrent", session(IngestOptions{Concurrent: true})},
	}

	var wantNode, wantRel []ivRow
	for i, d := range doors {
		g := newIngestGraph(t)
		n, r := ivSeed(t, g)
		if err := d.do(g, n.ID(), r.ID()); err != nil {
			t.Fatalf("%s: %v", d.name, err)
		}
		nh, err := g.Nodes.History(n.ID())
		if err != nil {
			t.Fatalf("%s: node history: %v", d.name, err)
		}
		rh, err := g.Rels.History(r.ID())
		if err != nil {
			t.Fatalf("%s: rel history: %v", d.name, err)
		}
		// Get keeps the head row; the correction rows are appended to History.
		cn, err := g.Nodes.Get(ctx, n.ID())
		if err != nil {
			t.Fatalf("%s: node get: %v", d.name, err)
		}
		cr, err := g.Rels.Get(ctx, r.ID())
		if err != nil {
			t.Fatalf("%s: rel get: %v", d.name, err)
		}
		gotNode := append(ivRowsOfNodes(t, []*types.Node{cn}), ivRowsOfNodes(t, nh)...)
		gotRel := append(ivRowsOfRels(t, []*types.Relationship{cr}), ivRowsOfRels(t, rh)...)
		if len(nh) < 1 || len(rh) < 1 {
			t.Fatalf("%s: history node=%v rel=%v; want an appended correction row", d.name, gotNode, gotRel)
		}
		if i == 0 {
			wantNode, wantRel = gotNode, gotRel
			continue
		}
		if fmt.Sprint(gotNode) != fmt.Sprint(wantNode) {
			t.Errorf("%s node history = %v, standalone = %v", d.name, gotNode, wantNode)
		}
		if fmt.Sprint(gotRel) != fmt.Sprint(wantRel) {
			t.Errorf("%s rel history = %v, standalone = %v", d.name, gotRel, wantRel)
		}
	}
}

// Direct tests of the queue errors at the core layer (sentinels via errors.Is).
func TestSessionSetVersionInterval_QueueErrorsCore(t *testing.T) {
	t.Parallel()
	g := newIngestGraph(t)
	s, err := g.Ingest.NewSession(IngestOptions{Sync: true})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	cases := []struct {
		name     string
		from, to types.Instant
		id       int64
		want     error
	}{
		{"from>=to", 2000, 2000, 7, ErrInvalidTimeRange},
		{"from zero", 0, 2000, 7, ErrInvalidTimeRange},
		{"zero id", 1000, 2000, 0, storepkg.ErrInvalidStoreMutation},
		{"negative id", 1000, 2000, -3, storepkg.ErrInvalidStoreMutation},
	}
	for _, tc := range cases {
		if err := s.SetNodeVersionInterval(types.NodeID(tc.id), tc.from, tc.to, nil); !errors.Is(err, tc.want) {
			t.Errorf("%s: SetNodeVersionInterval = %v, want %v", tc.name, err, tc.want)
		}
		if err := s.SetRelVersionInterval(types.RelID(tc.id), tc.from, tc.to, nil); !errors.Is(err, tc.want) {
			t.Errorf("%s: SetRelVersionInterval = %v, want %v", tc.name, err, tc.want)
		}
	}
	if got := s.Pending(); got != 0 {
		t.Errorf("Pending = %d after refused calls, want 0", got)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.SetNodeVersionInterval(1, 1000, 2000, nil); !errors.Is(err, ErrIngestClosed) {
		t.Errorf("closed SetNodeVersionInterval = %v, want ErrIngestClosed", err)
	}
	if err := s.SetRelVersionInterval(1, 1000, 2000, nil); !errors.Is(err, ErrIngestClosed) {
		t.Errorf("closed SetRelVersionInterval = %v, want ErrIngestClosed", err)
	}
	var nilS *Session
	if err := nilS.SetNodeVersionInterval(1, 1000, 2000, nil); !errors.Is(err, ErrNilSession) {
		t.Errorf("nil SetNodeVersionInterval = %v, want ErrNilSession", err)
	}
	if err := nilS.SetRelVersionInterval(1, 1000, 2000, nil); !errors.Is(err, ErrNilSession) {
		t.Errorf("nil SetRelVersionInterval = %v, want ErrNilSession", err)
	}
}

// A graph closed after the session was opened: the door surfaces ErrGraphClosed
// (queue-time check), and nothing is queued.
func TestSessionSetVersionInterval_GraphClosed(t *testing.T) {
	t.Parallel()
	g := newIngestGraph(t)
	n, r := ivSeed(t, g)
	s, err := g.Ingest.NewSession(IngestOptions{Sync: true})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.SetNodeVersionInterval(n.ID(), 1000, 5000, nil); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("SetNodeVersionInterval after graph close = %v, want ErrGraphClosed", err)
	}
	if err := s.SetRelVersionInterval(r.ID(), 1000, 5000, nil); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("SetRelVersionInterval after graph close = %v, want ErrGraphClosed", err)
	}
	if got := s.Pending(); got != 0 {
		t.Errorf("Pending = %d, want 0", got)
	}
}
