package core

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	shardedpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 32: a reader at a FIXED pin must find an entity that existed at the
// pin — the same row, at every moment — while a concurrent Update /
// CloseVersion / Delete / SetVersionInterval moves its current row into
// history. The faulty implementation these tests catch: a door that assembles
// the entity's (current ‖ history) chain from two independent store reads and
// observes the move half done (the new current row, the history without the
// moved row), so the pinned row is in neither read: ErrNoVersionValidAt / not
// found / a missing scan member.

// pdrValidAt is the valid instant every pinned door probes: inside every
// tracked entity's genesis interval [1000, ...) before and after every write
// the writers make (closes after the pin, cascades over [2000, 3000), deletes at the
// wall clock).
const pdrValidAt types.Instant = 1500

type pdrBackend struct {
	name string
	open func(t *testing.T) *Core
}

// pdrBackends is every backend: memory, badger in memory and on disk, tiered,
// sharded (badger shards).
func pdrBackends() []pdrBackend {
	newCore := func(t *testing.T, cfg Config) *Core {
		t.Helper()
		g, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = g.Close() })
		useTestClock(t, g)
		return g
	}
	return []pdrBackend{
		{"memory", func(t *testing.T) *Core { return newCore(t, Config{}) }},
		{"badger", func(t *testing.T) *Core { return newCore(t, Config{BadgerInMemory: true}) }},
		{"badger-disk", func(t *testing.T) *Core { return newCore(t, Config{BadgerDir: t.TempDir()}) }},
		{"tiered", func(t *testing.T) *Core {
			ts, err := tiered.New(tiered.Config{
				InMemory:      true,
				RefLabels:     []string{"Ref"},
				ShardWindow:   7 * 24 * time.Hour,
				FlushInterval: 1<<63 - 1,
			})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			t.Cleanup(func() { _ = ts.Close() })
			return newCore(t, Config{Store: ts})
		}},
		{"sharded", func(t *testing.T) *Core {
			st, err := shardedpkg.New(shardedpkg.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatalf("sharded.New: %v", err)
			}
			return newCore(t, Config{Store: st})
		}},
	}
}

// pdrRun runs body per backend × {node, rel}.
func pdrRun(t *testing.T, body func(t *testing.T, e *ccEnt)) {
	t.Helper()
	for _, be := range pdrBackends() {
		for _, rel := range []bool{false, true} {
			kind := "node"
			if rel {
				kind = "rel"
			}
			t.Run(be.name+"/"+kind, func(t *testing.T) {
				body(t, newCCEnt(t, be.open(t), rel))
			})
		}
	}
}

// pdrRow renders the identity of a row: version, payload and the stamps that
// place it in time (TxTo and DeletedAt are left out: a move stamps them on
// the moved row, and a pinned read normalizes them by design).
func pdrRow(version uint32, x any, tm *types.TemporalMetadata) string {
	var vf, vt, tx types.Instant
	if tm != nil {
		vf, vt, tx = tm.ValidFrom, tm.ValidTo, tm.TxFrom
	}
	return fmt.Sprintf("v%d x=%v vf=%d vt=%d tx=%d", version, x, vf, vt, tx)
}

func pdrNodeRow(n *types.Node) string {
	x, _ := n.Properties().Get("x")
	return pdrRow(n.Version(), x, n.Temporal())
}

func pdrRelRow(r *types.Relationship) string {
	x, _ := r.Properties().Get("x")
	return pdrRow(r.Version(), x, r.Temporal())
}

// pdrDoor is one door evaluated over the tracked entities: it renders the
// door's answer as "name=row" entries (sorted), an untracked member as
// "?id=row" (over-reporting shows), and returns an error for any failure
// other than the door's documented absence.
type pdrDoor struct {
	name   string
	pinned bool // exact equality to the pre-write answer (else: every tracked entity present)
	eval   func() (string, error)
}

func pdrRender(e *ccEnt, m map[int64]string) string {
	out := make([]string, 0, len(m))
	for id, row := range m {
		name, ok := e.names[id]
		if !ok {
			name = fmt.Sprintf("?%d", id)
		}
		out = append(out, name+"="+row)
	}
	sort.Strings(out)
	return strings.Join(out, " | ")
}

func pdrPoint(e *ccEnt, one func(id int64) (string, error), absent ...error) (string, error) {
	m := map[int64]string{}
	for _, id := range e.tracked() {
		row, err := one(id)
		if err != nil {
			isAbsent := false
			for _, a := range absent {
				if errors.Is(err, a) {
					isAbsent = true
				}
			}
			if !isAbsent {
				return "", fmt.Errorf("%s: %w", e.names[id], err)
			}
			continue
		}
		m[id] = row
	}
	return pdrRender(e, m), nil
}

func pdrNodeSet(e *ccEnt, ns []*types.Node, err error) (string, error) {
	if err != nil {
		return "", err
	}
	m := map[int64]string{}
	for _, n := range ns {
		m[int64(n.ID())] = pdrNodeRow(n)
	}
	return pdrRender(e, m), nil
}

func pdrRelSet(e *ccEnt, rs []*types.Relationship, err error) (string, error) {
	if err != nil {
		return "", err
	}
	m := map[int64]string{}
	for _, r := range rs {
		m[int64(r.ID())] = pdrRelRow(r)
	}
	return pdrRender(e, m), nil
}

func pdrCount(n int, err error) (string, error) {
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("count=%d", n), nil
}

// pdrDoors is every point and scan door the race can reach, at pin (pinned
// doors) or at the valid instant alone (current-knowledge doors).
func pdrDoors(e *ccEnt, pin types.Instant) []pdrDoor {
	g := e.g
	at := storepkg.QueryOpts{ValidAt: pdrValidAt, TxAt: pin}
	during := storepkg.QueryOpts{ValidStart: 1200, ValidEnd: 1800, TxAt: pin}
	asOf := storepkg.QueryOpts{TxPin: pin}
	now := storepkg.QueryOpts{ValidAt: pdrValidAt}
	contains := types.Contains.Set()
	nodes := func(ns []*types.Node, err error) (string, error) { return pdrNodeSet(e, ns, err) }
	rels := func(rs []*types.Relationship, err error) (string, error) { return pdrRelSet(e, rs, err) }
	if e.rel {
		return []pdrDoor{
			{"RelAtTx", true, func() (string, error) {
				return pdrPoint(e, func(id int64) (string, error) {
					r, err := g.Temporal.RelAtTx(types.RelID(id), pdrValidAt, pin)
					if err != nil {
						return "", err
					}
					return pdrRelRow(r), nil
				})
			}},
			{"RelAsOf", true, func() (string, error) {
				return pdrPoint(e, func(id int64) (string, error) {
					r, err := g.Temporal.RelAsOf(types.RelID(id), pin)
					if err != nil {
						return "", err
					}
					return pdrRelRow(r), nil
				})
			}},
			{"RelsAtTx", true, func() (string, error) { return rels(g.Temporal.RelsAtTx(pdrValidAt, pin)) }},
			{"RelsAsOf", true, func() (string, error) { return rels(g.Temporal.RelsAsOf(pin)) }},
			{"RelsDuringTx", true, func() (string, error) { return rels(g.Temporal.RelsDuringTx(1200, 1800, pin)) }},
			{"ByType{ValidAt,TxAt}", true, func() (string, error) { return rels(g.Rels.ByType(ccType, at)) }},
			{"ByType{ValidStart,ValidEnd,TxAt}", true, func() (string, error) { return rels(g.Rels.ByType(ccType, during)) }},
			{"ByType{TxPin}", true, func() (string, error) { return rels(g.Rels.ByType(ccType, asOf)) }},
			{"Rels.All{TxPin}", true, func() (string, error) { return rels(g.Rels.All(asOf)) }},
			{"CountByTypeAt{ValidAt,TxAt}", true, func() (string, error) { return pdrCount(g.Rels.CountByTypeAt(ccType, at)) }},
			{"CountByTypeAt{TxPin}", true, func() (string, error) { return pdrCount(g.Rels.CountByTypeAt(ccType, asOf)) }},
			{"RelAt", false, func() (string, error) {
				return pdrPoint(e, func(id int64) (string, error) {
					r, err := g.Temporal.RelAt(types.RelID(id), pdrValidAt)
					if err != nil {
						return "", err
					}
					return pdrRelRow(r), nil
				})
			}},
			{"RelsAt", false, func() (string, error) { return rels(g.Temporal.RelsAt(pdrValidAt)) }},
			{"ByType{ValidAt}", false, func() (string, error) { return rels(g.Rels.ByType(ccType, now)) }},
			{"RelsRelating", false, func() (string, error) { return rels(g.Temporal.RelsRelating(1200, 1800, contains)) }},
		}
	}
	return []pdrDoor{
		{"NodeAtTx", true, func() (string, error) {
			return pdrPoint(e, func(id int64) (string, error) {
				n, err := g.Temporal.NodeAtTx(types.NodeID(id), pdrValidAt, pin)
				if err != nil {
					return "", err
				}
				return pdrNodeRow(n), nil
			})
		}},
		{"NodeAsOf", true, func() (string, error) {
			return pdrPoint(e, func(id int64) (string, error) {
				n, err := g.Temporal.NodeAsOf(types.NodeID(id), pin)
				if err != nil {
					return "", err
				}
				return pdrNodeRow(n), nil
			})
		}},
		{"NodesAtTx", true, func() (string, error) { return nodes(g.Temporal.NodesAtTx(pdrValidAt, pin)) }},
		{"NodesAsOf", true, func() (string, error) { return nodes(g.Temporal.NodesAsOf(pin)) }},
		{"NodesDuringTx", true, func() (string, error) { return nodes(g.Temporal.NodesDuringTx(1200, 1800, pin)) }},
		{"ByLabel{ValidAt,TxAt}", true, func() (string, error) { return nodes(g.Nodes.ByLabel(ccLabel, at)) }},
		{"ByLabel{ValidStart,ValidEnd,TxAt}", true, func() (string, error) { return nodes(g.Nodes.ByLabel(ccLabel, during)) }},
		{"ByLabel{TxPin}", true, func() (string, error) { return nodes(g.Nodes.ByLabel(ccLabel, asOf)) }},
		{"Nodes.All{TxPin}", true, func() (string, error) { return nodes(g.Nodes.All(asOf)) }},
		{"CountByLabelAt{ValidAt,TxAt}", true, func() (string, error) { return pdrCount(g.Nodes.CountByLabelAt(ccLabel, at)) }},
		{"CountByLabelAt{TxPin}", true, func() (string, error) { return pdrCount(g.Nodes.CountByLabelAt(ccLabel, asOf)) }},
		{"NodeAt", false, func() (string, error) {
			return pdrPoint(e, func(id int64) (string, error) {
				n, err := g.Temporal.NodeAt(types.NodeID(id), pdrValidAt)
				if err != nil {
					return "", err
				}
				return pdrNodeRow(n), nil
			})
		}},
		{"NodesAt", false, func() (string, error) { return nodes(g.Temporal.NodesAt(pdrValidAt)) }},
		{"ByLabel{ValidAt}", false, func() (string, error) { return nodes(g.Nodes.ByLabel(ccLabel, now)) }},
		{"NodesRelating", false, func() (string, error) { return nodes(g.Temporal.NodesRelating(1200, 1800, contains)) }},
	}
}

// pdrPresent reports whether every tracked entity is a member of a rendered
// set (the current-knowledge doors: a write may legitimately change the row,
// never the membership, at pdrValidAt).
func pdrPresent(e *ccEnt, rendered string) bool {
	for _, id := range e.tracked() {
		if !strings.Contains(" | "+rendered+" | ", " | "+e.names[id]+"=") {
			return false
		}
	}
	return true
}

// pdrSetup creates n tracked entities valid from 1000; every second one gets
// an Update first so its pinned row is a history row, not the current one.
func pdrSetup(t *testing.T, e *ccEnt, n int) []int64 {
	t.Helper()
	ids := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		id := e.add(fmt.Sprintf("E%02d", i), 1000, nil)
		if i%2 == 1 {
			e.mustUpdate(id, map[string]any{"x": int64(-1)})
		}
		ids = append(ids, id)
	}
	return ids
}

// pdrFresh creates an untracked entity after the pin (it must never appear in
// a pinned door) — the writers' re-create step so the loop can continue.
func pdrFresh(e *ccEnt) (int64, error) {
	props := map[string]any{"tkg_valid_from": types.Instant(1000), "x": int64(7)}
	if e.rel {
		r, err := e.g.Rels.AddByID(e.ctx, ccType, e.start, e.end, props)
		if err != nil {
			return 0, err
		}
		return int64(r.ID()), nil
	}
	n, err := e.g.Nodes.Add(e.ctx, []string{ccLabel}, props)
	if err != nil {
		return 0, err
	}
	return int64(n.ID()), nil
}

// pdrWrite runs writer w's share of the schedule: round 0 moves each owned
// entity's current row by op (i % 5): Update, CloseVersion, Delete,
// SetVersionInterval cascade, Update then Delete; later rounds Update the
// open survivors, Delete the closed ones and re-create, churn and delete a
// fresh entity for every deleted one.
func pdrWrite(e *ccEnt, ids []int64, closeAt types.Instant, w, writers, rounds int) error {
	deleted := map[int64]bool{}
	closed := map[int64]bool{}
	for round := 0; round < rounds; round++ {
		for i := w; i < len(ids); i += writers {
			id := ids[i]
			if deleted[id] {
				fresh, err := pdrFresh(e)
				if err != nil {
					return fmt.Errorf("fresh: %w", err)
				}
				if err := e.update(fresh, map[string]any{"x": int64(round)}); err != nil {
					return fmt.Errorf("fresh update: %w", err)
				}
				if err := e.del(fresh); err != nil {
					return fmt.Errorf("fresh delete: %w", err)
				}
				continue
			}
			op := i % 5
			switch {
			case round > 0 && closed[id]:
				op = 2 // a closed entity takes no update: delete it
			case round > 0:
				op = 0
			}
			var err error
			switch op {
			case 0:
				err = e.update(id, map[string]any{"x": int64(100*round + i)})
			case 1:
				err = e.closeAt(id, closeAt)
				closed[id] = true
			case 2:
				err = e.del(id)
				deleted[id] = true
			case 3:
				err = e.cascade(id, 2000, 3000, map[string]any{"x": int64(1000 + i)})
			case 4:
				if err = e.update(id, map[string]any{"x": int64(2000 + i)}); err == nil {
					err = e.del(id)
					deleted[id] = true
				}
			}
			if err != nil {
				return fmt.Errorf("%s op %d round %d: %w", e.names[id], op, round, err)
			}
		}
	}
	return nil
}

// pdrMisses counts per door how often a reader saw a pinned door answer differ
// from its pre-write answer (or a current-knowledge door drop a tracked
// entity), with the first sample kept.
type pdrMisses struct {
	mu     sync.Mutex
	count  map[string]int
	sample map[string]string
}

func (m *pdrMisses) add(door, msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.count == nil {
		m.count = map[string]int{}
		m.sample = map[string]string{}
	}
	m.count[door]++
	if _, ok := m.sample[door]; !ok {
		m.sample[door] = msg
	}
}

// TestPointDoorRace_UnderMovingWriters is the backlog-32 stress test: writer
// goroutines move the tracked entities' current rows (Update, CloseVersion,
// Delete, SetVersionInterval cascade, re-create) while reader goroutines call
// every point and scan door at a pin taken before the writers started. Zero
// misses allowed; the per-door counts are logged ("pdr-miss") for the table.
func TestPointDoorRace_UnderMovingWriters(t *testing.T) {
	const (
		entities = 20
		writers  = 4
		readers  = 4
		rounds   = 3
	)
	pdrRun(t, func(t *testing.T, e *ccEnt) {
		ids := pdrSetup(t, e, entities)
		pin := e.pin()
		doors := pdrDoors(e, pin)
		want := make(map[string]string, len(doors))
		for _, d := range doors {
			got, err := d.eval()
			if err != nil {
				t.Fatalf("%s before the writers: %v", d.name, err)
			}
			if !d.pinned && !pdrPresent(e, got) {
				t.Fatalf("%s before the writers misses a tracked entity: %s", d.name, got)
			}
			want[d.name] = got
		}

		var misses pdrMisses
		var writing atomic.Bool
		writing.Store(true)
		var readersWG, writersWG sync.WaitGroup
		for r := 0; r < readers; r++ {
			readersWG.Add(1)
			go func() {
				defer readersWG.Done()
				for writing.Load() {
					for _, d := range doors {
						got, err := d.eval()
						switch {
						case err != nil:
							misses.add(d.name, "error: "+err.Error())
						case d.pinned && got != want[d.name]:
							misses.add(d.name, "got  "+got+"\nwant "+want[d.name])
						case !d.pinned && !pdrPresent(e, got):
							misses.add(d.name, "dropped a tracked entity: "+got)
						}
					}
				}
			}()
		}
		werrs := make(chan error, writers)
		for w := 0; w < writers; w++ {
			writersWG.Add(1)
			go func(w int) {
				defer writersWG.Done()
				werrs <- pdrWrite(e, ids, pin+1_000_000, w, writers, rounds)
			}(w)
		}
		writersWG.Wait()
		writing.Store(false)
		readersWG.Wait()
		close(werrs)
		for err := range werrs {
			if err != nil {
				t.Fatalf("writer: %v", err)
			}
		}

		// Quiescent: the pinned answers are unchanged by the writes (a
		// semantic guard, not the race), the current-knowledge doors still
		// hold every tracked entity.
		for _, d := range doors {
			got, err := d.eval()
			if err != nil {
				t.Fatalf("%s after the writers: %v", d.name, err)
			}
			if d.pinned && got != want[d.name] {
				t.Fatalf("%s after the writers (quiescent):\ngot  %s\nwant %s", d.name, got, want[d.name])
			}
			if !d.pinned && !pdrPresent(e, got) {
				t.Fatalf("%s after the writers (quiescent) misses a tracked entity: %s", d.name, got)
			}
		}

		names := make([]string, 0, len(misses.count))
		for name := range misses.count {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			t.Logf("pdr-miss door=%s count=%d", name, misses.count[name])
			t.Errorf("%s: %d misses under concurrent writers; first:\n%s", name, misses.count[name], misses.sample[name])
		}
	})
}
