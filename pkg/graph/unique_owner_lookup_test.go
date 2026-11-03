package graph_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// UNIQUELOOK (ai-soc, 3 Nov): a unique key's GetOrCreateByKey and create-time
// check on a plain badger store, and on a tiered reference label, read the
// owner index (one claim, one row) instead of seeking the on-disk property
// index, 2-3 seeks per edge in ai-soc's admission profile.

type uniqLookStore struct {
	name string
	open func(t *testing.T, dir string) *graphpkg.Graph
}

func uniqLookStores() []uniqLookStore {
	return []uniqLookStore{
		{"badger", func(t *testing.T, dir string) *graphpkg.Graph {
			t.Helper()
			bs, err := badger.New(badger.Config{Dir: dir, PropertyIndexOnDisk: true})
			if err != nil {
				t.Fatal(err)
			}
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 12, Store: bs})
			if err != nil {
				t.Fatal(err)
			}
			return g
		}},
		{"tiered-reference", func(t *testing.T, dir string) *graphpkg.Graph {
			t.Helper()
			ts, err := tiered.New(tiered.Config{DataDir: dir, RefLabels: []string{"Host"}, ShardWindow: 7 * 24 * time.Hour,
				PropertyIndexOnDisk: true})
			if err != nil {
				t.Fatal(err)
			}
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 12, Store: ts})
			if err != nil {
				t.Fatal(err)
			}
			return g
		}},
	}
}

// TestUniqueLookNoPropertyIndexSeeks: 1,000 GetOrCreateByKey calls on a
// unique key, half hits and half misses, at the default depth and at
// DepthHot, seek the on-disk property index zero times, and each answers
// right (a hit returns the holder, a miss creates once). Not parallel: the
// seek counter is the process's.
func TestUniqueLookNoPropertyIndexSeeks(t *testing.T) {
	ctx := context.Background()
	for _, st := range uniqLookStores() {
		t.Run(st.name, func(t *testing.T) {
			g := st.open(t, t.TempDir())
			defer g.Close()
			if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
				t.Fatal(err)
			}
			ids := map[string]types.NodeID{}
			for i := 0; i < 250; i++ {
				k := fmt.Sprintf("pre-%d", i)
				n, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": k})
				if err != nil {
					t.Fatal(err)
				}
				ids[k] = n.ID()
			}
			before := badger.PropertyDiskLookupsForTest()
			for i := 0; i < 1000; i++ {
				opts := storepkg.QueryOpts{}
				if i%2 == 1 {
					opts.Depth = storepkg.DepthHot
				}
				hit := i%4 < 2
				k := fmt.Sprintf("new-%d", i)
				if hit {
					k = fmt.Sprintf("pre-%d", i%250)
				}
				n, created, err := g.Nodes().GetOrCreateByKeyOpts(ctx, "Host", "key", k, nil, opts)
				if err != nil {
					t.Fatal(err)
				}
				if hit && (created || n.ID() != ids[k]) {
					t.Fatalf("hit %s: created %v id %d, want holder %d", k, created, n.ID(), ids[k])
				}
				if !hit && !created {
					t.Fatalf("miss %s: not created", k)
				}
			}
			if got := badger.PropertyDiskLookupsForTest() - before; got != 0 {
				t.Errorf("on-disk property index lookups for 1,000 GetOrCreateByKey = %d, want 0 (owner index)", got)
			}
		})
	}
}

// TestUniqueLookConcurrentOneValue: 16 goroutines GetOrCreateByKey one value
// on a ready owner index: exactly one creates, all return its node.
func TestUniqueLookConcurrentOneValue(t *testing.T) {
	ctx := context.Background()
	for _, st := range uniqLookStores() {
		t.Run(st.name, func(t *testing.T) {
			g := st.open(t, t.TempDir())
			defer g.Close()
			if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
				t.Fatal(err)
			}
			for round := 0; round < 20; round++ {
				k := fmt.Sprintf("v%d", round)
				var wg sync.WaitGroup
				var mu sync.Mutex
				created := 0
				got := map[types.NodeID]bool{}
				for w := 0; w < 16; w++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						n, c, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", k, nil)
						if err != nil {
							t.Error(err)
							return
						}
						mu.Lock()
						defer mu.Unlock()
						if c {
							created++
						}
						got[n.ID()] = true
					}()
				}
				wg.Wait()
				if created != 1 || len(got) != 1 {
					t.Fatalf("%s: created %d nodes, returned %d IDs; want 1 and 1", k, created, len(got))
				}
				rows, err := g.Nodes().ByLabelAndProperty("Host", "key", k, storepkg.QueryOpts{})
				if err != nil || len(rows) != 1 {
					t.Fatalf("%s: %d holders (%v), want 1", k, len(rows), err)
				}
			}
		})
	}
}

// TestUniqueLookValueChangeAndDelete: a claim whose row moved to another
// value, or was deleted, is ignored, and the old value is claimable again; a
// second holder is refused. Then the same over a reopen.
func TestUniqueLookValueChangeAndDelete(t *testing.T) {
	ctx := context.Background()
	for _, st := range uniqLookStores() {
		t.Run(st.name, func(t *testing.T) {
			dir := t.TempDir()
			g := st.open(t, dir)
			if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
				t.Fatal(err)
			}
			a, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": "a"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"key": "b"}); err != nil {
				t.Fatal(err)
			}
			n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", "a", nil)
			if err != nil || !created || n.ID() == a.ID() {
				t.Fatalf("GetOrCreate(a) after a->b: id %d created %v err %v; want a new node", n.ID(), created, err)
			}
			na := n.ID()
			n, created, err = g.Nodes().GetOrCreateByKey(ctx, "Host", "key", "b", nil)
			if err != nil || created || n.ID() != a.ID() {
				t.Fatalf("GetOrCreate(b): id %d created %v err %v; want %d", n.ID(), created, err, a.ID())
			}
			if _, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": "b"}); err == nil {
				t.Fatal("a second holder of b was admitted")
			}
			if err := g.Nodes().Delete(ctx, a.ID()); err != nil {
				t.Fatal(err)
			}
			n, created, err = g.Nodes().GetOrCreateByKey(ctx, "Host", "key", "b", nil)
			if err != nil || !created || n.ID() == a.ID() {
				t.Fatalf("GetOrCreate(b) after delete: id %d created %v err %v; want a new node", n.ID(), created, err)
			}
			nb := n.ID()
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g = st.open(t, dir)
			defer g.Close()
			before := badger.PropertyDiskLookupsForTest()
			for k, want := range map[string]types.NodeID{"a": na, "b": nb} {
				n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", k, nil)
				if err != nil || created || n.ID() != want {
					t.Fatalf("reopen GetOrCreate(%s): id %d created %v err %v; want %d", k, n.ID(), created, err, want)
				}
			}
			if got := badger.PropertyDiskLookupsForTest() - before; got != 0 {
				t.Errorf("property index lookups after reopen = %d, want 0", got)
			}
		})
	}
}

// TestUniqueLookSeedsExistingRows: CreateUnique over rows that exist seeds
// the owner index, so GetOrCreateByKey finds every one of them, over a
// reopen too, and a batch create of a held value is refused while a batch
// create's own value is found afterwards.
func TestUniqueLookSeedsExistingRows(t *testing.T) {
	ctx := context.Background()
	for _, st := range uniqLookStores() {
		t.Run(st.name, func(t *testing.T) {
			dir := t.TempDir()
			g := st.open(t, dir)
			ids := map[string]types.NodeID{}
			for i := 0; i < 50; i++ {
				k := fmt.Sprintf("s%d", i)
				n, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": k})
				if err != nil {
					t.Fatal(err)
				}
				ids[k] = n.ID()
			}
			if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
				t.Fatal(err)
			}
			b, err := g.Batch().New()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.AddNode([]string{"Host"}, map[string]any{"key": "batch-1"}); err != nil {
				t.Fatal(err)
			}
			if _, err := b.Execute(); err != nil {
				t.Fatal(err)
			}
			check := func(g *graphpkg.Graph) {
				t.Helper()
				for k, want := range ids {
					n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", k, nil)
					if err != nil || created || n.ID() != want {
						t.Fatalf("GetOrCreate(%s): id %d created %v err %v; want %d", k, n.ID(), created, err, want)
					}
				}
				_, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", "batch-1", nil)
				if err != nil || created {
					t.Fatalf("GetOrCreate(batch-1): created %v err %v; want the batch's node", created, err)
				}
				rows, err := g.Nodes().ByLabelAndProperty("Host", "key", "batch-1", storepkg.QueryOpts{})
				if err != nil || len(rows) != 1 {
					t.Fatalf("batch-1 holders %d (%v), want 1", len(rows), err)
				}
			}
			check(g)
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g = st.open(t, dir)
			defer g.Close()
			check(g)
		})
	}
}

// uniqLookBadger opens a graph on a badger store at dir and returns both.
func uniqLookBadger(t *testing.T, dir string) (*graphpkg.Graph, *badger.Store) {
	t.Helper()
	bs, err := badger.New(badger.Config{Dir: dir, PropertyIndexOnDisk: true})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 12, Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	return g, bs
}

func uniqLookVK(v any) string { return types.CanonicalIndexKey(types.IndexablePropertyValueKey(v)) }

// TestUniqueLookClaimWithoutRow: a claim whose row never landed (a crash
// between the two, played by a claim written alone) is ignored, over a
// reopen; and a claim that fails refuses the write, so no row is left
// without a claim (a claim written after the row would leave one, and the
// next GetOrCreateByKey would make a second holder).
func TestUniqueLookClaimWithoutRow(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	g, bs := uniqLookBadger(t, dir)
	if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
		t.Fatal(err)
	}
	a, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": "a"})
	if err != nil {
		t.Fatal(err)
	}
	tok := a.LabelTokenRawAt(0)
	if err := bs.PutUniqueOwner(tok, "key", uniqLookVK("ghost"), a.ID()+1); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutUniqueOwner(tok, "key", uniqLookVK("other"), a.ID()); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g, bs = uniqLookBadger(t, dir)
	defer g.Close()
	for _, k := range []string{"ghost", "other"} {
		n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", k, nil)
		if err != nil || !created || n.ID() == a.ID() {
			t.Fatalf("GetOrCreate(%s) over an unbacked claim: id %d created %v err %v; want a new node", k, n.ID(), created, err)
		}
	}
	boom := errors.New("claim fault")
	bs.FailNextUniqueOwnerClaimForTest(boom)
	if _, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": "f"}); !errors.Is(err, boom) {
		t.Fatalf("Add with a failing claim: %v, want the claim's error", err)
	}
	n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", "f", nil)
	if err != nil || !created {
		t.Fatalf("GetOrCreate(f): created %v err %v", created, err)
	}
	all, err := g.Nodes().ByLabel("Host", storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	var holders []types.NodeID
	for _, r := range all {
		if v, _ := r.GetProperty("key"); v == "f" {
			holders = append(holders, r.ID())
		}
	}
	if len(holders) != 1 || holders[0] != n.ID() {
		t.Fatalf("rows holding f: %v, want only %d", holders, n.ID())
	}
}

// TestUniqueLookSeedsAtOpen: a store whose constraint has no owner index (a
// constraint made by a release before UNIQUELOOK) is seeded at open, so
// GetOrCreateByKey finds every holder without a property index seek.
func TestUniqueLookSeedsAtOpen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	g, bs := uniqLookBadger(t, dir)
	if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
		t.Fatal(err)
	}
	ids := map[string]types.NodeID{}
	var tok uint16
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("o%d", i)
		n, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": k})
		if err != nil {
			t.Fatal(err)
		}
		ids[k], tok = n.ID(), n.LabelTokenRawAt(0)
	}
	if err := bs.DropUniqueOwnerIndex(tok, "key"); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g, bs = uniqLookBadger(t, dir)
	defer g.Close()
	if !bs.UniqueOwnerIndexReady(tok, "key") {
		t.Fatal("owner index not ready after the open")
	}
	before := badger.PropertyDiskLookupsForTest()
	for k, want := range ids {
		n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", k, nil)
		if err != nil || created || n.ID() != want {
			t.Fatalf("GetOrCreate(%s): id %d created %v err %v; want %d", k, n.ID(), created, err, want)
		}
	}
	if got := badger.PropertyDiskLookupsForTest() - before; got != 0 {
		t.Errorf("property index lookups = %d, want 0", got)
	}
}

// TestUniqueLookSeedReplacesStrayClaim: a claim left in the index for a
// node that does not hold the value (a stray a drop raced) does not stop the
// seed from claiming the value's holder.
func TestUniqueLookSeedReplacesStrayClaim(t *testing.T) {
	ctx := context.Background()
	g, bs := uniqLookBadger(t, t.TempDir())
	defer g.Close()
	a, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": "s"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": "t"})
	if err != nil {
		t.Fatal(err)
	}
	tok := a.LabelTokenRawAt(0)
	if err := bs.DeclareUniqueOwnerIndex(tok, "key"); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutUniqueOwner(tok, "key", uniqLookVK("s"), b.ID()); err != nil {
		t.Fatal(err)
	}
	if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
		t.Fatal(err)
	}
	n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", "s", nil)
	if err != nil || created || n.ID() != a.ID() {
		t.Fatalf("GetOrCreate(s): id %d created %v err %v; want %d", n.ID(), created, err, a.ID())
	}
}

// TestUniqueLookCascadeClaims: a SetNodeVersionInterval that makes a new
// value current claims it, so GetOrCreateByKey finds its node; the old value
// is free again.
func TestUniqueLookCascadeClaims(t *testing.T) {
	ctx := context.Background()
	for _, st := range uniqLookStores() {
		t.Run(st.name, func(t *testing.T) {
			g := st.open(t, t.TempDir())
			defer g.Close()
			if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
				t.Fatal(err)
			}
			a, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": "c1", "tkg_valid_from": types.Instant(1000)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, a.ID(), 2000, 0, map[string]any{"key": "c2"}); err != nil {
				t.Fatal(err)
			}
			n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", "c2", nil)
			if err != nil || created || n.ID() != a.ID() {
				t.Fatalf("GetOrCreate(c2): id %d created %v err %v; want %d", n.ID(), created, err, a.ID())
			}
			if _, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": "c2"}); err == nil {
				t.Fatal("a second holder of c2 was admitted")
			}
		})
	}
}

// TestUniqueLookImportClaims: rows an import writes are claimed, so
// GetOrCreateByKey finds them on the target.
func TestUniqueLookImportClaims(t *testing.T) {
	ctx := context.Background()
	src, _ := uniqLookBadger(t, t.TempDir())
	defer src.Close()
	ids := map[string]types.NodeID{}
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("i%d", i)
		n, err := src.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": k})
		if err != nil {
			t.Fatal(err)
		}
		ids[k] = n.ID()
	}
	var buf bytes.Buffer
	if err := src.IO().Export(&buf); err != nil {
		t.Fatal(err)
	}
	dst, _ := uniqLookBadger(t, t.TempDir())
	defer dst.Close()
	if err := dst.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
		t.Fatal(err)
	}
	if err := dst.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatal(err)
	}
	for k, want := range ids {
		n, created, err := dst.Nodes().GetOrCreateByKey(ctx, "Host", "key", k, nil)
		if err != nil || created || n.ID() != want {
			t.Fatalf("GetOrCreate(%s) after import: id %d created %v err %v; want %d", k, n.ID(), created, err, want)
		}
	}
}

// TestUniqueLookDuplicateStandsDown: a trusted import that skips the unique
// validation can land two holders of one value; the owner index then stands
// down (one owner per value), and the open's seed leaves it unready, so the
// lookups return both holders from the property index, also after a reopen.
func TestUniqueLookDuplicateStandsDown(t *testing.T) {
	ctx := context.Background()
	src, _ := uniqLookBadger(t, t.TempDir())
	defer src.Close()
	for i := 0; i < 2; i++ {
		if _, err := src.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": "dup"}); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if err := src.IO().Export(&buf); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	dst, _ := uniqLookBadger(t, dir)
	if err := dst.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
		t.Fatal(err)
	}
	if err := dst.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{SkipUniqueValidation: true}); err != nil {
		t.Fatal(err)
	}
	holders := func(g *graphpkg.Graph) int {
		t.Helper()
		rows, err := g.Nodes().ByLabelAndProperty("Host", "key", "dup", storepkg.QueryOpts{})
		if err != nil {
			t.Fatal(err)
		}
		return len(rows)
	}
	if n := holders(dst); n != 2 {
		t.Fatalf("holders after the import = %d, want 2", n)
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
	dst, _ = uniqLookBadger(t, dir)
	defer dst.Close()
	if n := holders(dst); n != 2 {
		t.Fatalf("holders after a reopen = %d, want 2", n)
	}
}

// TestUniqueLookForeverAndMarker:UniqueForever keeps no owner index (its
// ownership map answers) and still refuses a second holder; a badger store
// with an owner index carries format marker 16, which an older release
// (reading 15) refuses.
func TestUniqueLookForeverAndMarker(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	g, bs := uniqLookBadger(t, dir)
	if err := g.Constraints().CreateUniqueForever(ctx, "Acct", "mail"); err != nil {
		t.Fatal(err)
	}
	n, err := g.Nodes().Add(ctx, []string{"Acct"}, map[string]any{"mail": "m"})
	if err != nil {
		t.Fatal(err)
	}
	if bs.UniqueOwnerIndexDeclared(n.LabelTokenRawAt(0), "mail") {
		t.Fatal("UniqueForever declared an owner index")
	}
	if _, err := g.Nodes().Add(ctx, []string{"Acct"}, map[string]any{"mail": "m"}); err == nil {
		t.Fatal("UniqueForever admitted a second holder")
	}
	if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	restore := badger.SetMaxReadableStoreFormatVersionForTest(storeutil.CompositeDiskStoreFormatVersion)
	old, err := badger.New(badger.Config{Dir: dir})
	restore()
	if err == nil {
		_ = old.Close()
		t.Fatal("a release reading markers up to 15 opened a store with an owner index")
	}
}

// TestUniqueLookTieredDowngradeRefused (review F2): a tiered store whose
// reference label has a unique constraint carries marker 16, so a release
// reading markers up to 15 refuses it; this release reopens it and answers.
// Not parallel: the readable cap is the process's.
func TestUniqueLookTieredDowngradeRefused(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	open := func() (*tiered.Store, error) {
		return tiered.New(tiered.Config{DataDir: dir, RefLabels: []string{"Host"}, ShardWindow: 7 * 24 * time.Hour})
	}
	ts, err := open()
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 12, Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
		t.Fatal(err)
	}
	a, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": "d"})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	restore := badger.SetMaxReadableStoreFormatVersionForTest(storeutil.CompositeDiskStoreFormatVersion)
	old, err := open()
	restore()
	if err == nil {
		_ = old.Close()
		t.Fatal("a release reading markers up to 15 opened a tiered store with a unique constraint")
	}
	ts, err = open()
	if err != nil {
		t.Fatal(err)
	}
	g, err = graphpkg.New(graphpkg.Config{SnowflakeNodeID: 12, Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", "d", nil)
	if err != nil || created || n.ID() != a.ID() {
		t.Fatalf("GetOrCreate(d) after reopen: id %d created %v err %v; want %d", n.ID(), created, err, a.ID())
	}
}

// TestUniqueLookFailedCreateUniqueLeavesMarker (review F3): a CreateUnique
// refused by existing duplicates declares no owner index, so the store keeps
// its marker and a release reading markers up to 15 still opens it. Not
// parallel: the readable cap is the process's.
func TestUniqueLookFailedCreateUniqueLeavesMarker(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	g, bs := uniqLookBadger(t, dir)
	var tok uint16
	for i := 0; i < 2; i++ {
		n, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": "twin"})
		if err != nil {
			t.Fatal(err)
		}
		tok = n.LabelTokenRawAt(0)
	}
	if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err == nil {
		t.Fatal("CreateUnique over two holders succeeded")
	}
	if bs.UniqueOwnerIndexDeclared(tok, "key") {
		t.Fatal("a failed CreateUnique left an owner index declared")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	restore := badger.SetMaxReadableStoreFormatVersionForTest(storeutil.CompositeDiskStoreFormatVersion)
	old, err := badger.New(badger.Config{Dir: dir})
	restore()
	if err != nil {
		t.Fatalf("a failed CreateUnique raised the marker: a release reading 15 refuses the store: %v", err)
	}
	_ = old.Close()
}

// TestUniqueLookOldStoreReadOnly (review): a store whose constraint has no
// owner index (made before UNIQUELOOK) opened read-only cannot seed one; its
// lookups answer from the property index, right, and nothing is declared.
func TestUniqueLookOldStoreReadOnly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	g, bs := uniqLookBadger(t, dir)
	if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
		t.Fatal(err)
	}
	ids := map[string]types.NodeID{}
	var tok uint16
	for i := 0; i < 30; i++ {
		k := fmt.Sprintf("r%d", i)
		n, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": k})
		if err != nil {
			t.Fatal(err)
		}
		ids[k], tok = n.ID(), n.LabelTokenRawAt(0)
	}
	if err := bs.DropUniqueOwnerIndex(tok, "key"); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := badger.New(badger.Config{Dir: dir, ReadOnly: true, PropertyIndexOnDisk: true})
	if err != nil {
		t.Fatal(err)
	}
	g, err = graphpkg.New(graphpkg.Config{Store: ro})
	if err != nil {
		t.Fatalf("graph.New read-only: %v", err)
	}
	defer g.Close()
	if ro.UniqueOwnerIndexDeclared(tok, "key") || ro.UniqueOwnerIndexReady(tok, "key") {
		t.Fatal("a read-only open declared an owner index")
	}
	for k, want := range ids {
		rows, err := g.Nodes().ByLabelAndProperty("Host", "key", k, storepkg.QueryOpts{})
		if err != nil || len(rows) != 1 || rows[0].ID() != want {
			t.Fatalf("ByLabelAndProperty(%s) read-only: %d rows (%v), want %d", k, len(rows), err, want)
		}
	}
	rows, err := g.Nodes().ByLabelAndProperty("Host", "key", "absent", storepkg.QueryOpts{})
	if err != nil || len(rows) != 0 {
		t.Fatalf("ByLabelAndProperty(absent) read-only: %d rows (%v), want 0", len(rows), err)
	}
}

// TestUniqueLookThreeDoorsOneValue (review): GetOrCreateByKey, a batch
// Execute and a transaction's AddNodes race on one value; exactly one row
// holds it, every GetOrCreateByKey returns that row, and every other writer
// is refused with ErrUniqueViolation.
func TestUniqueLookThreeDoorsOneValue(t *testing.T) {
	ctx := context.Background()
	for _, st := range uniqLookStores() {
		t.Run(st.name, func(t *testing.T) {
			g := st.open(t, t.TempDir())
			defer g.Close()
			if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
				t.Fatal(err)
			}
			for round := 0; round < 10; round++ {
				k := fmt.Sprintf("door%d", round)
				var wg sync.WaitGroup
				var mu sync.Mutex
				writers := 0
				got := map[types.NodeID]bool{}
				done := func(id types.NodeID, wrote bool, err error) {
					mu.Lock()
					defer mu.Unlock()
					switch {
					case err != nil && !errors.Is(err, graphpkg.ErrUniqueViolation):
						t.Errorf("%s: %v", k, err)
					case err == nil && wrote:
						writers++
					}
					if err == nil && id != 0 {
						got[id] = true
					}
				}
				for w := 0; w < 4; w++ {
					wg.Add(3)
					go func() {
						defer wg.Done()
						n, c, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", k, nil)
						var id types.NodeID
						if err == nil {
							id = n.ID()
						}
						done(id, c, err)
					}()
					go func() {
						defer wg.Done()
						b, err := g.Batch().New()
						if err != nil {
							done(0, false, err)
							return
						}
						if _, err := b.AddNode([]string{"Host"}, map[string]any{"key": k}); err != nil {
							done(0, false, err)
							return
						}
						res, err := b.Execute()
						if res != nil && len(res.Errors) == 1 && res.Created == 0 {
							err = res.Errors[0].Err // the batch's one refused op
						}
						done(0, err == nil, err)
					}()
					go func() {
						defer wg.Done()
						err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
							_, err := tx.AddNodes([]string{"Host"}, []map[string]any{{"key": k}})
							return err
						})
						done(0, err == nil, err)
					}()
				}
				wg.Wait()
				rows, err := g.Nodes().ByLabelAndProperty("Host", "key", k, storepkg.QueryOpts{})
				if err != nil || len(rows) != 1 {
					t.Fatalf("%s: %d holders (%v), want 1", k, len(rows), err)
				}
				if writers != 1 {
					t.Fatalf("%s: %d writers succeeded, want 1", k, writers)
				}
				for id := range got {
					if id != rows[0].ID() {
						t.Fatalf("%s: GetOrCreateByKey returned %d, the holder is %d", k, id, rows[0].ID())
					}
				}
			}
		})
	}
}

// TestUniqueLookLongValue (review): a value over 1 KiB is claimed under its
// SHA-256 (form byte 1); GetOrCreateByKey finds its holder, a second holder
// is refused, and a long value sharing all but its last byte is a different
// value. Over a reopen too.
func TestUniqueLookLongValue(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	g, bs := uniqLookBadger(t, dir)
	if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
		t.Fatal(err)
	}
	long := make([]byte, 2*storeutil.UniqueOwnerVerbatimMax)
	for i := range long {
		long[i] = 'a' + byte(i%26)
	}
	v1, v2 := string(long)+"1", string(long)+"2"
	a, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", v1, nil)
	if err != nil || !created {
		t.Fatalf("GetOrCreate(v1): created %v err %v", created, err)
	}
	tok := a.LabelTokenRawAt(0)
	if id, ok, err := bs.UniqueOwner(tok, "key", uniqLookVK(v1)); err != nil || !ok || id != a.ID() {
		t.Fatalf("claim of v1: %d %v %v, want %d", id, ok, err, a.ID())
	}
	check := func(g *graphpkg.Graph) {
		t.Helper()
		n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", v1, nil)
		if err != nil || created || n.ID() != a.ID() {
			t.Fatalf("GetOrCreate(v1): id %d created %v err %v; want %d", n.ID(), created, err, a.ID())
		}
		if _, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": v1}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
			t.Fatalf("a second holder of v1: %v, want ErrUniqueViolation", err)
		}
	}
	check(g)
	b, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", v2, nil)
	if err != nil || !created || b.ID() == a.ID() {
		t.Fatalf("GetOrCreate(v2): id %d created %v err %v; want a new node", b.ID(), created, err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g, _ = uniqLookBadger(t, dir)
	defer g.Close()
	check(g)
}
