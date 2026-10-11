package badger

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Copied from exact11a for a same-process allocation/answer comparison.
// The original per-key conversion is deliberately retained only in this oracle.
func legacyIdentityOverlay(bs *Store, prefix []byte, size int) map[string]writeOp {
	var overlay map[string]writeOp
	bs.rangePending(func(key string, op writeOp) {
		if len(key) != size || !strings.HasPrefix(key, string(prefix)) {
			return
		}
		if overlay == nil {
			overlay = make(map[string]writeOp)
		}
		overlay[key] = op
	})
	return overlay
}

func assertIdentityOverlayEqual(t *testing.T, a, b map[string]writeOp) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatal("overlay cardinality", len(a), len(b))
	}
	for key, left := range a {
		right, ok := b[key]
		if !ok || left.opType != right.opType || !bytes.Equal(left.key, right.key) || !bytes.Equal(left.value, right.value) {
			t.Fatal("overlay differs", key, left, right)
		}
	}
}

func identityOverlayFixture(count int) (*Store, []byte, int) {
	payload, _ := storeutil.PropertyIndexValueBytes(types.IndexablePropertyValueKey(strings.Repeat("a", 64)))
	prefix := storeutil.PropertyIndexValuePrefix(1, payload)
	bs := &Store{pending: make(map[string]writeOp)}
	otherPayload, _ := storeutil.PropertyIndexValueBytes(types.IndexablePropertyValueKey(strings.Repeat("b", 64)))
	other := storeutil.PropertyIndexValuePrefix(1, otherPayload)
	for i := range count {
		key := make([]byte, len(other)+8)
		copy(key, other)
		for j := range 8 {
			key[len(other)+j] = byte(uint64(i+1) >> uint(8*(7-j)))
		}
		bs.pending[string(key)] = writeOp{opType: writeOpSet, key: key}
	}
	return bs, prefix, len(prefix) + 8
}

func TestIdentityOverlayPrefixAllocationDoesNotScaleWithUnrelatedBufferedKeys(t *testing.T) {
	for _, count := range []int{1000, 10000} {
		bs, prefix, size := identityOverlayFixture(count)
		legacy := testing.AllocsPerRun(3, func() {
			if got := legacyIdentityOverlay(bs, prefix, size); len(got) != 0 {
				t.Fatal("unexpected match")
			}
		})
		current := testing.AllocsPerRun(3, func() {
			if got := bs.currentKeyOverlay(prefix, size); len(got) != 0 {
				t.Fatal("unexpected match")
			}
		})
		t.Logf("unrelated_keys%d legacy_allocations%.0f patched_allocations%.0f", count, legacy, current)
		if legacy < float64(count) || current > 2 {
			t.Fatal("prefix allocation remains proportional to pending size", legacy, current)
		}
	}
	for _, count := range []int{1000, 10000} {
		bs, prefix, size := identityOverlayFixture(count)
		key := make([]byte, size)
		copy(key, prefix)
		key[size-1] = 1
		bs.pending[string(key)] = writeOp{opType: writeOpSet, key: key}
		current := testing.AllocsPerRun(3, func() {
			if got := bs.currentKeyOverlay(prefix, size); len(got) != 1 {
				t.Fatal("matching snapshot lost")
			}
		})
		t.Logf("unrelated_keys%d matching_keys1 patched_allocations%.0f", count, current)
		if current > 5 {
			t.Fatal("matching snapshot allocations grew with unrelated keys", current)
		}
	}
}

func TestIdentityOverlayPendingWinsBothDeleteDirectionsAndOwnedSnapshot(t *testing.T) {
	bs, prefix, size := identityOverlayFixture(1000)
	matching := func(id byte) []byte { k := make([]byte, size); copy(k, prefix); k[size-1] = id; return k }
	a, b, c := matching(1), matching(2), matching(3)
	bs.flushing = map[string]writeOp{string(a): {opType: writeOpSet, key: a, value: []byte("old")}, string(b): {opType: writeOpDelete, key: b}, string(c): {opType: writeOpSet, key: c, value: []byte("parked")}}
	bs.pending[string(a)] = writeOp{opType: writeOpDelete, key: a}
	bs.pending[string(b)] = writeOp{opType: writeOpSet, key: b, value: []byte("new")}
	got := bs.currentKeyOverlay(prefix, size)
	assertIdentityOverlayEqual(t, legacyIdentityOverlay(bs, prefix, size), got)
	if len(got) != 3 || got[string(a)].opType != writeOpDelete || string(got[string(b)].value) != "new" || string(got[string(c)].value) != "parked" {
		t.Fatal(got)
	}
	bs.wbMu.Lock()
	clear(bs.pending)
	bs.flushing = nil
	bs.wbMu.Unlock()
	if len(got) != 3 || string(got[string(c)].value) != "parked" {
		t.Fatal("snapshot depended on mutable buffer maps")
	}
}

func BenchmarkIdentityOverlayPrefix(b *testing.B) {
	for _, count := range []int{1000, 10000} {
		for _, legacy := range []bool{false, true} {
			b.Run(fmt.Sprintf("keys%d/legacy%v", count, legacy), func(b *testing.B) {
				bs, prefix, size := identityOverlayFixture(count)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if legacy {
						_ = legacyIdentityOverlay(bs, prefix, size)
					} else {
						_ = bs.currentKeyOverlay(prefix, size)
					}
				}
			})
		}
	}
}

func identityGenuineBatchFixture(t testing.TB) (*Store, []*types.Node) {
	t.Helper()
	source := os.Getenv("RHO_OVERLAY_GENUINE_FIXTURE")
	if source == "" {
		t.Skip("set RHO_OVERLAY_GENUINE_FIXTURE to the closed genuine Erv derivative")
	}
	srcCfg := diskIdentityConfig(source)
	srcCfg.ReadOnly = true
	srcCfg.MemTableSize = 32 << 20
	srcCfg.BlockCacheSize = 8 << 20
	srcCfg.IndexCacheSize = 8 << 20
	src, err := New(srcCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	var nodes []*types.Node
	if err := src.forEachCurrentNodeRow(0, func(n *types.Node) (bool, error) {
		if value, ok := n.GetProperty("key"); ok {
			if key, ok := value.(string); ok && len(key) == 64 {
				nodes = append(nodes, n.DeepCopy())
			}
		}
		return len(nodes) < 1000, nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1000 {
		t.Fatal("insufficient genuine rows", len(nodes))
	}
	cfg := diskIdentityConfig(t.TempDir())
	cfg.CacheCapacity = 1000
	cfg.CacheBudgetBytes = 8 << 20
	cfg.PropertyKeyRegistry = src.propKeyReg.Load()
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	labels := map[uint16]bool{}
	for _, n := range nodes {
		for i := range n.LabelTokenCount() {
			labels[n.LabelTokenRawAt(i)] = true
		}
	}
	for token := range labels {
		if err := bs.CreatePropertyIndex(token, "key"); err != nil {
			t.Fatal(err)
		}
	}
	bs.BeginGroupCommit()
	for _, n := range nodes {
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	return bs, nodes
}

func TestIdentityOverlayGenuineBatch1000PropertyAnswersThroughFlushReplay(t *testing.T) {
	bs, nodes := identityGenuineBatchFixture(t)
	hashAnswers := func() string {
		t.Helper()
		h := sha256.New()
		for _, n := range nodes {
			value, _ := n.GetProperty("key")
			payload, _ := storeutil.PropertyIndexValueBytes(types.IndexablePropertyValueKey(value))
			token, ok := bs.propKeyTokenFor("key")
			if !ok {
				t.Fatal("missing property token")
			}
			prefix := storeutil.PropertyIndexValuePrefix(token, payload)
			assertIdentityOverlayEqual(t, legacyIdentityOverlay(bs, prefix, len(prefix)+8), bs.currentKeyOverlay(prefix, len(prefix)+8))
			rows, err := bs.NodesByLabelAndProperty(uint16(n.PrimaryLabelToken()), "key", value, QueryOpts{Limit: 1})
			if err != nil || len(rows) != 1 || rows[0].ID() != n.ID() {
				t.Fatal("genuine property answer", rows, err)
			}
			raw, err := json.Marshal(struct {
				ID         types.NodeID
				Version    uint32
				Properties map[string]any
				Temporal   *types.TemporalMetadata
				Integrity  *types.NodeIntegrity
			}{rows[0].ID(), rows[0].Version(), rows[0].PropertiesMap(), rows[0].Temporal(), rows[0].Integrity()})
			if err != nil {
				t.Fatal(err)
			}
			h.Write(raw)
			h.Write([]byte("\n"))
		}
		return fmt.Sprintf("%x", h.Sum(nil))
	}
	before := hashAnswers()
	t.Log("genuine1000 pending answer SHA256", before)
	bs.wbMu.Lock()
	bs.flushing = bs.pending
	bs.pending = make(map[string]writeOp)
	bs.wbMu.Unlock()
	if parked := hashAnswers(); parked != before {
		t.Fatal("flushing answers changed", parked, before)
	}
	// Restore exactly as a failed flush does, then exercise the actual failure
	// seam and retry rather than treating parked rows as durable.
	bs.requeueOps(bs.flushing)
	bs.wbMu.Lock()
	bs.flushing = nil
	bs.wbMu.Unlock()
	injected := errors.New("overlay batch flush failure")
	bs.FailNextFlushForTest(injected)
	if err := bs.Flush(); !errors.Is(err, injected) {
		t.Fatal("failure hidden", err)
	}
	if replay := hashAnswers(); replay != before {
		t.Fatal("requeue answers changed", replay, before)
	}
	// Pause the actual flush with its full encoded map parked in flushing.
	// Property scans must retain every answer before and after that commit.
	parked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var unpark sync.Once
	defer unpark.Do(func() { close(release) })
	bs.historicalMembershipFlushTestHook = func(stage string) {
		if stage == "after-posting-sets" {
			once.Do(func() { close(parked); <-release })
		}
	}
	flushResult := make(chan error, 1)
	go func() { flushResult <- bs.Flush() }()
	<-parked
	if concurrent := hashAnswers(); concurrent != before {
		t.Fatal("active flush answers changed", concurrent, before)
	}
	unpark.Do(func() { close(release) })
	if err := <-flushResult; err != nil {
		t.Fatal(err)
	}
	bs.historicalMembershipFlushTestHook = nil
	if err := bs.EndGroupCommit(); err != nil {
		t.Fatal(err)
	}
	if durable := hashAnswers(); durable != before {
		t.Fatal("durable answers changed", durable, before)
	}
	// Exact sorted pagination through the property path remains independent
	// of the larger mixed-prefix buffer and index candidate order.
	ids := make([]types.NodeID, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID()
	}
	slices.Sort(ids)
	page, err := bs.AllNodeIDs(QueryOpts{After: types.EntityID(ids[500]), Limit: 3})
	if err != nil || !slices.Equal(page, ids[501:504]) {
		t.Fatal("ordered identity page", page, err)
	}
}

func BenchmarkIdentityOverlayGenuineBatch1000Snapshots(b *testing.B) {
	for _, legacy := range []bool{false, true} {
		b.Run(fmt.Sprintf("legacy%v", legacy), func(b *testing.B) {
			bs, nodes := identityGenuineBatchFixture(b)
			token, known := bs.propKeyTokenFor("key")
			if !known {
				b.Fatal("missing key token")
			}
			prefixes := make([][]byte, len(nodes))
			expected := make([]int, len(nodes))
			for i, n := range nodes {
				value, _ := n.GetProperty("key")
				payload, _ := storeutil.PropertyIndexValueBytes(types.IndexablePropertyValueKey(value))
				prefixes[i] = storeutil.PropertyIndexValuePrefix(token, payload)
				// Global property candidates can share one key across labels.
				// The exact label filter runs later in the public property path.
				expected[i] = len(legacyIdentityOverlay(bs, prefixes[i], len(prefixes[i])+8))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				for i, prefix := range prefixes {
					var got map[string]writeOp
					if legacy {
						got = legacyIdentityOverlay(bs, prefix, len(prefix)+8)
					} else {
						got = bs.currentKeyOverlay(prefix, len(prefix)+8)
					}
					if len(got) != expected[i] {
						b.Fatal("genuine snapshot lost", len(got))
					}
				}
			}
		})
	}
}

func BenchmarkIdentityOverlayGenuineBatch1000PropertyLookups(b *testing.B) {
	bs, nodes := identityGenuineBatchFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for _, n := range nodes {
			value, _ := n.GetProperty("key")
			rows, err := bs.NodesByLabelAndProperty(uint16(n.PrimaryLabelToken()), "key", value, QueryOpts{Limit: 1})
			if err != nil || len(rows) != 1 || rows[0].ID() != n.ID() {
				b.Fatal(rows, err)
			}
		}
	}
}
