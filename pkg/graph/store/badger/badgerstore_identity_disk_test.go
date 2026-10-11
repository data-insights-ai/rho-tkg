package badger

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func diskIdentityConfig(dir string) Config {
	return Config{Dir: dir, EntityIdentityOnDisk: true, HistoricalMembershipOnDisk: true, SyncWrites: true,
		LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true, PropertyIndexOnDisk: true,
		DisableOrdinals: true, DisablePlannerStats: true, HistoryPresenceProbeOnly: true,
		CacheCapacity: 2, CacheBudgetBytes: 1 << 20, FlushInterval: time.Hour, GCInterval: time.Hour}
}

func TestDiskIdentityCurrentRowsReplayAndReopen(t *testing.T) {
	dir := t.TempDir()
	config := diskIdentityConfig(dir)
	bs, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	bs.BeginGroupCommit()
	for _, id := range []int64{3, 1, 2} {
		putTestNode(t, bs, id, 1, nil)
	}
	putTestRel(t, bs, 10, 2, 1, 2)
	putTestRel(t, bs, 11, 3, 2, 3)
	if err := bs.PutNode(types.NewNode(1, 1, nil)); !errors.Is(err, ErrNodeExists) {
		t.Fatal("duplicate current row was admitted", err)
	}
	if err := bs.PutRelationship(types.NewRelationship(10, 2, 1, 2)); !errors.Is(err, ErrRelExists) {
		t.Fatal("duplicate relationship was admitted", err)
	}
	assert := func() {
		t.Helper()
		ids, err := bs.AllNodeIDs(QueryOpts{})
		if err != nil || !reflect.DeepEqual(ids, []types.NodeID{1, 2, 3}) {
			t.Fatal("current node identities", ids, err)
		}
		page, err := bs.AllNodeIDs(QueryOpts{After: 1, Limit: 1})
		if err != nil || !reflect.DeepEqual(page, []types.NodeID{2}) {
			t.Fatal("bounded identity page", page, err)
		}
		rels, err := bs.RelationshipsByType(2, QueryOpts{})
		if err != nil || len(rels) != 1 || rels[0].ID() != 10 {
			t.Fatal("disk relationship type", rels, err)
		}
		if len(bs.nodeIDs)+len(bs.nodeHashes)+len(bs.nodeRevs)+len(bs.relIDs)+len(bs.relRevs)+len(bs.typeIdx) != 0 {
			t.Fatal("resident identity state was populated")
		}
		if _, known := bs.NodeBeliefWatermark(1); known || bs.nodeBeliefWatermark != nil {
			t.Fatal("query populated an all-ID watermark map")
		}
	}
	assert()
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	assert()
	if err := bs.EndGroupCommit(); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs, err = New(config)
	if err != nil {
		t.Fatal(err)
	}
	assert()
	bs.BeginGroupCommit()
	if err := bs.DeleteRelationship(10); err != nil {
		t.Fatal(err)
	}
	if err := bs.DeleteNode(1); err != nil {
		t.Fatal(err)
	}
	if exists, err := bs.NodeExists(1); err != nil || exists {
		t.Fatal("pending deletion disappeared", exists, err)
	}
	if ids, err := bs.AllNodeIDs(QueryOpts{}); err != nil || !reflect.DeepEqual(ids, []types.NodeID{2, 3}) {
		t.Fatal("deleted row appeared in identity merge", ids, err)
	}
}

func TestDiskIdentityStalePrefetchAndClearCannotReuseProof(t *testing.T) {
	bs, err := New(diskIdentityConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	n := putTestNode(t, bs, 1, 1, nil)
	before, err := bs.prefetchNodeDeleteInfo(1)
	if err != nil {
		t.Fatal(err)
	}
	changed := n.DeepCopy()
	if err := changed.SetProperty("state", "changed"); err != nil {
		t.Fatal(err)
	}
	if err := bs.ReplaceNode(changed); err != nil {
		t.Fatal(err)
	}
	bs.idxMu.Lock()
	current, err := bs.currentNodeForPrefetchLocked(1, before)
	bs.idxMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if value, _ := current.GetProperty("state"); value != "changed" {
		t.Fatal("stale property prefetch accepted", value)
	}
	before, err = bs.prefetchNodeDeleteInfo(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}
	putTestNode(t, bs, 1, 2, nil)
	bs.idxMu.Lock()
	current, err = bs.currentNodeForPrefetchLocked(1, before)
	bs.idxMu.Unlock()
	if err != nil || current.PrimaryLabelToken() != 2 {
		t.Fatal("Clear/recreate inherited a previous proof", current, err)
	}
}

func TestDiskIdentityStorageFailureIsNotAbsence(t *testing.T) {
	bs, err := New(diskIdentityConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if err := bs.db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := bs.NodeExists(1); !errors.Is(err, badgerv4.ErrDBClosed) {
		t.Fatal("node I/O failure became absence", err)
	}
	if _, err := bs.RelationshipExists(1); !errors.Is(err, badgerv4.ErrDBClosed) {
		t.Fatal("relationship I/O failure became absence", err)
	}
	if err := bs.PutNode(types.NewNode(1, 1, nil)); !errors.Is(err, badgerv4.ErrDBClosed) {
		t.Fatal("write proceeded after failed existence proof", err)
	}
}

func TestDiskIdentityRejectsIncompatibleProfileBeforeOpen(t *testing.T) {
	config := diskIdentityConfig(t.TempDir())
	config.DisableOrdinals = false
	if _, err := New(config); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatal(err)
	}
	bs, err := New(diskIdentityConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if err := bs.CreateTemporalIndex(1); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatal("resident advanced index silently accepted", err)
	}
}

func TestDiskIdentityLegacyProbeFailsExplicitly(t *testing.T) {
	bs, err := New(diskIdentityConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	for _, probe := range []func(){func() { bs.HasNodeID(1) }, func() { bs.HasRelID(1) }} {
		func() {
			defer func() {
				cause, ok := recover().(UnsupportedIdentityProbeError)
				if !ok || !errors.Is(cause, storecontract.ErrCapabilityNotSupported) {
					t.Fatal("legacy probe returned an uncertain boolean", cause)
				}
				if !strings.Contains(cause.Error(), cause.Operation) || cause.Unwrap() != storecontract.ErrCapabilityNotSupported {
					t.Fatal("typed legacy error lost its operation or cause")
				}
			}()
			probe()
		}()
	}
}

func TestDiskIdentityPublicExistenceValidationAndMaintenanceError(t *testing.T) {
	var missing *Store
	if _, err := missing.NodeExists(1); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
	if _, err := missing.RelationshipExists(1); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
	bs, err := New(diskIdentityConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if _, err := bs.NodeExists(0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatal(err)
	}
	if _, err := bs.RelationshipExists(-1); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatal(err)
	}
	cause := IdentityCallbackMaintenanceError{Operation: "Clear"}
	if !strings.Contains(cause.Error(), "Clear") || cause.Unwrap() != storecontract.ErrCapabilityNotSupported {
		t.Fatal("maintenance rejection lost its reason")
	}
}

func TestDiskIdentityUncachedFlushingValuesAndPendingTombstones(t *testing.T) {
	bs, err := New(diskIdentityConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	n := types.NewNode(1, 1, nil)
	nodeBytes, err := bs.marshalNodeBytes(n)
	if err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(10, 2, 1, 2)
	relBytes, err := bs.marshalRelBytes(r)
	if err != nil {
		t.Fatal(err)
	}
	// Models a flush's captured values after clean-cache eviction. Pending
	// tombstones are newer than those captured sets and must mask them.
	bs.wbMu.Lock()
	bs.flushing = map[string]writeOp{
		string(storeutil.NodeKey(1)): {opType: writeOpSet, value: nodeBytes},
		string(storeutil.RelKey(10)): {opType: writeOpSet, value: relBytes},
	}
	bs.wbMu.Unlock()
	if exists, err := bs.NodeExists(1); err != nil || !exists {
		t.Fatal("uncached flushing node became absent", exists, err)
	}
	if exists, err := bs.RelationshipExists(10); err != nil || !exists {
		t.Fatal("uncached flushing relationship became absent", exists, err)
	}
	bs.appendOps(writeOp{opType: writeOpDelete, key: storeutil.NodeKey(1)}, writeOp{opType: writeOpDelete, key: storeutil.RelKey(10)})
	if exists, err := bs.NodeExists(1); err != nil || exists {
		t.Fatal("old flushing set overrode node tombstone", exists, err)
	}
	if exists, err := bs.RelationshipExists(10); err != nil || exists {
		t.Fatal("old flushing set overrode relationship tombstone", exists, err)
	}
	bs.wbMu.Lock()
	bs.flushing = nil
	bs.wbMu.Unlock()
}

func TestDiskIdentityRevisionCollisionAndExhaustionAreConservative(t *testing.T) {
	bs, err := New(diskIdentityConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	putTestNode(t, bs, 1, 1, nil)
	proof, err := bs.prefetchNodeDeleteInfo(1)
	if err != nil {
		t.Fatal(err)
	}
	var collision types.NodeID
	for id := types.NodeID(2); id < 100_000; id++ {
		if identityRevisionStripe(types.EntityID(id)) == identityRevisionStripe(1) {
			collision = id
			break
		}
	}
	if collision == 0 {
		t.Fatal("missing deliberate stripe collision")
	}
	putTestNode(t, bs, int64(collision), 1, nil)
	bs.idxMu.Lock()
	staleAccepted := bs.nodeDeleteInfoStillCurrentLocked(1, proof)
	current, err := bs.currentNodeForPrefetchLocked(1, proof)
	stripe := identityRevisionStripe(1)
	bs.identityRevisions.nodes[stripe] = math.MaxUint64
	advanceIdentityRevision(&bs.identityRevisions.nodes[stripe])
	exhausted := bs.nodeRevisionLocked(1)
	bs.idxMu.Unlock()
	if staleAccepted {
		t.Fatal("collision reused a stale proof")
	}
	if err != nil || current.ID() != 1 || exhausted != 0 {
		t.Fatal("revision collision or exhaustion changed identity", current, exhausted, err)
	}
}

func TestDiskIdentityFailedFlushRequeuesAndRetryKeepsExactRows(t *testing.T) {
	config := diskIdentityConfig(t.TempDir())
	bs, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	bs.BeginGroupCommit()
	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)
	injected := errors.New("injected durable commit failure")
	bs.FailNextFlushForTest(injected)
	if err := bs.Flush(); !errors.Is(err, injected) {
		t.Fatal("storage failure was hidden", err)
	}
	if bs.pendingLen() == 0 || len(bs.flushing) != 0 {
		t.Fatal("failed flush lost buffered evidence")
	}

	if err := bs.EndGroupCommit(); err != nil {
		t.Fatal(err)
	}
	ids, err := bs.AllNodeIDs(QueryOpts{})
	if err != nil || !reflect.DeepEqual(ids, []types.NodeID{1, 2}) || bs.pendingLen() != 0 {
		t.Fatal("retry changed the exact identity set", ids, err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs, err = New(config)
	if err != nil {
		t.Fatal(err)
	}
	ids, err = bs.AllNodeIDs(QueryOpts{})
	if err != nil || !reflect.DeepEqual(ids, []types.NodeID{1, 2}) {
		t.Fatal("reopen lost replayed rows", ids, err)
	}
}

func TestDiskIdentityOwnedBatchCacheBudgetAndScanReentry(t *testing.T) {
	config := diskIdentityConfig(t.TempDir())
	config.CacheCapacity = 0
	config.CacheBudgetBytes = 64 << 10
	bs, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	var nodes []*types.Node
	for i := 1; i <= 256; i++ {
		n := types.NewNode(types.NodeID(i), 1, nil)
		if err := n.SetProperty("key", fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 {
			if err := n.SetProperty("payload", strings.Repeat("x", 16<<10)); err != nil {
				t.Fatal(err)
			}
		}
		nodes = append(nodes, n)
	}
	bs.BeginGroupCommit()
	if err := bs.PutNodesBatchOwnedPreEncoded(nodes, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := nodes[0].SetProperty("forbidden", true); !errors.Is(err, types.ErrFrozenNode) {
		t.Fatal("ownership-transfer batch retained mutable rows", err)
	}
	if err := bs.EndGroupCommit(); err != nil {
		t.Fatal(err)
	}
	if bytes := bs.nodeCache.Bytes(); bytes > config.CacheBudgetBytes {
		t.Fatal("clean cache exceeded byte budget", bytes)
	}
	before := bs.nodeCache.Len()
	count := 0
	if err := bs.ForEachNodeByLabel(1, QueryOpts{}, func(n *types.Node) bool {
		count++
		if exists, err := bs.NodeExists(n.ID()); err != nil || !exists {
			t.Fatal("scan callback cannot safely reenter identity lookup", exists, err)
		}
		return true
	}); err != nil || count != 256 {
		t.Fatal("scan clipped the fixture", count, err)
	}
	if bs.nodeCache.Bytes() > config.CacheBudgetBytes || len(bs.nodeIDs) != 0 || len(bs.nodeHashes) != 0 || len(bs.nodeRevs) != 0 {
		t.Fatal("scan populated unbounded state")
	}
	if before >= 256 {
		t.Fatal("cache budget did not actually evict")
	}
	if err := bs.DeleteNodesBatch([]types.NodeID{255, 256}); err != nil {
		t.Fatal(err)
	}
	ids, err := bs.AllNodeIDs(QueryOpts{After: 253})
	if err != nil || !reflect.DeepEqual(ids, []types.NodeID{254}) {
		t.Fatal("batch deletion changed survivors", ids, err)
	}
}

func TestDiskIdentityExactEraseClosureAndRetention(t *testing.T) {
	bs, err := New(diskIdentityConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	for i := int64(1); i <= 3; i++ {
		putTestNode(t, bs, i, 7, nil)
	}
	putTestRel(t, bs, 101, 9, 1, 2)
	putTestRel(t, bs, 102, 9, 1, 3)
	history := types.NewNode(1, 7, nil)
	if err := bs.PutNodeVersion(1, 0, history); err != nil {
		t.Fatal(err)
	}
	request := storecontract.ExactErasureRequest{NodeIDs: []types.NodeID{1}, RelIDs: []types.RelID{101}, Bounds: badgerExactErasureBounds}
	if _, err := bs.ExactErase(request); !errors.Is(err, storecontract.ErrExactErasureRelationshipEscape) {
		t.Fatal("incomplete closure was accepted", err)
	}
	if exists, err := bs.NodeExists(1); err != nil || !exists {
		t.Fatal("failed closure changed source identity", exists, err)
	}
	request.RelIDs = append(request.RelIDs, 102)
	result, err := bs.ExactErase(request)
	if err != nil || result.NodesRemoved != 1 || result.RelsRemoved != 2 {
		t.Fatal("exact erasure", result, err)
	}
	result, err = bs.ExactErase(request)
	if err != nil || result.NodesRemoved != 0 || result.RelsRemoved != 0 {
		t.Fatal("exact erasure replay", result, err)
	}
	if history, err := bs.GetNodeHistory(1); err != nil || len(history) != 0 {
		t.Fatal("erased history remains visible", history, err)
	}
	for _, id := range []types.NodeID{4, 5} {
		n := types.NewNode(id, 7, nil)
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: 1, ValidTo: types.Instant(id) * 10, TxFrom: 1})
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	if result, err := bs.PurgeNodesByLabelValidToBefore(7, 45, 1); err != nil || result.NodesPurged != 1 {
		t.Fatal("bounded retention", result, err)
	}
	ids, err := bs.AllNodeIDs(QueryOpts{})
	if err != nil || !reflect.DeepEqual(ids, []types.NodeID{2, 3, 5}) {
		t.Fatal("retention changed unrelated identities", ids, err)
	}
}
