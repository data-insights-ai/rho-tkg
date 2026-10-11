package graphstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func TestCheckedUncachedRangeLookupConsumesSharedOutputAllowance(t *testing.T) {
	declaration, err := NewOwnershipDeclaration(graphstate.GraphID{1}, 9, []PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{3}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{8}}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	namespace := Namespace{Graph: declaration.Graph(), Partition: 3}
	store, _, _ := ownershipStore(t, vfs.NewMem(), namespace, declaration, [16]byte{3})
	seed := ownershipView(t, store, 1)
	effects, _, err := InitializeDeclaredPartition(t.Context(), seed, declaration, nil, Limits{}, GraphLimits{}, ownershipBudget())
	if err != nil {
		t.Fatal(err)
	}
	ready, err := effects.Root.AdvanceEffects([32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	image, err := EncodeRoot(ready)
	if err != nil {
		t.Fatal(err)
	}
	ownershipInstallRaw(t, store, image, effects.Writes)
	_, image, err = store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	configuration := [32]byte{1}
	publication := RangePublication{Graph: namespace.Graph, RangeID: 17, First: 17, Last: 32, InitialPartition: 3, InitialEpoch: 1, Configuration: configuration, SourcePartition: 3, SourceIndex: 1}
	owner := RangeOwnership{Graph: namespace.Graph, RangeID: 17, Partition: 3, Epoch: 1, Configuration: configuration}
	encoded, err := EncodeRangePublication(publication)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := EncodeRangeOwnership(owner)
	if err != nil {
		t.Fatal(err)
	}
	at := ownershipInstallRaw(t, store, image, []raftlog.KV{{Key: RangePublicationKey(namespace, 17), Value: encoded}, {Key: RangeOwnershipKey(namespace, 17), Value: fence}, {Key: RangeEndKey(namespace, 32), Value: encoded}})
	old := ownershipView(t, store, at)
	at = ownershipInstallRaw(t, store, image, []raftlog.KV{{Key: RangeEndKey(namespace, 24), Deleted: true}})
	current := ownershipView(t, store, at)
	for _, view := range []*raftlog.ApplicationView{old, current} {
		catalog, _, err := OpenPartitionCatalog(t.Context(), view, namespace, 2, Limits{}, GraphLimits{}, ownershipBudget())
		if err != nil {
			t.Fatal(err)
		}
		zero, _ := graphstate.NewOutputBudget(fullStageMetadataBytes)
		reader, err := catalog.readerWithOutputBudget(t.Context(), zero)
		if err != nil {
			t.Fatal(err)
		}
		scope := &partitionReadScope{view: view, namespace: namespace, configuration: configuration}
		if err := scope.owner(reader, 22); !errors.Is(err, ErrResourceLimit) || scope.ranges[0].publication != (RangePublication{}) || zero.Used() != fullStageMetadataBytes {
			t.Fatalf("uncached routing bypassed exhausted aggregate: err=%v records=%d bytes=%d used=%d range=%+v", err, reader.rows, reader.bytes, zero.Used(), scope.ranges[0].publication)
		}
		fitting, _ := graphstate.NewOutputBudget(4096)
		reader, err = catalog.readerWithOutputBudget(t.Context(), fitting)
		if err != nil {
			t.Fatal(err)
		}
		scope = &partitionReadScope{view: view, namespace: namespace, configuration: configuration}
		if err := scope.owner(reader, 22); err != nil || reader.rows != 4 || scope.ranges[0].publication != publication || fitting.Used() == 0 {
			t.Fatal("bounded empty-page routing guard", err, reader.rows, fitting.Used())
		}
		consumed, records := fitting.Used(), reader.rows
		if err := scope.owner(reader, 23); err != nil || fitting.Used() != consumed || reader.rows != records {
			t.Fatal("fixed captured-view witness was not reused", err)
		}
		if _, err := view.Root(); err != nil {
			t.Fatal("resource refusal consumed borrow", err)
		}
	}
}

func TestPartitionPersistedAxisConflictIsCorruptWhileRetainedAuthorityMatches(t *testing.T) {
	d, err := NewOwnershipDeclaration(graphstate.GraphID{1}, 9, []PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{3}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{8}}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	n := Namespace{Graph: d.Graph(), Partition: 3}
	s, _, _ := ownershipStore(t, vfs.NewMem(), n, d, [16]byte{3})
	seed := ownershipView(t, s, 1)
	binding, err := types.NewDefaultAxisBinding(types.GraphID(d.Graph()), temporal.AxisID{31}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	effects, _, err := InitializeDeclaredPartitionWithDefaultAxis(t.Context(), seed, d, binding, nil, Limits{}, GraphLimits{}, ownershipBudget())
	if err != nil {
		t.Fatal(err)
	}
	root, err := effects.Root.AdvanceEffects([32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	image, err := EncodeRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	ownershipInstallRaw(t, s, image, effects.Writes)
	routing, err := NewPartitionRouting(d, 1, 0, []BucketOwnership{{Kind: IdentityRoutingBucket, Partition: 3, Epoch: 1}, {Kind: UniqueRoutingBucket, Partition: 3, Epoch: 1}, {Kind: AxisRoutingBucket, Partition: 3, Epoch: 1}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	configuration := [32]byte{1}
	definition, err := EncodePartitionRoutingBinding(routing, d, binding, configuration, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	a := testAxis(t, 7, temporal.ProfileIntegerZ)
	good, err := encodeAxis(n, a, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	rows := []raftlog.KV{{Key: RoutingDefinitionKey(n), Value: definition}, {Key: axisKey(n, a.Descriptor().ID), Value: good}, {Key: AxisRegistrationKey(n, a.Descriptor().ID), Value: good}}
	slices.SortFunc(rows, func(a, b raftlog.KV) int { return bytes.Compare(a.Key, b.Key) })
	at := ownershipInstallRaw(t, s, image, rows)
	old := ownershipView(t, s, at)
	changed := a.Descriptor()
	changed.Reference = "corrupt-authority:v1"
	other, err := temporal.NewAxis(changed, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	bad, err := encodeAxis(n, other, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	at = ownershipInstallRaw(t, s, image, []raftlog.KV{{Key: AxisRegistrationKey(n, a.Descriptor().ID), Value: bad}})
	current := ownershipView(t, s, at)
	for _, test := range []struct {
		view    *raftlog.ApplicationView
		corrupt bool
	}{{old, false}, {current, true}} {
		v, _, err := OpenPartitionGraphReadView(t.Context(), test.view, d, binding, configuration, Limits{}, GraphLimits{}, ownershipBudget())
		if err != nil {
			t.Fatal(err)
		}
		q, err := v.c.reader(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		q.route = v.route
		err = q.checkAxis(a)
		if test.corrupt {
			if !errors.Is(err, ErrCorrupt) || errors.Is(err, ErrRebinding) {
				t.Fatal("persisted conflict downgraded", err)
			}
		} else if err != nil {
			t.Fatal("retained authority changed", err)
		}
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := test.view.Root(); err != nil {
			t.Fatal("validation consumed borrowed view", err)
		}
	}
	changed = binding.Axis().Descriptor()
	changed.Reference = "corrupt-default:v1"
	other, err = temporal.NewAxis(changed, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	c, _, err := OpenPartitionCatalog(t.Context(), old, n, 2, Limits{}, GraphLimits{}, ownershipBudget())
	if err != nil {
		t.Fatal(err)
	}
	q, err := c.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	scope := &partitionReadScope{binding: binding, namespace: n}
	if err := scope.axis(q, other, nil, false); !errors.Is(err, ErrCorrupt) {
		t.Fatal("default read mismatch was not corruption", err)
	}
	if err := scope.axis(q, other, nil, true); !errors.Is(err, ErrRebinding) {
		t.Fatal("caller default conflict lost cause", err)
	}
}

func TestSharedReaderWideScopeCodecRefusesBeforeUnadmittedNumericAllocation(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	c := f.catalog(t, f.index)
	defer c.view.Close()
	a := testAxis(t, 7, temporal.ProfileRationalQ)
	num, err := temporal.ParseInteger(strings.Repeat("9", 1000), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	den, err := temporal.ParseInteger(strings.Repeat("1", 999), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	fraction, err := temporal.Fraction(num, den, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	position, err := temporal.RationalPosition(a, fraction)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := temporal.Point(position)
	if err != nil {
		t.Fatal(err)
	}
	limits, err := (PageLimits{}).resolve()
	if err != nil {
		t.Fatal(err)
	}
	for _, lane := range []string{"query-scope", "scalar-equality"} {
		t.Run(lane, func(t *testing.T) {
			invoke := func(bytes int) (int, error) {
				arena, _ := graphstate.NewOutputBudget(bytes)
				q, err := c.readerWithOutputBudget(t.Context(), arena)
				if err != nil {
					return 0, err
				}
				pages := pageReader{q: q, limits: limits}
				if lane == "query-scope" {
					wire, err := pages.queryScope(scope)
					return len(wire), err
				}
				key, err := pages.equalityKey(graphstate.ScopeValue(scope))
				return len(key), err
			}
			var refused error
			allocations := testing.AllocsPerRun(20, func() { _, refused = invoke(fullStageMetadataBytes) })
			if !errors.Is(refused, ErrResourceLimit) || allocations > 6 {
				t.Fatalf("%s encoded wide coordinates before exhausted shared admission: allocs%.0f %v", lane, allocations, refused)
			}
			n, err := invoke(16 << 20)
			if err != nil || n < 800 {
				t.Fatal("fitting wide codec guard", n, err)
			}
			positive := testing.AllocsPerRun(20, func() { _, err = invoke(16 << 20) })
			if err != nil || positive <= allocations+4 {
				t.Fatal("allocation control did not execute numeric encoding", positive, allocations, err)
			}
			if _, err := c.view.Root(); err != nil || c.poison != nil {
				t.Fatal("codec refusal poisoned borrow", err, c.poison)
			}
		})
	}
}

func TestOpenedSharedReadViewComponentPageRefusesBeforeWideFingerprintAndProvider(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	f.axis = testAxis(t, 7, temporal.ProfileRationalQ)
	all, err := temporal.All(f.axis)
	if err != nil {
		t.Fatal(err)
	}
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: all})
	c := f.catalog(t, f.index)
	defer c.view.Close()
	num, err := temporal.ParseInteger(strings.Repeat("9", 1000), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	den, err := temporal.ParseInteger(strings.Repeat("1", 999), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	fraction, err := temporal.Fraction(num, den, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	position, err := temporal.RationalPosition(f.axis, fraction)
	if err != nil {
		t.Fatal(err)
	}
	point, err := temporal.Point(position)
	if err != nil {
		t.Fatal(err)
	}
	query := graphstate.ComponentQuery{Key: graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}, Window: point}
	parent, _ := graphstate.NewOutputBudget(16 << 20)
	v, err := openReadViewWithOutputBudget(t.Context(), c, GraphLimits{}, parent)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	// Consume legitimate caller-owned headroom on the same parent; no private
	// counter is manufactured and no independent provider allowance is granted.
	if err := parent.Reserve(parent.Remaining()); err != nil {
		t.Fatal(err)
	}
	before := v.Work()
	var result graphstate.ComponentPage
	var refused error
	allocations := testing.AllocsPerRun(20, func() {
		result, refused = v.ComponentPage(t.Context(), query, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	})
	if !errors.Is(refused, ErrResourceLimit) || !reflect.DeepEqual(result, graphstate.ComponentPage{}) || allocations > 4 || v.Work().Records != before.Records || len(v.pages.cursors) != 0 || len(v.cursors) != 0 {
		t.Fatalf("opened reader bypassed parent before fingerprint/provider: allocations%.0f work%+v->%+v page%+v %v", allocations, before, v.Work(), result, refused)
	}
	// Admit both outer and PageReader fingerprints exactly, leaving 63 bytes:
	// the first provider read cannot pay its 64-byte fixed source-row owner.
	bridgeParent, _ := graphstate.NewOutputBudget(16 << 20)
	bridge, err := openReadViewWithOutputBudget(t.Context(), c, GraphLimits{}, bridgeParent)
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()
	bounds, err := point.EncodingBounds(c.limits.Temporal)
	if err != nil {
		t.Fatal(err)
	}
	fingerprints := 2*bounds.NumericBytes + bounds.WireBytes + 256 + 2*bounds.WireBytes
	if err := bridgeParent.Reserve(bridgeParent.Remaining() - fingerprints - 63); err != nil {
		t.Fatal(err)
	}
	bridgeBefore := bridge.Work()
	page, err := bridge.ComponentPage(t.Context(), query, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(page, graphstate.ComponentPage{}) || bridge.Work().Records != bridgeBefore.Records+1 || bridgeParent.Remaining() != 63 || len(bridge.pages.cursors) != 0 {
		t.Fatal("provider reader escaped shared bridge allowance", bridge.Work(), bridgeBefore, bridgeParent.Remaining(), err)
	}
	fitting, _ := graphstate.NewOutputBudget(16 << 20)
	positive, err := openReadViewWithOutputBudget(t.Context(), c, GraphLimits{}, fitting)
	if err != nil {
		t.Fatal(err)
	}
	defer positive.Close()
	page, err = positive.ComponentPage(t.Context(), query, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if err != nil || !page.Complete || page.Next != 0 || page.Data.Usage().Pieces() != 1 || positive.Work().Records <= 5 {
		t.Fatal("fitting stored wide component", page, err)
	}
	pieces := page.Data.Pieces()
	same, err := pieces[0].Scope().SameSupport(point, temporal.Limits{})
	if err != nil || !same || !pieces[0].Cell().Present() || pieces[0].Cell().Value().ID() != 11 {
		t.Fatal("wrong exact stored wide component", same, err)
	}
	if _, err := c.view.Root(); err != nil || c.poison != nil {
		t.Fatal("parent refusal poisoned borrow", err, c.poison)
	}
}

func partitionAuthorityFixture(t *testing.T) (*raftlog.Store, OwnershipDeclaration, types.DefaultAxisBinding, [32]byte, uint64) {
	t.Helper()
	d, err := NewOwnershipDeclaration(graphstate.GraphID{1}, 9, []PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{3}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{8}}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	n := Namespace{Graph: d.Graph(), Partition: 3}
	store, _, _ := ownershipStore(t, vfs.NewMem(), n, d, [16]byte{3})
	seed := ownershipView(t, store, 1)
	binding, err := types.NewDefaultAxisBinding(types.GraphID(d.Graph()), temporal.AxisID{31}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	effects, _, err := InitializeDeclaredPartitionWithDefaultAxis(t.Context(), seed, d, binding, []graphstate.PropertyDefinition{{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}}, Limits{}, GraphLimits{}, ownershipBudget())
	if err != nil {
		t.Fatal(err)
	}
	root, err := effects.Root.AdvanceEffects([32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	image, err := EncodeRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	ownershipInstallRaw(t, store, image, effects.Writes)
	routing, err := NewPartitionRouting(d, 1, 0, []BucketOwnership{{Kind: IdentityRoutingBucket, Partition: 3, Epoch: 1}, {Kind: UniqueRoutingBucket, Partition: 3, Epoch: 1}, {Kind: AxisRoutingBucket, Partition: 3, Epoch: 1}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := [32]byte{1}
	definition, err := EncodePartitionRoutingBinding(routing, d, binding, cfg, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	publication := RangePublication{Graph: d.Graph(), RangeID: 1, First: 1, Last: 16, InitialPartition: 3, InitialEpoch: 1, Configuration: cfg, SourcePartition: 3, SourceIndex: 1}
	pub, err := EncodeRangePublication(publication)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := EncodeRangeOwnership(RangeOwnership{Graph: d.Graph(), RangeID: 1, Partition: 3, Epoch: 1, Configuration: cfg})
	if err != nil {
		t.Fatal(err)
	}
	rows := []raftlog.KV{{Key: RoutingDefinitionKey(n), Value: definition}, {Key: RangePublicationKey(n, 1), Value: pub}, {Key: RangeEndKey(n, 16), Value: pub}, {Key: RangeOwnershipKey(n, 1), Value: owner}}
	slices.SortFunc(rows, func(a, b raftlog.KV) int { return bytes.Compare(a.Key, b.Key) })
	at := ownershipInstallRaw(t, store, image, rows)
	return store, d, binding, cfg, at
}

func TestPartitionCheckedOpenDirectNilNamespaceBudgetCancelAndClosedRefusals(t *testing.T) {
	store, d, binding, cfg, at := partitionAuthorityFixture(t)
	view := ownershipView(t, store, at)
	wrong, err := NewOwnershipDeclaration(graphstate.GraphID{99}, 9, []PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{3}}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		ctx         context.Context
		view        *raftlog.ApplicationView
		declaration OwnershipDeclaration
		budget      OwnershipBudget
		want        error
	}{
		{nil, view, d, ownershipBudget(), ErrInvalid}, {t.Context(), nil, d, ownershipBudget(), ErrInvalid}, {cancelled, view, d, ownershipBudget(), context.Canceled}, {t.Context(), view, wrong, ownershipBudget(), ErrNamespace}, {t.Context(), view, d, OwnershipBudget{}, ErrInvalid}, {t.Context(), view, d, OwnershipBudget{SourceRows: 1, SourceBytes: 4 << 20, OutputBytes: 16 << 20}, ErrResourceLimit},
	} {
		v, _, err := OpenPartitionGraphReadView(tc.ctx, tc.view, tc.declaration, binding, cfg, Limits{}, GraphLimits{}, tc.budget)
		if !errors.Is(err, tc.want) || v != nil {
			t.Fatal(v, tc.want, err)
		}
		if _, err := view.Root(); err != nil {
			t.Fatal("opener refusal consumed borrow", err)
		}
	}
	for _, cap := range []int{1, 1024, 4096} {
		v, _, err := OpenPartitionGraphReadView(t.Context(), view, d, binding, cfg, Limits{}, GraphLimits{}, OwnershipBudget{SourceRows: 1024, SourceBytes: 4 << 20, OutputBytes: cap})
		if !errors.Is(err, ErrResourceLimit) || v != nil {
			t.Fatal(cap, err)
		}
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if v, _, err := OpenPartitionGraphReadView(t.Context(), view, d, binding, cfg, Limits{}, GraphLimits{}, ownershipBudget()); !errors.Is(err, raftlog.ErrClosed) || v != nil {
		t.Fatal(err)
	}
}

func TestPartitionIdentityCorruptCurrentAuthorityNeverMasksRetainedCanonicalValue(t *testing.T) {
	for _, field := range []string{"counter-checksum", "counter-zero", "counter-limit", "member-missing", "member-checksum", "member-config", "member-length", "member-zero", "member-hash", "local-missing"} {
		t.Run(field, func(t *testing.T) {
			store, d, binding, cfg, at := partitionAuthorityFixture(t)
			view := ownershipView(t, store, at)
			all, err := temporal.All(binding.Axis())
			if err != nil {
				t.Fatal(err)
			}
			revision, _ := state.NewRevision(1, 23)
			effects, _, err := StagePartitionOperations(t.Context(), view, d, binding, cfg, []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: all}, {Kind: graphstate.Set, Owner: 1, Life: 2, Name: "p", Value: graphstate.I64(7), ValueID: 3, Scope: all}}, revision, Limits{}, GraphLimits{}, OwnershipBudget{SourceRows: 1024, SourceBytes: 4 << 20, OutputBytes: 16 << 20})
			if err != nil {
				t.Fatal(err)
			}
			root, err := effects.Root.AdvanceEffects([32]byte{2})
			if err != nil {
				t.Fatal(err)
			}
			image, err := EncodeRoot(root)
			if err != nil {
				t.Fatal(err)
			}
			at = ownershipInstallRaw(t, store, image, effects.Writes)
			old := ownershipView(t, store, at)
			key, err := graphstate.I64(7).EqualityKey(graphstate.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256([]byte(key))
			n := root.Namespace()
			counterKey, memberKey := identityAuthorityKey(n, 6, hash, 0), identityAuthorityKey(n, 7, hash, 0)
			counter, found, err := old.Get(t.Context(), counterKey, 4096)
			if err != nil || !found {
				t.Fatal(found, err)
			}
			member, found, err := old.Get(t.Context(), memberKey, 4096)
			if err != nil || !found {
				t.Fatal(found, err)
			}
			row := raftlog.KV{Key: memberKey, Value: exactCopy(member.Value)}
			expected := ErrCorrupt
			switch field {
			case "counter-checksum":
				row.Key = counterKey
				row.Value = exactCopy(counter.Value)
				row.Value[len(row.Value)-1] ^= 1
			case "counter-zero":
				row.Key = counterKey
				row.Value = exactCopy(counter.Value)
				clear(row.Value[36:44])
				row.Value = sealRecord(row.Value[:len(row.Value)-32])
			case "counter-limit":
				row.Key = counterKey
				row.Value = exactCopy(counter.Value)
				binary.BigEndian.PutUint64(row.Value[36:44], uint64(DefaultLimits().MaxBucketValues+1))
				row.Value = sealRecord(row.Value[:len(row.Value)-32])
				expected = ErrResourceLimit
			case "member-missing":
				row.Deleted = true
				row.Value = nil
			case "member-checksum":
				row.Value[len(row.Value)-1] ^= 1
			case "member-config":
				row.Value[4] ^= 1
				row.Value = sealRecord(row.Value[:len(row.Value)-32])
			case "member-length":
				row.Value[44] ^= 1
				row.Value = sealRecord(row.Value[:len(row.Value)-32])
			case "member-zero":
				clear(row.Value[36:44])
				row.Value = sealRecord(row.Value[:len(row.Value)-32])
			case "member-hash":
				row.Value[48] ^= 1
				row.Value = sealRecord(row.Value[:len(row.Value)-32])
			case "local-missing":
				row.Key = valueKey(n, 3)
				row.Deleted = true
				row.Value = nil
			}
			at = ownershipInstallRaw(t, store, image, []raftlog.KV{row})
			current := ownershipView(t, store, at)
			for _, tc := range []struct {
				view *raftlog.ApplicationView
				bad  bool
			}{{old, false}, {current, true}} {
				v, _, err := OpenPartitionGraphReadView(t.Context(), tc.view, d, binding, cfg, Limits{}, GraphLimits{}, ownershipBudget())
				if err != nil {
					t.Fatal(err)
				}
				got, err := v.ValueIdentity(t.Context(), graphstate.I64(7))
				if tc.bad {
					if !errors.Is(err, expected) || got.Found || got.View != (graphstate.ViewID{}) {
						t.Fatal("corrupt current authority manufactured answer", field, got, err)
					}
				} else {
					if err != nil || !got.Found || got.ID != 3 {
						t.Fatal("current authority corruption changed retained value", got, err)
					}
					q, err := v.begin(t.Context(), graphstate.ReadBudget{})
					if err != nil {
						t.Fatal(err)
					}
					if err := v.route.registerIdentity(q.q, key, 3); err != nil {
						t.Fatal("exact identity retry", err)
					}
					if err := v.route.registerIdentity(q.q, key, 4); !errors.Is(err, ErrRebinding) || errors.Is(err, graphstate.ErrInvalidInput) {
						t.Fatal("derived identity collision became caller-invalid", err)
					}
					if err := v.route.registerIdentity(q.q, key, 0); !errors.Is(err, ErrInvalid) {
						t.Fatal(err)
					}
				}
				if err := v.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := tc.view.Root(); err != nil {
					t.Fatal("read failure consumed borrow", err)
				}
			}
		})
	}
}

func TestLookupPublishedRangeDirectNilCancellationNamespaceAndBorrowLifetime(t *testing.T) {
	store, d, _, cfg, at := partitionAuthorityFixture(t)
	view := ownershipView(t, store, at)
	n := Namespace{Graph: d.Graph(), Partition: 3}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		ctx    context.Context
		view   *raftlog.ApplicationView
		n      Namespace
		id     uint64
		cfg    [32]byte
		budget OwnershipBudget
		want   error
	}{
		{nil, view, n, 1, cfg, ownershipBudget(), ErrInvalid}, {t.Context(), nil, n, 1, cfg, ownershipBudget(), ErrInvalid}, {cancelled, view, n, 1, cfg, ownershipBudget(), context.Canceled}, {t.Context(), view, n, 0, cfg, ownershipBudget(), ErrInvalid}, {t.Context(), view, n, 1, [32]byte{}, ownershipBudget(), ErrInvalid}, {t.Context(), view, Namespace{}, 1, cfg, ownershipBudget(), ErrInvalid}, {t.Context(), view, Namespace{Graph: d.Graph(), Partition: 8}, 1, cfg, ownershipBudget(), ErrNamespace}, {t.Context(), view, n, 1, cfg, OwnershipBudget{}, ErrInvalid},
	} {
		p, o, _, err := LookupPublishedRange(tc.ctx, tc.view, tc.n, tc.id, tc.cfg, tc.budget)
		if !errors.Is(err, tc.want) || p != (RangePublication{}) || o != (RangeOwnership{}) {
			t.Fatal(tc.want, p, o, err)
		}
		if _, err := view.Root(); err != nil {
			t.Fatal("lookup refusal consumed borrow", err)
		}
	}
	if p, o, _, err := LookupPublishedRange(t.Context(), view, n, 1, cfg, ownershipBudget()); err != nil || !p.Contains(1) || o.Partition != 3 {
		t.Fatal(p, o, err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if p, o, _, err := LookupPublishedRange(t.Context(), view, n, 1, cfg, ownershipBudget()); !errors.Is(err, raftlog.ErrClosed) || p != (RangePublication{}) || o != (RangeOwnership{}) {
		t.Fatal(err)
	}
}
