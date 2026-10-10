package graphstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

func partitionInitDeclaration(t *testing.T) OwnershipDeclaration {
	t.Helper()
	d, err := NewOwnershipDeclaration(graphstate.GraphID{1}, 7, ownershipEntries(3), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestBootstrapBoundOwnershipExactBindingMembershipAndFreshness(t *testing.T) {
	d := partitionInitDeclaration(t)
	local := Namespace{d.Graph(), 2}
	config := ownershipConfig(vfs.NewMem(), local, [16]byte{9}, 2)
	s, err := raftlog.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	actual := s.ApplicationBinding()
	wrong := actual
	wrong.Identity.Group[0]++
	if err := BootstrapBoundOwnership(s, wrong, d, [3]uint64{1, 2, 3}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	wrong = actual
	wrong.Identity.Partition++
	if err := BootstrapBoundOwnership(s, wrong, d, [3]uint64{1, 2, 3}); !errors.Is(err, ErrNamespace) {
		t.Fatal(err)
	}
	for _, voters := range [][3]uint64{{1, 1, 3}, {1, 3, 4}, {0, 2, 3}} {
		if err := BootstrapBoundOwnership(s, actual, d, voters); !errors.Is(err, ErrInvalid) {
			t.Fatal(voters, err)
		}
	}
	if err := BootstrapBoundOwnership(nil, actual, d, [3]uint64{1, 2, 3}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := BootstrapBoundOwnership(s, actual, d, [3]uint64{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	read, work, err := ReadOwnershipDeclaration(t.Context(), ownershipView(t, s, 1), Limits{}, ownershipBudget())
	if err != nil || read.Root().ownershipMode != ownershipPending || read.Root().topology.epoch != 7 || read.Root().owner != 11 || read.Binding() != actual || read.Published() || work.Records != 1 {
		t.Fatal(read, work, err)
	}
	if err := BootstrapBoundOwnership(s, actual, d, [3]uint64{1, 2, 3}); !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal("reinitialized store", err)
	}
}

func TestInitializeDeclaredPartitionPendingPublishedRetainedAndReopen(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "published"}[published], func(t *testing.T) {
			d := partitionInitDeclaration(t)
			local := Namespace{d.Graph(), 1}
			s, config, seed := ownershipStore(t, vfs.NewMem(), local, d, [16]byte{9})
			at := uint64(1)
			if published {
				_, at = ownershipPublish(t, s, d)
			}
			old := ownershipView(t, s, at)
			base, err := old.Root()
			if err != nil {
				t.Fatal(err)
			}
			before, err := s.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			schema := graphstate.PropertyDefinition{Name: "name", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}
			effects, work, err := InitializeDeclaredPartition(t.Context(), old, d, []graphstate.PropertyDefinition{schema}, Limits{}, GraphLimits{}, ownershipBudget())
			if err != nil || effects.Root.ownershipMode != ownershipInitialized || effects.Root.topology != (topologyDeclaration{7, 1, 3}) || effects.Root.epoch != 0 || effects.Root.effect != seed.effect || effects.Root.next != 6 || work.Records < 2 || effects.Work != work || !reflect.DeepEqual(effects.Base, base) {
				t.Fatal(effects.Root, work, err)
			}
			wantWrites := 8
			if published {
				wantWrites--
			}
			if len(effects.Writes) != wantWrites {
				t.Fatal("physical/schema/declaration writes", len(effects.Writes), wantWrites)
			}
			declKey := ownershipKey(d.Graph(), d.TopologyEpoch(), d.Digest())
			declWrites := 0
			for _, row := range effects.Writes {
				if bytes.Equal(row.Key, declKey) {
					declWrites++
				}
				if cap(row.Key) != len(row.Key) || cap(row.Value) != len(row.Value) {
					t.Fatal("output spare capacity")
				}
			}
			if declWrites != map[bool]int{false: 1, true: 0}[published] {
				t.Fatal("published declaration rewritten", declWrites)
			}
			usage, err := s.ApplicationUsage()
			if err != nil || usage != before {
				t.Fatal("computed initialization published", usage, before, err)
			}
			staged, err := EncodeRoot(effects.Root)
			if err != nil || len(staged) != 172 || staged[3] != 5 {
				t.Fatal(staged, err)
			}
			legacy := bytes.Clone(staged)
			binary.BigEndian.PutUint64(legacy[100:108], 2)
			sum := sha256.Sum256(legacy[:140])
			copy(legacy[140:], sum[:])
			if _, err := DecodeRoot(legacy); !errors.Is(err, ErrCorrupt) {
				t.Fatal("invented GR3 legacy2 admission", err)
			}
			installed, err := effects.Root.AdvanceEffects(sha256.Sum256([]byte("co-composed logical initialization")))
			if err != nil {
				t.Fatal(err)
			}
			image, err := EncodeRoot(installed)
			if err != nil {
				t.Fatal(err)
			}
			index := ownershipInstallRaw(t, s, image, effects.Writes)
			current := ownershipView(t, s, index)
			check := func(v *raftlog.ApplicationView) {
				t.Helper()
				c, used, err := OpenPartitionCatalog(t.Context(), v, local, seed.owner, Limits{}, GraphLimits{}, ownershipBudget())
				if err != nil || c == nil || used.Records != 7 || c.localFull == nil || c.localFull.topology != 7 {
					t.Fatal(c, used, err)
				}
				got, found, err := c.Property(t.Context(), graphstate.Node, "name")
				if err != nil || !found || got != schema {
					t.Fatal(got, found, err)
				}
				if _, found, err := c.Property(t.Context(), graphstate.Node, "phantom"); err != nil || found {
					t.Fatal("phantom schema", found, err)
				}
				if _, found, err := c.Entity(t.Context(), EntityRef{d.Graph(), 999}); err != nil || found {
					t.Fatal("phantom entity", found, err)
				}
				if _, err := c.Root(); err != nil {
					t.Fatal(err)
				}
				if _, err := OpenReadView(t.Context(), c, GraphLimits{}); !errors.Is(err, ErrTopologyUnsupported) {
					t.Fatal("local storage became complete graph", err)
				}
				revision, err := state.NewRevision(1, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := StageOperations(t.Context(), c, nil, revision, GraphLimits{}); !errors.Is(err, ErrTopologyUnsupported) {
					t.Fatal("local storage became graph writer", err)
				}
				if _, err := c.NewStage(t.Context()); !errors.Is(err, ErrTopologyUnsupported) {
					t.Fatal(err)
				}
				if _, err := NewPageReader(c, PageLimits{}); !errors.Is(err, ErrTopologyUnsupported) {
					t.Fatal("local catalog gained paging capability", err)
				}
				if _, found, err := c.Property(t.Context(), graphstate.Node, "name"); err != nil || !found {
					t.Fatal("capability refusal poisoned borrowed catalog", found, err)
				}
				if _, err := v.Root(); err != nil {
					t.Fatal("capability refusal closed borrowed view", err)
				}
				if _, err := OpenCatalog(v, local, seed.owner, Limits{}); !errors.Is(err, ErrTopologyUnsupported) {
					t.Fatal("legacy catalog gained GR3", err)
				}
				if _, err := installed.SinglePartition(); !errors.Is(err, ErrTopologyUnsupported) {
					t.Fatal(err)
				}
			}
			check(current)
			if c, _, err := OpenPartitionCatalog(t.Context(), old, local, seed.owner, Limits{}, GraphLimits{}, ownershipBudget()); !errors.Is(err, ErrTopologyUnsupported) || c != nil {
				t.Fatal("old metadata gained current readiness", c, err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			config.Create = false
			reopened, err := raftlog.Open(config)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			check(ownershipView(t, reopened, index))
			if c, _, err := OpenPartitionCatalog(t.Context(), ownershipView(t, reopened, at), local, seed.owner, Limits{}, GraphLimits{}, ownershipBudget()); !errors.Is(err, ErrTopologyUnsupported) || c != nil {
				t.Fatal("reopen old metadata readiness", c, err)
			}
		})
	}
}

func TestInitializeDeclaredPartitionRefusesHiddenVersionsBindingAndBudgets(t *testing.T) {
	d := partitionInitDeclaration(t)
	local := Namespace{d.Graph(), 1}
	for _, published := range []bool{false, true} {
		s, _, root := ownershipStore(t, vfs.NewMem(), local, d, [16]byte{9})
		if published {
			_, _ = ownershipPublish(t, s, d)
			root.ownershipMode = ownershipPublished
		}
		image, err := EncodeRoot(root)
		if err != nil {
			t.Fatal(err)
		}
		index := ownershipInstallRaw(t, s, image, []raftlog.KV{{Key: []byte("hidden"), Deleted: true}})
		v := ownershipView(t, s, index)
		effects, work, err := InitializeDeclaredPartition(t.Context(), v, d, nil, Limits{}, GraphLimits{}, ownershipBudget())
		if !errors.Is(err, raftlog.ErrInvalid) || !reflect.DeepEqual(effects, GraphEffects{}) || work.Records < 1 {
			t.Fatal("hidden tombstone reinitialized", effects, work, err)
		}
		if _, err := v.Root(); err != nil {
			t.Fatal("ordinary refusal poisoned borrow", err)
		}
	}
	s, _, _ := ownershipStore(t, vfs.NewMem(), local, d, [16]byte{9})
	v := ownershipView(t, s, 1)
	success, work, err := InitializeDeclaredPartition(t.Context(), v, d, nil, Limits{}, GraphLimits{}, ownershipBudget())
	if err != nil {
		t.Fatal(err)
	}
	for _, dimension := range []string{"source", "output"} {
		for _, delta := range []int{-1, 0, 1} {
			budget := ownershipBudget()
			if dimension == "source" {
				budget.SourceBytes = work.Bytes + delta
			} else {
				budget.OutputBytes = success.OwnedBytes + delta
			}
			got, used, err := InitializeDeclaredPartition(t.Context(), v, d, nil, Limits{}, GraphLimits{}, budget)
			if delta < 0 {
				if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(got, GraphEffects{}) || used.Bytes > budget.SourceBytes {
					t.Fatal(dimension, delta, got, used, err)
				}
			} else if err != nil || got.Root != success.Root || used != work {
				t.Fatal(dimension, delta, got.Root, used, err)
			}
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, used, err := InitializeDeclaredPartition(ctx, v, d, nil, Limits{}, GraphLimits{}, ownershipBudget()); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, GraphEffects{}) || used != (PageWork{}) {
		t.Fatal(got, used, err)
	}
	if _, _, err := InitializeDeclaredPartition(t.Context(), nil, d, nil, Limits{}, GraphLimits{}, ownershipBudget()); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	wrong, _, _ := ownershipStore(t, vfs.NewMem(), local, d, [16]byte{99})
	if got, used, err := InitializeDeclaredPartition(t.Context(), ownershipView(t, wrong, 1), d, nil, Limits{}, GraphLimits{}, ownershipBudget()); !errors.Is(err, ErrNamespace) || !reflect.DeepEqual(got, GraphEffects{}) || used.Bytes == 0 {
		t.Fatal(got, used, err)
	}
}

func TestOpenPartitionCatalogCorruptCurrentRetainsOldAndReadBudgets(t *testing.T) {
	d := partitionInitDeclaration(t)
	local := Namespace{d.Graph(), 1}
	for _, mutation := range []string{"descriptor", "own page", "declaration", "staged epoch"} {
		t.Run(mutation, func(t *testing.T) {
			s, _, seed := ownershipStore(t, vfs.NewMem(), local, d, [16]byte{9})
			effects, _, err := InitializeDeclaredPartition(t.Context(), ownershipView(t, s, 1), d, nil, Limits{}, GraphLimits{}, ownershipBudget())
			if err != nil {
				t.Fatal(err)
			}
			root, err := effects.Root.AdvanceEffects(sha256.Sum256([]byte("initialized")))
			if err != nil {
				t.Fatal(err)
			}
			image, err := EncodeRoot(root)
			if err != nil {
				t.Fatal(err)
			}
			index := ownershipInstallRaw(t, s, image, effects.Writes)
			old := ownershipView(t, s, index)
			c, work, err := OpenPartitionCatalog(t.Context(), old, local, seed.owner, Limits{}, GraphLimits{}, ownershipBudget())
			if err != nil || c == nil || work.Records != 7 {
				t.Fatal(c, work, err)
			}
			if mutation == "descriptor" {
				for _, dimension := range []string{"bytes", "rows", "output"} {
					b := ownershipBudget()
					switch dimension {
					case "bytes":
						b.SourceBytes = work.Bytes - 1
					case "rows":
						b.SourceRows = 6
					case "output":
						b.OutputBytes = ownershipMetadataBytes + fullStageMetadataBytes - 1
					}
					got, used, err := OpenPartitionCatalog(t.Context(), old, local, seed.owner, Limits{}, GraphLimits{}, b)
					if !errors.Is(err, ErrResourceLimit) || got != nil || used.Bytes > b.SourceBytes || used.Records > b.SourceRows {
						t.Fatal(dimension, got, used, err)
					}
				}
			}
			var row raftlog.KV
			for _, candidate := range effects.Writes {
				matches := mutation == "descriptor" && bytes.Equal(candidate.Key, componentIndexDescriptorKey(local)) || mutation == "own page" && candidate.Key[0] == byte(currentPresenceRecord) || mutation == "declaration" && candidate.Key[0] == ownershipDeclarationRecord
				if matches {
					row = raftlog.KV{Key: bytes.Clone(candidate.Key), Value: bytes.Clone(candidate.Value)}
					row.Value[0] ^= 1
					break
				}
			}
			want := ErrCorrupt
			var writes []raftlog.KV
			if mutation == "staged epoch" {
				root.epoch = 0
				image, err = EncodeRoot(root)
				if err != nil {
					t.Fatal(err)
				}
				want = ErrTopologyUnsupported
			} else {
				if len(row.Key) == 0 {
					t.Fatal("fixture failed to select actual record", mutation)
				}
				writes = []raftlog.KV{row}
			}
			badIndex := ownershipInstallRaw(t, s, image, writes)
			if got, used, err := OpenPartitionCatalog(t.Context(), ownershipView(t, s, badIndex), local, seed.owner, Limits{}, GraphLimits{}, ownershipBudget()); !errors.Is(err, want) || got != nil || used.Bytes == 0 {
				t.Fatal("corrupt/unpublished current readiness", got, used, err)
			}
			if got, _, err := OpenPartitionCatalog(t.Context(), old, local, seed.owner, Limits{}, GraphLimits{}, ownershipBudget()); err != nil || got == nil {
				t.Fatal("later corruption repaired/invalidated retained catalog", got, err)
			}
		})
	}
}

func TestOpenPartitionCatalogInvalidBorrowAndQualifiedRefusals(t *testing.T) {
	d := partitionInitDeclaration(t)
	local := Namespace{d.Graph(), 1}
	s, _, root := ownershipStore(t, vfs.NewMem(), local, d, [16]byte{9})
	v := ownershipView(t, s, 1)
	before, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		name  string
		ctx   context.Context
		view  *raftlog.ApplicationView
		local Namespace
		owner uint64
		want  error
	}{
		{"nil view", t.Context(), nil, local, root.owner, ErrInvalid},
		{"nil context", nil, v, local, root.owner, ErrInvalid},
		{"cancellation", canceled, v, local, root.owner, context.Canceled},
		{"foreign graph", t.Context(), v, Namespace{graphstate.GraphID{2}, 1}, root.owner, ErrNamespace},
		{"foreign partition", t.Context(), v, Namespace{d.Graph(), 2}, root.owner, ErrNamespace},
		{"stale owner", t.Context(), v, local, root.owner + 1, ErrStaleOwner},
		{"zero owner", t.Context(), v, local, 0, ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, work, err := OpenPartitionCatalog(tc.ctx, tc.view, tc.local, tc.owner, Limits{}, GraphLimits{}, ownershipBudget())
			if !errors.Is(err, tc.want) || got != nil {
				t.Fatal(got, work, err)
			}
			if tc.want == ErrNamespace || tc.want == ErrStaleOwner {
				if work.Bytes != ownershipMetadataBytes+ownershipRootBytes || work.Records != 0 {
					t.Fatal("qualified refusal lost actual root work", work)
				}
			} else if work != (PageWork{}) {
				t.Fatal("pre-admission refusal reported work", work)
			}
			if _, err := v.Root(); err != nil {
				t.Fatal("refusal closed/poisoned borrowed view", err)
			}
			if usage, err := s.ApplicationUsage(); err != nil || usage != before {
				t.Fatal("refusal changed application accounting", usage, before, err)
			}
		})
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if got, work, err := OpenPartitionCatalog(t.Context(), v, local, root.owner, Limits{}, GraphLimits{}, ownershipBudget()); !errors.Is(err, raftlog.ErrClosed) || got != nil || work.Bytes != ownershipMetadataBytes+ownershipRootBytes {
		t.Fatal(got, work, err)
	}
}

func TestCheckDeclaredPartitionSeedExactProofHistoryAndAdmission(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "published"}[published], func(t *testing.T) {
			d := partitionInitDeclaration(t)
			s, _, _ := ownershipStore(t, vfs.NewMem(), Namespace{d.Graph(), 1}, d, [16]byte{9})
			old := ownershipView(t, s, 1)
			at := uint64(1)
			if published {
				_, at = ownershipPublish(t, s, d)
			}
			v := ownershipView(t, s, at)
			original, err := v.Root()
			if err != nil {
				t.Fatal(err)
			}
			before, err := s.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			base, root, work, err := CheckDeclaredPartitionSeed(t.Context(), v, d, Limits{}, ownershipBudget())
			if err != nil || !reflect.DeepEqual(base, original) || root.next != 1 || root.epoch != 0 || root.topology.index != 0 || root.ownershipMode != map[bool]byte{false: ownershipPending, true: ownershipPublished}[published] || work.Records != map[bool]int{false: 1, true: 2}[published] || cap(base.Image) != len(base.Image) {
				t.Fatal(base, root, work, err)
			}
			base.Image[0] ^= 1
			for _, delta := range []int{-1, 0, 1} {
				b := ownershipBudget()
				b.SourceBytes = work.Bytes + delta
				got, seed, used, err := CheckDeclaredPartitionSeed(t.Context(), v, d, Limits{}, b)
				if delta < 0 {
					if !errors.Is(err, ErrResourceLimit) || got.Index != 0 || seed != (Root{}) || used.Bytes > b.SourceBytes {
						t.Fatal(got, seed, used, err)
					}
				} else if err != nil || !reflect.DeepEqual(got, original) || seed != root || used != work {
					t.Fatal("proof alias/exact-cap boundary", got, seed, used, err)
				}
			}
			b := ownershipBudget()
			b.OutputBytes = ownershipMetadataBytes + ownershipRootBytes - 1
			if got, seed, used, err := CheckDeclaredPartitionSeed(t.Context(), v, d, Limits{}, b); !errors.Is(err, ErrResourceLimit) || got.Index != 0 || seed != (Root{}) || used.Bytes != ownershipMetadataBytes+ownershipRootBytes || used.Records != 0 {
				t.Fatal("seed output admission", got, seed, used, err)
			}
			if published {
				b = ownershipBudget()
				b.SourceRows = 1
				if got, seed, used, err := CheckDeclaredPartitionSeed(t.Context(), v, d, Limits{}, b); !errors.Is(err, ErrResourceLimit) || got.Index != 0 || seed != (Root{}) || used.Records != 1 {
					t.Fatal("proof point read did not share declaration row budget", got, seed, used, err)
				}
				if got, seed, _, err := CheckDeclaredPartitionSeed(t.Context(), old, d, Limits{}, ownershipBudget()); !errors.Is(err, raftlog.ErrInvalid) || got.Index != 0 || seed != (Root{}) {
					t.Fatal("retained pending seed became current authority", got, seed, err)
				}
			}
			if usage, err := s.ApplicationUsage(); err != nil || usage != before {
				t.Fatal("proof allocated roots/persisted effects", usage, before, err)
			}
			if _, err := v.Root(); err != nil {
				t.Fatal("proof refusal closed/poisoned borrow", err)
			}
		})
	}
}

func TestCheckDeclaredPartitionSeedAndInitializerRejectMalformedAndAdvancedState(t *testing.T) {
	d := partitionInitDeclaration(t)
	local := Namespace{d.Graph(), 1}
	s, _, root := ownershipStore(t, vfs.NewMem(), local, d, [16]byte{9})
	v := ownershipView(t, s, 1)
	other, err := NewOwnershipDeclaration(d.Graph(), 8, ownershipEntries(3), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		name        string
		ctx         context.Context
		view        *raftlog.ApplicationView
		declaration OwnershipDeclaration
		limits      Limits
		graph       GraphLimits
		budget      OwnershipBudget
		want        error
	}{
		{"nil view", t.Context(), nil, d, Limits{}, GraphLimits{}, ownershipBudget(), ErrInvalid},
		{"nil context", nil, v, d, Limits{}, GraphLimits{}, ownershipBudget(), ErrInvalid},
		{"canceled", canceled, v, d, Limits{}, GraphLimits{}, ownershipBudget(), context.Canceled},
		{"invalid catalog", t.Context(), v, d, Limits{MaxReadRows: -1}, GraphLimits{}, ownershipBudget(), ErrInvalid},
		{"invalid graph", t.Context(), v, d, Limits{}, GraphLimits{MaxSourceRows: -1}, ownershipBudget(), ErrInvalid},
		{"invalid budget", t.Context(), v, d, Limits{}, GraphLimits{}, OwnershipBudget{0, 4096, 4096}, ErrInvalid},
		{"exhausted input", t.Context(), v, d, Limits{}, GraphLimits{}, OwnershipBudget{64, 1, 4096}, ErrResourceLimit},
		{"zero declaration", t.Context(), v, OwnershipDeclaration{}, Limits{}, GraphLimits{}, ownershipBudget(), ErrInvalid},
		{"rebound declaration", t.Context(), v, other, Limits{}, GraphLimits{}, ownershipBudget(), ErrRebinding},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name != "invalid graph" {
				base, seed, used, err := CheckDeclaredPartitionSeed(tc.ctx, tc.view, tc.declaration, tc.limits, tc.budget)
				if !errors.Is(err, tc.want) || base.Index != 0 || seed != (Root{}) || used.Bytes > tc.budget.SourceBytes {
					t.Fatal(base, seed, used, err)
				}
			}
			effects, used, err := InitializeDeclaredPartition(tc.ctx, tc.view, tc.declaration, nil, tc.limits, tc.graph, tc.budget)
			if !errors.Is(err, tc.want) || !reflect.DeepEqual(effects, GraphEffects{}) || used.Bytes > tc.budget.SourceBytes {
				t.Fatal(effects, used, err)
			}
			if _, err := v.Root(); err != nil {
				t.Fatal("ordinary rejection poisoned borrow", err)
			}
		})
	}
	advanced, _, err := root.ReservePhysical(1)
	if err != nil {
		t.Fatal(err)
	}
	image, err := EncodeRoot(advanced)
	if err != nil {
		t.Fatal(err)
	}
	index := ownershipInstallRaw(t, s, image, nil)
	if base, seed, used, err := CheckDeclaredPartitionSeed(t.Context(), ownershipView(t, s, index), d, Limits{}, ownershipBudget()); !errors.Is(err, ErrTopologyUnsupported) || base.Index != 0 || seed != (Root{}) || used.Records != 0 {
		t.Fatal(base, seed, used, err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if base, seed, _, err := CheckDeclaredPartitionSeed(t.Context(), v, d, Limits{}, ownershipBudget()); !errors.Is(err, raftlog.ErrClosed) || base.Index != 0 || seed != (Root{}) {
		t.Fatal(base, seed, err)
	}
}

func TestBootstrapBoundOwnershipRejectsConfigAndDeclarationBeforePublication(t *testing.T) {
	d := partitionInitDeclaration(t)
	local := Namespace{d.Graph(), 1}
	for _, name := range []string{"disabled", "unbound", "zero binding", "zero declaration", "foreign graph", "missing partition", "wrong group", "configured membership", "image cap"} {
		t.Run(name, func(t *testing.T) {
			cfg := ownershipConfig(vfs.NewMem(), local, [16]byte{9}, 1)
			expected := raftlog.ApplicationBinding{Identity: cfg.Transfer.Identity, SemanticContractID: cfg.SemanticContractID}
			decl := d
			voters := [3]uint64{1, 2, 3}
			want := ErrInvalid
			switch name {
			case "disabled":
				cfg = raftlog.Config{Dir: "disabled", FS: vfs.NewMem(), Create: true}
				want = ErrTopologyUnsupported
			case "unbound":
				cfg = raftlog.Config{Dir: "unbound", FS: vfs.NewMem(), Create: true, Application: raftlog.DefaultApplicationPolicy(1)}
			case "zero binding":
				expected = raftlog.ApplicationBinding{}
			case "zero declaration":
				decl = OwnershipDeclaration{}
			case "foreign graph":
				decl, _ = NewOwnershipDeclaration(graphstate.GraphID{2}, 7, ownershipEntries(3), Limits{})
				want = ErrNamespace
			case "missing partition":
				decl, _ = NewOwnershipDeclaration(d.Graph(), 7, []PartitionOwnership{{2, 11, [16]byte{9}}}, Limits{})
				want = ErrNamespace
			case "wrong group":
				entries := ownershipEntries(3)
				entries[0].Group[0] = 99
				decl, _ = NewOwnershipDeclaration(d.Graph(), 7, entries, Limits{})
				want = ErrNamespace
			case "configured membership":
				voters = [3]uint64{1, 2, 4}
			case "image cap":
				cfg.Application.MaxImageBytes = ownershipRootBytes - 1
				cfg.Transfer.Contract = raftlog.ApplicationContractForPolicy(cfg.Application)
				want = ErrResourceLimit
			}
			s, err := raftlog.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err := BootstrapBoundOwnership(s, expected, decl, voters); !errors.Is(err, want) {
				t.Fatal(name, err)
			}
			if hard, conf, err := s.InitialState(); err != nil || hard.GetCommit() != 0 || len(conf.GetVoters()) != 0 {
				t.Fatal("rejection published seed", hard, conf, err)
			}
		})
	}
}

func TestInitializeDeclaredPartitionSchemaRefusalsAndReadyRetry(t *testing.T) {
	d := partitionInitDeclaration(t)
	local := Namespace{d.Graph(), 1}
	s, _, root := ownershipStore(t, vfs.NewMem(), local, d, [16]byte{9})
	v := ownershipView(t, s, 1)
	definition := graphstate.PropertyDefinition{Name: "name", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}
	wide := make([]graphstate.PropertyDefinition, 400)
	for i := range wide {
		wide[i] = definition
		wide[i].Name = fmt.Sprintf("%0256d", i)
	}
	for _, tc := range []struct {
		name    string
		schemas []graphstate.PropertyDefinition
		limits  Limits
		want    error
	}{
		{"invalid schema", []graphstate.PropertyDefinition{{Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}}, Limits{}, ErrInvalid},
		{"duplicate schema", []graphstate.PropertyDefinition{definition, definition}, Limits{}, ErrInvalid},
		{"stage rows", []graphstate.PropertyDefinition{definition}, Limits{MaxStageRecords: 7}, ErrResourceLimit},
		{"stage bytes", wide, Limits{MaxStageBytes: DefaultLimits().MaxRecordBytes + 256}, ErrResourceLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, err := s.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			effects, used, err := InitializeDeclaredPartition(t.Context(), v, d, tc.schemas, tc.limits, GraphLimits{}, ownershipBudget())
			if !errors.Is(err, tc.want) || !reflect.DeepEqual(effects, GraphEffects{}) || used.Bytes == 0 {
				t.Fatal(effects, used, err)
			}
			if usage, err := s.ApplicationUsage(); err != nil || usage != before {
				t.Fatal("refusal changed durable/borrow accounting", usage, before, err)
			}
			if _, err := v.Root(); err != nil {
				t.Fatal(err)
			}
		})
	}
	effects, _, err := InitializeDeclaredPartition(t.Context(), v, d, nil, Limits{}, GraphLimits{}, ownershipBudget())
	if err != nil {
		t.Fatal(err)
	}
	ready, err := effects.Root.AdvanceEffects(sha256.Sum256([]byte("logical ready")))
	if err != nil {
		t.Fatal(err)
	}
	image, err := EncodeRoot(ready)
	if err != nil {
		t.Fatal(err)
	}
	at := ownershipInstallRaw(t, s, image, effects.Writes)
	current := ownershipView(t, s, at)
	if out, used, err := InitializeDeclaredPartition(t.Context(), current, d, nil, Limits{}, GraphLimits{}, ownershipBudget()); !errors.Is(err, ErrTopologyUnsupported) || !reflect.DeepEqual(out, GraphEffects{}) || used.Records != 0 {
		t.Fatal(out, used, err)
	}
	if base, seed, used, err := CheckDeclaredPartitionSeed(t.Context(), current, d, Limits{}, ownershipBudget()); !errors.Is(err, ErrTopologyUnsupported) || base.Index != 0 || seed != (Root{}) || used.Records != 0 {
		t.Fatal(base, seed, used, err)
	}
	if c, _, err := OpenPartitionCatalog(t.Context(), current, local, root.owner, Limits{}, GraphLimits{}, ownershipBudget()); err != nil || c == nil {
		t.Fatal(c, err)
	}
}

func TestCheckDeclaredPartitionSeedRejectsHiddenAndCorruptPublishedRecords(t *testing.T) {
	d := partitionInitDeclaration(t)
	for _, mutation := range []string{"hidden tombstone", "duplicate declaration version", "missing declaration", "corrupt declaration", "pending with declaration"} {
		t.Run(mutation, func(t *testing.T) {
			s, _, root := ownershipStore(t, vfs.NewMem(), Namespace{d.Graph(), 1}, d, [16]byte{9})
			wire, err := encodeOwnershipDeclaration(d, DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			row := raftlog.KV{Key: ownershipKey(d.Graph(), d.TopologyEpoch(), d.Digest()), Value: wire}
			root.ownershipMode = ownershipPublished
			image, err := EncodeRoot(root)
			if err != nil {
				t.Fatal(err)
			}
			var rows []raftlog.KV
			want := ErrCorrupt
			switch mutation {
			case "hidden tombstone", "duplicate declaration version":
				at := ownershipInstallRaw(t, s, image, []raftlog.KV{row})
				base, _, _, err := CheckDeclaredPartitionSeed(t.Context(), ownershipView(t, s, at), d, Limits{}, ownershipBudget())
				if err != nil || base.Index != at {
					t.Fatal("positive sole declaration control", base, err)
				}
				rows = []raftlog.KV{{Key: []byte("unrelated"), Deleted: true}}
				if mutation == "duplicate declaration version" {
					rows = []raftlog.KV{row}
				}
				want = raftlog.ErrInvalid
			case "corrupt declaration":
				row.Value[0] ^= 1
				rows = []raftlog.KV{row}
			case "pending with declaration":
				root.ownershipMode = ownershipPending
				image, err = EncodeRoot(root)
				if err != nil {
					t.Fatal(err)
				}
				rows = []raftlog.KV{row}
			}
			index := ownershipInstallRaw(t, s, image, rows)
			v := ownershipView(t, s, index)
			before, err := s.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			base, seed, work, err := CheckDeclaredPartitionSeed(t.Context(), v, d, Limits{}, ownershipBudget())
			if !errors.Is(err, want) || base.Index != 0 || seed != (Root{}) || work.Records != 1 || work.Bytes == 0 {
				t.Fatal(base, seed, work, err)
			}
			if usage, err := s.ApplicationUsage(); err != nil || usage != before {
				t.Fatal("seed proof refusal changed application", usage, before, err)
			}
			if _, err := v.Root(); err != nil {
				t.Fatal("proof refusal closed/poisoned borrow", err)
			}
		})
	}
}
