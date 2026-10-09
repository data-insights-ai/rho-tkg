package graphstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func indexSchemas() []graphstate.PropertyDefinition {
	return []graphstate.PropertyDefinition{{Name: "scalar", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}, {Name: "scalar", Owner: graphstate.Relationship, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}, {Name: "set", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.SetCardinality}, {Name: "set", Owner: graphstate.Relationship, Type: graphstate.ScalarI64, Cardinality: graphstate.SetCardinality}}
}
func emptyIndexed(t *testing.T, l PageLimits) (*raftlog.Store, Root, uint64) {
	t.Helper()
	db := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	seed := bootstrapRoot(t, db)
	c := openCatalog(t, db, 1, Limits{})
	initialized, err := initializeIndexes(t.Context(), c, indexSchemas(), l)
	if err != nil {
		t.Fatal(err)
	}
	if initialized.baseIndex != 1 || initialized.baseGeneration != 0 {
		t.Fatal("wrong captured base", initialized)
	}
	if seed.SemanticEpoch() != initialized.root.SemanticEpoch() || seed.EffectDigest() != initialized.root.EffectDigest() || initialized.root.next != 2 {
		t.Fatal("initializer allocated semantic IDs/effects", initialized.root)
	}
	rows, err := initialized.stage.Writes()
	if err != nil || len(rows) != 6 {
		t.Fatal(rows, err)
	}
	root, index := commitStage(t, db, initialized.root, initialized.stage)
	if err := initialized.stage.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.view.Close(); err != nil {
		t.Fatal(err)
	}
	return db, root, index
}
func indexedFixture(t *testing.T, l PageLimits) (*pageFixture, uint64) {
	t.Helper()
	db, root, emptyIndex := emptyIndexed(t, l)
	c := openCatalog(t, db, emptyIndex, Limits{})
	s, err := newIndexedStage(t.Context(), c, l)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	axis := testAxis(t, 1, temporal.ProfileIntegerZ)
	for _, entity := range []graphstate.EntityRecord{{ID: 1, Kind: graphstate.Node, Axis: axis}, {ID: 2, Kind: graphstate.Relationship, Axis: axis, Type: "LINK", Source: 1, Target: 1, Mode: graphstate.IdentityReference}} {
		if err := s.Entity(t.Context(), refEntity(uint64(entity.ID)), entity); err != nil {
			t.Fatal(err)
		}
		for _, life := range []graphstate.LifeID{11, 12} {
			if err := s.Life(t.Context(), refLife(uint64(entity.ID), uint64(life)), graphstate.LifeRecord{Owner: entity.ID, Life: life}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.Value(t.Context(), refValue(99), graphstate.I64(7)); err != nil {
		t.Fatal(err)
	}
	root, index := commitStage(t, db, root, s)
	resolved, err := l.resolve()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.c.view.Close(); err != nil {
		t.Fatal(err)
	}
	return &pageFixture{db: db, root: root, index: index, axis: axis, key: graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}, limits: resolved}, emptyIndex
}
func indexedStage(t *testing.T, f *pageFixture) *Stage {
	t.Helper()
	c := openCatalog(t, f.db, f.index, Limits{})
	s, err := newIndexedStage(t.Context(), c, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func indexCommitPatches(t *testing.T, f *pageFixture, patches ...graphstate.ComponentPatch) {
	t.Helper()
	s := indexedStage(t, f)
	result, err := StageComponentPatches(t.Context(), s, f.root, patches, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	if result.Root != s.indexed.root {
		t.Fatal("root authority did not advance atomically")
	}
	f.root, f.index = commitStage(t, f.db, result.Root, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.c.view.Close(); err != nil {
		t.Fatal(err)
	}
}
func readIndexedKeys(t *testing.T, p *componentKeyReader, predicate graphstate.KeyPredicate) []graphstate.ComponentKey {
	t.Helper()
	var keys []graphstate.ComponentKey
	var token graphstate.Cursor
	for i := 0; i < 200; i++ {
		page, err := p.ComponentKeys(t.Context(), predicate, token, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if page.View != p.id || page.Version != graphstate.ReadVersion(p.index) {
			t.Fatal("unbound page")
		}
		keys = append(keys, page.Keys...)
		if page.Complete {
			if page.Next != 0 {
				t.Fatal("terminal cursor")
			}
			return keys
		}
		if page.Next == 0 || page.Next == token {
			t.Fatal("stalled continuation")
		}
		token = page.Next
	}
	t.Fatal("iteration did not finish")
	return nil
}
func TestIndexedKeysExactPredicatesRetractionsAndHistoricalEmpty(t *testing.T) {
	f, emptyIndex := indexedFixture(t, PageLimits{MaxCells: 2, MaxChildren: 3})
	names := []string{"a", "a\x00", "a\x00z", "aa", "å", "😀"}
	var expected []graphstate.ComponentKey
	for _, life := range []graphstate.LifeID{11, 12} {
		for _, name := range names {
			key := graphstate.ComponentKey{Owner: 1, Life: life, Kind: graphstate.Label, Name: name}
			patch := pagePatch(t, key, emptyPageState(t, f.axis), pagePoint(t, f.axis, 1), state.Null(), uint64(len(expected)+1), false)
			indexCommitPatches(t, f, patch)
			expected = append(expected, key)
		}
	}
	// Explicit Unset creates a retained retraction; a truly absent no-op creates no key.
	unsetKey := graphstate.ComponentKey{Owner: 2, Life: 11, Kind: graphstate.ScalarProperty, Name: "scalar"}
	unset := pagePatch(t, unsetKey, emptyPageState(t, f.axis), pagePoint(t, f.axis, 2), state.Null(), 99, true)
	indexCommitPatches(t, f, unset)
	noopKey := graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.Label, Name: "never"}
	indexCommitPatches(t, f, graphstate.ComponentPatch{Key: noopKey, Owned: pagePoint(t, f.axis, 3), State: emptyPageState(t, f.axis)})
	p, err := newComponentKeyReader(openCatalog(t, f.db, f.index, Limits{}), f.limits)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	slices.SortFunc(expected, compareComponentKeys)
	if got := readIndexedKeys(t, p, graphstate.KeyPredicate{Owner: 1}); !reflect.DeepEqual(got, expected) {
		t.Fatal("wrong exact key set", got, expected)
	}
	for _, name := range names {
		want := []graphstate.ComponentKey{{Owner: 1, Life: 11, Kind: graphstate.Label, Name: name}}
		if got := readIndexedKeys(t, p, graphstate.KeyPredicate{Owner: 1, Life: 11, Kind: graphstate.Label, Name: name}); !reflect.DeepEqual(got, want) {
			t.Fatal("NUL/UTF8/name alias", name, got)
		}
	}
	for _, predicate := range []graphstate.KeyPredicate{{Owner: 1, Name: "a\x00missing"}, {Owner: 1, Name: "never"}, {Owner: 3}, {Owner: 1, Life: 13}, {Owner: 1, Kind: graphstate.SetMember}} {
		if got := readIndexedKeys(t, p, predicate); len(got) != 0 {
			t.Fatal("phantom", predicate, got)
		}
	}
	if got := readIndexedKeys(t, p, graphstate.KeyPredicate{Owner: 2}); !reflect.DeepEqual(got, []graphstate.ComponentKey{unsetKey}) {
		t.Fatal("retraction membership lost", got)
	}
	// Open the old empty root only AFTER every later page/split exists.
	old, err := newComponentKeyReader(openCatalog(t, f.db, emptyIndex, Limits{}), f.limits)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	page, err := old.ComponentKeys(t.Context(), graphstate.KeyPredicate{Owner: 1}, 0, graphstate.ReadBudget{Rows: 1, Bytes: 4 << 20})
	if err != nil || !page.Complete || len(page.Keys) != 0 || old.last.Records != 1 {
		t.Fatal("empty MVCC root visited future keys", page, old.last, err)
	}
}
func TestIndexedStageLateFailuresPreserveRootFloorAndPreviousWrites(t *testing.T) {
	for _, failure := range []string{"split", "reference", "shared-cap"} {
		t.Run(failure, func(t *testing.T) {
			limits := PageLimits{MaxCells: 2, MaxChildren: 3}
			if failure == "split" {
				limits.MaxLevels = 1
			}
			f, _ := indexedFixture(t, limits)
			s := indexedStage(t, f)
			if err := s.Value(t.Context(), refValue(100), graphstate.I64(8)); err != nil {
				t.Fatal(err)
			}
			before, _ := s.Writes()
			authority := *s.indexed
			records, bytes := s.c.records, s.c.stageBytes
			patches := []graphstate.ComponentPatch{}
			for i, name := range []string{"a", "b", "c"} {
				key := graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.Label, Name: name}
				patches = append(patches, pagePatch(t, key, emptyPageState(t, f.axis), pagePoint(t, f.axis, 1), state.Null(), uint64(i+1), false))
			}
			want := ErrResourceLimit
			if failure == "reference" {
				patches[2].Key.Owner = 99
				want = ErrInvalid
			}
			if failure == "shared-cap" {
				other, err := newIndexedStage(t.Context(), s.c, f.limits)
				if err != nil {
					t.Fatal(err)
				}
				defer other.Close()
				if err := other.Property(t.Context(), graphstate.PropertyDefinition{Name: "other", Owner: graphstate.Node, Type: graphstate.ScalarBool, Cardinality: graphstate.ScalarCardinality}); err != nil {
					t.Fatal(err)
				}
				s.c.limits.MaxStageRecords = 7
				patches = patches[:1]
				records, bytes = s.c.records, s.c.stageBytes
			}
			got, err := StageComponentPatches(t.Context(), s, f.root, patches, f.limits)
			after, _ := s.Writes()
			if !errors.Is(err, want) || !reflect.DeepEqual(got, StagedComponents{}) || !sameWrites(before, after) || *s.indexed != authority || s.c.records != records || s.c.stageBytes != bytes {
				t.Fatal("late refusal changed private authority/staging", failure, got, err)
			}
			if _, err := s.c.Root(); err != nil {
				t.Fatal("ordinary refusal poisoned", err)
			}
		})
	}
}
func TestIndexInitializerFreshnessNoopsAndBaseGuard(t *testing.T) {
	db := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	seed := bootstrapRoot(t, db)
	stale := openCatalog(t, db, 1, Limits{})
	base, wire, err := db.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	entry := &pb.Entry{Index: new(base + 1), Term: new(uint64(2)), Type: pb.EntryNormal.Enum()}
	if err := db.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(base + 1)}, Entries: []*pb.Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	if err := db.InstallApplication(base+1, raftlog.ApplicationBatch{BaseIndex: base, BaseImageHash: sha256.Sum256(wire), Image: wire}); err != nil {
		t.Fatal(err)
	}
	if _, err := initializeIndexes(t.Context(), stale, nil, PageLimits{}); !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal("stale empty catalog initialized", err)
	}
	c := openCatalog(t, db, 2, Limits{})
	initialized, err := initializeIndexes(t.Context(), c, nil, PageLimits{})
	if err != nil {
		t.Fatal("genuine noop prevented initialization", err)
	}
	defer initialized.stage.Close()
	rows, _ := initialized.stage.Writes()
	image, _ := EncodeRoot(initialized.root)
	// A concurrent semantic installation changes the real base after initialization.
	_, index := commitRows(t, db, seed, nil)
	next := index + 1
	e := &pb.Entry{Index: new(next), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: []byte("private init")}
	if err := db.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(next)}, Entries: []*pb.Entry{e}}); err != nil {
		t.Fatal(err)
	}
	batch := raftlog.ApplicationBatch{BaseGeneration: initialized.baseGeneration, BaseIndex: initialized.baseIndex, BaseImageHash: initialized.baseImageHash, Image: image, Writes: rows}
	if err := db.InstallApplication(next, batch); !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal("stale initializer installed", err)
	}
	if _, err := c.Root(); err != nil {
		t.Fatal("stale proof poisoned catalog", err)
	}
}
func TestIndexInitializerHiddenKVInvalidSchemaAndPrimitiveRefuse(t *testing.T) {
	db := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	seed := bootstrapRoot(t, db)
	c := openCatalog(t, db, 1, Limits{})
	invalid := indexSchemas()
	invalid = append(invalid, invalid[0])
	if _, err := initializeIndexes(t.Context(), c, invalid, PageLimits{}); !errors.Is(err, ErrInvalid) || c.stages != 0 || c.records != 0 || c.stageBytes != 0 {
		t.Fatal("invalid init retained staging", err)
	}
	invalid[0].Name = ""
	if _, err := initializeIndexes(t.Context(), c, invalid, PageLimits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	// Preserve the semantic-empty GR2 image while writing unrelated hidden KV.
	_, image, _ := db.Checkpoint()
	entry := &pb.Entry{Index: new(uint64(2)), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: []byte("hidden")}
	if err := db.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(2))}, Entries: []*pb.Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	if err := db.InstallApplication(2, raftlog.ApplicationBatch{BaseIndex: 1, BaseImageHash: sha256.Sum256(image), Image: image, Writes: []raftlog.KV{{Key: []byte("unrelated"), Deleted: true}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := initializeIndexes(t.Context(), openCatalog(t, db, 2, Limits{}), nil, PageLimits{}); !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal("hidden tombstone accepted", seed, err)
	}
	primitive, _ := newStore(t, vfs.NewMem())
	if _, err := initializeIndexes(t.Context(), openCatalog(t, primitive, 1, Limits{}), nil, PageLimits{}); !errors.Is(err, ErrTopologyUnsupported) {
		t.Fatal(err)
	}
	var nilCatalog *Catalog
	if _, err := initializeIndexes(t.Context(), nilCatalog, nil, PageLimits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := newIndexedStage(t.Context(), nilCatalog, PageLimits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := newComponentKeyReader(nilCatalog, PageLimits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := initializeIndexes(ctx, c, nil, PageLimits{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestIndexDescriptorCanonicalBindingAndMissingTreeRefuse(t *testing.T) {
	for _, mutation := range []string{"missing-descriptor", "missing-tree", "count", "coverage", "owner", "topology", "schema", "format", "level", "trailing", "cycle"} {
		t.Run(mutation, func(t *testing.T) {
			db, root, index := emptyIndexed(t, PageLimits{})
			c := openCatalog(t, db, index, Limits{})
			q, _ := c.reader(t.Context())
			d, _, err := q.componentIndexDescriptor(root)
			if err != nil {
				t.Fatal(err)
			}
			wire, _ := encodeComponentIndexDescriptor(d, root, c.limits)
			row := raftlog.KV{Key: componentIndexDescriptorKey(root.namespace), Value: wire}
			switch mutation {
			case "missing-descriptor":
				row.Deleted = true
				row.Value = nil
			case "missing-tree":
				row = raftlog.KV{Key: physicalKey(root.namespace, componentKeyTreeRecord, d.tree.id), Deleted: true}
			case "count":
				binary.BigEndian.PutUint64(wire[72:], 1)
			case "coverage":
				wire[29] = 2
			case "owner":
				binary.BigEndian.PutUint64(wire[32:], root.owner+1)
			case "topology":
				binary.BigEndian.PutUint64(wire[40:], 2)
			case "schema":
				binary.BigEndian.PutUint64(wire[48:], 2)
			case "format":
				binary.BigEndian.PutUint64(wire[56:], 2)
			case "level":
				wire[30] = 8
			case "trailing":
				row.Value = append(wire, 0)
			case "cycle":
				key := graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}
				// Forged raw self child bypasses codec input validation; fail closed at read.
				b := binary.BigEndian.AppendUint64(recordHeader(root.namespace, componentKeyTreeRecord), d.tree.id)
				b = append(b, 1)
				b = binary.BigEndian.AppendUint32(b, 2)
				for i := range 2 {
					b = binary.BigEndian.AppendUint64(b, d.tree.id)
					b = binary.BigEndian.AppendUint64(b, 1)
					key.Owner = graphstate.EntityID(i + 1)
					b = appendComponent(b, key)
					b = appendComponent(b, key)
				}
				row = raftlog.KV{Key: physicalKey(root.namespace, componentKeyTreeRecord, d.tree.id), Value: b}
			}
			root, index = commitRows(t, db, root, []raftlog.KV{row})
			for _, factory := range []func(*Catalog) error{func(c *Catalog) error { _, e := newIndexedStage(t.Context(), c, PageLimits{}); return e }, func(c *Catalog) error { _, e := newComponentKeyReader(c, PageLimits{}); return e }} {
				bad := openCatalog(t, db, index, Limits{})
				if err := factory(bad); !errors.Is(err, ErrCorrupt) {
					t.Fatal("invalid descriptor/tree accepted", mutation, err)
				}
				if _, err := bad.Root(); !errors.Is(err, ErrPoisoned) {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestIndexedMembershipSymmetryAndCursorBounds(t *testing.T) {
	f, _ := indexedFixture(t, PageLimits{})
	key := graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.Label, Name: "a"}
	indexCommitPatches(t, f, pagePatch(t, key, emptyPageState(t, f.axis), pagePoint(t, f.axis, 1), state.Null(), 1, false))
	second := key
	second.Name = "b"
	indexCommitPatches(t, f, pagePatch(t, second, emptyPageState(t, f.axis), pagePoint(t, f.axis, 1), state.Null(), 2, false))
	p, err := newComponentKeyReader(openCatalog(t, f.db, f.index, Limits{}), PageLimits{MaxCursors: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	predicate := graphstate.KeyPredicate{Owner: 1}
	first, err := p.ComponentKeys(t.Context(), predicate, 0, graphstate.ReadBudget{Rows: 12, Bytes: 4 << 20})
	if err != nil || first.Next == 0 {
		t.Fatal(first, err)
	}
	if _, err := p.ComponentKeys(t.Context(), predicate, 0, graphstate.ReadBudget{Rows: 12, Bytes: 4 << 20}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal("cursor retention bypass", err)
	}
	if _, err := p.ComponentKeys(t.Context(), graphstate.KeyPredicate{Owner: 2}, first.Next, graphstate.ReadBudget{Rows: 12, Bytes: 4 << 20}); !errors.Is(err, ErrInvalid) {
		t.Fatal("foreign predicate cursor", err)
	}
	if _, err := p.ComponentKeys(t.Context(), predicate, first.Next, graphstate.ReadBudget{Rows: 1, Bytes: 4 << 20}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal("work budget ignored", err)
	}
	done, err := p.ComponentKeys(t.Context(), predicate, first.Next, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if err != nil || !done.Complete {
		t.Fatal("refusal consumed cursor", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ComponentKeys(t.Context(), predicate, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := (*componentKeyReader)(nil).Close(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, mutation := range []string{"metadata-only", "key-only"} {
		independent, _ := indexedFixture(t, PageLimits{})
		indexCommitPatches(t, independent, pagePatch(t, key, emptyPageState(t, independent.axis), pagePoint(t, independent.axis, 1), state.Null(), 1, false))
		// Corrupt exactly one side in a separate fully valid durable fixture.
		db, root, index := independent.db, independent.root, independent.index
		row := raftlog.KV{Key: componentKey(root.namespace, key), Deleted: true}
		if mutation == "metadata-only" {
			c := openCatalog(t, db, index, Limits{})
			q, _ := c.reader(t.Context())
			d, _, _ := q.componentIndexDescriptor(root)
			wire, _ := encodeComponentKeyTreeNode(componentKeyTreeNode{id: d.tree.id}, c, defaultComponentKeyTreeLimits())
			row = raftlog.KV{Key: physicalKey(root.namespace, componentKeyTreeRecord, d.tree.id), Value: wire}
			d.tree.count = 0
			descriptor, _ := encodeComponentIndexDescriptor(d, root, c.limits)
			root, index = commitRows(t, db, root, []raftlog.KV{row, {Key: componentIndexDescriptorKey(root.namespace), Value: descriptor}})
		} else {
			root, index = commitRows(t, db, root, []raftlog.KV{row})
		}
		c := openCatalog(t, db, index, Limits{})
		reader, err := NewPageReader(c, PageLimits{})
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		if _, err := reader.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: key, Window: pagePoint(t, f.axis, 1)}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}); !errors.Is(err, ErrCorrupt) {
			t.Fatal("missing membership side accepted", mutation, err)
		}
	}
}
func TestGenerationBoundAssociationContinuationAndPortableRoot(t *testing.T) {
	n := testNamespace()
	hash, query := [32]byte{1}, [32]byte{2}
	lower := []byte{1, 2}
	last := binary.BigEndian.AppendUint64(bytes.Clone(lower), 7)
	upper := associationPrefixEnd(lower)
	token := associationContinuation(n, 1, 9, hash, query, last)
	if _, err := parseAssociationContinuation(token, n, 2, 9, hash, query, lower, upper, 4096); !errors.Is(err, ErrInvalid) {
		t.Fatal("generation ABA cursor admitted", err)
	}
	if got, err := parseAssociationContinuation(token, n, 1, 9, hash, query, lower, upper, 4096); err != nil || !bytes.Equal(got, last) {
		t.Fatal(got, err)
	}
	root, _ := NewRoot(n, 3)
	root.topology = keysOnlyTopology
	before, _ := EncodeRoot(root)
	after, _ := EncodeRoot(root)
	if !bytes.Equal(before, after) {
		t.Fatal("local generation entered portable root")
	}
}

func TestIndexedLateExactPrefixSeeksAndLargePageReusesTree(t *testing.T) {
	f, _ := indexedFixture(t, PageLimits{MaxCells: 3, MaxChildren: 3})
	var expected []graphstate.ComponentKey
	// Earlier lives/kinds are real populated components, not unbound tree rows.
	for i := range 40 {
		key := graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.Label, Name: fmt.Sprintf("early-%03d", i)}
		indexCommitPatches(t, f, pagePatch(t, key, emptyPageState(t, f.axis), pagePoint(t, f.axis, 1), state.Null(), uint64(i+1), false))
	}
	for i := range 10 {
		key := graphstate.ComponentKey{Owner: 1, Life: 12, Kind: graphstate.Label, Name: "late" + string(rune('a'+i))}
		indexCommitPatches(t, f, pagePatch(t, key, emptyPageState(t, f.axis), pagePoint(t, f.axis, 1), state.Null(), uint64(i+100), false))
		expected = append(expected, key)
	}
	p, err := newComponentKeyReader(openCatalog(t, f.db, f.index, Limits{}), f.limits)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	exact := graphstate.KeyPredicate{Owner: 1, Life: 12, Kind: graphstate.Label, Name: expected[9].Name}
	page, err := p.ComponentKeys(t.Context(), exact, 0, graphstate.ReadBudget{Rows: 32, Bytes: 4 << 20})
	if err != nil || !page.Complete || !reflect.DeepEqual(page.Keys, expected[9:]) || p.last.Records > 16 {
		t.Fatal("late exact prefix walked earlier owner keys", page, p.last, err)
	}
	page, err = p.ComponentKeys(t.Context(), graphstate.KeyPredicate{Owner: 1, Life: 12, Kind: graphstate.Label}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if err != nil || !page.Complete || !reflect.DeepEqual(page.Keys, expected) || p.last.DirectoryPages > 20 {
		t.Fatal("generous page restarted root for every key", page, p.last, err)
	}
	// Kind is an exact sparse filter when Life is unspecified. One checked earlier
	// label can fill work without matching, and must advance rather than complete.
	page, err = p.ComponentKeys(t.Context(), graphstate.KeyPredicate{Owner: 1, Kind: graphstate.SetMember}, 0, graphstate.ReadBudget{Rows: 16, Bytes: 4 << 20})
	if err != nil || page.Complete || page.Next == 0 || len(page.Keys) != 0 {
		t.Fatal("filtered page did not advance", page, p.last, err)
	}
	if got := readIndexedKeys(t, p, graphstate.KeyPredicate{Owner: 1, Kind: graphstate.SetMember}); len(got) != 0 {
		t.Fatal("sparse phantom", got)
	}
}
func TestIndexedReaderInputLifetimeAndTightOutput(t *testing.T) {
	f, _ := indexedFixture(t, PageLimits{})
	k := graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.Label, Name: "key"}
	indexCommitPatches(t, f, pagePatch(t, k, emptyPageState(t, f.axis), pagePoint(t, f.axis, 1), state.Null(), 1, false))
	c := openCatalog(t, f.db, f.index, Limits{})
	p, err := newComponentKeyReader(c, PageLimits{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, pred := range []graphstate.KeyPredicate{{}, {Owner: 1, Kind: 99}, {Owner: 1, Name: " "}} {
		if got, err := p.ComponentKeys(t.Context(), pred, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}); !errors.Is(err, ErrInvalid) || !reflect.DeepEqual(got, graphstate.KeyPage{}) {
			t.Fatal(err)
		}
	}
	if _, err := p.ComponentKeys(t.Context(), graphstate.KeyPredicate{Owner: 1}, 999, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := p.ComponentKeys(t.Context(), graphstate.KeyPredicate{Owner: 1}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 100}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := p.ComponentKeys(ctx, graphstate.KeyPredicate{Owner: 1}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := (*componentKeyReader)(nil).ComponentKeys(t.Context(), graphstate.KeyPredicate{}, 0, graphstate.ReadBudget{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := newComponentKeyReader(c, PageLimits{MaxLevels: 9}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := newIndexedStage(t.Context(), c, PageLimits{MaxLevels: 9}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := c.view.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ComponentKeys(t.Context(), graphstate.KeyPredicate{Owner: 1}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
	if _, err := newComponentKeyReader(c, PageLimits{}); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
	primitive, _ := newStore(t, vfs.NewMem())
	plain := openCatalog(t, primitive, 1, Limits{})
	if _, err := newComponentKeyReader(plain, PageLimits{}); !errors.Is(err, ErrTopologyUnsupported) {
		t.Fatal(err)
	}
	if _, err := newIndexedStage(t.Context(), plain, PageLimits{}); !errors.Is(err, ErrTopologyUnsupported) {
		t.Fatal(err)
	}
}
func TestIndexDescriptorCodecTruncationAndPhysicalFloor(t *testing.T) {
	db, root, index := emptyIndexed(t, PageLimits{})
	c := openCatalog(t, db, index, Limits{})
	q, _ := c.reader(t.Context())
	d, _, _ := q.componentIndexDescriptor(root)
	wire, _ := encodeComponentIndexDescriptor(d, root, c.limits)
	key := componentIndexDescriptorKey(root.namespace)
	for n := 0; n < len(wire); n++ {
		q := &reader{c: c, ctx: t.Context(), pending: map[string]raftlog.KV{string(key): {Key: key, Value: wire[:n]}}}
		if _, _, err := q.componentIndexDescriptor(root); !errors.Is(err, ErrCorrupt) {
			t.Fatal("descriptor prefix admitted", n, err)
		}
	}
	for _, mutation := range []func(*componentIndexDescriptor){func(d *componentIndexDescriptor) { d.tree.id = root.next }, func(d *componentIndexDescriptor) { d.tree.level = -1 }, func(d *componentIndexDescriptor) { d.tree.level = 1; d.tree.count = 1 }, func(d *componentIndexDescriptor) { d.coverage = 2 }} {
		bad := d
		mutation(&bad)
		if _, err := encodeComponentIndexDescriptor(bad, root, c.limits); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid descriptor encoded", bad, err)
		}
	}
}
func TestLocalGenerationChangesPageAndKeyViewIdentity(t *testing.T) {
	db, root, index := emptyIndexed(t, PageLimits{})
	c := openCatalog(t, db, index, Limits{})
	q, _ := c.reader(t.Context())
	d, _, _ := q.componentIndexDescriptor(root)
	wire, _ := encodeComponentIndexDescriptor(d, root, c.limits)
	image, _ := EncodeRoot(root)
	a := raftlog.ApplicationRoot{Generation: 1, Index: index, Image: image, ImageHash: sha256.Sum256(image)}
	b := a
	b.Generation = 2
	if pageViewIdentity(root.namespace, a) == pageViewIdentity(root.namespace, b) || componentKeysViewIdentity(wire, a) == componentKeysViewIdentity(wire, b) {
		t.Fatal("physical ABA reused view identity")
	}
	if !bytes.Equal(a.Image, b.Image) || a.ImageHash != b.ImageHash {
		t.Fatal("generation changed portable semantic hash")
	}
}

func TestIndexInitializationSharedCapsAndInvalidPolicies(t *testing.T) {
	db := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	_ = bootstrapRoot(t, db)
	c := openCatalog(t, db, 1, Limits{MaxStages: 1})
	held, err := allocateIndexedStage(c, indexedStageState{root: c.root, limits: defaultComponentKeyTreeLimits()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := initializeIndexes(t.Context(), c, nil, PageLimits{}); !errors.Is(err, ErrResourceLimit) || c.stages != 1 || c.records != 0 {
		t.Fatal("init ignored shared stage cap", err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	for _, limits := range []PageLimits{{MaxLevels: 9}, {MaxCheckpointBytes: 32}} {
		if _, err := initializeIndexes(t.Context(), c, nil, limits); !errors.Is(err, ErrInvalid) || c.stages != 0 {
			t.Fatal("invalid index policy retained authority", err)
		}
	}
	c.limits.MaxStageRecords = 2
	if _, err := initializeIndexes(t.Context(), c, indexSchemas(), PageLimits{}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal("schema inventory cap ignored", err)
	}
	c.poison = ErrCorrupt
	if _, err := allocateIndexedStage(c, indexedStageState{}); !errors.Is(err, ErrPoisoned) {
		t.Fatal(err)
	}
}

func TestIndexedClosedLifeRetainsLabelsPropertiesAndHistoricalPresence(t *testing.T) {
	f, _ := indexedFixture(t, PageLimits{})
	presence := graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}
	label := graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.Label, Name: "active"}
	scalar := graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "scalar"}
	lifeValue, err := state.NewValueRef(11, 0)
	if err != nil {
		t.Fatal(err)
	}
	c := openCatalog(t, f.db, f.index, Limits{})
	entry, found, err := c.Value(t.Context(), refValue(99))
	if err != nil || !found {
		t.Fatal(err)
	}
	_ = c.view.Close()
	scalarValue, err := state.NewValueRef(99, entry.PayloadBytes)
	if err != nil {
		t.Fatal(err)
	}
	w := pagePoint(t, f.axis, 1)
	empty := emptyPageState(t, f.axis)
	alive := pagePatch(t, presence, empty, w, lifeValue, 1, false)
	indexCommitPatches(t, f, alive, pagePatch(t, label, empty, w, state.Null(), 2, false), pagePatch(t, scalar, empty, w, scalarValue, 3, false))
	aliveIndex := f.index
	indexCommitPatches(t, f, pagePatch(t, presence, alive.State, w, state.ValueRef{}, 4, true))
	want := []graphstate.ComponentKey{presence, label, scalar}
	for _, index := range []uint64{aliveIndex, f.index} {
		c := openCatalog(t, f.db, index, Limits{})
		p, err := newComponentKeyReader(c, PageLimits{})
		if err != nil {
			t.Fatal(err)
		}
		got := readIndexedKeys(t, p, graphstate.KeyPredicate{Owner: 1})
		if !reflect.DeepEqual(got, want) {
			t.Fatal("closed life dropped retained component keys", index, got)
		}
		reader, err := NewPageReader(c, PageLimits{})
		if err != nil {
			t.Fatal(err)
		}
		page, err := reader.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: presence, Window: w}, 0, graphstate.ReadBudget{Rows: 32, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		pieces := page.Data.Pieces()
		if len(pieces) != 1 || pieces[0].Cell().Present() != (index == aliveIndex) {
			t.Fatal("historical presence silently read current closure", index, pieces)
		}
		_ = reader.Close()
		_ = p.Close()
		_ = c.view.Close()
	}
}

func TestIndexedStageFixedMetadataSharedByteBoundaryAndRelease(t *testing.T) {
	db, _, index := emptyIndexed(t, PageLimits{})
	// Resolved policy permits a small actual byte-cap boundary while preserving
	// all minimum record/value/descriptor constraints.
	c := openCatalog(t, db, index, Limits{MaxValueBytes: 64, MaxRecordBytes: 512, MaxStageBytes: 4096, Temporal: temporal.Limits{MaxDescriptorBytes: 64}})
	held, err := newIndexedStage(t.Context(), c, PageLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if held.bytes != indexedStageBaseBytes || c.stageBytes != indexedStageBaseBytes {
		t.Fatal("indexed metadata omitted from initial retained ledger", held.bytes, c.stageBytes)
	}
	definition := graphstate.PropertyDefinition{Name: "reserved", Owner: graphstate.Node, Type: graphstate.ScalarBool, Cardinality: graphstate.ScalarCardinality}
	if err := held.Property(t.Context(), definition); err != nil {
		t.Fatal(err)
	}
	retained := held.bytes
	records := c.records
	before, _ := held.Writes()
	exact := retained + indexedStageBaseBytes
	c.limits.MaxStageBytes = exact - 1
	if _, err := c.limits.resolve(); err != nil {
		t.Fatal("test boundary is not a valid resolved policy", err)
	}
	if got, err := newIndexedStage(t.Context(), c, PageLimits{}); got != nil || !errors.Is(err, ErrResourceLimit) {
		t.Fatal("one-short metadata budget admitted stage", got, err)
	}
	after, _ := held.Writes()
	if c.stageBytes != retained || c.stages != 1 || c.records != records || !sameWrites(before, after) {
		t.Fatal("metadata budget refusal changed existing staged records/accounting")
	}
	c.limits.MaxStageBytes = exact
	second, err := newIndexedStage(t.Context(), c, PageLimits{})
	if err != nil {
		t.Fatal("exact metadata allowance refused", err)
	}
	if c.stageBytes != exact || second.bytes != indexedStageBaseBytes || c.stages != 2 {
		t.Fatal("exact metadata allowance not charged")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if second.indexed != nil || c.stageBytes != retained || c.stages != 1 {
		t.Fatal("close retained indexed authority or failed to release fixed allowance")
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if held.indexed != nil || c.stageBytes != 0 || c.records != 0 || c.stages != 0 {
		t.Fatal("indexed metadata/data allowance not fully released")
	}
}

func TestIndexedEmptyPatchBudgetIncludesAuthorityCopyAndReportsWork(t *testing.T) {
	f, _ := indexedFixture(t, PageLimits{})
	s := indexedStage(t, f)
	if err := s.Value(t.Context(), refValue(100), graphstate.I64(8)); err != nil {
		t.Fatal(err)
	}
	before, _ := s.Writes()
	authority := *s.indexed
	records, bytes := s.c.records, s.c.stageBytes
	exact := s.c.rootImageBytes + indexedStageMetadataBytes
	for _, capBytes := range []int{192, exact - 1} {
		limits := PageLimits{MaxCheckpointBytes: 64, MaxWorkBytes: capBytes}
		if _, err := limits.resolve(); err != nil {
			t.Fatal("invalid test budget", err)
		}
		result, err := StageComponentPatches(t.Context(), s, f.root, nil, limits)
		after, _ := s.Writes()
		if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(result, StagedComponents{}) || !sameWrites(before, after) || *s.indexed != authority || s.c.records != records || s.c.stageBytes != bytes {
			t.Fatal("empty patch sequence bypassed work cap or changed staging", capBytes, result, err)
		}
	}
	result, err := StageComponentPatches(t.Context(), s, f.root, nil, PageLimits{MaxCheckpointBytes: 64, MaxWorkBytes: exact})
	if err != nil || result.Root != f.root || len(result.Groups) != 0 || result.Work.Bytes != exact || result.Work.Records != 0 {
		t.Fatal("exact-fit empty sequence reported inaccurate work", result, err)
	}
}
func TestIndexInitializationChecksFixedCopyUnderTightWorkCap(t *testing.T) {
	db := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	_ = bootstrapRoot(t, db)
	c := openCatalog(t, db, 1, Limits{})
	result, err := initializeIndexes(t.Context(), c, nil, PageLimits{MaxCheckpointBytes: 64, MaxWorkBytes: 192})
	if !errors.Is(err, ErrResourceLimit) || result != (stagedIndexes{}) || c.stages != 0 || c.records != 0 || c.stageBytes != 0 {
		t.Fatal("initializer published root/descriptor beyond work cap", result, err)
	}
	if _, err := c.Root(); err != nil {
		t.Fatal("tight initializer budget poisoned catalog", err)
	}
}
