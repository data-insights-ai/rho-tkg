package graph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// One partition bound to the graphstore seams: genesis is
// raftlog.Store.Initialize + graphstore.InitializeDeclaredPartitionWithDefaultAxis
// (+ the routing and range rows of a single-partition declaration); every Update
// is StagePartitionOperations + Persist + raftlog.Store.InstallApplication; every
// read is OpenPartitionGraphReadView at one application index. No replica.
const (
	partitionID = 1
	ownerEpoch  = 1
	// A value dictionary handle is commitIndex<<valSeqBits | seq.
	valSeqBits = 20
)

var (
	contractID = sha256.Sum256([]byte("rho-tkg:v5/pkg/graph:single-partition:v1"))
	lifeID     = graphstate.LifeID(1)
	allocName  = []byte("rho-tkg:v5/pkg/graph:id-allocator:v1")
	instantKey = []byte("rho-tkg:v5/pkg/graph:commit-instant:v1:")
	multiName  = []byte("rho-tkg:v5/pkg/graph:multi-life:v1")
	metaPrefix = []byte("rho-tkg:v5/pkg/graph:meta:v1:")
)

// metaKey is the engine row of a Tx.SetMeta key.
func metaKey(key string) []byte { return append(bytes.Clone(metaPrefix), key...) }

// lifeSpan is the valid-time extent of one life of an entity.
type lifeSpan struct {
	life  graphstate.LifeID
	scope temporal.Scope
}

// livesOf reads the entity's lives at index: the extent of each present
// Presence piece, and the highest life number (probed: lives are dense).
func (e *pebbleEngine) livesOf(ctx context.Context, index uint64, id graphstate.EntityID) (spans []lifeSpan, maxLife graphstate.LifeID, err error) {
	r, err := e.readerAt(ctx, index)
	if err != nil {
		return nil, 0, err
	}
	defer r.close()
	window, err := temporal.All(e.cfg.DefaultAxis.Axis())
	if err != nil {
		return nil, 0, err
	}
	pages, err := r.pieces(graphstate.ComponentKey{Owner: id, Kind: graphstate.Presence}, window)
	if err != nil {
		return nil, 0, err
	}
	for _, pg := range pages {
		for _, pc := range pg.Data.Pieces() {
			if pc.Cell().Present() {
				spans = append(spans, lifeSpan{graphstate.LifeID(pc.Cell().Value().ID()), pc.Scope()})
			}
		}
	}
	for n := graphstate.LifeID(1); n < 1<<16; n++ {
		lr, err := r.rv.Life(ctx, id, n)
		if err != nil {
			return nil, 0, err
		}
		if !lr.Found {
			break
		}
		maxLife = n
	}
	return spans, maxLife, nil
}

// commitInstantKey is the row holding the knowledge instant (milliseconds) of
// the commit at index.
func commitInstantKey(index uint64) []byte {
	return binary.BigEndian.AppendUint64(append([]byte(nil), instantKey...), index)
}

// endAt reads the committed End position of one life of the entity at index from
// its LifeEnd cells: the lowest lower bound of the regions an End marked.
func (e *pebbleEngine) endAt(ctx context.Context, index uint64, id graphstate.EntityID, life graphstate.LifeID) (temporal.Position, bool, error) {
	r, err := e.readerAt(ctx, index)
	if err != nil {
		return temporal.Position{}, false, err
	}
	defer r.close()
	window, err := temporal.All(e.cfg.DefaultAxis.Axis())
	if err != nil {
		return temporal.Position{}, false, err
	}
	pages, err := r.pieces(graphstate.ComponentKey{Owner: id, Kind: graphstate.LifeEnd}, window)
	if err != nil {
		return temporal.Position{}, false, err
	}
	var end temporal.Position
	found := false
	for _, pg := range pages {
		for _, pc := range pg.Data.Pieces() {
			if !pc.Cell().Present() || graphstate.LifeID(pc.Cell().Value().ID()) != life {
				continue
			}
			for _, part := range pc.Scope().Parts() {
				lo, _, ok := part.Bounds()
				if !ok {
					continue
				}
				p, fin := lo.Position()
				if !fin {
					continue
				}
				if !found {
					end, found = p, true
				} else if c, err := temporal.ComparePositions(p, end, e.cfg.Limits); err != nil {
					return temporal.Position{}, false, fmt.Errorf("%w: stored End: %v", ErrCommitNotDurable, err)
				} else if c == temporal.Less {
					end = p
				}
			}
		}
	}
	return end, found, nil
}

func sortedMetaKeys(m map[string][]byte) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	return ks
}

func readBudget() graphstore.OwnershipBudget {
	return graphstore.OwnershipBudget{SourceRows: 65536, SourceBytes: 64 << 20, OutputBytes: 64 << 20}
}

type pebbleEngine struct {
	mu                       sync.Mutex
	scanCandidates, scanHits atomic.Uint64
	scanPages                atomic.Uint64 // entity record pages fetched by unindexed scans
	cfg                      Config
	store                    *raftlog.Store
	decl                     graphstore.OwnershipDeclaration
	ns                       graphstore.Namespace
	conf                     [32]byte
	binding                  types.DefaultAxisBinding
	node                     map[string]graphstate.PropertyDefinition
	rel                      map[string]graphstate.PropertyDefinition
	closed                   atomic.Bool
	broken                   atomic.Pointer[error]
	nonce                    atomic.Uint64
	alloc                    allocState
	multi                    bool // some entity has had a second life (persisted); false: every entity has life 1 only
	now                      func() int64
	last                     atomic.Int64 // knowledge instant of the newest commit
	first                    uint64       // application index of the first commit (genesis); earlier indexes have no instant row
	readers                  atomic.Uint64
}

var _ engine = (*pebbleEngine)(nil)

func openEngine(ctx context.Context, cfg Config) (*pebbleEngine, error) {
	gid := graphstate.GraphID(cfg.GraphID)
	group := [16]byte(cfg.GraphID)
	decl, err := graphstore.NewOwnershipDeclaration(gid, 1, []graphstore.PartitionOwnership{{Partition: partitionID, OwnershipEpoch: ownerEpoch, Group: group}}, graphstore.Limits{})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	e := &pebbleEngine{cfg: cfg, now: func() int64 { return time.Now().UnixMilli() }, decl: decl, ns: graphstore.Namespace{Graph: gid, Partition: partitionID}, binding: cfg.DefaultAxis,
		node: map[string]graphstate.PropertyDefinition{}, rel: map[string]graphstate.PropertyDefinition{}}
	e.conf = sha256.Sum256(append([]byte("rho-tkg:v5/pkg/graph:configuration:v1"), cfg.GraphID[:]...))
	for _, p := range cfg.NodeProperties {
		k, _ := kindToScalar(p.Kind)
		d := graphstate.PropertyDefinition{Name: p.Key, Owner: graphstate.Node, Type: k, Cardinality: graphstate.ScalarCardinality}
		if p.Unique {
			d.Unique = graphstate.UniqueScalar
		}
		e.node[p.Key] = d
	}
	for _, p := range cfg.RelProperties {
		k, _ := kindToScalar(p.Kind)
		e.rel[p.Key] = graphstate.PropertyDefinition{Name: p.Key, Owner: graphstate.Relationship, Type: k, Cardinality: graphstate.ScalarCardinality}
	}
	var fs vfs.FS
	dir, create := "mem", true
	if cfg.Memory {
		fs = vfs.NewMem()
	} else {
		fs = vfs.Default
		dir = cfg.Dir
		if ents, err := os.ReadDir(dir); err == nil && len(ents) > 0 {
			create = false
		}
	}
	rc := raftConfig(fs, dir, create, e.ns, group)
	st, err := raftlog.Open(rc)
	if err != nil {
		return nil, fmt.Errorf("%w: open store: %v", ErrInvalidConfig, err)
	}
	e.store = st
	if create {
		err = e.genesis(ctx)
	} else {
		err = e.recover(ctx)
	}
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	return e, nil
}

func raftConfig(fs vfs.FS, dir string, create bool, ns graphstore.Namespace, group [16]byte) raftlog.Config {
	p := raftlog.DefaultApplicationPolicy(1)
	p.MaxInstallWrites = 4096
	p.RetainedApplicationRecords = 1 << 20
	p.RetainedApplicationBytes = 256 << 20
	return raftlog.Config{Dir: dir, FS: fs, Create: create, Application: p, Limits: raftlog.DefaultLimits(), SemanticContractID: contractID,
		Replication:   raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}},
		Generations:   raftlog.ApplicationGenerationLimits{MaxBytes: 1 << 30, MaxRecords: 1 << 21},
		PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 1024},
		Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: ns.Graph, Partition: ns.Partition, Group: group}, Contract: raftlog.ApplicationContractForPolicy(p),
			Limits: raftlog.ApplicationTransferLimits{MaxExports: 2, MaxChunkRows: 64, MaxChunkBytes: 256 << 10, MaxStagedBytes: 16 << 20, MaxStagedRecords: 8192, MaxPinnedLogicalBytes: 64 << 20}}}
}

func (e *pebbleEngine) genesis(ctx context.Context) error {
	seed, err := graphstore.NewOwnershipRoot(e.ns, e.decl)
	if err != nil {
		return err
	}
	image, err := graphstore.EncodeRoot(seed)
	if err != nil {
		return err
	}
	if err := e.store.Initialize([]uint64{1, 2, 3}, image); err != nil {
		return fmt.Errorf("%w: initialize: %v", ErrInvalidConfig, err)
	}
	idx, prev, err := e.store.Checkpoint()
	if err != nil {
		return err
	}
	view, err := e.store.ApplicationView(idx)
	if err != nil {
		return err
	}
	defer view.Close()
	var schemas []graphstate.PropertyDefinition
	for _, p := range e.cfg.NodeProperties {
		schemas = append(schemas, e.node[p.Key])
	}
	for _, p := range e.cfg.RelProperties {
		schemas = append(schemas, e.rel[p.Key])
	}
	effects, _, err := graphstore.InitializeDeclaredPartitionWithDefaultAxis(ctx, view, e.decl, e.binding, schemas, graphstore.Limits{}, graphstore.GraphLimits{}, readBudget())
	if err != nil {
		return fmt.Errorf("%w: genesis: %v", ErrInvalidConfig, err)
	}
	root, err := effects.Root.AdvanceEffects(sha256.Sum256([]byte("rho-tkg:v5/pkg/graph:genesis")))
	if err != nil {
		return err
	}
	rows, err := e.routingRows()
	if err != nil {
		return err
	}
	e.alloc, err = newAllocState(e.cfg)
	if err != nil {
		return err
	}
	ar, err := e.alloc.row()
	if err != nil {
		return err
	}
	rows = append(rows, ar)
	writes := append(slices.Clone(effects.Writes), rows...)
	inst := e.nextInstant()
	writes = append(writes, instantRow(idx+1, inst))
	e.last.Store(inst)
	e.first = idx + 1
	slices.SortFunc(writes, func(a, b raftlog.KV) int { return bytes.Compare(a.Key, b.Key) })
	img, err := graphstore.EncodeRoot(root)
	if err != nil {
		return err
	}
	return e.install(idx+1, sha256.Sum256(prev), img, writes)
}

// routingRows is the single-partition routing definition and one published
// range holding every public ID.
func (e *pebbleEngine) routingRows() ([]raftlog.KV, error) {
	routing, err := graphstore.NewPartitionRouting(e.decl, 1, 0, []graphstore.BucketOwnership{
		{Kind: graphstore.IdentityRoutingBucket, Partition: partitionID, Epoch: 1},
		{Kind: graphstore.UniqueRoutingBucket, Partition: partitionID, Epoch: 1},
		{Kind: graphstore.AxisRoutingBucket, Partition: partitionID, Epoch: 1}}, graphstore.Limits{})
	if err != nil {
		return nil, err
	}
	def, err := graphstore.EncodePartitionRoutingBinding(routing, e.decl, e.binding, e.conf, graphstore.Limits{})
	if err != nil {
		return nil, err
	}
	const last = uint64(1<<63 - 1)
	pub := graphstore.RangePublication{Graph: e.ns.Graph, RangeID: 1, First: 1, Last: last, InitialPartition: partitionID, InitialEpoch: ownerEpoch, Configuration: e.conf, SourcePartition: partitionID, SourceIndex: 1}
	pw, err := graphstore.EncodeRangePublication(pub)
	if err != nil {
		return nil, err
	}
	ow, err := graphstore.EncodeRangeOwnership(graphstore.RangeOwnership{Graph: e.ns.Graph, RangeID: 1, Partition: partitionID, Epoch: ownerEpoch, Configuration: e.conf})
	if err != nil {
		return nil, err
	}
	return []raftlog.KV{{Key: graphstore.RoutingDefinitionKey(e.ns), Value: def}, {Key: graphstore.RangePublicationKey(e.ns, 1), Value: pw},
		{Key: graphstore.RangeOwnershipKey(e.ns, 1), Value: ow}, {Key: graphstore.RangeEndKey(e.ns, last), Value: pw}}, nil
}

// recover validates the stored schema and default axis against the Config.
func (e *pebbleEngine) recover(ctx context.Context) error {
	idx, _, err := e.store.Checkpoint()
	if err != nil || idx == 0 {
		return fmt.Errorf("%w: store holds no graph", ErrInvalidConfig)
	}
	view, err := e.store.ApplicationView(idx)
	if err != nil {
		return err
	}
	defer view.Close()
	c, _, err := graphstore.OpenPartitionCatalogWithDefaultAxis(ctx, view, e.ns, ownerEpoch, e.binding, graphstore.Limits{}, graphstore.GraphLimits{}, readBudget())
	if err != nil {
		return fmt.Errorf("%w: stored graph does not match the config: %v", ErrInvalidConfig, err)
	}
	row, found, err := view.Get(ctx, allocName, 4096)
	if err != nil || !found {
		return fmt.Errorf("%w: stored graph has no ID allocator", ErrInvalidConfig)
	}
	if e.alloc, err = decodeAllocState(row.Value); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	_, e.multi, err = view.Get(ctx, multiName, 64)
	if err != nil {
		return err
	}
	irow, found, err := view.Get(ctx, commitInstantKey(idx), 64)
	if err != nil || !found || len(irow.Value) != 8 {
		return fmt.Errorf("%w: stored graph has no commit knowledge instant", ErrInvalidConfig)
	}
	e.last.Store(int64(binary.BigEndian.Uint64(irow.Value))) // #nosec G115 -- written from an int64
	lowest, err := view.Scan(ctx, instantKey, append(bytes.Clone(instantKey[:len(instantKey)-1]), instantKey[len(instantKey)-1]+1), nil, raftlog.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil || len(lowest.Rows) != 1 || len(lowest.Rows[0].Key) != len(instantKey)+8 {
		return fmt.Errorf("%w: stored graph has no first commit instant", ErrInvalidConfig)
	}
	e.first = binary.BigEndian.Uint64(lowest.Rows[0].Key[len(instantKey):])
	for _, defs := range []map[string]graphstate.PropertyDefinition{e.node, e.rel} {
		for name, want := range defs {
			got, found, err := c.Property(ctx, want.Owner, name)
			if err != nil || !found || got != want {
				return fmt.Errorf("%w: stored property schema differs from the config", ErrInvalidConfig)
			}
		}
	}
	return nil
}

// install persists the raft entry for index and installs the batch.
func (e *pebbleEngine) install(index uint64, prevImageHash [32]byte, image []byte, writes []raftlog.KV) error {
	term := uint64(2)
	commit := index
	typ := pb.EntryNormal
	data := binary.BigEndian.AppendUint64([]byte("rho-v5 commit "), index)
	if err := e.store.Persist(raft.Ready{HardState: &pb.HardState{Term: &term, Commit: &commit}, Entries: []*pb.Entry{{Index: &index, Term: &term, Type: &typ, Data: data}}}); err != nil {
		return err
	}
	b := raftlog.ApplicationBatch{BaseGeneration: e.store.ApplicationGeneration(), BaseIndex: index - 1, BaseImageHash: prevImageHash, Image: image, Writes: writes}
	return e.store.InstallApplication(index, b)
}

// nextInstant is the knowledge instant of the next commit: the local wall clock
// in milliseconds, but never at or before the previous commit's, so instants
// strictly increase with the application index (also across a restart or a
// clock that steps back). Callers hold e.mu or are the genesis.
func (e *pebbleEngine) nextInstant() int64 { return max(e.now(), e.last.Load()+1) }

func instantRow(index uint64, inst int64) raftlog.KV {
	return raftlog.KV{Key: commitInstantKey(index), Value: binary.BigEndian.AppendUint64(nil, uint64(inst))} // #nosec G115 -- instants are positive milliseconds
}

// instantPosition is inst on the knowledge axis.
func (e *pebbleEngine) instantPosition(inst int64) (temporal.Position, error) {
	return types.InstantPosition(e.cfg.KnowledgeAxis.Axis(), types.Instant(inst), e.cfg.Limits)
}

func (e *pebbleEngine) check() error {
	if e.closed.Load() {
		return ErrClosed
	}
	if p := e.broken.Load(); p != nil {
		return *p
	}
	return nil
}

func (e *pebbleEngine) close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed.Swap(true) {
		return ErrClosed
	}
	return e.store.Close()
}

// ---- tokens ----

const (
	tokCut     = 'C'
	tokReceipt = 'R'
)

func (e *pebbleEngine) token(kind byte, index uint64) []byte {
	b := []byte{'R', 'V', '5', kind, 1}
	b = append(b, e.ns.Graph[:]...)
	return binary.BigEndian.AppendUint64(b, index)
}

// receiptToken is a receipt token carrying the commit's knowledge instant.
func (e *pebbleEngine) receiptToken(index uint64, inst int64) []byte {
	return binary.BigEndian.AppendUint64(e.token(tokReceipt, index), uint64(inst)) // #nosec G115 -- positive milliseconds
}

func (e *pebbleEngine) parseToken(kind byte, b []byte) (uint64, bool) {
	if kind == tokReceipt && len(b) == 5+16+8+8 {
		b = b[:5+16+8]
	}
	if len(b) != 5+16+8 || !bytes.Equal(b[:5], []byte{'R', 'V', '5', kind, 1}) || !bytes.Equal(b[5:21], e.ns.Graph[:]) {
		return 0, false
	}
	i := binary.BigEndian.Uint64(b[21:])
	return i, i > 0
}

// ---- update ----

func (e *pebbleEngine) update(ctx context.Context, fn func(Tx) error) (Receipt, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.check(); err != nil {
		return Receipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	base, prev, err := e.store.Checkpoint()
	if err != nil {
		return Receipt{}, err
	}
	next := base + 1
	tx := &engineTx{e: e, ctx: ctx, base: base, next: next, alloc: e.alloc}
	inst := e.nextInstant()
	if tx.ceiling, err = e.instantPosition(inst); err != nil {
		return Receipt{}, fmt.Errorf("%w: commit instant: %v", ErrInvalidRequest, err)
	}
	fnErr := func() (err error) {
		defer func() { tx.done = true }()
		return fn(tx)
	}()
	if fnErr != nil {
		return Receipt{}, fnErr
	}
	if tx.err != nil {
		return Receipt{}, tx.err
	}
	if len(tx.ops) == 0 && len(tx.meta) == 0 {
		return Receipt{tok: e.receiptToken(base, e.last.Load())}, nil
	}
	rev, err := state.NewRevision(next, 0)
	if err != nil {
		return Receipt{}, err
	}
	for i := range tx.ops {
		if tx.ops[i].ValueID != 0 {
			tx.ops[i].ValueID = graphstate.ValueID(next<<valSeqBits | uint64(tx.ops[i].ValueID))
		}
	}
	view, err := e.store.ApplicationView(base)
	if err != nil {
		return Receipt{}, err
	}
	defer view.Close()
	effects, _, err := graphstore.StagePartitionOperations(ctx, view, e.decl, e.binding, e.conf, tx.ops, rev, graphstore.Limits{MaxStageRecords: 4096, MaxReadRows: 65536, MaxReadBytes: 64 << 20}, graphstore.GraphLimits{MaxOutputBytes: 64 << 20, MaxSourceRows: 1 << 20, MaxSourceBytes: 64 << 20, RecordedCeiling: tx.ceil(), RevisionRows: true, Pages: graphstore.PageLimits{MaxPatches: 4096, MaxTailAtoms: 4096, MaxWorkRecords: 65536, MaxWorkBytes: 32 << 20}}, readBudget(), tx.decls...)
	if err != nil {
		return Receipt{}, mapStageErr(err)
	}
	digest := sha256.Sum256(binary.BigEndian.AppendUint64(append([]byte("rho-v5 commit:"), prev...), next))
	root, err := effects.Root.AdvanceEffects(digest)
	if err != nil {
		return Receipt{}, err
	}
	img, err := graphstore.EncodeRoot(root)
	if err != nil {
		return Receipt{}, err
	}
	writes := slices.Clone(effects.Writes)
	if tx.alloc.dirty(e.alloc) {
		ar, err := tx.alloc.row()
		if err != nil {
			return Receipt{}, err
		}
		writes = append(writes, ar)
	}
	writes = append(writes, instantRow(next, inst))
	if tx.reopened && !e.multi {
		writes = append(writes, raftlog.KV{Key: multiName, Value: []byte{1}})
	}
	for _, k := range sortedMetaKeys(tx.meta) {
		writes = append(writes, raftlog.KV{Key: metaKey(k), Value: tx.meta[k]})
	}
	slices.SortFunc(writes, func(a, b raftlog.KV) int { return bytes.Compare(a.Key, b.Key) })
	if err := e.commit(next, prev, img, writes); err != nil {
		return Receipt{}, err
	}
	e.last.Store(inst)
	e.alloc = tx.alloc
	e.multi = e.multi || tx.reopened
	for _, d := range tx.decls {
		e.defs(d.Owner)[d.Name] = d
	}
	return Receipt{tok: e.receiptToken(next, inst)}, nil
}

// commit installs one batch at index over the image prev; a limit refusal leaves
// the store usable, any other failure stops the engine.
func (e *pebbleEngine) commit(index uint64, prev, image []byte, writes []raftlog.KV) error {
	if err := e.install(index, sha256.Sum256(prev), image, writes); err != nil {
		if errors.Is(err, raftlog.ErrLimit) {
			return fmt.Errorf("%w: commit exceeds the install limit: %v", ErrInvalidRequest, err)
		}
		be := fmt.Errorf("%w: %v", ErrCommitNotDurable, err)
		e.broken.Store(&be)
		return be
	}
	return nil
}

func (e *pebbleEngine) defs(owner graphstate.EntityKind) map[string]graphstate.PropertyDefinition {
	if owner == graphstate.Node {
		return e.node
	}
	return e.rel
}

// definition finds a declared property: seeds and earlier declarations are in
// the maps, anything else is read from the committed catalog.
func (e *pebbleEngine) definition(ctx context.Context, index uint64, owner graphstate.EntityKind, name string) (graphstate.PropertyDefinition, bool, error) {
	if d, ok := e.defs(owner)[name]; ok {
		return d, true, nil
	}
	view, err := e.store.ApplicationView(index)
	if err != nil {
		return graphstate.PropertyDefinition{}, false, err
	}
	defer view.Close()
	c, _, err := graphstore.OpenPartitionCatalogWithDefaultAxis(ctx, view, e.ns, ownerEpoch, e.binding, graphstore.Limits{}, graphstore.GraphLimits{}, readBudget())
	if err != nil {
		return graphstate.PropertyDefinition{}, false, err
	}
	d, found, err := c.Property(ctx, owner, name)
	if err == nil && found {
		e.defs(owner)[name] = d
	}
	return d, found, err
}

func mapStageErr(err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, graphstate.ErrNotFound):
		return fmt.Errorf("%w: %v", ErrNotFound, err)
	case errors.Is(err, graphstate.ErrUniqueOverlap):
		return fmt.Errorf("%w: %v", ErrUniqueViolation, err)
	case errors.Is(err, graphstate.ErrTypeMismatch), errors.Is(err, graphstate.ErrSchemaMismatch):
		return fmt.Errorf("%w: %v", ErrInvalidValue, err)
	case errors.Is(err, graphstate.ErrValidityRequired), errors.Is(err, graphstate.ErrEmptyMutation), errors.Is(err, graphstate.ErrOwnerValidity), errors.Is(err, graphstate.ErrLifecycleOverlap):
		return fmt.Errorf("%w: %v", ErrInvalidScope, err)
	case errors.Is(err, graphstate.ErrResourceLimit), errors.Is(err, graphstore.ErrResourceLimit):
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	case errors.Is(err, graphstate.ErrAlreadyExists):
		return fmt.Errorf("%w: %v", ErrAlreadyExists, err)
	}
	return fmt.Errorf("%w: %v", ErrInvalidOp, err)
}

// ---- snapshot ----

func (e *pebbleEngine) openSnapshot(ctx context.Context, spec SnapshotSpec) (snapshotEngine, error) {
	// No e.mu: a snapshot may be opened from inside an Update function.
	if err := e.check(); err != nil {
		return nil, err
	}
	applied, _, err := e.store.Checkpoint()
	if err != nil {
		return nil, err
	}
	index := applied
	switch spec.kind {
	case snapAt:
		i, ok := e.parseToken(tokCut, spec.cut.tok)
		if !ok || i > applied {
			return nil, fmt.Errorf("%w: not a cut of this graph", ErrInvalidCut)
		}
		index = i
	case snapAfter:
		i, ok := e.parseToken(tokReceipt, spec.rcpt.tok)
		if !ok || i > applied {
			return nil, fmt.Errorf("%w: not a receipt of this graph", ErrInvalidReceipt)
		}
	case snapKnown:
		if index, err = e.indexAtKnown(ctx, applied, int64(spec.at)); err != nil {
			return nil, err
		}
	}
	// Probe: the view must still be retained.
	v, err := e.store.ApplicationView(index)
	if err != nil {
		if errors.Is(err, raftlog.ErrApplicationExpired) {
			return nil, ErrSnapshotExpired
		}
		return nil, err
	}
	vInst := int64(-1)
	if row, found, gerr := v.Get(ctx, commitInstantKey(index), 64); gerr == nil && found && len(row.Value) == 8 {
		vInst = int64(binary.BigEndian.Uint64(row.Value)) // #nosec G115 -- written from an int64
	}
	_ = v.Close()
	return &engineSnap{e: e, index: index, vInst: vInst, nonce: e.nonce.Add(1), cuts: Cut{tok: e.token(tokCut, index)}, pages: map[uint64]*pageState{}}, nil
}

// indexAtKnown is the newest commit at or before applied whose knowledge
// instant is at most inst (binary search: instants increase with the index).
// An instant before the first commit pins the first commit, the empty graph.
func (e *pebbleEngine) indexAtKnown(ctx context.Context, applied uint64, inst int64) (uint64, error) {
	v, err := e.store.ApplicationView(applied)
	if err != nil {
		return 0, err
	}
	defer v.Close()
	at := func(i uint64) (int64, bool, error) {
		row, found, err := v.Get(ctx, commitInstantKey(i), 64)
		if err != nil || !found {
			return 0, false, err
		}
		if len(row.Value) != 8 {
			return 0, false, fmt.Errorf("%w: commit instant row", ErrCommitNotDurable)
		}
		return int64(binary.BigEndian.Uint64(row.Value)), true, nil // #nosec G115 -- written from an int64
	}
	// Indexes before the first commit count as "at or before"; a missing row at
	// or after it is a damaged store, never a pass.
	lo, hi := uint64(0), applied
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		if mid < e.first {
			lo = mid
			continue
		}
		got, found, err := at(mid)
		if err != nil {
			return 0, err
		}
		if !found {
			return 0, fmt.Errorf("%w: no commit instant for index %d", ErrCommitNotDurable, mid)
		}
		if got <= inst {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	if lo < e.first {
		lo = e.first // before the first commit: the first one, the empty graph
	}
	return min(max(lo, 1), applied), nil
}
