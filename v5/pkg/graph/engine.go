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

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
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
	// commitSeqBits leaves 12 bits of per-commit sequence in an entity ID and 20
	// in a value handle: IDs are index<<12|seq, so they are unique across reopen
	// without a persisted counter (idalloc is a later binding).
	idSeqBits  = 12
	valSeqBits = 20
)

var (
	contractID = sha256.Sum256([]byte("rho-tkg:v5/pkg/graph:single-partition:v1"))
	lifeID     = graphstate.LifeID(1)
)

func readBudget() graphstore.OwnershipBudget {
	return graphstore.OwnershipBudget{SourceRows: 65536, SourceBytes: 64 << 20, OutputBytes: 64 << 20}
}

type pebbleEngine struct {
	mu      sync.Mutex
	cfg     Config
	store   *raftlog.Store
	decl    graphstore.OwnershipDeclaration
	ns      graphstore.Namespace
	conf    [32]byte
	binding types.DefaultAxisBinding
	node    map[string]graphstate.PropertyDefinition
	rel     map[string]graphstate.PropertyDefinition
	closed  bool
	broken  error
	nonce   uint64
}

var _ engine = (*pebbleEngine)(nil)

func openEngine(ctx context.Context, cfg Config) (*pebbleEngine, error) {
	gid := graphstate.GraphID(cfg.GraphID)
	group := [16]byte(cfg.GraphID)
	decl, err := graphstore.NewOwnershipDeclaration(gid, 1, []graphstore.PartitionOwnership{{Partition: partitionID, OwnershipEpoch: ownerEpoch, Group: group}}, graphstore.Limits{})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	e := &pebbleEngine{cfg: cfg, decl: decl, ns: graphstore.Namespace{Graph: gid, Partition: partitionID}, binding: cfg.DefaultAxis,
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
	writes := append(slices.Clone(effects.Writes), rows...)
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

func (e *pebbleEngine) check() error {
	if e.closed {
		return ErrClosed
	}
	return e.broken
}

func (e *pebbleEngine) close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	e.closed = true
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

func (e *pebbleEngine) parseToken(kind byte, b []byte) (uint64, bool) {
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
	tx := &engineTx{e: e, ctx: ctx, base: base, next: next}
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
	if len(tx.ops) == 0 {
		return Receipt{tok: e.token(tokReceipt, base)}, nil
	}
	rev, err := state.NewRevision(next, 0)
	if err != nil {
		return Receipt{}, err
	}
	view, err := e.store.ApplicationView(base)
	if err != nil {
		return Receipt{}, err
	}
	defer view.Close()
	effects, _, err := graphstore.StagePartitionOperations(ctx, view, e.decl, e.binding, e.conf, tx.ops, rev, graphstore.Limits{}, graphstore.GraphLimits{}, readBudget())
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
	if err := e.install(next, sha256.Sum256(prev), img, effects.Writes); err != nil {
		if errors.Is(err, raftlog.ErrLimit) {
			return Receipt{}, fmt.Errorf("%w: commit exceeds the install limit: %v", ErrInvalidRequest, err)
		}
		e.broken = fmt.Errorf("%w: %v", ErrCommitNotDurable, err)
		return Receipt{}, e.broken
	}
	return Receipt{tok: e.token(tokReceipt, next)}, nil
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
		return fmt.Errorf("%w: %v", ErrConflict, err)
	}
	return fmt.Errorf("%w: %v", ErrInvalidOp, err)
}

// ---- snapshot ----

func (e *pebbleEngine) openSnapshot(ctx context.Context, spec SnapshotSpec) (snapshotEngine, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
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
	}
	// Probe: the view must still be retained.
	v, err := e.store.ApplicationView(index)
	if err != nil {
		if errors.Is(err, raftlog.ErrApplicationExpired) {
			return nil, ErrSnapshotExpired
		}
		return nil, err
	}
	_ = v.Close()
	e.nonce++
	return &engineSnap{e: e, index: index, nonce: e.nonce, cuts: Cut{tok: e.token(tokCut, index)}, pages: map[uint64]*pageState{}}, nil
}
