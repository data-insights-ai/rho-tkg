package graphstore

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const ownershipDeclarationRecord byte = 0x16
const ownershipPending byte = 3
const ownershipPublished byte = 4
const ownershipInitialized byte = 5
const ownershipRecordFixedBytes = 64
const ownershipKeyBytes = 57
const ownershipEntryBytes = 32
const ownershipMetadataBytes = 1536 // Conservative representation charge, not heap/RSS.
const ownershipDeclarationMetadataBytes = 128
const ownershipHashDomain = "rho-tkg:ownership-declaration:v1\x00"

func (r Root) hasOwnershipDeclaration() bool {
	return r.ownershipMode != 0 || r.ownershipDigest != ([32]byte{})
}

// PartitionOwnership identifies one qualified placement. Group bytes may repeat
// across different (Graph, Partition) identities; no voter/server is recorded.
type PartitionOwnership struct {
	Partition, OwnershipEpoch uint64
	Group                     [16]byte
}

// OwnershipBudget is mandatory per-operation work/retained-output headroom.
// It does not change Catalog policy floors, reserve installation or bound RSS.
type OwnershipBudget struct{ SourceRows, SourceBytes, OutputBytes int }

func (b OwnershipBudget) validate() error {
	if b.SourceRows < 1 || b.SourceRows > 65536 || b.SourceBytes < 1 || b.SourceBytes > 64<<20 || b.OutputBytes < 1 || b.OutputBytes > 64<<20 {
		return ErrInvalid
	}
	return nil
}

// OwnershipDeclaration owns inaccessible immutable exact-cap entry backing.
// Copies may share that backing; every accessor returns only fixed values.
type OwnershipDeclaration struct {
	graph   graphstate.GraphID
	epoch   uint64
	digest  [32]byte
	entries []PartitionOwnership
}

// NewOwnershipDeclaration checks bounds before one owned copy, sorts by
// partition and refuses duplicates. It does not establish live ownership.
func NewOwnershipDeclaration(graph graphstate.GraphID, topologyEpoch uint64, partitions []PartitionOwnership, limits Limits) (OwnershipDeclaration, error) {
	l, err := limits.resolve()
	if err != nil {
		return OwnershipDeclaration{}, err
	}
	if graph == (graphstate.GraphID{}) || topologyEpoch == 0 || len(partitions) == 0 {
		return OwnershipDeclaration{}, ErrInvalid
	}
	if len(partitions) > (l.MaxRecordBytes-ownershipRecordFixedBytes)/ownershipEntryBytes || ownershipDeclarationMetadataBytes+ownershipEntryBytes*len(partitions) > l.MaxReadBytes {
		return OwnershipDeclaration{}, ErrResourceLimit
	}
	for _, p := range partitions {
		if !validPartitionOwnership(p) {
			return OwnershipDeclaration{}, ErrInvalid
		}
	}
	entries := make([]PartitionOwnership, len(partitions))
	copy(entries, partitions)
	slices.SortFunc(entries, func(a, b PartitionOwnership) int { return cmp.Compare(a.Partition, b.Partition) })
	for i := 1; i < len(entries); i++ {
		if entries[i-1].Partition == entries[i].Partition {
			return OwnershipDeclaration{}, ErrInvalid
		}
	}
	return OwnershipDeclaration{graph, topologyEpoch, ownershipDigest(graph, topologyEpoch, entries), entries}, nil
}

func validPartitionOwnership(p PartitionOwnership) bool {
	return p.Partition != 0 && p.OwnershipEpoch != 0 && p.Group != ([16]byte{})
}
func (d OwnershipDeclaration) valid() bool {
	if d.graph == (graphstate.GraphID{}) || d.epoch == 0 || d.digest == ([32]byte{}) || len(d.entries) == 0 || len(d.entries) > (1<<20-ownershipRecordFixedBytes)/ownershipEntryBytes {
		return false
	}
	for i, p := range d.entries {
		if !validPartitionOwnership(p) || i > 0 && d.entries[i-1].Partition >= p.Partition {
			return false
		}
	}
	return true
}
func ownershipDigest(graph graphstate.GraphID, epoch uint64, entries []PartitionOwnership) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte(ownershipHashDomain))
	_, _ = h.Write(graph[:])
	var fixed [12]byte
	binary.BigEndian.PutUint64(fixed[:8], epoch)
	binary.BigEndian.PutUint32(fixed[8:], uint32(len(entries))) // #nosec G115 -- callers bound entries below 32767 before hashing.
	_, _ = h.Write(fixed[:])
	var row [ownershipEntryBytes]byte
	for _, p := range entries {
		binary.BigEndian.PutUint64(row[:8], p.Partition)
		binary.BigEndian.PutUint64(row[8:16], p.OwnershipEpoch)
		copy(row[16:], p.Group[:])
		_, _ = h.Write(row[:])
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

// Graph returns declaration identity, zero for an empty value.
func (d OwnershipDeclaration) Graph() graphstate.GraphID { return d.graph }

// TopologyEpoch returns the immutable declaration version, never a clock.
func (d OwnershipDeclaration) TopologyEpoch() uint64 { return d.epoch }

// Digest returns logical declaration identity by value.
func (d OwnershipDeclaration) Digest() [32]byte { return d.digest }

// Len returns declared entry count; zero is not an authoritative empty topology.
func (d OwnershipDeclaration) Len() int { return len(d.entries) }

// PartitionAt enumerates immutable entries without allocating or exposing backing.
func (d OwnershipDeclaration) PartitionAt(index int) (PartitionOwnership, bool) {
	if index < 0 || index >= len(d.entries) {
		return PartitionOwnership{}, false
	}
	return d.entries[index], true
}

// Partition looks up one declared partition, not its present owner/lease.
func (d OwnershipDeclaration) Partition(partition uint64) (PartitionOwnership, bool) {
	i, found := slices.BinarySearchFunc(d.entries, partition, func(p PartitionOwnership, id uint64) int { return cmp.Compare(p.Partition, id) })
	if !found {
		return PartitionOwnership{}, false
	}
	return d.entries[i], true
}

// NewOwnershipRoot creates a pending metadata seed, not a ready graph. Store
// initialization remains an explicit separate action with configured membership.
func NewOwnershipRoot(local Namespace, declaration OwnershipDeclaration) (Root, error) {
	if err := local.validate(); err != nil {
		return Root{}, err
	}
	if !declaration.valid() {
		return Root{}, ErrInvalid
	}
	if declaration.graph != local.Graph {
		return Root{}, ErrNamespace
	}
	p, found := declaration.Partition(local.Partition)
	if !found {
		return Root{}, ErrNamespace
	}
	r, err := NewRoot(local, p.OwnershipEpoch)
	if err != nil {
		return Root{}, err
	}
	r.topology = topologyDeclaration{epoch: declaration.epoch, schema: 1}
	r.ownershipDigest, r.ownershipMode = declaration.digest, ownershipPending
	return r, nil
}

func ownershipKey(graph graphstate.GraphID, epoch uint64, digest [32]byte) []byte {
	b := make([]byte, ownershipKeyBytes)
	b[0] = ownershipDeclarationRecord
	copy(b[1:17], graph[:])
	binary.BigEndian.PutUint64(b[17:25], epoch)
	copy(b[25:], digest[:])
	return b
}
func encodeOwnershipDeclaration(d OwnershipDeclaration, l Limits) ([]byte, error) {
	if !d.valid() {
		return nil, ErrInvalid
	}
	if len(d.entries) > (l.MaxRecordBytes-ownershipRecordFixedBytes)/ownershipEntryBytes {
		return nil, ErrResourceLimit
	}
	b := make([]byte, ownershipRecordFixedBytes+ownershipEntryBytes*len(d.entries))
	copy(b, []byte{'G', 'O', 'D', 1})
	copy(b[4:20], d.graph[:])
	binary.BigEndian.PutUint64(b[20:28], d.epoch)
	binary.BigEndian.PutUint32(b[28:32], uint32(len(d.entries))) // #nosec G115 -- the record limit bounds entries below 32767.
	for i, p := range d.entries {
		offset := 32 + ownershipEntryBytes*i
		binary.BigEndian.PutUint64(b[offset:offset+8], p.Partition)
		binary.BigEndian.PutUint64(b[offset+8:offset+16], p.OwnershipEpoch)
		copy(b[offset+16:offset+32], p.Group[:])
	}
	copy(b[len(b)-32:], d.digest[:])
	return b, nil
}

// OwnershipViewReference names local captured coordinates, not a cut/receipt.
type OwnershipViewReference struct {
	Generation, Index uint64
	ImageHash         [32]byte
}

// OwnershipRead owns immutable declaration metadata, never a Full ReadView.
// Pending/missing reports only checked root/local namespace binding: it has no
// declaration entries with which to verify Group or ownership epoch agreement.
type OwnershipRead struct {
	root        Root
	binding     raftlog.ApplicationBinding
	declaration OwnershipDeclaration
	reference   OwnershipViewReference
	published   bool
	ownedBytes  int
}

// Root returns copied root metadata, with no lifetime/admission authority.
func (r OwnershipRead) Root() Root { return r.root }

// Binding returns the actual same-view store configuration, not a lease.
func (r OwnershipRead) Binding() raftlog.ApplicationBinding { return r.binding }

// Declaration returns immutable shared metadata; pending/missing returns zero.
func (r OwnershipRead) Declaration() OwnershipDeclaration { return r.declaration }

// Reference returns captured local generation/index/hash, never semantic time.
func (r OwnershipRead) Reference() OwnershipViewReference { return r.reference }

// Published means declaration record/root/local entry validation succeeded. It
// never grants graph readiness, quorum/cut coverage or a current ownership lease.
func (r OwnershipRead) Published() bool { return r.published }

// OwnedBytes returns retained representation accounting, not heap/RSS.
func (r OwnershipRead) OwnedBytes() int { return r.ownedBytes }

// OwnershipEffects owns computed private buffers for the trusted outer batch.
// It exposes no Stage, installs nothing and admits no caller-supplied KV/Delta.
// Keep returned buffers unchanged between outer Preflight and installation.
type OwnershipEffects struct {
	Base       raftlog.ApplicationRoot
	Root       Root
	Writes     []raftlog.KV
	OwnedBytes int
}

type ownershipReader struct {
	ctx    context.Context
	view   *raftlog.ApplicationView
	limits Limits
	budget OwnershipBudget
	work   PageWork
}

func (q *ownershipReader) charge(bytes int) error {
	if bytes < 0 || bytes > q.budget.SourceBytes-q.work.Bytes {
		return ErrResourceLimit
	}
	q.work.Bytes += bytes
	return nil
}
func (q *ownershipReader) output(bytes int) error {
	if bytes < 0 || bytes > q.budget.OutputBytes {
		return ErrResourceLimit
	}
	return nil
}
func ownershipOperational(err error) error {
	if errors.Is(err, raftlog.ErrLimit) {
		return errors.Join(ErrResourceLimit, err)
	}
	return err
}
func startOwnershipReader(ctx context.Context, view *raftlog.ApplicationView, limits Limits, budget OwnershipBudget) (*ownershipReader, raftlog.ApplicationRoot, Root, raftlog.ApplicationBinding, error) {
	if ctx == nil || view == nil {
		return nil, raftlog.ApplicationRoot{}, Root{}, raftlog.ApplicationBinding{}, ErrInvalid
	}
	l, err := limits.resolve()
	if err != nil {
		return nil, raftlog.ApplicationRoot{}, Root{}, raftlog.ApplicationBinding{}, err
	}
	if err := budget.validate(); err != nil {
		return nil, raftlog.ApplicationRoot{}, Root{}, raftlog.ApplicationBinding{}, err
	}
	budget.SourceRows = min(budget.SourceRows, l.MaxReadRows)
	budget.SourceBytes = min(budget.SourceBytes, l.MaxReadBytes)
	budget.OutputBytes = min(budget.OutputBytes, l.MaxReadBytes)
	if err := ctx.Err(); err != nil {
		return nil, raftlog.ApplicationRoot{}, Root{}, raftlog.ApplicationBinding{}, err
	}
	if budget.SourceBytes < ownershipMetadataBytes+ownershipRootBytes || budget.OutputBytes < ownershipMetadataBytes {
		return nil, raftlog.ApplicationRoot{}, Root{}, raftlog.ApplicationBinding{}, ErrResourceLimit
	}
	q := &ownershipReader{ctx: ctx, view: view, limits: l, budget: budget}
	if err := q.charge(ownershipMetadataBytes + ownershipRootBytes); err != nil {
		return q, raftlog.ApplicationRoot{}, Root{}, raftlog.ApplicationBinding{}, err
	}
	base, err := view.RootBounded(ctx, ownershipRootBytes)
	if err != nil {
		return q, raftlog.ApplicationRoot{}, Root{}, raftlog.ApplicationBinding{}, ownershipOperational(err)
	}
	r, err := DecodeRoot(base.Image)
	if err != nil {
		return q, raftlog.ApplicationRoot{}, Root{}, raftlog.ApplicationBinding{}, err
	}
	if !r.hasOwnershipDeclaration() {
		return q, raftlog.ApplicationRoot{}, Root{}, raftlog.ApplicationBinding{}, ErrTopologyUnsupported
	}
	binding, err := view.ApplicationBinding(ctx)
	if err != nil {
		return q, raftlog.ApplicationRoot{}, Root{}, raftlog.ApplicationBinding{}, err
	}
	if err := binding.Validate(); err != nil {
		return q, raftlog.ApplicationRoot{}, Root{}, raftlog.ApplicationBinding{}, errors.Join(ErrInvalid, err)
	}
	if binding.Identity.Graph != r.namespace.Graph || binding.Identity.Partition != r.namespace.Partition {
		return q, raftlog.ApplicationRoot{}, Root{}, raftlog.ApplicationBinding{}, ErrNamespace
	}
	return q, base, r, binding, nil
}
func ownershipWork(q *ownershipReader) PageWork {
	if q == nil {
		return PageWork{}
	}
	return q.work
}

func (q *ownershipReader) readDeclaration(root Root) (OwnershipDeclaration, bool, error) {
	if err := q.ctx.Err(); err != nil {
		return OwnershipDeclaration{}, false, err
	}
	if q.work.Records >= q.budget.SourceRows {
		return OwnershipDeclaration{}, false, ErrResourceLimit
	}
	if err := q.charge(ownershipKeyBytes + 64); err != nil {
		return OwnershipDeclaration{}, false, err
	}
	key := ownershipKey(root.namespace.Graph, root.topology.epoch, root.ownershipDigest)
	q.work.Records++
	maxBytes := min(ownershipKeyBytes+q.limits.MaxRecordBytes, ownershipKeyBytes+q.budget.SourceBytes-q.work.Bytes, q.view.ReadLimits().Bytes)
	row, found, err := q.view.Get(q.ctx, key, maxBytes)
	if err != nil {
		return OwnershipDeclaration{}, false, ownershipOperational(err)
	}
	if !found {
		return OwnershipDeclaration{}, false, nil
	}
	if err := q.charge(cap(row.Value)); err != nil {
		return OwnershipDeclaration{}, false, err
	}
	if row.Deleted || len(row.Value) < ownershipRecordFixedBytes || !bytes.Equal(row.Value[:4], []byte{'G', 'O', 'D', 1}) {
		return OwnershipDeclaration{}, false, ErrCorrupt
	}
	n := binary.BigEndian.Uint32(row.Value[28:32])
	if uint64(n) != uint64((len(row.Value)-ownershipRecordFixedBytes)/ownershipEntryBytes) || (len(row.Value)-ownershipRecordFixedBytes)%ownershipEntryBytes != 0 || n == 0 {
		return OwnershipDeclaration{}, false, ErrCorrupt
	}
	if err := q.charge(ownershipEntryBytes * int(n)); err != nil {
		return OwnershipDeclaration{}, false, err
	}
	if err := q.output(ownershipMetadataBytes + ownershipEntryBytes*int(n)); err != nil {
		return OwnershipDeclaration{}, false, err
	}
	var graph graphstate.GraphID
	copy(graph[:], row.Value[4:20])
	d := OwnershipDeclaration{graph: graph, epoch: binary.BigEndian.Uint64(row.Value[20:28]), entries: make([]PartitionOwnership, int(n))}
	copy(d.digest[:], row.Value[len(row.Value)-32:])
	for i := range d.entries {
		b := row.Value[32+ownershipEntryBytes*i : 32+ownershipEntryBytes*(i+1)]
		d.entries[i].Partition = binary.BigEndian.Uint64(b[:8])
		d.entries[i].OwnershipEpoch = binary.BigEndian.Uint64(b[8:16])
		copy(d.entries[i].Group[:], b[16:])
	}
	if !d.valid() || d.graph != root.namespace.Graph || d.epoch != root.topology.epoch || d.digest != root.ownershipDigest || ownershipDigest(d.graph, d.epoch, d.entries) != d.digest {
		return OwnershipDeclaration{}, false, ErrCorrupt
	}
	return d, true, nil
} // #nosec G115 -- delivered <=1MiB record length proves n below32767 before every int conversion/allocation.

func checkOwnershipEntry(root Root, binding raftlog.ApplicationBinding, d OwnershipDeclaration) error {
	if d.graph != root.namespace.Graph || d.epoch != root.topology.epoch {
		return ErrNamespace
	}
	p, found := d.Partition(root.namespace.Partition)
	if !found || p.Group != binding.Identity.Group {
		return ErrNamespace
	}
	if p.OwnershipEpoch != root.owner {
		return ErrStaleOwner
	}
	return nil
}

// ReadOwnershipDeclaration borrows exactly one ApplicationView for root, binding
// and one point record. Errors return zero metadata and separate consumed work.
// Pending/missing cannot validate a group against an absent declaration entry.
func ReadOwnershipDeclaration(ctx context.Context, view *raftlog.ApplicationView, limits Limits, budget OwnershipBudget) (OwnershipRead, PageWork, error) {
	q, base, root, binding, err := startOwnershipReader(ctx, view, limits, budget)
	if err != nil {
		return OwnershipRead{}, ownershipWork(q), err
	}
	if err := q.output(ownershipMetadataBytes); err != nil {
		return OwnershipRead{}, q.work, err
	}
	d, found, err := q.readDeclaration(root)
	if err != nil {
		return OwnershipRead{}, q.work, err
	}
	if root.ownershipMode == ownershipPending {
		if found {
			return OwnershipRead{}, q.work, ErrCorrupt
		}
	} else if !found {
		return OwnershipRead{}, q.work, ErrCorrupt
	} else if err := checkOwnershipEntry(root, binding, d); err != nil {
		return OwnershipRead{}, q.work, err
	}
	owned := ownershipMetadataBytes + ownershipEntryBytes*len(d.entries)
	return OwnershipRead{root, binding, d, OwnershipViewReference{base.Generation, base.Index, base.ImageHash}, found, owned}, q.work, nil
}

// StageOwnershipDeclaration emits computed private effects only. Pending seeds
// require authoritative same-view emptiness and selected owner/group binding;
// exact published retries emit no KV and never allocate graph/recipient IDs.
func StageOwnershipDeclaration(ctx context.Context, view *raftlog.ApplicationView, declaration OwnershipDeclaration, limits Limits, budget OwnershipBudget) (OwnershipEffects, PageWork, error) {
	q, base, root, binding, err := startOwnershipReader(ctx, view, limits, budget)
	if err != nil {
		return OwnershipEffects{}, ownershipWork(q), err
	}
	if len(declaration.entries) > (q.limits.MaxRecordBytes-ownershipRecordFixedBytes)/ownershipEntryBytes {
		return OwnershipEffects{}, q.work, ErrResourceLimit
	}
	if err := q.charge(ownershipEntryBytes * len(declaration.entries)); err != nil {
		return OwnershipEffects{}, q.work, err
	}
	if !declaration.valid() {
		return OwnershipEffects{}, q.work, ErrInvalid
	}
	if declaration.digest != root.ownershipDigest {
		return OwnershipEffects{}, q.work, ErrRebinding
	}
	if err := checkOwnershipEntry(root, binding, declaration); err != nil {
		return OwnershipEffects{}, q.work, err
	}
	if root.ownershipMode == ownershipPublished {
		actual, found, err := q.readDeclaration(root)
		if err != nil {
			return OwnershipEffects{}, q.work, err
		}
		if !found || actual.digest != declaration.digest {
			return OwnershipEffects{}, q.work, ErrCorrupt
		}
		cost := ownershipMetadataBytes + cap(base.Image)
		if err := q.output(cost); err != nil {
			return OwnershipEffects{}, q.work, err
		}
		return OwnershipEffects{Base: base, Root: root, OwnedBytes: cost}, q.work, nil
	}
	seed, err := NewOwnershipRoot(root.namespace, declaration)
	if err != nil {
		return OwnershipEffects{}, q.work, err
	}
	if root != seed {
		return OwnershipEffects{}, q.work, ErrTopologyUnsupported
	}
	if len(declaration.entries) > (q.limits.MaxRecordBytes-ownershipRecordFixedBytes)/ownershipEntryBytes {
		return OwnershipEffects{}, q.work, ErrResourceLimit
	}
	recordBytes := ownershipRecordFixedBytes + ownershipEntryBytes*len(declaration.entries)
	stageBytes := 128 + 2*ownershipKeyBytes + recordBytes + 64
	if q.limits.MaxStageRecords < 1 || stageBytes > q.limits.MaxStageBytes {
		return OwnershipEffects{}, q.work, ErrResourceLimit
	}
	// Existing base and empty-proof image coexist with the one exact wire/key
	// output and its header. Fixed charges cover typed effects and hashing.
	if err := q.charge(ownershipRootBytes + ownershipKeyBytes + recordBytes + 64); err != nil {
		return OwnershipEffects{}, q.work, err
	}
	output := ownershipMetadataBytes + ownershipRootBytes + 64 + ownershipKeyBytes + recordBytes
	if err := q.output(output); err != nil {
		return OwnershipEffects{}, q.work, err
	}
	proof, err := view.ProveNoApplicationData(ctx)
	if err != nil {
		return OwnershipEffects{}, q.work, err
	}
	if proof.Generation != base.Generation || proof.Index != base.Index || proof.ImageHash != base.ImageHash || !bytes.Equal(proof.Image, base.Image) {
		return OwnershipEffects{}, q.work, ErrInvalid
	}
	wire, err := encodeOwnershipDeclaration(declaration, q.limits)
	if err != nil {
		return OwnershipEffects{}, q.work, err
	}
	root.ownershipMode = ownershipPublished
	writes := []raftlog.KV{{Key: ownershipKey(root.namespace.Graph, root.topology.epoch, root.ownershipDigest), Value: wire}}
	return OwnershipEffects{Base: base, Root: root, Writes: writes, OwnedBytes: output}, q.work, nil
}
