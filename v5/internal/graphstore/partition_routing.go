package graphstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// RoutingBucketKind separates authorities with distinct logical keys. Hashing
// locates a bucket only; identity/unique registries retain the full exact key.
type RoutingBucketKind uint8

// Routing bucket families separate identity, uniqueness and axis authorities.
const (
	IdentityRoutingBucket RoutingBucketKind = iota + 1
	UniqueRoutingBucket
	AxisRoutingBucket
)

// BucketOwnership is one explicit genesis-selected logical bucket assignment.
// Epoch is a bucket epoch, independent of the partition's ownership epoch.
type BucketOwnership struct {
	Kind             RoutingBucketKind
	Bucket           uint32
	Partition, Epoch uint64
}

const routingFixedBytes = 4 + 16 + 8 + 32 + 8 + 1 + 4 + 32
const routingEntryBytes = 21

// PartitionRouting is immutable, bounded graph-qualified genesis data. It grants
// no write/lease authority; a current retained view must bind its stored digest.
type PartitionRouting struct {
	graph             graphstate.GraphID
	declaration       [32]byte
	topology, version uint64
	bits              uint8
	owners            []BucketOwnership
	digest            [32]byte
}

// NewPartitionRouting validates a complete exact-capacity table under record/read
// policy. It binds the declaration and selects no implicit placement.
func NewPartitionRouting(d OwnershipDeclaration, version uint64, bits uint8, owners []BucketOwnership, l Limits) (PartitionRouting, error) {
	l, err := l.resolve()
	if err != nil {
		return PartitionRouting{}, err
	}
	if bits > 32 || version == 0 || !d.valid() {
		return PartitionRouting{}, ErrInvalid
	}
	count64 := uint64(3) << bits
	if count64 > uint64((l.MaxRecordBytes-routingFixedBytes)/routingEntryBytes) { // #nosec G115 -- resolved record minimum130 exceeds fixed105; quotient is nonnegative and<=1MiB.
		return PartitionRouting{}, ErrResourceLimit
	}
	count := int(count64) // #nosec G115 -- count is bounded by the<=1MiB record policy.
	if len(owners) != count {
		return PartitionRouting{}, ErrInvalid
	}
	if routingFixedBytes+routingEntryBytes*count > l.MaxRecordBytes {
		return PartitionRouting{}, ErrResourceLimit
	}
	if 512+64*count+4*(routingFixedBytes+routingEntryBytes*count) > l.MaxReadBytes {
		return PartitionRouting{}, ErrResourceLimit
	}
	r := PartitionRouting{graph: d.Graph(), declaration: d.Digest(), topology: d.TopologyEpoch(), version: version, bits: bits, owners: make([]BucketOwnership, count)}
	for i, o := range owners {
		family := int(o.Kind) - 1
		bucket := int(o.Bucket)
		if family < 0 || family >= 3 || bucket >= 1<<bits || o.Epoch == 0 {
			return PartitionRouting{}, ErrInvalid
		}
		if i != family*(1<<bits)+bucket {
			return PartitionRouting{}, ErrInvalid
		}
		if _, found := d.Partition(o.Partition); !found {
			return PartitionRouting{}, ErrNamespace
		}
		r.owners[i] = o
	}
	wire := r.body()
	r.digest = sha256.Sum256(wire)
	return r, nil
}

// Graph returns the declaration-qualified graph identity by value.
func (r PartitionRouting) Graph() graphstate.GraphID { return r.graph }

// DeclarationDigest returns the immutable declaration fence.
func (r PartitionRouting) DeclarationDigest() [32]byte { return r.declaration }

// Version returns the explicit routing-definition version.
func (r PartitionRouting) Version() uint64 { return r.version }

// Digest returns the canonical full-table digest.
func (r PartitionRouting) Digest() [32]byte { return r.digest }

// Bucket locates an exact logical key in its declared authority family. The hash
// selects placement only; it does not prove key equality or current write authority.
func (r PartitionRouting) Bucket(kind RoutingBucketKind, key []byte) (BucketOwnership, error) {
	if r.graph == (graphstate.GraphID{}) || r.digest == ([32]byte{}) || kind < IdentityRoutingBucket || kind > AxisRoutingBucket || r.bits > 32 || len(key) == 0 {
		return BucketOwnership{}, ErrInvalid
	}
	if kind == AxisRoutingBucket && len(key) != 16 {
		return BucketOwnership{}, ErrInvalid
	}
	hash := sha256.Sum256(key)
	bucket := uint32(0)
	if r.bits != 0 {
		bucket = binary.BigEndian.Uint32(hash[:4]) >> (32 - r.bits)
	}
	count := len(r.owners) / 3
	if uint64(bucket) >= uint64(count) {
		return BucketOwnership{}, ErrCorrupt
	}
	o := r.owners[(int(kind)-1)*count+int(bucket)] // #nosec G115 -- bucket is checked against the bounded table length.
	if o.Kind != kind || o.Partition == 0 || o.Epoch == 0 {
		return BucketOwnership{}, ErrCorrupt
	}
	return o, nil
}

// EncodedBytes preflights the exact immutable-table framing without encoding.
func (r PartitionRouting) EncodedBytes(l Limits) (int, error) {
	l, err := l.resolve()
	if err != nil {
		return 0, err
	}
	if r.graph == (graphstate.GraphID{}) || r.version == 0 || r.bits > 32 || r.digest == ([32]byte{}) || uint64(len(r.owners)) != uint64(3)<<r.bits {
		return 0, ErrInvalid
	}
	if len(r.owners) > (l.MaxRecordBytes-routingFixedBytes)/routingEntryBytes {
		return 0, ErrResourceLimit
	}
	return routingFixedBytes + routingEntryBytes*len(r.owners), nil
}
func (r PartitionRouting) body() []byte {
	count := 3 * (1 << r.bits)
	b := make([]byte, 0, routingFixedBytes-32+count*routingEntryBytes)
	b = append(b, 'R', 'P', 'D', 1)
	b = append(b, r.graph[:]...)
	b = binary.BigEndian.AppendUint64(b, r.topology)
	b = append(b, r.declaration[:]...)
	b = binary.BigEndian.AppendUint64(b, r.version)
	b = append(b, r.bits)
	b = binary.BigEndian.AppendUint32(b, uint32(count)) // #nosec G115 -- immutable table count bounded by<=1MiB record policy.
	for family := range 3 {
		for bucket := range 1 << r.bits {
			o := r.owners[family*(len(r.owners)/3)+bucket]
			b = append(b, byte(o.Kind))
			b = binary.BigEndian.AppendUint32(b, o.Bucket)
			b = binary.BigEndian.AppendUint64(b, o.Partition)
			b = binary.BigEndian.AppendUint64(b, o.Epoch)
		}
	}
	return b
}

// EncodePartitionRouting owns canonical bounded genesis routing bytes.
func EncodePartitionRouting(r PartitionRouting, d OwnershipDeclaration, l Limits) ([]byte, error) {
	l, err := l.resolve()
	if err != nil {
		return nil, err
	}
	if r.graph != d.Graph() || r.declaration != d.Digest() || r.topology != d.TopologyEpoch() || r.version == 0 || r.bits > 32 {
		return nil, ErrInvalid
	}
	if _, err := r.EncodedBytes(l); err != nil {
		return nil, err
	}
	wire := r.body()
	hash := sha256.Sum256(wire)
	if hash != r.digest {
		return nil, ErrInvalid
	}
	return checkRecord(exactCopy(append(wire, hash[:]...)), l)
}

// DecodePartitionRouting validates policy, full declaration binding and canonical bytes.
func DecodePartitionRouting(wire []byte, d OwnershipDeclaration, l Limits) (PartitionRouting, error) {
	l, err := l.resolve()
	if err != nil {
		return PartitionRouting{}, err
	}
	if len(wire) < routingFixedBytes || len(wire) > l.MaxRecordBytes || !bytes.Equal(wire[:4], []byte{'R', 'P', 'D', 1}) {
		return PartitionRouting{}, ErrCorrupt
	}
	bits := wire[68]
	if bits > 32 {
		return PartitionRouting{}, ErrCorrupt
	}
	count64 := uint64(binary.BigEndian.Uint32(wire[69:73]))
	if count64 != uint64(3)<<bits || count64 != uint64((len(wire)-routingFixedBytes)/routingEntryBytes) || (len(wire)-routingFixedBytes)%routingEntryBytes != 0 { // #nosec G115 -- wire length already admitted>=fixed105 and<=1MiB; quotient is nonnegative.
		return PartitionRouting{}, ErrCorrupt
	}
	count := int(count64) // #nosec G115 -- count proven by delivered<=1MiB input length.
	if 512+64*count+4*len(wire) > l.MaxReadBytes {
		return PartitionRouting{}, ErrResourceLimit
	}
	if len(wire) != routingFixedBytes+count*routingEntryBytes {
		return PartitionRouting{}, ErrCorrupt
	}
	hash := sha256.Sum256(wire[:len(wire)-32])
	if !bytes.Equal(hash[:], wire[len(wire)-32:]) {
		return PartitionRouting{}, ErrCorrupt
	}
	var graph graphstate.GraphID
	copy(graph[:], wire[4:20])
	var declaration [32]byte
	copy(declaration[:], wire[28:60])
	if graph != d.Graph() || declaration != d.Digest() || binary.BigEndian.Uint64(wire[20:28]) != d.TopologyEpoch() {
		return PartitionRouting{}, ErrCorrupt
	}
	owners := make([]BucketOwnership, count)
	for i := range owners {
		v := wire[73+i*routingEntryBytes : 73+(i+1)*routingEntryBytes]
		owners[i] = BucketOwnership{Kind: RoutingBucketKind(v[0]), Bucket: binary.BigEndian.Uint32(v[1:5]), Partition: binary.BigEndian.Uint64(v[5:13]), Epoch: binary.BigEndian.Uint64(v[13:21])}
	}
	r, err := NewPartitionRouting(d, binary.BigEndian.Uint64(wire[60:68]), bits, owners, l)
	if err != nil {
		return PartitionRouting{}, errors.Join(ErrCorrupt, err)
	}
	if r.digest != hash {
		return PartitionRouting{}, ErrCorrupt
	}
	return r, nil
}

// RangePublication is immutable provenance for an actual allocated inclusive
// block. RangeID equals its first globally nonreused ID, qualified by GraphID.
// Current placement/fencing is a separate RangeOwnership MVCC record.
type RangePublication struct {
	Graph                                                graphstate.GraphID
	RangeID, First, Last, InitialPartition, InitialEpoch uint64
	Configuration                                        [32]byte
	SourcePartition, SourceIndex                         uint64
}

// Validate checks immutable reservation provenance and inclusive range framing.
func (p RangePublication) Validate() error {
	if p.Graph == (graphstate.GraphID{}) || p.RangeID == 0 || p.RangeID != p.First || p.Last < p.First || p.InitialPartition == 0 || p.InitialEpoch == 0 || p.Configuration == ([32]byte{}) || p.SourcePartition == 0 || p.SourceIndex == 0 {
		return ErrInvalid
	}
	return nil
}

// Contains checks inclusive membership in structurally valid provenance. It
// does not assert current ownership, lease or grant admission.
func (p RangePublication) Contains(id uint64) bool {
	return p.Validate() == nil && id >= p.First && id <= p.Last
}

// RangeOwnership is mutable routing/fence state, not grant provenance. Epochs
// may advance without changing publication bytes or reactivating an old owner.
type RangeOwnership struct {
	Graph                     graphstate.GraphID
	RangeID, Partition, Epoch uint64
	Configuration             [32]byte
	Fenced                    bool
}

// Check binds current routing to immutable provenance and the expected owner/epoch.
// A fenced or different current owner refuses without changing the publication.
func (o RangeOwnership) Check(p RangePublication, partition, epoch uint64) error {
	if err := o.checkPublication(p); err != nil {
		return err
	}
	if o.Fenced || o.Partition != partition || o.Epoch != epoch {
		return ErrStaleOwner
	}
	return nil
}

func (o RangeOwnership) checkPublication(p RangePublication) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if o.Graph != p.Graph || o.RangeID != p.RangeID || o.Configuration != p.Configuration || o.Partition == 0 || o.Epoch < p.InitialEpoch {
		return ErrCorrupt
	}
	return nil
}

const rangePublicationBytes = 4 + 16 + 5*8 + 32 + 2*8 + 32
const rangeOwnershipBytes = 4 + 16 + 3*8 + 32 + 1 + 32

// EncodeRangePublication owns canonical immutable reservation provenance bytes.
func EncodeRangePublication(p RangePublication) ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	b := append([]byte{'R', 'P', 'B', 1}, p.Graph[:]...)
	for _, v := range []uint64{p.RangeID, p.First, p.Last, p.InitialPartition, p.InitialEpoch} {
		b = binary.BigEndian.AppendUint64(b, v)
	}
	b = append(b, p.Configuration[:]...)
	b = binary.BigEndian.AppendUint64(b, p.SourcePartition)
	b = binary.BigEndian.AppendUint64(b, p.SourceIndex)
	h := sha256.Sum256(b)
	return exactCopy(append(b, h[:]...)), nil
}

// DecodeRangePublication validates complete framing, checksum and provenance.
func DecodeRangePublication(wire []byte) (RangePublication, error) {
	if len(wire) != rangePublicationBytes || !bytes.Equal(wire[:4], []byte{'R', 'P', 'B', 1}) {
		return RangePublication{}, ErrCorrupt
	}
	h := sha256.Sum256(wire[:len(wire)-32])
	if !bytes.Equal(h[:], wire[len(wire)-32:]) {
		return RangePublication{}, ErrCorrupt
	}
	var p RangePublication
	copy(p.Graph[:], wire[4:20])
	p.RangeID = binary.BigEndian.Uint64(wire[20:28])
	p.First = binary.BigEndian.Uint64(wire[28:36])
	p.Last = binary.BigEndian.Uint64(wire[36:44])
	p.InitialPartition = binary.BigEndian.Uint64(wire[44:52])
	p.InitialEpoch = binary.BigEndian.Uint64(wire[52:60])
	copy(p.Configuration[:], wire[60:92])
	p.SourcePartition = binary.BigEndian.Uint64(wire[92:100])
	p.SourceIndex = binary.BigEndian.Uint64(wire[100:108])
	if err := p.Validate(); err != nil {
		return RangePublication{}, errors.Join(ErrCorrupt, err)
	}
	return p, nil
}

// EncodeRangeOwnership owns canonical mutable owner/epoch/fence bytes.
func EncodeRangeOwnership(o RangeOwnership) ([]byte, error) {
	if o.Graph == (graphstate.GraphID{}) || o.RangeID == 0 || o.Partition == 0 || o.Epoch == 0 || o.Configuration == ([32]byte{}) {
		return nil, ErrInvalid
	}
	b := append([]byte{'R', 'O', 'F', 1}, o.Graph[:]...)
	for _, v := range []uint64{o.RangeID, o.Partition, o.Epoch} {
		b = binary.BigEndian.AppendUint64(b, v)
	}
	b = append(b, o.Configuration[:]...)
	if o.Fenced {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	h := sha256.Sum256(b)
	return exactCopy(append(b, h[:]...)), nil
}

// DecodeRangeOwnership validates current routing framing and checksum.
func DecodeRangeOwnership(wire []byte) (RangeOwnership, error) {
	if len(wire) != rangeOwnershipBytes || !bytes.Equal(wire[:4], []byte{'R', 'O', 'F', 1}) || wire[76] > 1 {
		return RangeOwnership{}, ErrCorrupt
	}
	h := sha256.Sum256(wire[:len(wire)-32])
	if !bytes.Equal(h[:], wire[len(wire)-32:]) {
		return RangeOwnership{}, ErrCorrupt
	}
	var o RangeOwnership
	copy(o.Graph[:], wire[4:20])
	o.RangeID = binary.BigEndian.Uint64(wire[20:28])
	o.Partition = binary.BigEndian.Uint64(wire[28:36])
	o.Epoch = binary.BigEndian.Uint64(wire[36:44])
	copy(o.Configuration[:], wire[44:76])
	o.Fenced = wire[76] == 1
	if o.Graph == (graphstate.GraphID{}) || o.RangeID == 0 || o.Partition == 0 || o.Epoch == 0 || o.Configuration == ([32]byte{}) {
		return RangeOwnership{}, ErrCorrupt
	}
	return o, nil
}
func routingKey(n Namespace, tag byte) []byte {
	b := append([]byte{'g', 'r', tag}, n.Graph[:]...)
	return binary.BigEndian.AppendUint64(b, n.Partition)
}

// RoutingDefinitionKey builds the graph/partition-qualified definition key.
func RoutingDefinitionKey(n Namespace) []byte { return routingKey(n, 1) }

// RangePublicationKey builds the qualified immutable reservation key.
func RangePublicationKey(n Namespace, rangeID uint64) []byte {
	return binary.BigEndian.AppendUint64(routingKey(n, 2), rangeID)
}

// RangeOwnershipKey builds the qualified MVCC current-ownership key.
func RangeOwnershipKey(n Namespace, rangeID uint64) []byte {
	return binary.BigEndian.AppendUint64(routingKey(n, 3), rangeID)
}

// RangeEndKey builds the qualified inclusive-End seek locator.
func RangeEndKey(n Namespace, end uint64) []byte {
	return binary.BigEndian.AppendUint64(routingKey(n, 4), end)
}

// ErrRoutingUnknown refuses a missing/gapped range; it never proves graph absence.
var ErrRoutingUnknown = errors.New("graphstore: range routing unknown")

// ErrRemoteParticipant refuses a footprint requiring a remote authority/participant.
var ErrRemoteParticipant = errors.New("graphstore: distributed participant required")

// LookupPublishedRange performs a bounded MVCC End>=ID seek. Empty pages with
// visited versions/tombstones continue; an encountered gap never proves absence.
// It borrows the view and returns zero outputs plus consumed work on refusal.
func LookupPublishedRange(ctx context.Context, view *raftlog.ApplicationView, n Namespace, id uint64, configuration [32]byte, budget OwnershipBudget) (RangePublication, RangeOwnership, PageWork, error) {
	return lookupPublishedRangeBudget(ctx, view, n, id, configuration, budget, nil)
}
func lookupPublishedRangeBudget(ctx context.Context, view *raftlog.ApplicationView, n Namespace, id uint64, configuration [32]byte, budget OwnershipBudget, arena *graphstate.OutputBudget) (publication RangePublication, owner RangeOwnership, work PageWork, err error) {
	if ctx == nil || view == nil || id == 0 || n.validate() != nil || configuration == ([32]byte{}) {
		return publication, owner, work, ErrInvalid
	}
	if err := budget.validate(); err != nil {
		return publication, owner, work, err
	}
	if err := ctx.Err(); err != nil {
		return publication, owner, work, err
	}
	// Fixed publication/owner values plus one bounded page, key/continuation and
	// codec scratch coexist. No retained wire can exceed the512-byte page cap.
	if budget.OutputBytes < 1024 || budget.SourceBytes < 512 {
		return publication, owner, work, ErrResourceLimit
	}
	if arena != nil {
		if err := arena.Reserve(512); err != nil {
			return publication, owner, work, callerError(err)
		}
	}
	work.Bytes = 512
	base, err := view.RootBounded(ctx, 172)
	if err != nil {
		return publication, owner, work, err
	}
	binding, err := view.ApplicationBinding(ctx)
	if err != nil {
		return publication, owner, work, err
	}
	if binding != (raftlog.ApplicationBinding{}) && (binding.Identity.Graph != [16]byte(n.Graph) || binding.Identity.Partition != n.Partition) {
		return publication, owner, work, ErrNamespace
	}
	prefix := routingKey(n, 4)
	lower := RangeEndKey(n, id)
	upper := associationPrefixEnd(prefix)
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return RangePublication{}, RangeOwnership{}, work, err
		}
		if work.Records >= budget.SourceRows || budget.SourceBytes-work.Bytes < 256 {
			return RangePublication{}, RangeOwnership{}, work, ErrResourceLimit
		}
		pageBytes := min(512, budget.SourceBytes-work.Bytes, view.ReadLimits().Bytes)
		if arena != nil {
			// Prefix/seek/version-key construction is distinct from delivered rows.
			if err := arena.Reserve(128 + 4*(len(lower)+len(upper)+len(after))); err != nil {
				return RangePublication{}, RangeOwnership{}, work, callerError(err)
			}
			pageBytes = min(pageBytes, arena.Remaining())
			if pageBytes < 1 {
				return RangePublication{}, RangeOwnership{}, work, ErrResourceLimit
			}
		}
		page, e := view.Scan(ctx, lower, upper, after, raftlog.ReadBudget{Rows: min(1, budget.SourceRows-work.Records), Bytes: pageBytes})
		work.Records += page.Visited
		work.Bytes += page.Bytes
		if arena != nil && e == nil {
			if err := arena.Reserve(page.Bytes); err != nil {
				return RangePublication{}, RangeOwnership{}, work, callerError(err)
			}
		}
		if e != nil {
			return RangePublication{}, RangeOwnership{}, work, ownershipOperational(e)
		}
		if len(page.Rows) != 0 {
			row := page.Rows[0]
			p, e := DecodeRangePublication(row.Value)
			if e != nil || len(row.Key) != len(prefix)+8 || !bytes.HasPrefix(row.Key, prefix) || binary.BigEndian.Uint64(row.Key[len(prefix):]) != p.Last || p.Graph != n.Graph || p.Configuration != configuration {
				return RangePublication{}, RangeOwnership{}, work, errors.Join(ErrCorrupt, e)
			}
			if p.SourcePartition == n.Partition && p.SourceIndex > base.Index {
				return RangePublication{}, RangeOwnership{}, work, ErrCorrupt
			}
			// The locator is not provenance authority: verify its complete publication
			// against the separate immutable record before loading mutable placement.
			if work.Records >= budget.SourceRows || budget.SourceBytes-work.Bytes < 256 {
				return RangePublication{}, RangeOwnership{}, work, ErrResourceLimit
			}
			publicationKey := RangePublicationKey(n, p.RangeID)
			work.Records++
			original, present, e := routingGetBounded(ctx, view, publicationKey, min(len(publicationKey)+rangePublicationBytes, budget.SourceBytes-work.Bytes, view.ReadLimits().Bytes), arena)
			work.Bytes += cap(original.Key) + cap(original.Value)
			if e != nil {
				return RangePublication{}, RangeOwnership{}, work, ownershipOperational(e)
			}
			if !present || original.Deleted || !bytes.Equal(original.Value, row.Value) {
				return RangePublication{}, RangeOwnership{}, work, ErrCorrupt
			}
			if !p.Contains(id) {
				return RangePublication{}, RangeOwnership{}, work, ErrRoutingUnknown
			}
			if work.Records >= budget.SourceRows || budget.SourceBytes-work.Bytes < 256 {
				return RangePublication{}, RangeOwnership{}, work, ErrResourceLimit
			}
			key := RangeOwnershipKey(n, p.RangeID)
			work.Records++
			stored, found, e := routingGetBounded(ctx, view, key, min(len(key)+rangeOwnershipBytes, budget.SourceBytes-work.Bytes, view.ReadLimits().Bytes), arena)
			work.Bytes += cap(stored.Key) + cap(stored.Value)
			if e != nil {
				return RangePublication{}, RangeOwnership{}, work, ownershipOperational(e)
			}
			if !found || stored.Deleted {
				return RangePublication{}, RangeOwnership{}, work, ErrCorrupt
			}
			o, e := DecodeRangeOwnership(stored.Value)
			if e != nil || o.Graph != p.Graph || o.RangeID != p.RangeID || o.Configuration != configuration {
				return RangePublication{}, RangeOwnership{}, work, errors.Join(ErrCorrupt, e)
			}
			if err := o.checkPublication(p); err != nil {
				return RangePublication{}, RangeOwnership{}, work, err
			}
			if rangePublicationBytes+rangeOwnershipBytes+256 > budget.OutputBytes {
				return RangePublication{}, RangeOwnership{}, work, ErrResourceLimit
			}
			return p, o, work, nil
		}
		if page.Complete {
			return RangePublication{}, RangeOwnership{}, work, ErrRoutingUnknown
		}
		if len(page.Next) == 0 || bytes.Equal(page.Next, after) {
			return RangePublication{}, RangeOwnership{}, work, ErrCorrupt
		}
		after = page.Next
	}
}

func routingGetBounded(ctx context.Context, view *raftlog.ApplicationView, key []byte, maxBytes int, arena *graphstate.OutputBudget) (raftlog.KV, bool, error) {
	if arena != nil {
		if err := arena.Reserve(128 + 4*len(key)); err != nil {
			return raftlog.KV{}, false, callerError(err)
		}
		maxBytes = min(maxBytes, arena.Remaining())
		if maxBytes < len(key) {
			return raftlog.KV{}, false, ErrResourceLimit
		}
	}
	row, found, err := view.Get(ctx, key, maxBytes)
	if err == nil && arena != nil {
		err = callerError(arena.Reserve(cap(row.Key) + cap(row.Value)))
	}
	if err != nil {
		return raftlog.KV{}, false, err
	}
	return row, found, nil
}
