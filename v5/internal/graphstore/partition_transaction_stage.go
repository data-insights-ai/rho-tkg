package graphstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

type transactionInstallation struct {
	view      *PartitionTransactionView
	pages     []*pageStage
	states    []fullStageState
	produced  []graphstate.ComponentPatch
	active    *graphstate.ComponentPatch
	tags      map[string][]int
	groupTags [][]int
	tagSlots  int
}

// PartitionTransactionTemplate is an internally planned, bounded immutable
// complete graph effect template. Decoding inspection bytes cannot create one.
type PartitionTransactionTemplate struct {
	view                                graphstate.ViewID
	graph                               graphstate.GraphID
	declaration, configuration, routing [32]byte
	participants                        []partitionTemplateEffects
	digest                              [32]byte
	dependencies                        []graphstate.Dependency
	bases                               []raftlog.ApplicationRoot
	provisional                         state.Revision
	defaultAxis                         types.DefaultAxisBinding
	limits                              Limits
}
type partitionTemplateEffects struct {
	partition  uint64
	before     Root
	effects    GraphEffects
	writeTags  [][]int
	groupTags  [][]int
	groupWires [][]byte
	digest     [32]byte
}

// PreparedKernelBinding binds one participant's prepared graph kernel to the
// outer transaction protocol. Graphstore supplies no decision/quorum authority.
type PreparedKernelBinding struct {
	TxID                                                                          [16]byte
	Graph                                                                         graphstate.GraphID
	Partition, OwnershipEpoch                                                     uint64
	Configuration, DeclarationDigest, RoutingDigest, TemplateDigest, EffectDigest [32]byte
	CapturedRoot                                                                  Root
	CapturedBase                                                                  raftlog.ApplicationRoot
}

// PreparedPartitionKernel is stageable only after internal planning or a checked
// read of its co-committed durable local record. It exposes no raw Delta input.
type PreparedPartitionKernel struct {
	binding     PreparedKernelBinding
	provisional state.Revision
	part        partitionTemplateEffects
	wire        []byte
	defaultAxis types.DefaultAxisBinding
	limits      Limits
}

// PreparedKernelRestoreProof names graphapply's already checked immutable local
// prepare control. Graphstore independently verifies its actual value and
// insertion index against the co-installed kernel; the outer protocol supplies
// its complete typed prepare/lock/decision interpretation.
type PreparedKernelRestoreProof struct {
	PrepareKey    []byte
	PrepareDigest [32]byte
	PrepareIndex  uint64
}

// PreparedKernelIdentities owns only internally planned fresh identity metadata,
// for graphapply's separate allocator/grant checks before durable prepare.
type PreparedKernelIdentities struct {
	Entities []graphstate.EntityID
	Lives    []graphstate.LifeRecord
	Values   []graphstate.ValueID
	// Credits describe this exact kernel's owned graph installation/CDC body;
	// graphapply additionally accounts its own entry/control/outcome framing.
	WriteRecords, WriteBytes, Groups, ChangeBytes int
}

func preparedKernelKey(binding PreparedKernelBinding) [44]byte {
	var key [44]byte
	copy(key[:4], []byte{'G', 'P', 'K', 1})
	copy(key[4:20], binding.Graph[:])
	binary.BigEndian.PutUint64(key[20:28], binding.Partition)
	copy(key[28:], binding.TxID[:])
	return key
}

// PreparedKernelControlKey owns the canonical graph/participant/Tx control key.
func PreparedKernelControlKey(binding PreparedKernelBinding, output *graphstate.OutputBudget) ([]byte, error) {
	if output == nil || binding.Graph == (graphstate.GraphID{}) || binding.Partition == 0 || binding.TxID == ([16]byte{}) {
		return nil, ErrInvalid
	}
	if err := output.Reserve(108); err != nil {
		return nil, callerError(err)
	}
	key := preparedKernelKey(binding)
	return exactCopy(key[:]), nil
}

// PreparedKernelPrepareControlKey owns the shared canonical immutable graph
// prepare identity. Graphapply owns and checks the prepare record's typed body.
func PreparedKernelPrepareControlKey(binding PreparedKernelBinding, output *graphstate.OutputBudget) ([]byte, error) {
	key, err := PreparedKernelControlKey(binding, output)
	if err != nil {
		return nil, err
	}
	key[2] = 'P'
	return key, nil
}

// Digest returns the complete transaction template identity.
func (t *PartitionTransactionTemplate) Digest() [32]byte {
	if t == nil {
		return [32]byte{}
	}
	return t.digest
}

// Participants returns an owned exact declared participant set under admission.
func (t *PartitionTransactionTemplate) Participants(output *graphstate.OutputBudget) ([]uint64, error) {
	if t == nil || output == nil {
		return nil, ErrInvalid
	}
	if err := output.Reserve(64 + 8*len(t.participants)); err != nil {
		return nil, callerError(err)
	}
	out := make([]uint64, len(t.participants))
	for n, p := range t.participants {
		out[n] = p.partition
	}
	return out, nil
}

// Dependencies returns all nine internally derived semantic dependency kinds.
func (t *PartitionTransactionTemplate) Dependencies(output *graphstate.OutputBudget) ([]graphstate.Dependency, error) {
	if t == nil || output == nil {
		return nil, ErrInvalid
	}
	if err := output.Reserve(64 + fullDependencyOutputBytes*len(t.dependencies)); err != nil {
		return nil, callerError(err)
	}
	out := make([]graphstate.Dependency, len(t.dependencies))
	copy(out, t.dependencies)
	return out, nil
}

// Bases returns owned actual source coordinates, never graph-cut certificates.
func (t *PartitionTransactionTemplate) Bases(output *graphstate.OutputBudget) ([]raftlog.ApplicationRoot, error) {
	if t == nil || output == nil {
		return nil, ErrInvalid
	}
	cost := 64 + 128*len(t.bases)
	for _, b := range t.bases {
		cost += len(b.Image)
	}
	if err := output.Reserve(cost); err != nil {
		return nil, callerError(err)
	}
	out := make([]raftlog.ApplicationRoot, len(t.bases))
	copy(out, t.bases)
	for n := range out {
		out[n].Image = exactCopy(out[n].Image)
	}
	return out, nil
}

// ParticipantKernel binds the private participant effects to an outer TxID.
func (t *PartitionTransactionTemplate) ParticipantKernel(partition uint64, tx [16]byte, output *graphstate.OutputBudget) (*PreparedPartitionKernel, error) {
	if t == nil || tx == ([16]byte{}) || output == nil {
		return nil, ErrInvalid
	}
	for _, p := range t.participants {
		if p.partition == partition {
			binding := PreparedKernelBinding{TxID: tx, Graph: t.graph, Partition: partition, OwnershipEpoch: p.before.owner, Configuration: t.configuration, DeclarationDigest: t.declaration, RoutingDigest: t.routing, TemplateDigest: t.digest, EffectDigest: p.digest, CapturedRoot: p.before, CapturedBase: p.effects.Base}
			kernel := &PreparedPartitionKernel{binding: binding, provisional: t.provisional, part: p, defaultAxis: t.defaultAxis, limits: t.limits}
			wire, err := kernel.encode(output)
			if err != nil {
				return nil, err
			}
			kernel.wire = wire
			return kernel, nil
		}
	}
	return nil, ErrNamespace
}

// Binding returns copied fixed fields and an admitted owned source image.
func (k *PreparedPartitionKernel) Binding(output *graphstate.OutputBudget) (PreparedKernelBinding, error) {
	if k == nil || output == nil {
		return PreparedKernelBinding{}, ErrInvalid
	}
	if err := output.Reserve(512 + len(k.binding.CapturedBase.Image)); err != nil {
		return PreparedKernelBinding{}, callerError(err)
	}
	b := k.binding
	b.CapturedBase.Image = exactCopy(b.CapturedBase.Image)
	return b, nil
}

// Digest returns the prepared participant effect identity, distinct from template.
func (k *PreparedPartitionKernel) Digest() [32]byte {
	if k == nil {
		return [32]byte{}
	}
	return k.binding.EffectDigest
}

// Bytes owns canonical inspection/persistence bytes; bytes alone confer no authority.
func (k *PreparedPartitionKernel) Bytes(output *graphstate.OutputBudget) ([]byte, error) {
	if k == nil || output == nil {
		return nil, ErrInvalid
	}
	if err := output.Reserve(64 + len(k.wire)); err != nil {
		return nil, callerError(err)
	}
	return exactCopy(k.wire), nil
}

// InspectPreparedPartitionKernel validates canonical bounded inspection bytes
// and returns only owned binding/revision metadata. It cannot confer an opaque
// stageable kernel; pre-prepare admission and durability remain graphapply's.
func InspectPreparedPartitionKernel(wire []byte, l Limits, g GraphLimits, output *graphstate.OutputBudget) (PreparedKernelBinding, state.Revision, error) {
	if output == nil {
		return PreparedKernelBinding{}, state.Revision{}, ErrInvalid
	}
	l, err := l.resolve()
	if err != nil {
		return PreparedKernelBinding{}, state.Revision{}, err
	}
	g, err = g.resolve()
	if err != nil {
		return PreparedKernelBinding{}, state.Revision{}, err
	}
	if len(wire) > l.MaxRecordBytes {
		return PreparedKernelBinding{}, state.Revision{}, ErrResourceLimit
	}
	arena, err := selectedGraphOutputBudget(g, output)
	if err != nil {
		return PreparedKernelBinding{}, state.Revision{}, err
	}
	kernel, err := decodePreparedKernel(wire, l, g, arena)
	if err != nil {
		return PreparedKernelBinding{}, state.Revision{}, storedKernelError(err)
	}
	binding, err := kernel.Binding(arena)
	if err != nil {
		return PreparedKernelBinding{}, state.Revision{}, err
	}
	return binding, kernel.provisional, nil
}

// InspectPreparedKernelIdentities exposes no effects or stageable authority.
// Equal-content aliases that were not written are absent from the exact set.
func InspectPreparedKernelIdentities(wire []byte, l Limits, g GraphLimits, output *graphstate.OutputBudget) (PreparedKernelIdentities, error) {
	if output == nil {
		return PreparedKernelIdentities{}, ErrInvalid
	}
	l, err := l.resolve()
	if err != nil {
		return PreparedKernelIdentities{}, err
	}
	g, err = g.resolve()
	if err != nil {
		return PreparedKernelIdentities{}, err
	}
	if len(wire) > l.MaxRecordBytes {
		return PreparedKernelIdentities{}, ErrResourceLimit
	}
	arena, err := selectedGraphOutputBudget(g, output)
	if err != nil {
		return PreparedKernelIdentities{}, err
	}
	kernel, err := decodePreparedKernel(wire, l, g, arena)
	if err != nil {
		return PreparedKernelIdentities{}, storedKernelError(err)
	}
	return kernel.identities(arena, l)
}

func (kernel *PreparedPartitionKernel) identities(arena *graphstate.OutputBudget, l Limits) (PreparedKernelIdentities, error) {
	delta := kernel.part.effects.Delta
	cost := 192 + 8*len(delta.Entities) + 64*len(delta.Lives) + 8*len(delta.Values)
	if err := arena.Reserve(cost); err != nil {
		return PreparedKernelIdentities{}, callerError(err)
	}
	out := PreparedKernelIdentities{Entities: make([]graphstate.EntityID, len(delta.Entities)), Lives: make([]graphstate.LifeRecord, len(delta.Lives)), Values: make([]graphstate.ValueID, len(delta.Values))}
	out.WriteRecords = len(kernel.part.effects.Writes)
	out.Groups = len(kernel.part.effects.Groups)
	for _, row := range kernel.part.effects.Writes {
		out.WriteBytes += len(row.Key) + len(row.Value) + 64
	}
	for n, group := range kernel.part.effects.Groups {
		bounds, err := group.Owned.EncodingBounds(l.Temporal)
		if err != nil {
			return PreparedKernelIdentities{}, err
		}
		out.ChangeBytes += 128 + len(group.Key.Name) + axisVariableBytes(group.Owned.Axis()) + bounds.WireBytes + len(kernel.part.groupWires[n])
	}
	for n, e := range delta.Entities {
		out.Entities[n] = e.ID
	}
	copy(out.Lives, delta.Lives)
	for n, value := range delta.Values {
		out.Values[n] = value.ID
	}
	return out, nil
}

// InspectPreparedKernelMetadata validates canonical bounded bytes once and owns
// both binding/revision and fresh-identity metadata. It confers no stageability
// or transaction authority; callers still validate their complete outer binding.
func InspectPreparedKernelMetadata(wire []byte, l Limits, g GraphLimits, output *graphstate.OutputBudget) (PreparedKernelBinding, state.Revision, PreparedKernelIdentities, error) {
	if output == nil {
		return PreparedKernelBinding{}, state.Revision{}, PreparedKernelIdentities{}, ErrInvalid
	}
	l, err := l.resolve()
	if err != nil {
		return PreparedKernelBinding{}, state.Revision{}, PreparedKernelIdentities{}, err
	}
	g, err = g.resolve()
	if err != nil {
		return PreparedKernelBinding{}, state.Revision{}, PreparedKernelIdentities{}, err
	}
	if len(wire) > l.MaxRecordBytes {
		return PreparedKernelBinding{}, state.Revision{}, PreparedKernelIdentities{}, ErrResourceLimit
	}
	arena, err := selectedGraphOutputBudget(g, output)
	if err != nil {
		return PreparedKernelBinding{}, state.Revision{}, PreparedKernelIdentities{}, err
	}
	kernel, err := decodePreparedKernel(wire, l, g, arena)
	if err != nil {
		return PreparedKernelBinding{}, state.Revision{}, PreparedKernelIdentities{}, storedKernelError(err)
	}
	binding, err := kernel.Binding(arena)
	if err != nil {
		return PreparedKernelBinding{}, state.Revision{}, PreparedKernelIdentities{}, err
	}
	identities, err := kernel.identities(arena, l)
	if err != nil {
		return PreparedKernelBinding{}, state.Revision{}, PreparedKernelIdentities{}, err
	}
	return binding, kernel.provisional, identities, nil
}

func appendBase(dst []byte, b raftlog.ApplicationRoot) []byte {
	dst = binary.BigEndian.AppendUint64(dst, b.Generation)
	dst = binary.BigEndian.AppendUint64(dst, b.Index)
	dst = append(dst, b.ImageHash[:]...)
	return appendField(dst, b.Image)
}
func appendTags(dst []byte, tags []int) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(tags)))
	for _, offset := range tags {
		dst = binary.BigEndian.AppendUint32(dst, uint32(offset))
	}
	return dst
} // #nosec G115 -- callers preflight all tag counts and offsets against delivered bounded codec bytes.
func appendTemplateScope(dst []byte, scope temporal.Scope, arena *graphstate.OutputBudget, l Limits) ([]byte, error) {
	wire, err := arena.ScopeBytes(scope, l.Temporal)
	if err != nil {
		return nil, callerError(err)
	}
	if err := arena.Reserve(2 * len(wire)); err != nil {
		return nil, callerError(err)
	}
	return appendField(dst, wire), nil
}
func appendTemplateScalar(dst []byte, value graphstate.Scalar, arena *graphstate.OutputBudget, l Limits) ([]byte, error) {
	if value.Kind() == graphstate.ScalarInvalid {
		return appendField(dst, nil), nil
	}
	key, err := arena.ScalarKey(value, l.valueLimits())
	if err != nil {
		return nil, callerError(err)
	}
	return appendField(dst, []byte(key)), nil
}
func dependencyWire(dep graphstate.Dependency, arena *graphstate.OutputBudget, l Limits) ([]byte, error) {
	if err := arena.Reserve(2048 + 8*(len(dep.Name)+len(dep.Key.Name)+len(dep.Prefix.Name)+len(dep.Unique.Definition.Name))); err != nil {
		return nil, callerError(err)
	}
	b := []byte{byte(dep.Kind), byte(dep.OwnerKind)}
	b = append(b, dep.View[:]...)
	for _, n := range []uint64{uint64(dep.Version), uint64(dep.Owner), uint64(dep.Life), uint64(dep.ValueID)} {
		b = binary.BigEndian.AppendUint64(b, n)
	}
	if dep.Absent {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = appendField(b, []byte(dep.Name))
	b = appendComponent(b, dep.Key)
	b = appendComponent(b, graphstate.ComponentKey{Owner: dep.Prefix.Owner, Life: dep.Prefix.Life, Kind: dep.Prefix.Kind, Name: dep.Prefix.Name})
	d := dep.Unique.Definition
	b = append(b, byte(d.Owner), byte(d.Type), byte(d.Cardinality), byte(d.Unique))
	b = appendField(b, []byte(d.Name))
	b = binary.BigEndian.AppendUint64(b, uint64(dep.Incident.Endpoint))
	b = binary.BigEndian.AppendUint64(b, uint64(dep.Incident.Life))
	var err error
	for _, value := range []graphstate.Scalar{dep.Value, dep.Unique.Value} {
		b, err = appendTemplateScalar(b, value, arena, l)
		if err != nil {
			return nil, err
		}
	}
	for _, scope := range []temporal.Scope{dep.Window, dep.Unique.Window, dep.Incident.Window} {
		if scope.Kind() == temporal.ScopeInvalid {
			b = appendField(b, nil)
		} else {
			b, err = appendTemplateScope(b, scope, arena, l)
			if err != nil {
				return nil, err
			}
		}
	}
	return b, nil
}
func (p *partitionTemplateEffects) body(arena *graphstate.OutputBudget, l Limits) ([]byte, error) {
	root, err := EncodeRoot(p.effects.Root)
	if err != nil {
		return nil, err
	}
	before, err := EncodeRoot(p.before)
	if err != nil {
		return nil, err
	}
	cost := 1024 + len(root) + len(before)
	for n, row := range p.effects.Writes {
		cost += 128 + len(row.Key) + len(row.Value) + 4*len(p.writeTags[n])
	}
	for n, g := range p.effects.Groups {
		cost += 1024 + len(g.Key.Name) + len(p.groupWires[n]) + 4*len(p.groupTags[n]) + p.effects.Delta.Patches[n].State.Usage().MetadataBytes()
	}
	if cost > l.MaxRecordBytes-512 {
		return nil, ErrResourceLimit
	}
	if err := arena.Reserve(4 * cost); err != nil {
		return nil, callerError(err)
	}
	b := binary.BigEndian.AppendUint64([]byte{'P', 'E', 'F', 1}, p.partition)
	b = appendField(b, before)
	b = appendBase(b, p.effects.Base)
	b = appendField(b, root)
	b = binary.BigEndian.AppendUint32(b, uint32(len(p.effects.Writes)))
	for n, row := range p.effects.Writes {
		b = appendField(b, row.Key)
		b = appendField(b, row.Value)
		b = appendTags(b, p.writeTags[n])
	}
	b = binary.BigEndian.AppendUint32(b, uint32(len(p.effects.Groups)))
	for n, g := range p.effects.Groups {
		b = appendComponent(b, g.Key)
		axis, e := encodeAxis(p.before.namespace, g.Owned.Axis(), l)
		if e != nil {
			return nil, e
		}
		b = appendField(b, axis)
		b, e = appendTemplateScope(b, g.Owned, arena, l)
		if e != nil {
			return nil, e
		}
		b = appendField(b, p.groupWires[n])
		b = appendTags(b, p.groupTags[n])
		codec := DefaultPageLimits().codecLimits(&Catalog{limits: l})
		if _, e := arena.ReserveStateEncoding(p.effects.Delta.Patches[n].State, codec); e != nil {
			return nil, callerError(e)
		}
		wire, e := state.AppendState(nil, p.effects.Delta.Patches[n].State, codec)
		if e != nil {
			return nil, e
		}
		b = appendField(b, wire)
		// Planned replacement state owns only the mutated window; all cells are
		// newly produced. CDC Before tags remain separately explicit.
		offsets, e := cellOffsets(wire, false)
		if e != nil {
			return nil, e
		}
		tags := make([]int, len(offsets))
		for j, offset := range offsets {
			tags[j] = offset[0]
		}
		b = appendTags(b, tags)
	}
	return b, nil
} // #nosec G115 -- bounded private arrays are admitted under64MiB before uint32 count/offset conversion.
func (t *PartitionTransactionTemplate) seal(arena *graphstate.OutputBudget, l Limits) error {
	if err := arena.Reserve(1024 + 128*len(t.bases)); err != nil {
		return callerError(err)
	}
	h := sha256.New()
	_, _ = h.Write([]byte("rho-tkg:partition-transaction-template:v1\x00"))
	_, _ = h.Write(t.graph[:])
	_, _ = h.Write(t.view[:])
	_, _ = h.Write(t.declaration[:])
	_, _ = h.Write(t.configuration[:])
	_, _ = h.Write(t.routing[:])
	var fixed [16]byte
	binary.BigEndian.PutUint64(fixed[:8], t.provisional.ID())
	binary.BigEndian.PutUint64(fixed[8:], t.provisional.Provenance())
	_, _ = h.Write(fixed[:])
	for n := range t.participants {
		body, err := t.participants[n].body(arena, l)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(body)
		t.participants[n].digest = digest
		_, _ = h.Write(digest[:])
		base := appendBase(nil, t.bases[n])
		_, _ = h.Write(base)
	}
	for _, dep := range t.dependencies {
		wire, err := dependencyWire(dep, arena, l)
		if err != nil {
			return err
		}
		_, _ = h.Write(wire)
	}
	copy(t.digest[:], h.Sum(nil))
	return nil
}
func (k *PreparedPartitionKernel) encode(arena *graphstate.OutputBudget) ([]byte, error) {
	if err := arena.Reserve(2048); err != nil {
		return nil, callerError(err)
	}
	b := append([]byte{'G', 'P', 'K', 1}, k.binding.TxID[:]...)
	b = append(b, k.binding.Graph[:]...)
	b = binary.BigEndian.AppendUint64(b, k.binding.Partition)
	b = binary.BigEndian.AppendUint64(b, k.binding.OwnershipEpoch)
	for _, hash := range [][32]byte{k.binding.Configuration, k.binding.DeclarationDigest, k.binding.RoutingDigest, k.binding.TemplateDigest, k.binding.EffectDigest} {
		b = append(b, hash[:]...)
	}
	b = binary.BigEndian.AppendUint64(b, k.provisional.ID())
	b = binary.BigEndian.AppendUint64(b, k.provisional.Provenance())
	axis, err := encodeAxis(k.part.before.namespace, k.defaultAxis.Axis(), k.limits)
	if err != nil {
		return nil, err
	}
	b = appendField(b, axis)
	body, err := k.part.body(arena, k.limits)
	if err != nil {
		return nil, err
	}
	if sha256.Sum256(body) != k.binding.EffectDigest {
		return nil, ErrCorrupt
	}
	b = appendField(b, body)
	if err := arena.Reserve(2 * (len(b) + 32)); err != nil {
		return nil, callerError(err)
	}
	return sealRecord(b), nil
}

func readTemplateBase(c *cursor) (raftlog.ApplicationRoot, error) {
	g, err := c.number()
	if err != nil {
		return raftlog.ApplicationRoot{}, err
	}
	index, err := c.number()
	if err != nil {
		return raftlog.ApplicationRoot{}, err
	}
	hash, err := c.take(32)
	if err != nil {
		return raftlog.ApplicationRoot{}, err
	}
	image, err := c.field(ownershipRootBytes)
	if err != nil {
		return raftlog.ApplicationRoot{}, err
	}
	root := raftlog.ApplicationRoot{Generation: g, Index: index, Image: exactCopy(image)}
	copy(root.ImageHash[:], hash)
	if index == 0 || sha256.Sum256(image) != root.ImageHash {
		return raftlog.ApplicationRoot{}, ErrCorrupt
	}
	if _, err := DecodeRoot(image); err != nil {
		return raftlog.ApplicationRoot{}, err
	}
	return root, nil
}
func readTemplateTags(c *cursor, wire []byte, allowed []int, arena *graphstate.OutputBudget) ([]int, error) {
	b, err := c.take(4)
	if err != nil {
		return nil, err
	}
	count := uint64(binary.BigEndian.Uint32(b))
	if count > uint64(len(c.src)/4) || count > uint64(len(allowed)) {
		return nil, ErrCorrupt
	}
	if err := arena.Reserve(64 + 8*int(count)); err != nil {
		return nil, callerError(err)
	}
	tags := make([]int, int(count))
	for n := range tags {
		b, err := c.take(4)
		if err != nil {
			return nil, err
		}
		offset := int(binary.BigEndian.Uint32(b))
		if offset < 0 || offset > len(wire)-8 || n > 0 && offset <= tags[n-1] || !slices.Contains(allowed, offset) {
			return nil, ErrCorrupt
		}
		tags[n] = offset
	}
	return tags, nil
} // #nosec G115 -- tag count/offset conversion is bounded by delivered<=1MiB kernel bytes.
func physicalRevisionSlots(key, record []byte, l Limits, g GraphLimits) ([]int, error) {
	if len(key) < 25 {
		return nil, ErrCorrupt
	}
	kind := recordKind(key[0])
	if kind != checkpointRecord && kind != patchRecord {
		return nil, nil
	}
	n := Namespace{}
	copy(n.Graph[:], key[1:17])
	n.Partition = binary.BigEndian.Uint64(key[17:25])
	c, err := inspectRecord(record, n, kind, l)
	if err != nil {
		return nil, err
	}
	if _, err := c.number(); err != nil {
		return nil, err
	}
	if _, err := decodeComponent(&c, l); err != nil {
		return nil, err
	}
	if kind == patchRecord {
		if _, err := c.number(); err != nil {
			return nil, err
		}
		if _, err := c.field(l.Temporal.MaxValueBytes); err != nil {
			return nil, err
		}
	}
	wire, err := c.field(g.Pages.codecLimits(&Catalog{limits: l}).MaxEncodedBytes)
	if err != nil || c.done() != nil {
		return nil, ErrCorrupt
	}
	offset := len(record) - len(wire)
	pairs, err := cellOffsets(wire, kind == patchRecord)
	if err != nil {
		return nil, err
	}
	allowed := make([]int, 0, 2*len(pairs))
	for _, p := range pairs {
		allowed = append(allowed, offset+p[0])
		if p[1] >= 0 {
			allowed = append(allowed, offset+p[1])
		}
	}
	return allowed, nil
}
func decodePartitionEffects(wire []byte, l Limits, g GraphLimits, arena *graphstate.OutputBudget) (partitionTemplateEffects, error) {
	if len(wire) < 12 || !bytes.Equal(wire[:4], []byte{'P', 'E', 'F', 1}) {
		return partitionTemplateEffects{}, ErrCorrupt
	}
	if err := arena.Reserve(1024 + 6*len(wire)); err != nil {
		return partitionTemplateEffects{}, callerError(err)
	}
	c := cursor{src: wire[4:]}
	partition, err := c.number()
	if err != nil {
		return partitionTemplateEffects{}, err
	}
	before, err := c.field(ownershipRootBytes)
	if err != nil {
		return partitionTemplateEffects{}, err
	}
	root, err := DecodeRoot(before)
	if err != nil || root.namespace.Partition != partition {
		return partitionTemplateEffects{}, ErrCorrupt
	}
	base, err := readTemplateBase(&c)
	if err != nil {
		return partitionTemplateEffects{}, err
	}
	if !bytes.Equal(base.Image, before) {
		return partitionTemplateEffects{}, ErrCorrupt
	}
	rootBytes, err := c.field(ownershipRootBytes)
	if err != nil {
		return partitionTemplateEffects{}, err
	}
	after, err := DecodeRoot(rootBytes)
	if err != nil || after.namespace != root.namespace || after.owner != root.owner || after.epoch != root.epoch || after.effect != root.effect || after.next < root.next || after.ownershipDigest != root.ownershipDigest || after.topology != root.topology {
		return partitionTemplateEffects{}, ErrCorrupt
	}
	number, err := c.take(4)
	if err != nil {
		return partitionTemplateEffects{}, err
	}
	count := uint64(binary.BigEndian.Uint32(number))
	if count > uint64(l.MaxStageRecords) || count > uint64(len(c.src)/12) {
		return partitionTemplateEffects{}, ErrCorrupt
	}
	if err := arena.Reserve(192 * int(count)); err != nil {
		return partitionTemplateEffects{}, callerError(err)
	}
	p := partitionTemplateEffects{partition: partition, before: root, effects: GraphEffects{Base: base, Root: after, Writes: make([]raftlog.KV, int(count)), Delta: graphstate.Delta{Graph: root.namespace.Graph}}, writeTags: make([][]int, int(count))}
	for n := range p.effects.Writes {
		key, err := c.field(l.MaxRecordBytes)
		if err != nil {
			return p, err
		}
		value, err := c.field(l.MaxRecordBytes)
		if err != nil {
			return p, err
		}
		if n > 0 && bytes.Compare(p.effects.Writes[n-1].Key, key) >= 0 {
			return p, ErrCorrupt
		}
		if len(key) < 25 {
			return p, ErrCorrupt
		}
		offset := 17
		if len(key) >= 3 && key[0] == 'g' && key[1] == 'r' {
			offset = 19
		}
		if len(key) < offset+8 || !bytes.Equal(key[offset-16:offset], root.namespace.Graph[:]) || binary.BigEndian.Uint64(key[offset:offset+8]) != partition {
			return p, ErrNamespace
		}
		allowed, err := physicalRevisionSlots(key, value, l, g)
		if err != nil {
			return p, err
		}
		tags, err := readTemplateTags(&c, value, allowed, arena)
		if err != nil {
			return p, err
		}
		p.effects.Writes[n] = raftlog.KV{Key: exactCopy(key), Value: exactCopy(value)}
		p.writeTags[n] = tags
		switch recordKind(key[0]) {
		case entityRecord:
			r, err := readEntity(value, root.namespace, l)
			if err != nil {
				return p, err
			}
			if len(key) != 33 || uint64(r.ID) != binary.BigEndian.Uint64(key[25:]) {
				return p, ErrCorrupt
			}
			p.effects.Delta.Entities = append(p.effects.Delta.Entities, r)
		case lifeRecord:
			r, err := readLife(value, root.namespace, l)
			if err != nil {
				return p, err
			}
			if len(key) != 41 || uint64(r.Owner) != binary.BigEndian.Uint64(key[25:33]) || uint64(r.Life) != binary.BigEndian.Uint64(key[33:]) {
				return p, ErrCorrupt
			}
			p.effects.Delta.Lives = append(p.effects.Delta.Lives, r)
		case valueRecord:
			q := reader{arena: arena, c: &Catalog{root: root, limits: l}, fullView: new(fullIndexDescriptor)}
			if err := q.preflightFullValue(value); err != nil {
				return p, err
			}
			r, _, _, err := readValue(value, root.namespace, l)
			if err != nil {
				return p, err
			}
			if len(key) != 33 || uint64(r.Ref.ID) != binary.BigEndian.Uint64(key[25:]) {
				return p, ErrCorrupt
			}
			p.effects.Delta.Values = append(p.effects.Delta.Values, graphstate.ValueWrite{ID: r.Ref.ID, Value: r.Value})
		}
	}
	number, err = c.take(4)
	if err != nil {
		return p, err
	}
	count = uint64(binary.BigEndian.Uint32(number))
	if count > uint64(g.Pages.MaxPatches) || count > uint64(len(c.src)/100) {
		return p, ErrCorrupt
	}
	if err := arena.Reserve((fullGroupOutputBytes + fullPatchOutputBytes + 128) * int(count)); err != nil {
		return p, callerError(err)
	}
	p.effects.Groups = make([]ComponentChangeGroup, int(count))
	p.effects.Delta.Patches = make([]graphstate.ComponentPatch, int(count))
	p.groupWires = make([][]byte, int(count))
	p.groupTags = make([][]int, int(count))
	for n := range p.effects.Groups {
		key, err := decodeComponent(&c, l)
		if err != nil {
			return p, err
		}
		axisWire, err := c.field(l.MaxRecordBytes)
		if err != nil {
			return p, err
		}
		axis, err := readAxis(axisWire, root.namespace, l)
		if err != nil {
			return p, err
		}
		ownedWire, err := c.field(l.Temporal.MaxValueBytes)
		if err != nil {
			return p, err
		}
		backing, err := scopeOwnedBacking(ownedWire)
		if err != nil {
			return p, err
		}
		if err := arena.Reserve(128 + backing + 4*len(ownedWire)); err != nil {
			return p, callerError(err)
		}
		owned, err := temporal.DecodeScope(ownedWire, axis, l.Temporal)
		if err != nil {
			return p, err
		}
		changeWire, err := c.field(g.Pages.MaxChangeBytes)
		if err != nil {
			return p, err
		}
		if err := reserveKernelComponent(changeWire, true, arena); err != nil {
			return p, err
		}
		changes, _, err := state.DecodeChanges(changeWire, axis, g.Pages.codecLimits(&Catalog{limits: l}))
		if err != nil {
			return p, err
		}
		pairs, err := cellOffsets(changeWire, true)
		if err != nil {
			return p, err
		}
		allowed := make([]int, 0, 2*len(pairs))
		for _, pair := range pairs {
			allowed = append(allowed, pair[0], pair[1])
		}
		tags, err := readTemplateTags(&c, changeWire, allowed, arena)
		if err != nil {
			return p, err
		}
		for _, pair := range pairs {
			if !slices.Contains(tags, pair[1]) {
				return p, ErrCorrupt
			}
		}
		stateWire, err := c.field(g.Pages.codecLimits(&Catalog{limits: l}).MaxEncodedBytes)
		if err != nil {
			return p, err
		}
		if err := reserveKernelComponent(stateWire, false, arena); err != nil {
			return p, err
		}
		component, err := state.DecodeState(stateWire, axis, g.Pages.codecLimits(&Catalog{limits: l}))
		if err != nil {
			return p, err
		}
		pairs, err = cellOffsets(stateWire, false)
		if err != nil {
			return p, err
		}
		allowed = make([]int, len(pairs))
		for j, pair := range pairs {
			allowed[j] = pair[0]
		}
		stateTags, err := readTemplateTags(&c, stateWire, allowed, arena)
		if err != nil || !slices.Equal(stateTags, allowed) {
			return p, ErrCorrupt
		}
		p.effects.Groups[n] = ComponentChangeGroup{Key: key, Owned: owned, Changes: changes}
		p.effects.Delta.Patches[n] = graphstate.ComponentPatch{Key: key, Owned: owned, Changes: changes, State: component}
		p.groupWires[n] = exactCopy(changeWire)
		p.groupTags[n] = tags
	}
	if c.done() != nil {
		return p, ErrCorrupt
	}
	p.digest = sha256.Sum256(wire)
	canonical, err := p.body(arena, l)
	if err != nil {
		return p, err
	}
	if !bytes.Equal(canonical, wire) {
		return p, ErrCorrupt
	}
	return p, nil
} // #nosec G115 -- all count conversions are checked against delivered<=1MiB kernel bytes and finite policy before allocation.
func decodePreparedKernel(wire []byte, l Limits, g GraphLimits, arena *graphstate.OutputBudget) (*PreparedPartitionKernel, error) {
	if len(wire) < 4+16+16+16+160+16+4+32 || len(wire) > l.MaxRecordBytes || !bytes.Equal(wire[:4], []byte{'G', 'P', 'K', 1}) {
		return nil, ErrCorrupt
	}
	hash := sha256.Sum256(wire[:len(wire)-32])
	if !bytes.Equal(hash[:], wire[len(wire)-32:]) {
		return nil, ErrCorrupt
	}
	if err := arena.Reserve(2048 + 2*len(wire)); err != nil {
		return nil, callerError(err)
	}
	c := cursor{src: wire[4 : len(wire)-32]}
	k := &PreparedPartitionKernel{limits: l, wire: exactCopy(wire)}
	for _, dst := range [][]byte{k.binding.TxID[:], k.binding.Graph[:]} {
		b, e := c.take(16)
		if e != nil {
			return nil, e
		}
		copy(dst, b)
	}
	var err error
	k.binding.Partition, err = c.number()
	if err != nil {
		return nil, err
	}
	k.binding.OwnershipEpoch, err = c.number()
	if err != nil {
		return nil, err
	}
	for _, dst := range [][]byte{k.binding.Configuration[:], k.binding.DeclarationDigest[:], k.binding.RoutingDigest[:], k.binding.TemplateDigest[:], k.binding.EffectDigest[:]} {
		b, e := c.take(32)
		if e != nil {
			return nil, e
		}
		copy(dst, b)
	}
	round, err := c.number()
	if err != nil {
		return nil, err
	}
	provenance, err := c.number()
	if err != nil {
		return nil, err
	}
	k.provisional, err = state.NewRevision(round, provenance)
	if err != nil {
		return nil, err
	}
	axisWire, err := c.field(l.MaxRecordBytes)
	if err != nil {
		return nil, err
	}
	axis, err := readAxis(axisWire, Namespace{k.binding.Graph, k.binding.Partition}, l)
	if err != nil {
		return nil, err
	}
	k.defaultAxis, err = types.NewDefaultAxisBinding(types.GraphID(k.binding.Graph), axis.Descriptor().ID, l.Temporal)
	if err != nil || k.defaultAxis.Check(types.GraphID(k.binding.Graph), axis, l.Temporal) != nil {
		return nil, ErrCorrupt
	}
	body, err := c.field(l.MaxRecordBytes)
	if err != nil || c.done() != nil {
		return nil, ErrCorrupt
	}
	k.part, err = decodePartitionEffects(body, l, g, arena)
	if err != nil {
		return nil, err
	}
	if k.part.partition != k.binding.Partition || k.part.before.namespace.Graph != k.binding.Graph || k.part.before.owner != k.binding.OwnershipEpoch || k.part.digest != k.binding.EffectDigest || k.part.before.ownershipDigest != k.binding.DeclarationDigest {
		return nil, ErrCorrupt
	}
	k.binding.CapturedRoot = k.part.before
	k.binding.CapturedBase = k.part.effects.Base
	if k.binding.TxID == ([16]byte{}) || k.binding.Configuration == ([32]byte{}) || k.binding.TemplateDigest == ([32]byte{}) || k.binding.RoutingDigest == ([32]byte{}) {
		return nil, ErrCorrupt
	}
	canonical, err := k.encode(arena)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(canonical, wire) {
		return nil, ErrCorrupt
	}
	return k, nil
}
func reserveKernelComponent(wire []byte, changes bool, arena *graphstate.OutputBudget) error {
	if len(wire) < 56 {
		return ErrCorrupt
	}
	count := uint64(binary.BigEndian.Uint32(wire[52:56]))
	stride := 34
	if changes {
		stride = 68
	}
	if count > uint64((len(wire)-56)/(4+53+stride)) {
		return ErrCorrupt
	}
	if count > uint64(arena.Remaining()/2048) {
		return ErrResourceLimit
	}
	return callerError(arena.Reserve(1024 + 2048*int(count) + 4*len(wire)))
} // #nosec G115 -- count is checked against delivered bytes and bounded remaining reservation before conversion.
func sameKernelBinding(a, b PreparedKernelBinding) bool {
	return a.TxID == b.TxID && a.Graph == b.Graph && a.Partition == b.Partition && a.OwnershipEpoch == b.OwnershipEpoch && a.Configuration == b.Configuration && a.DeclarationDigest == b.DeclarationDigest && a.RoutingDigest == b.RoutingDigest && a.TemplateDigest == b.TemplateDigest && a.EffectDigest == b.EffectDigest && a.CapturedRoot == b.CapturedRoot && a.CapturedBase.Generation == b.CapturedBase.Generation && a.CapturedBase.Index == b.CapturedBase.Index && a.CapturedBase.ImageHash == b.CapturedBase.ImageHash
}

// ReadPreparedPartitionKernel restores stageability only by reading the actual
// co-committed local durable kernel and unchanged semantic base. The outer
// protocol still proves prepare locks, quorum durability and immutable decision.
func ReadPreparedPartitionKernel(ctx context.Context, local *raftlog.ApplicationView, proof PreparedKernelRestoreProof, expected PreparedKernelBinding, l Limits, g GraphLimits, budget OwnershipBudget, output *graphstate.OutputBudget) (kernel *PreparedPartitionKernel, work PageWork, err error) {
	if ctx == nil || local == nil || output == nil || len(proof.PrepareKey) == 0 || proof.PrepareDigest == ([32]byte{}) || proof.PrepareIndex == 0 || expected.TxID == ([16]byte{}) {
		return nil, work, ErrInvalid
	}
	prepareIdentity := preparedKernelKey(expected)
	prepareIdentity[2] = 'P'
	if !bytes.Equal(proof.PrepareKey, prepareIdentity[:]) || proof.PrepareIndex <= expected.CapturedBase.Index {
		return nil, work, ErrRebinding
	}
	l, err = l.resolve()
	if err != nil {
		return nil, work, err
	}
	g, err = g.resolve()
	if err != nil {
		return nil, work, err
	}
	if err := budget.validate(); err != nil {
		return nil, work, err
	}
	g.MaxOutputBytes = min(g.MaxOutputBytes, budget.OutputBytes)
	arena, err := selectedGraphOutputBudget(g, output)
	if err != nil {
		return nil, work, err
	}
	prepare, first, err := readKernelControl(ctx, local, proof.PrepareKey, l, budget, arena)
	work = first
	if err != nil {
		return nil, work, err
	}
	if prepare.Index != proof.PrepareIndex || sha256.Sum256(prepare.Value) != proof.PrepareDigest {
		return nil, work, ErrRebinding
	}
	remaining := budget
	remaining.SourceRows -= work.Records
	remaining.SourceBytes -= work.Bytes
	remaining.OutputBytes = min(remaining.OutputBytes, arena.Remaining())
	if remaining.SourceRows < 1 || remaining.SourceBytes < 1 || remaining.OutputBytes < 1 {
		return nil, work, ErrResourceLimit
	}
	key := preparedKernelKey(expected)
	row, second, err := readKernelControl(ctx, local, key[:], l, remaining, arena)
	work = addWork(work, second)
	if err != nil {
		return nil, work, err
	}
	if row.Index != prepare.Index {
		return nil, work, ErrRebinding
	}
	decodeArena, e := arena.Child(max(0, budget.SourceBytes-work.Bytes))
	if e != nil {
		return nil, work, callerError(e)
	}
	kernel, err = decodePreparedKernel(row.Value, l, g, decodeArena)
	work.Bytes += decodeArena.Used()
	if err != nil {
		return nil, work, storedKernelError(err)
	}
	if !sameKernelBinding(kernel.binding, expected) {
		return nil, work, ErrRebinding
	}

	remaining = budget
	remaining.SourceRows -= work.Records
	remaining.SourceBytes -= work.Bytes
	remaining.OutputBytes = min(remaining.OutputBytes, arena.Remaining())
	if remaining.SourceRows < 1 || remaining.SourceBytes < 1 || remaining.OutputBytes < 1 {
		return nil, work, ErrResourceLimit
	}
	_, checked, e := checkPreparedKernelLocal(ctx, local, kernel, l, g, remaining, arena)
	work = addWork(work, checked)
	if e != nil {
		return nil, work, e
	}
	return kernel, work, nil
}
func readKernelControl(ctx context.Context, view *raftlog.ApplicationView, key []byte, l Limits, budget OwnershipBudget, arena *graphstate.OutputBudget) (raftlog.ApplicationControlRecord, PageWork, error) {
	if len(key) == 0 || len(key) > l.MaxNameBytes || budget.SourceRows < 1 {
		return raftlog.ApplicationControlRecord{}, PageWork{}, ErrInvalid
	}
	maximum := min(256+8*len(key)+l.MaxRecordBytes, budget.SourceBytes, view.ReadLimits().Bytes, arena.Remaining())
	if maximum < 256+8*len(key) {
		return raftlog.ApplicationControlRecord{}, PageWork{}, ErrResourceLimit
	}
	if err := arena.Reserve(maximum); err != nil {
		return raftlog.ApplicationControlRecord{}, PageWork{}, callerError(err)
	}
	row, found, work, err := view.GetControl(ctx, key, raftlog.ReadBudget{Rows: min(budget.SourceRows, view.ReadLimits().Rows), Bytes: maximum})
	out := PageWork{Records: max(1, work.Rows), Bytes: work.Bytes}
	if err != nil {
		return raftlog.ApplicationControlRecord{}, out, ownershipOperational(err)
	}
	if !found {
		return raftlog.ApplicationControlRecord{}, out, ErrCorrupt
	}
	out.Bytes += len(row.Value)
	if out.Bytes > budget.SourceBytes {
		return raftlog.ApplicationControlRecord{}, out, ErrResourceLimit
	}
	return row, out, nil
}
func checkPreparedKernelLocal(ctx context.Context, local *raftlog.ApplicationView, k *PreparedPartitionKernel, l Limits, g GraphLimits, budget OwnershipBudget, arena *graphstate.OutputBudget) (*Catalog, PageWork, error) {
	n := Namespace{k.binding.Graph, k.binding.Partition}
	// Known fixed opening metadata/descriptor backing is admitted before the
	// legacy physical opener allocates. Actual additional work is charged below.
	reserved := 4096 + 8*axisVariableBytes(k.defaultAxis.Axis())
	if err := arena.Reserve(reserved); err != nil {
		return nil, PageWork{}, callerError(err)
	}
	c, work, err := OpenPartitionCatalogWithDefaultAxis(ctx, local, n, k.binding.OwnershipEpoch, k.defaultAxis, l, g, budget)
	if work.Bytes > reserved {
		if e := arena.Reserve(work.Bytes - reserved); e != nil {
			return nil, work, callerError(e)
		}
	}
	if err != nil {
		return nil, work, err
	}
	if c.root != k.binding.CapturedRoot {
		return nil, work, ErrPatchConflict
	}
	q, err := c.readerWithOutputBudget(ctx, arena)
	if err != nil {
		return nil, work, err
	}
	q.maxRows = budget.SourceRows - work.Records
	q.maxBytes = budget.SourceBytes - work.Bytes
	q.denyReads = q.maxRows == 0
	if q.maxRows < 1 || q.maxBytes < c.rootImageBytes {
		return nil, work, ErrResourceLimit
	}
	wire, found, err := q.get(RoutingDefinitionKey(n))
	work = addWork(work, PageWork{q.rows, q.bytes, 0, 0, 0, 0})
	if err != nil {
		return nil, work, err
	}
	if !found {
		return nil, work, ErrCorrupt
	}
	remaining := budget
	remaining.SourceRows -= work.Records
	remaining.SourceBytes -= work.Bytes
	remaining.OutputBytes = min(remaining.OutputBytes, arena.Remaining())
	if remaining.SourceRows < 1 || remaining.SourceBytes < 1 || remaining.OutputBytes < 1 {
		return nil, work, ErrResourceLimit
	}
	if err := arena.Reserve(ownershipMetadataBytes + 256); err != nil {
		return nil, work, callerError(err)
	}
	d, ownedWork, err := ReadOwnershipDeclaration(ctx, local, l, remaining)
	work = addWork(work, ownedWork)
	if err != nil {
		return nil, work, err
	}
	routing, err := DecodePartitionRoutingBinding(wire, d.Declaration(), k.defaultAxis, k.binding.Configuration, l)
	if err != nil {
		return nil, work, err
	}
	if routing.Digest() != k.binding.RoutingDigest || d.Declaration().Digest() != k.binding.DeclarationDigest {
		return nil, work, ErrRebinding
	}
	return c, work, nil
}

// StagePreparedPartitionKernel checks the unchanged LOCAL semantic base and
// binds only explicit new-cell/CDC revision slots. It neither replans nor reads
// a remote source, and supplies no durable decision/installation authority.
func StagePreparedPartitionKernel(ctx context.Context, local *raftlog.ApplicationView, k *PreparedPartitionKernel, revision state.Revision, l Limits, g GraphLimits, budget OwnershipBudget, output *graphstate.OutputBudget) (effects GraphEffects, work PageWork, err error) {
	if ctx == nil || local == nil || k == nil || output == nil || revision.ID() == 0 || revision.ID() < k.provisional.ID() || revision.Provenance() != k.provisional.Provenance() {
		return effects, work, ErrInvalid
	}
	l, err = l.resolve()
	if err != nil {
		return effects, work, err
	}
	g, err = g.resolve()
	if err != nil {
		return effects, work, err
	}
	if err := budget.validate(); err != nil {
		return effects, work, err
	}
	g.MaxOutputBytes = min(g.MaxOutputBytes, budget.OutputBytes)
	arena, err := selectedGraphOutputBudget(g, output)
	if err != nil {
		return effects, work, err
	}
	c, work, err := checkPreparedKernelLocal(ctx, local, k, l, g, budget, arena)
	if err != nil {
		return effects, work, err
	}
	decodeArena, e := arena.Child(max(0, budget.SourceBytes-work.Bytes))
	if e != nil {
		return effects, work, callerError(e)
	}
	owned, err := decodePreparedKernel(k.wire, l, g, decodeArena)
	if err != nil {
		return effects, work, storedKernelError(err)
	}
	if !sameKernelBinding(owned.binding, k.binding) {
		return effects, work, ErrRebinding
	}
	for n := range owned.part.effects.Writes {
		for _, offset := range owned.part.writeTags[n] {
			binary.BigEndian.PutUint64(owned.part.effects.Writes[n].Value[offset:offset+8], revision.ID())
		}
	}
	for n, group := range owned.part.effects.Groups {
		wire := owned.part.groupWires[n]
		for _, offset := range owned.part.groupTags[n] {
			binary.BigEndian.PutUint64(wire[offset:offset+8], revision.ID())
		}
		if err := reserveKernelComponent(wire, true, decodeArena); err != nil {
			return effects, work, err
		}
		changes, _, e := state.DecodeChanges(wire, group.Owned.Axis(), g.Pages.codecLimits(c))
		if e != nil {
			return effects, work, e
		}
		owned.part.effects.Groups[n].Changes = changes
		owned.part.effects.Delta.Patches[n].Changes = changes
		if _, e := decodeArena.ReserveStateEncoding(owned.part.effects.Delta.Patches[n].State, g.Pages.codecLimits(c)); e != nil {
			return effects, work, callerError(e)
		}
		stateWire, e := state.AppendState(nil, owned.part.effects.Delta.Patches[n].State, g.Pages.codecLimits(c))
		if e != nil {
			return effects, work, e
		}
		slots, e := cellOffsets(stateWire, false)
		if e != nil {
			return effects, work, e
		}
		for _, slot := range slots {
			binary.BigEndian.PutUint64(stateWire[slot[0]:slot[0]+8], revision.ID())
		}
		if err := reserveKernelComponent(stateWire, false, decodeArena); err != nil {
			return effects, work, err
		}
		component, e := state.DecodeState(stateWire, group.Owned.Axis(), g.Pages.codecLimits(c))
		if e != nil {
			return effects, work, e
		}
		owned.part.effects.Delta.Patches[n].State = component
	}
	base, err := local.Root()
	if err != nil {
		return effects, work, err
	}
	work.Bytes += decodeArena.Used()
	owned.part.effects.Base = base
	owned.part.effects.Work = work
	owned.part.effects.OwnedBytes = arena.Used()
	return owned.part.effects, work, nil
}
func storedKernelError(err error) error {
	if errors.Is(err, ErrResourceLimit) || errors.Is(err, graphstate.ErrResourceLimit) || errors.Is(err, state.ErrResourceLimit) || errors.Is(err, temporal.ErrResourceLimit) {
		return errors.Join(ErrResourceLimit, err)
	}
	return errors.Join(ErrCorrupt, err)
}

func (q *pageStage) withPrimary(owner graphstate.EntityID, fn func(*pageStage) error) error {
	t := q.q.transaction()
	if t == nil || t.installation == nil {
		return fn(q)
	}
	partition, err := t.owner(q.q, uint64(owner))
	if err != nil {
		return err
	}
	return t.installation.onPartition(q.q, partition, fn)
}
func (i *transactionInstallation) onPartition(q *reader, partition uint64, fn func(*pageStage) error) error {
	for n, s := range i.view.views {
		if s.c.root.namespace.Partition == partition {
			return q.onPartition(partition, func() error { p := i.pages[n]; p.q = q; q.full = &i.states[n]; q.full.root = p.root; return fn(p) })
		}
	}
	return ErrRemoteParticipant
}
func (q *pageReader) uniquePartition(key postingKey) (uint64, error) {
	t := q.q.transaction()
	if t == nil {
		return q.q.c.root.namespace.Partition, nil
	}
	owner, found, err := q.q.entity(EntityRef{t.declaration.Graph(), key.component.Owner})
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, ErrCorrupt
	}
	d, found, err := q.q.property(owner.Kind, key.name)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, ErrCorrupt
	}
	_, wire, found, err := q.q.value(ValueRef{t.declaration.Graph(), key.value})
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, ErrCorrupt
	}
	if err := q.q.materialize(128 + 2*len(wire) + 2*len(d.Name)); err != nil {
		return 0, err
	}
	o, err := q.q.route.routing.Bucket(UniqueRoutingBucket, uniqueRoutingKey(d, string(wire)))
	return o.Partition, err
}
func (q *pageStage) insertRoutedPosting(key postingKey) error {
	t := q.q.transaction()
	if t == nil || t.installation == nil {
		return q.insertLocalPosting(key)
	}
	var partition uint64
	var err error
	if key.family == uniquePostingRecord {
		partition, err = q.uniquePartition(key)
	} else {
		partition, err = t.owner(q.q, uint64(key.endpoint))
	}
	if err != nil {
		return err
	}
	return t.installation.onPartition(q.q, partition, func(local *pageStage) error { return local.insertLocalPosting(key) })
}
func (q *pageStage) insertLocalPosting(key postingKey) error {
	d := &q.q.full.descriptor
	root := &d.canonical
	if key.family == declaredIncidentRecord {
		root = &d.declared
	}
	if key.family == uniquePostingRecord {
		root = &d.unique
	}
	next, err := q.insertPostingKey(*root, key, keyTreeLimits(q.limits))
	if err != nil {
		return err
	}
	*root = next
	return nil
}
func (q *pageReader) routedUniqueMembership(root postingTreeRoot, key postingKey) (bool, error) {
	if q.q.transaction() == nil {
		return q.hasPostingKey(root, key, keyTreeLimits(q.limits))
	}
	partition, err := q.uniquePartition(key)
	if err != nil {
		return false, err
	}
	if partition == q.q.c.root.namespace.Partition {
		return q.hasPostingKey(root, key, keyTreeLimits(q.limits))
	}
	var found bool
	err = q.q.onPartition(partition, func() error {
		p := pageReader{q: q.q, limits: q.limits}
		d, e := p.fullDescriptor()
		if e != nil {
			return e
		}
		found, e = p.hasPostingKey(d.unique, key, keyTreeLimits(p.limits))
		return e
	})
	return found, err
}
func (q *pageStage) captureStateRecord(key, record []byte, k graphstate.ComponentKey, s state.State) error {
	t := q.q.transaction()
	if t == nil || t.installation == nil {
		return nil
	}
	wire, err := state.AppendState(nil, s, q.limits.codecLimits(q.q.c))
	if err != nil {
		return err
	}
	if !bytes.HasSuffix(record, wire) {
		return ErrCorrupt
	}
	tags, err := t.installation.stateTags(k, s, wire)
	if err != nil {
		return err
	}
	return t.installation.recordTags(key, tags, len(record)-len(wire))
}
func (q *pageStage) captureChangeRecord(key, record []byte, patch patchPage) error {
	t := q.q.transaction()
	if t == nil || t.installation == nil {
		return nil
	}
	wire, err := state.AppendChanges(nil, patch.Owned.Axis(), patch.Changes, q.limits.codecLimits(q.q.c))
	if err != nil {
		return err
	}
	if !bytes.HasSuffix(record, wire) {
		return ErrCorrupt
	}
	tags, err := t.installation.changeTags(patch.Key, patch.Changes, wire, true)
	if err != nil {
		return err
	}
	return t.installation.recordTags(key, tags, len(record)-len(wire))
}
func (i *transactionInstallation) recordTags(key []byte, tags []int, offset int) error {
	var err error
	i.tagSlots, err = i.view.arena.ReserveMap(len(i.tags), i.tagSlots, 128)
	if err != nil {
		return callerError(err)
	}
	if err := i.view.arena.Reserve(len(key) + 16*len(tags)); err != nil {
		return callerError(err)
	}
	for n := range tags {
		tags[n] += offset
	}
	i.tags[string(key)] = tags
	return nil
}

// PlanPartitionTransaction owns operations, invokes the semantic planner once,
// and uses the same physical installer as local writes. All staged primary and
// secondary effects remain private; no quorum or request authority is inferred.
func PlanPartitionTransaction(ctx context.Context, v *PartitionTransactionView, ops []graphstate.Operation, revision state.Revision) (out *PartitionTransactionTemplate, work PageWork, err error) {
	if v == nil || ctx == nil {
		return nil, work, ErrInvalid
	}
	if revision.ID() == 0 {
		return nil, v.Work(), callerError(state.ErrInvalidRevision)
	}
	v.mu.Lock()
	if err := v.check(ctx); err != nil {
		work = v.work
		v.mu.Unlock()
		return nil, work, err
	}
	if v.planningRevision != 0 {
		v.mu.Unlock()
		return nil, v.Work(), ErrInvalid
	}
	v.planningRevision = revision.ID()
	defer func() { v.mu.Lock(); v.planningRevision = 0; v.mu.Unlock() }()
	// cloneOperations uses the same bounded codec/axis authority as local staging.
	first := v.views[0]
	before := v.prepareSource(first)
	owned, err := cloneOperations(ctx, first, ops)
	v.finishSource(first, before)
	work = v.work
	v.mu.Unlock()
	if err != nil {
		return nil, work, err
	}
	planned, err := graphstate.PlanWithOutputBudget(ctx, v, owned, revision, v.limits.Planner, v.arena)
	if err != nil {
		return nil, v.Work(), planFailure(err)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(ctx); err != nil {
		return nil, v.work, err
	}
	defer func() {
		work = v.work
		if err != nil {
			out = nil
		}
	}()
	if v.installation != nil {
		return nil, v.work, ErrInvalid
	}
	if err := v.arena.Reserve(2048 + 1024*len(v.views)); err != nil {
		return nil, v.work, callerError(err)
	}
	i := &transactionInstallation{view: v, pages: make([]*pageStage, len(v.views)), states: make([]fullStageState, len(v.views)), tags: make(map[string][]int)}
	v.installation = i
	defer func() { v.installation = nil }()
	stages := make([]*Stage, len(v.views))
	defer func() {
		for _, s := range stages {
			if s != nil {
				_ = s.Close()
			}
		}
	}()
	q, err := first.c.readerWithOutputBudget(ctx, v.arena)
	if err != nil {
		return nil, v.work, err
	}
	q.maxRows = min(v.limits.Pages.MaxWorkRecords, v.limits.MaxSourceRows-v.work.Records)
	q.maxBytes = min(v.limits.Pages.MaxWorkBytes, v.limits.MaxSourceBytes-v.work.Bytes)
	q.denyReads = q.maxRows == 0
	q.route = first.route
	if q.maxRows < 1 || q.maxBytes < first.c.rootImageBytes {
		return nil, v.work, ErrResourceLimit
	}
	defer func() { v.work = addWork(v.work, PageWork{Records: q.rows, Bytes: q.bytes}) }()
	if err := v.arena.Reserve(128); err != nil {
		return nil, v.work, callerError(err)
	}
	q.pending = make(map[string]raftlog.KV)
	for n, s := range v.views {
		stage, e := newFullStage(ctx, s.c, s.descriptor, v.limits.Pages)
		if e != nil {
			return nil, v.work, e
		}
		stages[n] = stage
		stage.arena = v.arena
		stage.route = s.route
		i.states[n] = *stage.full
		i.pages[n] = &pageStage{pageReader: &pageReader{q: q, limits: v.limits.Pages}, root: s.c.root}
		i.pages[n].allocation = &i.pages[n].root
	}
	q.full = &i.states[0]
	planned, groups, err := installFullPlan(i.pages[0], planned, v.limits, v.arena)
	if err != nil {
		return nil, v.work, err
	}
	for n, s := range v.views {
		err = i.onPartition(q, s.c.root.namespace.Partition, func(p *pageStage) error {
			q.full.root = p.root
			wire, e := encodeFullDescriptor(q.full.descriptor, p.root, q.c.limits)
			if e != nil {
				return e
			}
			return q.put(componentIndexDescriptorKey(q.c.root.namespace), wire)
		})
		if err != nil {
			return nil, v.work, err
		}
		stages[n].full = &i.states[n]
	}
	// All partition outputs are preflighted before publishing private stage maps.
	counts, costs := make([]int, len(stages)), make([]int, len(stages))
	for _, row := range q.pending {
		n, e := transactionWritePartition(v, row.Key)
		if e != nil {
			return nil, v.work, e
		}
		counts[n]++
		costs[n] += 2*len(row.Key) + len(row.Value) + 64
	}
	for n, s := range stages {
		s.c.mu.Lock()
		allowed := counts[n] <= s.c.limits.MaxStageRecords-s.c.records && costs[n] <= s.c.limits.MaxStageBytes-s.c.stageBytes
		s.c.mu.Unlock()
		if !allowed {
			return nil, v.work, ErrResourceLimit
		}
		if err := v.arena.Reserve(192 * counts[n]); err != nil {
			return nil, v.work, callerError(err)
		}
	}
	for _, row := range q.pending {
		n, _ := transactionWritePartition(v, row.Key)
		stages[n].writes[string(row.Key)] = row
	}
	for n, s := range stages {
		s.bytes += costs[n]
		s.c.mu.Lock()
		s.c.records += counts[n]
		s.c.stageBytes += costs[n]
		s.c.mu.Unlock()
	}
	if err := v.arena.Reserve(1024 + 512*len(stages) + fullDependencyOutputBytes*len(planned.Dependencies)); err != nil {
		return nil, v.work, callerError(err)
	}
	out = &PartitionTransactionTemplate{view: v.id, graph: v.declaration.Graph(), declaration: v.declaration.Digest(), configuration: v.configuration, routing: first.route.routing.Digest(), participants: make([]partitionTemplateEffects, len(stages)), dependencies: planned.Dependencies, bases: make([]raftlog.ApplicationRoot, len(stages)), provisional: revision, defaultAxis: v.binding, limits: first.c.limits}
	for n, s := range stages {
		partition := s.c.root.namespace.Partition
		delta, selected, e := partitionPlan(v, q, partition, planned, groups)
		if e != nil {
			return nil, v.work, e
		}
		effects, e := fullEffects(s, v.views[n].base, delta, selected, PageWork{}, v.limits)
		if e != nil {
			return nil, v.work, e
		}
		part := partitionTemplateEffects{partition: partition, before: s.c.root, effects: effects, writeTags: make([][]int, len(effects.Writes)), groupTags: make([][]int, len(selected)), groupWires: make([][]byte, len(selected))}
		for k, row := range effects.Writes {
			part.writeTags[k] = slices.Clone(i.tags[string(row.Key)])
		}
		selectedIndex := 0
		for k, g := range groups {
			primary, e := v.owner(q, uint64(g.Key.Owner))
			if e != nil {
				return nil, v.work, e
			}
			if primary != partition {
				continue
			}
			wire, e := state.AppendChanges(nil, g.Owned.Axis(), g.Changes, v.limits.Pages.codecLimits(s.c))
			if e != nil {
				return nil, v.work, e
			}
			part.groupWires[selectedIndex] = wire
			part.groupTags[selectedIndex] = i.groupTags[k]
			selectedIndex++
		}
		out.participants[n] = part
		out.bases[n] = effects.Base
	}
	if err := out.seal(v.arena, v.views[0].c.limits); err != nil {
		return nil, v.work, err
	}
	return out, v.work, nil
}
func transactionWritePartition(v *PartitionTransactionView, key []byte) (int, error) {
	offset := 17
	if len(key) >= 3 && key[0] == 'g' && key[1] == 'r' {
		offset = 19
	}
	if len(key) < offset+8 || !bytes.Equal(key[offset-16:offset], v.declaration.graph[:]) {
		return 0, ErrCorrupt
	}
	id := binary.BigEndian.Uint64(key[offset : offset+8])
	for n, s := range v.views {
		if s.c.root.namespace.Partition == id {
			return n, nil
		}
	}
	return 0, ErrNamespace
}
func partitionPlan(v *PartitionTransactionView, q *reader, partition uint64, delta graphstate.Delta, groups []ComponentChangeGroup) (graphstate.Delta, []ComponentChangeGroup, error) {
	out := graphstate.Delta{Graph: delta.Graph, View: delta.View, Dependencies: delta.Dependencies}
	selected := []ComponentChangeGroup{}
	if err := v.arena.Reserve(512 + 256*len(delta.Entities) + 64*len(delta.Lives) + 256*len(delta.Values) + fullPatchOutputBytes*len(delta.Patches) + fullGroupOutputBytes*len(groups)); err != nil {
		return out, nil, callerError(err)
	}
	for _, e := range delta.Entities {
		p, err := v.owner(q, uint64(e.ID))
		if err != nil {
			return out, nil, err
		}
		if p == partition {
			out.Entities = append(out.Entities, e)
		}
	}
	for _, e := range delta.Lives {
		p, err := v.owner(q, uint64(e.Owner))
		if err != nil {
			return out, nil, err
		}
		if p == partition {
			out.Lives = append(out.Lives, e)
		}
	}
	for _, e := range delta.Values {
		p, err := v.owner(q, uint64(e.ID))
		if err != nil {
			return out, nil, err
		}
		if p == partition {
			out.Values = append(out.Values, e)
		}
	}
	for n, e := range delta.Patches {
		p, err := v.owner(q, uint64(e.Key.Owner))
		if err != nil {
			return out, nil, err
		}
		if p == partition {
			out.Patches = append(out.Patches, e)
			selected = append(selected, groups[n])
		}
	}
	return out, selected, nil
}

func (i *transactionInstallation) producedWithin(key graphstate.ComponentKey, scope temporal.Scope, active bool) (bool, error) {
	remaining := scope
	for _, p := range i.produced {
		if p.Key != key {
			continue
		}
		var err error
		remaining, err = remaining.Difference(p.Owned, i.view.views[0].c.limits.Temporal)
		if err != nil {
			return false, err
		}
	}
	if active && i.active != nil && i.active.Key == key {
		var err error
		remaining, err = remaining.Difference(i.active.Owned, i.view.views[0].c.limits.Temporal)
		if err != nil {
			return false, err
		}
	}
	return remaining.Kind() == temporal.ScopeEmpty, nil
}

// SC/CC revision positions are explicit producer tags, not value searches.
func cellOffsets(wire []byte, changes bool) ([][2]int, error) {
	if len(wire) < 56 {
		return nil, ErrCorrupt
	}
	count := int(binary.BigEndian.Uint32(wire[52:56]))
	stride := 34
	if changes {
		stride = 68
	}
	if count > (len(wire)-56)/(4+53+stride) {
		return nil, ErrCorrupt
	}
	out := make([][2]int, 0, count)
	offset := 56
	for range count {
		if len(wire)-offset < 4 {
			return nil, ErrCorrupt
		}
		n := int(binary.BigEndian.Uint32(wire[offset : offset+4]))
		offset += 4
		if n < 53 || n > len(wire)-offset-stride {
			return nil, ErrCorrupt
		}
		offset += n
		second := -1
		if changes {
			second = offset + 34 + 18
		}
		out = append(out, [2]int{offset + 18, second})
		offset += stride
	}
	if offset != len(wire) {
		return nil, ErrCorrupt
	}
	return out, nil
}
func (i *transactionInstallation) stateTags(key graphstate.ComponentKey, s state.State, wire []byte) ([]int, error) {
	if err := i.view.arena.Reserve(64 + 64*s.Usage().Pieces() + 256*s.Usage().Pieces()); err != nil {
		return nil, callerError(err)
	}
	offsets, err := cellOffsets(wire, false)
	if err != nil {
		return nil, err
	}
	tags := []int{}
	for n, p := range s.Pieces() {
		born, e := i.producedWithin(key, p.Scope(), true)
		if e != nil {
			return nil, e
		}
		if born {
			tags = append(tags, offsets[n][0])
		}
	}
	return tags, nil
}
func (i *transactionInstallation) changeTags(key graphstate.ComponentKey, changes []state.Change, wire []byte, physical bool) ([]int, error) {
	if err := i.view.arena.Reserve(64 + 128*len(changes)); err != nil {
		return nil, callerError(err)
	}
	offsets, err := cellOffsets(wire, true)
	if err != nil {
		return nil, err
	}
	tags := make([]int, 0, 2*len(offsets))
	for n, c := range changes {
		born, e := i.producedWithin(key, c.Scope(), false)
		if e != nil {
			return nil, e
		}
		if born && physical {
			tags = append(tags, offsets[n][0])
		}
		tags = append(tags, offsets[n][1])
	}
	return tags, nil
}
