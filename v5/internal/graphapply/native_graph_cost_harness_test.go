package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Fresh test-support profile only. Facts means relationship identities, not
// RPCs/history rows; three anchor nodes and allocator/setup are reported apart.
// No ordinary fixture limit or production storage/materializer changes here.
type nativeGraphCostSpec struct {
	Version      int    `json:"version"`
	Facts        uint64 `json:"facts"`
	CrossPercent uint64 `json:"crossPercent"`
	Writers      int    `json:"writers"`
}

func (s nativeGraphCostSpec) validate() error {
	if s.Version == 9 {
		return nativeCreateUpdateValidate(s)
	}
	if s.Version == 6 {
		return nativeSelectorsValidate(s)
	}
	if s.Version == 5 {
		return nativeFeedValidate(s)
	}
	if s.Version == 4 {
		return nativeGraphEdgeUniqueValidate(s)
	}
	if s.Version != 1 || s.Facts < 1 || s.Facts > 1_000_000 || (s.Writers != 1 && s.Writers != 8 && s.Writers != 32) || s.CrossPercent != 0 && s.CrossPercent != 10 && s.CrossPercent != 50 && s.CrossPercent != 100 {
		return errInvalid
	}
	return nil
}
func nativeGraphCostSpecFromEnv() (*nativeGraphCostSpec, error) {
	text := os.Getenv("RHO_GRAPH_COST_SPEC")
	if text == "" {
		return nil, nil
	}
	if len(text) > 1024 {
		return nil, errLimit
	}
	var s nativeGraphCostSpec
	d := json.NewDecoder(bytes.NewBufferString(text))
	d.DisallowUnknownFields()
	if err := d.Decode(&s); err != nil {
		return nil, errors.Join(errInvalid, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.Join(errInvalid, err)
	}
	if err := s.validate(); err != nil {
		return nil, err
	}
	return &s, nil
}
func (s nativeGraphCostSpec) homeCount() uint64 {
	if s.Version == 4 {
		return 5*s.Facts + 7
	}
	return 6 + 2*s.Facts
}
func (s nativeGraphCostSpec) remoteNode() uint64 { return s.homeCount() + 1 }
func (s nativeGraphCostSpec) cross(i uint64) bool {
	return (i+1)*s.CrossPercent/100 > i*s.CrossPercent/100
}
func (s nativeGraphCostSpec) edge(i uint64) (uint64, uint64, uint64, uint64) {
	target, targetLife := uint64(3), uint64(4)
	if s.cross(i) {
		target, targetLife = s.remoteNode(), s.remoteNode()+1
	}
	return 7 + 2*i, 8 + 2*i, target, targetLife
}
func (s nativeGraphCostSpec) deliveries() int {
	if s.Version == 9 {
		frames, err := nativeCreateUpdateFrames(s)
		if err != nil || frames > uint64(^uint(0)>>1)/1024 {
			return 0
		}
		return int(frames * 1024) // #nosec G115 -- checked native int bound; numeric allowance only.
	}
	if s.Version == 6 {
		frames, err := nativeSelectorsFrames(s)
		if err != nil || frames > uint64(^uint(0)>>1)/1024 {
			return 0
		}
		return int(frames * 1024) // #nosec G115 -- checked finite numeric grant, not an allocation.
	}
	if s.Version == 5 {
		frames, err := nativeFeedFrames(s)
		if err != nil || frames > uint64(^uint(0)>>1)/1024 {
			return 0
		}
		return int(frames * 1024) // #nosec G115 -- checked numeric allowance, never a queue allocation.
	}
	if s.Version == 4 {
		frames, err := nativeGraphEdgeUniqueFrames(s)
		if err != nil || frames > uint64(^uint(0)>>1)/4096 {
			return 0
		}
		return int(frames * 4096) // #nosec G115 -- checked native int bound; numeric allowance, never a queue allocation.
	}
	return int((2*s.Facts + 128) * 4096) // #nosec G115 -- selected darwin/linux arm64 fixture, Facts<=1M, allowance<9Gi.
}

func nativeGraphCostProfile(s *nativeGraphCostSpec, init *declaredInitCommand, l *raftlog.Limits, p *raftlog.ApplicationPolicy, generations *raftlog.ApplicationGenerationLimits) error {
	if s == nil {
		return nil
	}
	if s.Version == 9 {
		return nativeCreateUpdateProfile(*s, init, l, p, generations)
	}
	if s.Version == 6 {
		return nativeSelectorsProfile(*s, init, l, p, generations)
	}
	if s.Version == 5 {
		return nativeFeedProfile(*s, init, l, p, generations)
	}
	if s.Version == 4 {
		return nativeGraphEdgeUniqueProfile(*s, init, l, p, generations)
	}
	if err := s.validate(); err != nil {
		return err
	}
	block, err := nativeGraphCostBlockSize(*s)
	if err != nil {
		return err
	}
	init.maxBlock = block
	// Logical retained quotas are explicit per dataset, never resident/RSS limits.
	// Every old root, graph version, original outcome, CDC and index remains.
	retainedBytes, retainedRecords := uint64(64<<20)+s.Facts*(64<<10), uint64(65536)+s.Facts*512
	l.MaxRetainedBytes, l.MaxRetainedEntries = retainedBytes, 1_000_000
	p.RetainedApplicationBytes, p.RetainedApplicationRecords = retainedBytes, retainedRecords
	*generations = raftlog.ApplicationGenerationLimits{MaxBytes: 2 * retainedBytes, MaxRecords: 2 * retainedRecords}
	return nil
}
func (o *certifiedProcessOwners) grantParameters(data []byte) (uint64, uint64, error) {
	if o.cost == nil {
		return graphE17GrantParameters(o.host, data)
	}
	return nativeGraphCostGrantParameters(o, data)
}

// Inputs select declared typed operations; the actual Host authenticates grants,
// checks dependencies and creates the original proposal. This builds no outcome.
// index1/2/3: three anchors; index4/5 with data=i: edge create/property update.
func nativeGraphCostProposal(o *certifiedProcessOwners, r processRequest, id requestID) (allocationProposal, error) {
	if o != nil && o.cost != nil && o.cost.Version == 4 {
		return nativeGraphEdgeUniqueProposal(o, r, id)
	}
	if o == nil || o.cost == nil || o.recipient == nil || r.index < 1 || r.index > 5 {
		return allocationProposal{}, errInvalid
	}
	cfg, err := o.host.currentConfiguration()
	if err != nil {
		return allocationProposal{}, err
	}
	scope, err := temporal.All(cfg.defaultAxis.Axis())
	if err != nil {
		return allocationProposal{}, err
	}
	var ops []graphstate.Operation
	var claims []freshBinding
	provenance := r.index
	if r.index <= 3 {
		if len(r.data) != 0 {
			return allocationProposal{}, errInvalid
		}
		owner, life := uint64(1), uint64(2)
		if r.index == 2 {
			owner, life = 3, 4
		}
		if r.index == 3 {
			owner, life = o.cost.remoteNode(), o.cost.remoteNode()+1
		}
		ops = []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: graphstate.EntityID(owner), Life: graphstate.LifeID(life), Scope: scope, Record: graphstate.EntityRecord{Interpretation: graphstate.InterpretationObservation, TemporalRole: graphstate.TemporalRoleValidity}}}
		claims = []freshBinding{{role: entityBinding, id: owner}, {role: lifeBinding, owner: graphstate.EntityID(owner), id: life}}
	} else {
		if len(r.data) != 8 {
			return allocationProposal{}, errInvalid
		}
		i := binary.BigEndian.Uint64(r.data)
		if i >= o.cost.Facts {
			return allocationProposal{}, errInvalid
		}
		edge, life, target, targetLife := o.cost.edge(i)
		value, valueID := int64(5), uint64(5)
		provenance = 4 + i
		if r.index == 4 {
			ops = append(ops, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: graphstate.EntityID(edge), Life: graphstate.LifeID(life), Scope: scope, Record: graphstate.EntityRecord{Type: "edge", Source: 1, Target: graphstate.EntityID(target), Mode: graphstate.LifeBound, Interpretation: graphstate.InterpretationAssertedRelation, TemporalRole: graphstate.TemporalRoleOccurrenceTime}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: graphstate.LifeID(targetLife)}})
			claims = append(claims, freshBinding{role: entityBinding, id: edge}, freshBinding{role: lifeBinding, owner: graphstate.EntityID(edge), id: life})
		} else {
			value, valueID = 6, 6
			provenance = 4 + o.cost.Facts + i
		}
		set := graphstate.Operation{Kind: graphstate.Set, Owner: graphstate.EntityID(edge), Life: graphstate.LifeID(life), Scope: scope, Name: "p", Value: graphstate.I64(value), ValueID: graphstate.ValueID(valueID)}
		if o.cost.Version == 9 && r.index == 5 {
			ops = nativeScalarGuardedUpdate(ops, set)
		} else {
			ops = append(ops, set)
		}
		if i == 0 {
			claims = append(claims, freshBinding{role: valueBinding, id: valueID})
		}
	}
	for i := range claims {
		claims[i].grant, err = nativeGraphCostFreshGrant(o, claims[i].id)
		if err != nil {
			return allocationProposal{}, err
		}
	}
	return o.host.PreparePartitionGraphOperations(id, ops, provenance, claims)
}

// Exactly one entity's retained presence/property/life is observed from the real
// child-owned certified view. High-degree endpoint scans use separate paging;
// this never widens the old32-ID lifecycle census or resets its source ledger.
func nativeGraphCostObserve(ctx context.Context, s *graphProcessCertifiedSessions, r processRequest) ([]byte, error) {
	if ctx == nil || s == nil || len(r.data) != 0 || r.entity == 0 {
		return nil, errInvalid
	}
	s.mu.Lock()
	owner, ok := s.owners[r.index]
	s.mu.Unlock()
	if !ok {
		return nil, errInvalid
	}
	view, err := owner.read.ReadView()
	if err != nil {
		return nil, err
	}
	s.coordinator.host.mu.Lock()
	cfg, err := s.coordinator.host.currentConfiguration()
	s.coordinator.host.mu.Unlock()
	if err != nil {
		return nil, err
	}
	entity, err := view.Entity(ctx, graphstate.EntityID(r.entity))
	if err != nil {
		return nil, err
	}
	out := nativeLifecycleEntity{ID: r.entity, Found: entity.Found, Labels: []string{}}
	if entity.Found {
		point, err := temporal.RationalPosition(cfg.defaultAxis.Axis(), temporal.RationalInt64(0))
		if err != nil {
			return nil, err
		}
		scope, err := temporal.All(cfg.defaultAxis.Axis())
		if err != nil {
			return nil, err
		}
		l := s.coordinator.host.machine.limits.graph.Planner
		out.Kind, out.Type, out.Mode, out.Source, out.Target = byte(entity.Record.Kind), entity.Record.Type, byte(entity.Record.Mode), uint64(entity.Record.Source), uint64(entity.Record.Target)
		out.Presence, err = nativeLifecycleCellRead(ctx, view, graphstate.ComponentKey{Owner: graphstate.EntityID(r.entity), Kind: graphstate.Presence}, scope, point, l.Component)
		if err != nil {
			return nil, err
		}
		projection, err := graphstate.Project(ctx, view, graphstate.EntityID(r.entity), point, graphstate.Effective, l)
		if err != nil {
			return nil, err
		}
		out.Effective, out.EffectiveLife, out.Labels = projection.Active, uint64(projection.Life), projection.Labels
		if entity.Record.Kind == graphstate.Relationship {
			out.Property, err = nativeLifecycleCellRead(ctx, view, graphstate.ComponentKey{Owner: graphstate.EntityID(r.entity), Life: projection.Life, Kind: graphstate.ScalarProperty, Name: "p"}, scope, point, l.Component)
			if err != nil {
				return nil, err
			}
		}
	}
	return json.Marshal(out)
}

func nativeGraphCostRecordWire(ctx context.Context, h *allocationHost, index uint64) ([]byte, error) {
	out, err := nativeGraphRecordRead(ctx, h, index, true)
	if err != nil {
		return nil, err
	}
	wire, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	if len(wire) > nativeLifecycleBytes {
		return nil, errLimit
	}
	return wire, nil
}

const nativeGraphCostBatchCommands = 16 // retained wire page size; independent of client window
const nativeGraphCostWindowCommands = 32
const nativeGraphCostPageBytes = 1 << 20

// Inspect the original coordinator receipt at its immutable decision index.
// This is a bounded local read inside the original Coordinate worker; it never
// submits a command or changes the general coordinator-result envelope.
func nativeGraphCostCoordinatorManifest(ctx context.Context, h *allocationHost, result crossProcessResult) (wire []byte, err error) {
	if ctx == nil || h == nil || h.machine == nil || h.machine.store == nil || result.resume.scope != h.machine.scope() || result.decision.ns != h.machine.ns || result.decision.tx != result.resume.tx || result.decision.phase != crossDecide || result.decision.commit && result.decision.round == 0 {
		return nil, errInvalid
	}
	view, err := h.machine.store.ApplicationView(result.decision.index)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, view.Close())
		if err != nil {
			wire = nil
		}
	}()
	root, err := view.RootBounded(ctx, 172)
	if err != nil {
		return nil, err
	}
	l := h.machine.limits
	arena, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		return nil, err
	}
	limits := l.allocation
	limits.readRows, limits.readBytes = min(limits.readRows, l.sourceRows), min(limits.readBytes, l.sourceBytes)
	q := reader{ctx: ctx, view: view, ns: h.machine.ns, base: root, limits: limits, arena: arena, bytes: cap(root.Image)}
	decision, found, err := crossControl(&q, crossControlKey(q.ns, result.resume.tx, 'D'), 256)
	if err != nil || !found || decision.Index != result.decision.index {
		return nil, errors.Join(errCorrupt, err)
	}
	status, err := decodeCrossStatus(decision.Value)
	if err != nil || status != result.decision {
		return nil, errors.Join(errCorrupt, err)
	}
	registration, found, err := crossControl(&q, crossControlKey(q.ns, result.resume.tx, 'R'), min(l.commandBytes, 1<<20))
	if err != nil || !found || registration.Index > decision.Index || sha256.Sum256(registration.Value) != status.digest {
		return nil, errors.Join(errCorrupt, err)
	}
	manifest, err := decodeCrossManifest(registration.Value, l, arena)
	if err != nil || manifest.tx != result.resume.tx || manifest.request.id != result.resume.request || manifest.request.ns != h.machine.ns || manifest.coordinator != h.machine.ns.partition {
		return nil, errors.Join(errCorrupt, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return registration.Value, nil
}

// Additive cost-only inspection metadata comes from genuine retained roots,
// controls, outcomes and canonical CDC, never the runner's expected commands.
// The existing lifecycle exporter remains unchanged.
type nativeGraphCostOriginal struct {
	Kind, Graph, Request, Transaction, Digest        string
	Partition, Index, Coordinator, Provenance, Round uint64
	Phase                                            byte
	Commit                                           bool
	Reason, Disposition                              byte
	Participants                                     []uint64
}
type nativeGraphCostBoundary struct {
	Index, Generation uint64
	ImageSHA256       string
}

// A replay entry retains its ACTUAL position/root and empty current CDC. These
// earlier raw frames are read from the same native retained source, never from
// expected commands or ACK artifacts. Their earlier index is not the new entry.
type nativeGraphCostReplay struct {
	Outcome, CDC, Registration []byte
	Boundary, Previous         nativeGraphCostBoundary
}
type nativeGraphCostRecord struct {
	nativeLifecycleRecord
	Original       nativeGraphCostOriginal
	CDCClass       string
	Replay         *nativeGraphCostReplay `json:",omitzero"`
	RawHashes      struct{ OutcomeSHA256, CDCSHA256 string }
	SourceBoundary nativeGraphCostBoundary
	Logical        struct{ Entities, Lives, Values, Components, Schemas int }
}

func nativeGraphCostInspectRecord(ctx context.Context, h *allocationHost, record nativeLifecycleRecord) (out nativeGraphCostRecord, err error) {
	if ctx == nil || h == nil || h.machine == nil || h.machine.store == nil || record.Index == 0 || record.Partition != h.machine.ns.partition {
		return out, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	view, err := h.machine.store.ApplicationView(record.Index)
	if err != nil {
		return out, err
	}
	defer func() {
		err = errors.Join(err, view.Close())
		if err != nil {
			out = nativeGraphCostRecord{}
		}
	}()
	root, err := view.RootBounded(ctx, 172)
	if err != nil {
		return out, err
	}
	decodedRoot, err := graphstore.DecodeRoot(root.Image)
	if err != nil || root.Index != record.Index || decodedRoot.Namespace() != (graphstore.Namespace{Graph: graphstate.GraphID(h.machine.ns.graph), Partition: record.Partition}) {
		return out, errors.Join(errCorrupt, err)
	}
	// Missing frames fail in ApplicationRecord. Present empty frames remain
	// nonnil; neither copied nil DTOs nor bytes.Equal(nil, empty) supply presence.
	actualOutcome, err := view.ApplicationRecord(ctx, record.Index, true, 64<<10)
	if err != nil {
		return out, err
	}
	actualCDC, err := view.ApplicationRecord(ctx, record.Index, false, 1<<20)
	if err != nil {
		return out, err
	}
	if actualOutcome == nil || actualCDC == nil || record.Outcome == nil || record.CDC == nil || !bytes.Equal(actualOutcome, record.Outcome) || !bytes.Equal(actualCDC, record.CDC) {
		return out, errCorrupt
	}
	// SAME-request retries can append later retained entries without repeating
	// business CDC. Recognize only the native authenticated replay grammars.
	var replayIndex uint64
	if bytes.HasPrefix(actualOutcome, []byte("GCS\x01")) {
		original, e := decodeCrossStatus(actualOutcome)
		if e != nil || original.ns != h.machine.ns || original.index > record.Index {
			return out, errors.Join(errCorrupt, e)
		}
		if original.index < record.Index {
			replayIndex = original.index
		}
	} else if bytes.HasPrefix(actualOutcome, []byte("GRO\x04")) {
		original, e := decodePartitionGraphOutcome(actualOutcome, h.machine.ns)
		if e != nil || original.index > record.Index {
			return out, errors.Join(errCorrupt, e)
		}
		if original.index < record.Index {
			if original.disposition != requestReplay {
				return out, errCorrupt
			}
			replayIndex = original.index
		}
	}
	if replayIndex != 0 {
		return nativeGraphCostInspectReplay(ctx, h, view, root, record, replayIndex)
	}
	l := h.machine.limits
	arena, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		return out, err
	}
	limits := l.allocation
	limits.readRows, limits.readBytes = min(limits.readRows, l.sourceRows), min(limits.readBytes, l.sourceBytes)
	q := reader{ctx: ctx, view: view, ns: h.machine.ns, base: root, limits: limits, arena: arena, bytes: cap(root.Image) + cap(actualOutcome) + cap(actualCDC)}
	metadata := nativeGraphCostOriginal{Graph: hex.EncodeToString(h.machine.ns.graph[:]), Partition: record.Partition, Index: record.Index, Participants: []uint64{}}
	var bound outcome
	class := ""
	switch {
	case len(actualOutcome) == 0:
		if len(actualCDC) != 0 || record.Index <= 1 {
			return out, errCorrupt
		}
		previous, err := view.RootAt(ctx, record.Index-1, 172)
		if err != nil || previous.Generation != root.Generation || !bytes.Equal(previous.Image, root.Image) {
			return out, errors.Join(errCorrupt, err)
		}
		// This witnesses actual present empty application records and unchanged
		// application image. It makes no claim about the compacted Raft command.
		metadata.Kind, class = "no-application-output", "read-control"
	case bytes.HasPrefix(actualOutcome, []byte("GSO\x01")):
		original, err := decodeGraphCutResult(actualOutcome)
		if err != nil || original.index != record.Index || len(actualCDC) != 0 {
			return out, errors.Join(errCorrupt, err)
		}
		metadata.Kind, metadata.Round, metadata.Phase, metadata.Disposition = "read-control", original.round, original.phase, original.status
		metadata.Commit = original.status == graphCutApplied
		class = "read-control"
	case bytes.HasPrefix(actualOutcome, []byte("GCS\x01")):
		original, err := decodeCrossStatus(actualOutcome)
		if err != nil || original.ns != h.machine.ns || original.index != record.Index {
			return out, errors.Join(errCorrupt, err)
		}
		stored, found, err := q.getBounded(crossKey(q.ns, original.tx), 256)
		if err != nil || !found || !bytes.Equal(stored, actualOutcome) {
			return out, errors.Join(errCorrupt, err)
		}
		registration, found, err := crossControl(&q, crossControlKey(q.ns, original.tx, 'R'), min(l.commandBytes, 1<<20))
		if err != nil || !found || registration.Index > record.Index || sha256.Sum256(registration.Value) != original.digest {
			return out, errors.Join(errCorrupt, err)
		}
		manifest, err := decodeCrossManifest(registration.Value, l, arena)
		if err != nil || manifest.tx != original.tx || manifest.digest != original.digest {
			return out, errors.Join(errCorrupt, err)
		}
		metadata.Kind, metadata.Request, metadata.Transaction, metadata.Digest = "cross", hex.EncodeToString(manifest.request.id[:]), hex.EncodeToString(original.tx[:]), hex.EncodeToString(original.digest[:])
		metadata.Coordinator, metadata.Provenance, metadata.Round, metadata.Phase, metadata.Commit, metadata.Reason = manifest.coordinator, manifest.request.revision.Provenance(), original.round, byte(original.phase), original.commit, byte(original.why)
		for _, part := range manifest.parts {
			metadata.Participants = append(metadata.Participants, part.binding.Partition)
		}
		if len(actualCDC) == 0 {
			if original.phase == crossResolve && original.commit {
				return out, errCorrupt
			}
			class = "protocol-empty"
		} else {
			if original.phase != crossResolve || !original.commit || original.round == 0 {
				return out, errCorrupt
			}
			bound = outcome{ns: original.ns, kind: graphOperations, identity: original.tx, hash: original.digest, index: original.index, disposition: applied}
		}
	default:
		original, err := decodePartitionGraphOutcome(actualOutcome, h.machine.ns)
		if err != nil || original.index != record.Index || original.kind != graphOperations || original.disposition != applied || original.reason == reasonNone && len(actualCDC) == 0 || original.reason != reasonNone && len(actualCDC) != 0 {
			return out, errors.Join(errCorrupt, err)
		}
		stored, found, err := q.getBounded(outcomeKey(request{ns: original.ns, kind: original.kind, id: requestID(original.identity)}), graphOutcomeBytes)
		if err != nil || !found || !bytes.Equal(stored, actualOutcome) {
			return out, errors.Join(errCorrupt, err)
		}
		metadata.Kind, metadata.Request, metadata.Transaction, metadata.Digest = "local-graph", hex.EncodeToString(original.identity[:]), hex.EncodeToString(original.identity[:]), hex.EncodeToString(original.hash[:])
		metadata.Coordinator, metadata.Disposition, metadata.Reason = original.ns.partition, byte(original.disposition), byte(original.reason)
		metadata.Participants, metadata.Commit = []uint64{original.ns.partition}, original.reason == reasonNone
		if original.reason != reasonNone {
			class = "protocol-empty"
		}
		bound = original
	}
	if len(actualCDC) == 0 {
		if len(record.Changes)+len(record.Entities)+len(record.Lives)+len(record.Values) != 0 {
			return out, errCorrupt
		}
	} else {
		logical, err := decodeTypedChangeEnvelope(actualCDC, bound, l, true)
		if err != nil {
			return out, errors.Join(errCorrupt, err)
		}
		out.Logical.Entities, out.Logical.Lives, out.Logical.Values, out.Logical.Schemas = len(logical.entities), len(logical.lives), len(logical.values), len(logical.schemas)
		for _, group := range logical.groups {
			out.Logical.Components += len(group.Changes)
			for _, change := range group.Changes {
				revision := change.After().Revision()
				if metadata.Kind == "local-graph" && metadata.Round == 0 {
					metadata.Round, metadata.Provenance = revision.ID(), revision.Provenance()
				}
				if revision.ID() != metadata.Round || revision.Provenance() != metadata.Provenance {
					return nativeGraphCostRecord{}, errCorrupt
				}
			}
		}
		if out.Logical.Components != len(record.Changes) || out.Logical.Entities != len(record.Entities) || out.Logical.Lives != len(record.Lives) || out.Logical.Values != len(record.Values) {
			return nativeGraphCostRecord{}, errCorrupt
		}
		class = "logical"
		if !logical.nonempty() {
			class = "participant-empty"
		}
	}
	if err := ctx.Err(); err != nil {
		return nativeGraphCostRecord{}, err
	}
	out.nativeLifecycleRecord, out.Original, out.CDCClass = record, metadata, class
	out.RawHashes.OutcomeSHA256 = fmt.Sprintf("%x", sha256.Sum256(actualOutcome))
	out.RawHashes.CDCSHA256 = fmt.Sprintf("%x", sha256.Sum256(actualCDC))
	out.SourceBoundary.Index, out.SourceBoundary.Generation, out.SourceBoundary.ImageSHA256 = root.Index, root.Generation, fmt.Sprintf("%x", root.ImageHash)
	return out, nil
}

func nativeGraphCostInspectReplay(ctx context.Context, h *allocationHost, view *raftlog.ApplicationView, root raftlog.ApplicationRoot, record nativeLifecycleRecord, index uint64) (out nativeGraphCostRecord, err error) {
	if index == 0 || index >= record.Index || record.Index <= 1 || record.CDC == nil || len(record.CDC)+len(record.Changes)+len(record.Entities)+len(record.Lives)+len(record.Values) != 0 {
		return out, errCorrupt
	}
	earlierOutcome, err := view.ApplicationRecord(ctx, index, true, 64<<10)
	if err != nil {
		return out, err
	}
	earlierCDC, err := view.ApplicationRecord(ctx, index, false, 1<<20)
	if err != nil {
		return out, err
	}
	earlierRoot, err := view.RootAt(ctx, index, 172)
	if err != nil {
		return out, err
	}
	previous, err := view.RootAt(ctx, record.Index-1, 172)
	if err != nil {
		return out, err
	}
	if earlierOutcome == nil || earlierCDC == nil || earlierRoot.Generation != root.Generation || previous.Generation != root.Generation || !bytes.Equal(previous.Image, root.Image) {
		return out, errCorrupt
	}
	replay := &nativeGraphCostReplay{Outcome: earlierOutcome, CDC: earlierCDC, Boundary: nativeGraphCostBoundary{index, earlierRoot.Generation, fmt.Sprintf("%x", earlierRoot.ImageHash)}, Previous: nativeGraphCostBoundary{previous.Index, previous.Generation, fmt.Sprintf("%x", previous.ImageHash)}}
	if bytes.HasPrefix(record.Outcome, []byte("GCS\x01")) {
		status, e := decodeCrossStatus(earlierOutcome)
		if e != nil || status.ns != h.machine.ns || status.index != index || !bytes.Equal(record.Outcome, earlierOutcome) {
			return out, errors.Join(errCorrupt, e)
		}
		l := h.machine.limits
		arena, e := graphstate.NewOutputBudget(l.outputBytes)
		if e != nil {
			return out, e
		}
		q := reader{ctx: ctx, view: view, ns: h.machine.ns, base: root, limits: l.allocation, arena: arena, bytes: cap(root.Image) + cap(record.Outcome) + cap(earlierOutcome) + cap(earlierCDC)}
		registration, found, e := crossControl(&q, crossControlKey(q.ns, status.tx, 'R'), min(l.commandBytes, 1<<20))
		if e != nil || !found || registration.Index > index || sha256.Sum256(registration.Value) != status.digest {
			return out, errors.Join(errCorrupt, e)
		}
		replay.Registration = registration.Value
	} else {
		current, e := decodePartitionGraphOutcome(record.Outcome, h.machine.ns)
		if e != nil {
			return out, e
		}
		original, e := decodePartitionGraphOutcome(earlierOutcome, h.machine.ns)
		if e != nil || original.index != index || original.disposition != applied || current.disposition != requestReplay || current.identity != original.identity || current.hash != original.hash || current.kind != original.kind || current.reason != original.reason {
			return out, errors.Join(errCorrupt, e)
		}
	}
	// Earlier originals must themselves be original-index entries. This is a
	// bounded single level, never a recursive replay chain or latest-state alias.
	raw, err := nativeGraphRecordRead(ctx, h, index, true)
	if err != nil {
		return out, err
	}
	if !bytes.Equal(raw.Outcome, earlierOutcome) || !bytes.Equal(raw.CDC, earlierCDC) {
		return out, errCorrupt
	}
	original, err := nativeGraphCostInspectRecord(ctx, h, raw)
	if err != nil {
		return out, err
	}
	if original.Replay != nil || original.Original.Index != index || original.SourceBoundary != replay.Boundary {
		return out, errCorrupt
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	out.nativeLifecycleRecord, out.Original, out.CDCClass, out.Replay = record, original.Original, "replay-empty", replay
	out.RawHashes.OutcomeSHA256 = fmt.Sprintf("%x", sha256.Sum256(record.Outcome))
	out.RawHashes.CDCSHA256 = fmt.Sprintf("%x", sha256.Sum256(record.CDC))
	out.SourceBoundary = nativeGraphCostBoundary{root.Index, root.Generation, fmt.Sprintf("%x", root.ImageHash)}
	return out, nil
}

type nativeGraphCostRecordPage struct {
	Partition, End, Next uint64
	Image                []byte
	Records              []nativeGraphCostRecord
}

// Request index is the prior exported index; entity is the actual certificate
// sourceIndex. Every page owns a genuine retained view at that exact boundary.
// The typed leader path hydrates original records; other voters return raw
// originals/CDC for exact equality. Neither copied root nor page grants a cut.
func nativeGraphCostRecordsWire(ctx context.Context, h *allocationHost, r processRequest) (wire []byte, err error) {
	if ctx == nil || h == nil || h.machine == nil || h.machine.store == nil || r.entity == 0 || r.index > r.entity || len(r.data) != 1 || r.data[0] > 1 {
		return nil, errInvalid
	}
	view, err := h.machine.store.ApplicationView(r.entity)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, view.Close())
		if err != nil {
			wire = nil
		}
	}()
	root, err := view.RootBounded(ctx, 172)
	if err != nil {
		return nil, err
	}
	page := nativeGraphCostRecordPage{Partition: h.machine.ns.partition, End: root.Index, Next: r.index, Image: root.Image, Records: []nativeGraphCostRecord{}}
	owned := len(root.Image) + 256
	for index := r.index + 1; index <= r.entity && len(page.Records) < nativeGraphCostBatchCommands; index++ {
		var record nativeGraphCostRecord
		if r.data[0] == 1 {
			var raw nativeLifecycleRecord
			raw, err = nativeGraphRecordRead(ctx, h, index, true)
			if err == nil {
				record, err = nativeGraphCostInspectRecord(ctx, h, raw)
			}
		} else {
			record.Partition, record.Index = page.Partition, index
			record.Outcome, err = view.ApplicationRecord(ctx, index, true, 64<<10)
			if err == nil {
				record.CDC, err = view.ApplicationRecord(ctx, index, false, 1<<20)
			}
		}
		if err != nil {
			return nil, err
		}
		encoded, e := json.Marshal(record)
		if e != nil {
			return nil, e
		}
		if len(encoded) > nativeLifecycleBytes || owned+len(encoded)+1 > nativeGraphCostPageBytes {
			return nil, errLimit
		}
		owned += len(encoded) + 1
		page.Records = append(page.Records, record)
		page.Next = index
	}
	wire, err = nativeGraphCostEncodePage(page)
	if len(wire) > nativeGraphCostPageBytes {
		return nil, errLimit
	}
	return wire, err
}

func nativeGraphCostEncodePage(page nativeGraphCostRecordPage) ([]byte, error) {
	if len(page.Image) != 172 || len(page.Records) > nativeGraphCostBatchCommands {
		return nil, errLimit
	}
	wire, err := processEncoding(func(w *boundedWriter) {
		w.add([]byte("GCP1"))
		w.u64(page.Partition)
		w.u64(page.End)
		w.u64(page.Next)
		w.field(page.Image)
		w.u32(len(page.Records))
		for _, record := range page.Records {
			encoded, err := json.Marshal(record)
			if err != nil {
				w.err = err
				return
			}
			if len(encoded) > nativeLifecycleBytes {
				w.err = errLimit
				return
			}
			w.field(encoded)
		}
	})
	if len(wire) > nativeGraphCostPageBytes {
		return nil, errLimit
	}
	return wire, err
}

// Validate the complete page, including empty root-only pages, before any
// expected-command matching. Count/bytes are checked before collection allocation.
// Missing/reordered/extra records cannot advance the retained prefix.
func nativeGraphCostDecodePage(wire []byte, partition, from uint64, certificate graphCertificate) (nativeGraphCostRecordPage, error) {
	if certificate.scope.partition != partition {
		return nativeGraphCostRecordPage{}, errCorrupt
	}
	return nativeGraphCostDecodeRawPage(wire, partition, from, certificate.sourceIndex, certificate.image[:])
}

// A State-bound raw page proves retained bytes only. It grants no graph cut or
// quorum authority; the certified caller above still supplies its real boundary.
func nativeGraphCostDecodeRawPage(wire []byte, partition, from, end uint64, image []byte) (nativeGraphCostRecordPage, error) {
	if len(wire) > nativeGraphCostPageBytes {
		return nativeGraphCostRecordPage{}, errLimit
	}
	c := graphCursor{b: wire}
	if !bytes.Equal(c.take(4), []byte("GCP1")) {
		return nativeGraphCostRecordPage{}, errCorrupt
	}
	page := nativeGraphCostRecordPage{Partition: c.u64(), End: c.u64(), Next: c.u64(), Image: bytes.Clone(c.field(172))}
	n := c.count(nativeGraphCostBatchCommands, 4)
	if c.err != nil {
		return page, errors.Join(errCorrupt, c.err)
	}
	page.Records = make([]nativeGraphCostRecord, n)
	for i := range page.Records {
		encoded := c.field(nativeLifecycleBytes)
		if c.err != nil {
			return page, errors.Join(errCorrupt, c.err)
		}
		d := json.NewDecoder(bytes.NewReader(encoded))
		d.DisallowUnknownFields()
		if err := d.Decode(&page.Records[i]); err != nil {
			return page, errors.Join(errCorrupt, err)
		}
		if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
			return page, errors.Join(errCorrupt, err)
		}
	}
	if c.err != nil || len(c.b) != 0 || from > end {
		return page, errors.Join(errCorrupt, c.err)
	}
	want := min(end-from, uint64(nativeGraphCostBatchCommands))
	if len(image) != 172 || page.Partition != partition || page.End != end || !bytes.Equal(page.Image, image) || uint64(len(page.Records)) != want || page.Next != from+want {
		return page, errCorrupt
	}
	for i, record := range page.Records {
		if record.Partition != partition || record.Index != from+1+uint64(i) || record.Outcome == nil || record.CDC == nil {
			return page, errCorrupt
		}
	}
	return page, nil
}
