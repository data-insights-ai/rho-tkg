package graphapply

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

const nativeGraphEdgeUniqueFamily = "cross-edge-endpoint-uniqueness"
const nativeGraphEdgeUniqueMarker = "GEU1"

func nativeGraphEdgeUniqueValidate(s nativeGraphCostSpec) error {
	if s.Version != 4 || s.Facts < 20 || s.Facts > 1_000_000 || s.Facts%20 != 0 || (s.CrossPercent != 0 && s.CrossPercent != 10 && s.CrossPercent != 50 && s.CrossPercent != 100) || (s.Writers != 1 && s.Writers != 8 && s.Writers != 32) {
		return errInvalid
	}
	return nil
}
func nativeGraphEdgeUniqueProcessProfile(s *nativeGraphCostSpec) (bool, error) {
	flag := os.Getenv("RHO_GRAPH_EDGE_UNIQUE_PROCESS")
	if flag == "" {
		if s != nil && s.Version == 4 {
			return false, errInvalid
		}
		return false, nil
	}
	if flag != "1" || s == nil || nativeGraphEdgeUniqueValidate(*s) != nil || os.Getenv("RHO_GRAPH_CERTIFIED_PROCESS") != "1" || os.Getenv("RHO_GRAPH_SOURCE_PROCESS") != "1" {
		return false, errInvalid
	}
	for _, name := range []string{"RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_NATIVE_FAULT_FAMILY"} {
		if value := os.Getenv(name); value != "" && value != "0" {
			return false, errInvalid
		}
	}
	if _, _, err := nativeGraphEdgeUniqueAdmission(*s); err != nil {
		return false, err
	}
	return true, nil
}
func nativeGraphEdgeUniqueProfile(s nativeGraphCostSpec, init *declaredInitCommand, l *raftlog.Limits, p *raftlog.ApplicationPolicy, g *raftlog.ApplicationGenerationLimits) error {
	if init == nil || l == nil || p == nil || g == nil || nativeGraphEdgeUniqueValidate(s) != nil {
		return errInvalid
	}
	block, err := nativeGraphCostBlockSize(s)
	if err != nil {
		return err
	}
	init.maxBlock = block
	// Explicit new family quota only. Real log/application/RS2/index/original
	// accounting still refuses before effects; these are not RSS/capacity claims.
	frames, err := nativeGraphEdgeUniqueFrames(s)
	if err != nil || p.MaxInstallWrites != 256 || p.MaxImageBytes != 172 {
		return errors.Join(errInvalid, err)
	}
	retainedBytes := uint64(64<<20) + s.Facts*(512<<10)
	// Max256 graph/control puts + Root/Outcome/CDC + actual RS2 root/index.
	retainedRecords := uint64(65536) + frames*260
	// Native Raft tail admission is independent of immutable application
	// history and has its own hard10M entry ceiling. Keep the existing bounded
	// tail; native recovery-base reclamation never deletes application history.
	l.MaxRetainedBytes, l.MaxRetainedEntries = retainedBytes, 1_000_000
	p.RetainedApplicationBytes, p.RetainedApplicationRecords = retainedBytes, retainedRecords
	*g = raftlog.ApplicationGenerationLimits{MaxBytes: 2 * retainedBytes, MaxRecords: 2 * retainedRecords}
	return nil
}
func nativeGraphEdgeUniqueGrantPlan(s nativeGraphCostSpec, block, partition, sequence uint64) (uint64, uint64, error) {
	if nativeGraphEdgeUniqueValidate(s) != nil || block < 2 || block > idalloc.MaxBlockSize || sequence == 0 {
		return 0, 0, errInvalid
	}
	if partition == 3 {
		grants := (s.homeCount() + block - 1) / block
		if sequence > grants {
			return 0, 0, errInvalid
		}
		first := 1 + (sequence-1)*block
		return first, min(block, s.homeCount()-first+1), nil
	}
	if partition == 8 && sequence == 1 {
		return s.remoteNode(), 3, nil
	}
	return 0, 0, errInvalid
}
func nativeGraphEdgeUniqueSchemas() []graphstate.PropertyDefinition {
	return []graphstate.PropertyDefinition{{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}, {Name: "p", Owner: graphstate.Relationship, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality, Unique: graphstate.UniqueScalar}}
}

type nativeGraphEdgeUniqueCommand struct {
	Version                  uint8
	Family, Phase            string
	Fact, Candidate, Ordinal uint64
	Reopened                 bool
}

func nativeGraphEdgeUniqueInput(q nativeGraphEdgeUniqueCommand) ([]byte, error) {
	body, err := json.Marshal(q)
	if err != nil || len(body) > 1024 {
		return nil, errors.Join(errInvalid, err)
	}
	return append([]byte(nativeGraphEdgeUniqueMarker), body...), nil
}
func nativeGraphEdgeUniqueDecode(s nativeGraphCostSpec, r processRequest) (nativeGraphEdgeUniqueCommand, error) {
	var q nativeGraphEdgeUniqueCommand
	if nativeGraphEdgeUniqueValidate(s) != nil || r.index != 0 || !bytes.HasPrefix(r.data, []byte(nativeGraphEdgeUniqueMarker)) || len(r.data) > 1024+len(nativeGraphEdgeUniqueMarker) {
		return q, errInvalid
	}
	body := r.data[len(nativeGraphEdgeUniqueMarker):]
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(&q); err != nil {
		return q, errors.Join(errInvalid, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return q, errors.Join(errInvalid, err)
	}
	canonical, err := json.Marshal(q)
	if err != nil || !bytes.Equal(canonical, body) || q.Version != 1 || q.Family != nativeGraphEdgeUniqueFamily || q.Candidate > 1 {
		return q, errInvalid
	}
	ordinal := uint64(0)
	switch q.Phase {
	case "setup":
		if q.Fact < 3 && q.Candidate == 0 && !q.Reopened {
			ordinal = 1 + q.Fact
		}
	case "claim":
		if q.Fact < s.Facts && !q.Reopened {
			ordinal = 4 + 2*q.Fact + q.Candidate
		}
	case "stale":
		if q.Fact < s.Facts && !q.Reopened {
			ordinal = 4 + 2*s.Facts + q.Fact
		}
	case "replace":
		if q.Fact < s.Facts && q.Reopened {
			ordinal = 4 + 3*s.Facts + q.Fact
		}
	case "frozen-close":
		if q.Fact < s.Facts && !q.Reopened {
			ordinal = 4 + 4*s.Facts + q.Fact
		}
	case "close":
		if q.Fact < s.Facts && !q.Reopened {
			ordinal = 4 + 5*s.Facts + q.Fact
		}
	case "life":
		if q.Fact < 4 && q.Candidate == 0 && q.Reopened == (q.Fact%2 == 1) {
			ordinal = 4 + 6*s.Facts + q.Fact
		}
	}
	if ordinal == 0 || q.Ordinal != ordinal {
		return q, errInvalid
	}
	return q, nil
}
func nativeGraphEdgeUniqueProposal(o *certifiedProcessOwners, r processRequest, id requestID) (allocationProposal, error) {
	if o == nil || o.host == nil || o.cost == nil || o.recipient == nil || o.host.machine.SemanticContractID() != durableColumnGraphSemanticContractID() {
		return allocationProposal{}, errInvalid
	}
	s := *o.cost
	q, err := nativeGraphEdgeUniqueDecode(s, r)
	if err != nil {
		return allocationProposal{}, err
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
	if q.Phase == "setup" || q.Phase == "life" {
		owner, life := uint64(1), uint64(2)
		kind := graphstate.CreateNode
		if q.Phase == "setup" {
			if q.Fact == 1 {
				owner, life = 3, 4
			}
			if q.Fact == 2 {
				owner, life = s.remoteNode(), s.remoteNode()+1
			}
		} else {
			owner, life = 3, 4
			if q.Fact >= 2 {
				owner, life = s.remoteNode(), s.remoteNode()+1
			}
			kind = graphstate.Close
			if q.Reopened {
				kind = graphstate.Reopen
				life = 4*s.Facts + 7
				if q.Fact >= 2 {
					life = s.remoteNode() + 2
				}
			}
		}
		op := graphstate.Operation{Kind: kind, Owner: graphstate.EntityID(owner), Life: graphstate.LifeID(life), Scope: scope}
		if kind == graphstate.CreateNode {
			op.Record = graphstate.EntityRecord{Interpretation: graphstate.InterpretationObservation, TemporalRole: graphstate.TemporalRoleValidity}
			claims = append(claims, freshBinding{role: entityBinding, id: owner})
		}
		if kind != graphstate.Close {
			claims = append(claims, freshBinding{role: lifeBinding, owner: graphstate.EntityID(owner), id: life})
		}
		ops = []graphstate.Operation{op}
	} else {
		edge := 7 + 4*q.Fact + 2*q.Candidate
		life := edge + 1
		if q.Phase == "close" || q.Phase == "frozen-close" {
			ops = []graphstate.Operation{{Kind: graphstate.Close, Owner: graphstate.EntityID(edge), Life: graphstate.LifeID(life), Scope: scope}}
		} else {
			target, targetLife := uint64(3), uint64(4)
			if s.cross(q.Fact) {
				target, targetLife = s.remoteNode(), s.remoteNode()+1
			}
			if q.Reopened {
				targetLife = 4*s.Facts + 7
				if s.cross(q.Fact) {
					targetLife = s.remoteNode() + 2
				}
			}
			value, valueID := int64(q.Fact+5), 4*s.Facts+8+q.Fact // #nosec G115 -- admitted fact<1M.
			ops = []graphstate.Operation{{Kind: graphstate.CreateRelationship, Owner: graphstate.EntityID(edge), Life: graphstate.LifeID(life), Scope: scope, Record: graphstate.EntityRecord{Type: "edge", Source: 1, Target: graphstate.EntityID(target), Mode: graphstate.LifeBound, Interpretation: graphstate.InterpretationAssertedRelation, TemporalRole: graphstate.TemporalRoleOccurrenceTime}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: graphstate.LifeID(targetLife)}}, {Kind: graphstate.Set, Owner: graphstate.EntityID(edge), Life: graphstate.LifeID(life), Scope: scope, Name: "p", Value: graphstate.I64(value), ValueID: graphstate.ValueID(valueID)}}
			claims = append(claims, freshBinding{role: entityBinding, id: edge}, freshBinding{role: lifeBinding, owner: graphstate.EntityID(edge), id: life}, freshBinding{role: valueBinding, id: valueID})
		}
	}
	for i := range claims {
		claims[i].grant, err = nativeGraphCostFreshGrant(o, claims[i].id)
		if err != nil {
			return allocationProposal{}, err
		}
	}
	return o.host.PreparePartitionGraphOperations(id, ops, q.Ordinal, claims)
}

func nativeGraphEdgeUniqueColumnPolicy(h *allocationHost) (nativeColumnPolicy, error) {
	if h == nil || h.machine == nil || h.machine.store == nil || h.machine.SemanticContractID() != durableColumnGraphSemanticContractID() {
		return nativeColumnPolicy{}, errInvalid
	}
	spec, err := nativeGraphCostSpecFromEnv()
	if err != nil || spec == nil || nativeGraphEdgeUniqueValidate(*spec) != nil {
		return nativeColumnPolicy{}, errors.Join(errInvalid, err)
	}
	seconds, err := strconv.ParseUint(os.Getenv("RHO_GRAPH_WORKLOAD_SECONDS"), 10, 32)
	if err != nil || seconds == 0 || seconds > 365*24*60*60 {
		return nativeColumnPolicy{}, errors.Join(errInvalid, err)
	}
	frames, err := nativeGraphEdgeUniqueFrames(*spec)
	if err != nil {
		return nativeColumnPolicy{}, err
	}
	// Fresh retain-all workload, no source collection between Initial/Final.
	// AS7 capability alone is not a claim that absolute Through counts sparse
	// records: this admitted profile never dispatches a GC operation.
	p := nativeColumnPolicy{Version: 1, ChunkRecords: 16, ChunkRows: 16, ChunkBytes: 64 << 10, FrameBytes: 32 << 10, MaxGenerations: 2, MaxChunks: 1 << 40, MaxRetainedBytes: 1 << 62, MaxWorkRecords: ^uint64(0), MaxWorkBytes: ^uint64(0), MaxDecodedRows: ^uint64(0), MaxOutputBytes: ^uint64(0), MaxRPCs: ^uint64(0), MaxWireBytes: ^uint64(0), CacheBytes: 2 << 20, MemTableBytes: 1 << 20, Lifetime: time.Minute}
	l := h.machine.limits
	opening, page, err := nativeColumnLimitAllowance(h.machine.store.ApplicationLimits(), l, p)
	if err != nil {
		return p, err
	}
	opening, err = opening.scaled(2)
	if err != nil {
		return p, err
	}
	opening, err = nativeColumnPeerOpening(opening, l)
	if err != nil {
		return p, err
	}
	failedPages := 32 + seconds/60 + 4
	const passes = 8 // dependency, publication, native/fill/cache and reopened native/cache, plus old-At
	if frames == 0 || frames >= 1<<40 || frames+1 > (^uint64(0)-failedPages)/passes {
		return p, errLimit
	}
	pages, err := page.scaled((frames+1)*passes + failedPages)
	if err != nil {
		return p, err
	}
	starts := seconds/60 + 13
	captures, err := opening.scaled(8*starts + 4*passes)
	if err != nil {
		return p, err
	}
	budget, err := pages.plus(captures)
	if err != nil {
		return p, err
	}
	retention, err := nativeColumnRetentionWork(l).scaled(8*starts + 32)
	if err != nil {
		return p, err
	}
	budget, err = budget.plus(retention)
	if err != nil {
		return p, err
	}
	p.MaxWorkRecords, p.MaxWorkBytes, p.MaxDecodedRows, p.MaxOutputBytes, p.MaxRPCs, p.MaxWireBytes = budget.Records, budget.Bytes, budget.DecodedRows, budget.OutputBytes, budget.RPCs, budget.WireBytes
	p.MaxChunks = 2 * frames
	perChunk := uint64(p.ChunkBytes + 128)
	if frames > ((1<<62)-8*nativeColumnMetadataBytes)/(2*perChunk) {
		return p, errLimit
	}
	p.MaxRetainedBytes = 8*nativeColumnMetadataBytes + 2*frames*perChunk
	if p.validate() != nil || !opening.admit(page, p) {
		return p, errLimit
	}
	return p, nil
}

// Complete admitted schedule, not retained-key count/3. Claims register <=3N
// originals (two frozen contenders and at most one priority); stale <=N;
// replacement <=2N; frozen close N; final close <=2N; setup/life seven.
// Each first invocation can append six cross stages plus three capacity-help
// stages and one Prepare retry. The separate frozen Register contributes 3N;
// exact final-close duplicate contributes <=2N Resolve entries. The global64
// extra invocations contribute <=640, never64 per original. Fresh controls are
// bounded by4*(ceil(N/16)+16), and recipient/grant/bootstrap controls by256.
// Four generation owners (old/final x A/B) use at most32 terminal/issuance
// control commands each; live source renewals authenticate existing originals
// and append no new controls. The remaining fixed slack covers actual final
// duplicate/reopen and diagnostic controls, not a promise of unlimited retries.
func nativeGraphEdgeUniqueFrames(s nativeGraphCostSpec) (uint64, error) {
	if nativeGraphEdgeUniqueValidate(s) != nil {
		return 0, errInvalid
	}
	return (9*s.Facts+7)*10 + 3*s.Facts + 2*s.Facts +
		4*((s.Facts+15)/16+16) + 256 + 4*32 + nativeEdgeExtraInvocations*10 + 128, nil
}

// The bounded command envelope is also the family provenance/key recipe used
// by the independent raw oracle. It contains no decision or ownership proof.
func nativeGraphEdgeUniqueCommandBytes(phase string, fact, candidate, ordinal uint64, reopened bool) []byte {
	q := nativeGraphEdgeUniqueCommand{1, nativeGraphEdgeUniqueFamily, phase, fact, candidate, ordinal, reopened}
	body, _ := nativeGraphEdgeUniqueInput(q)
	return body
}
func nativeGraphEdgeUniqueFact(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, errInvalid
	}
	return binary.BigEndian.Uint64(data), nil
}
