package core

import (
	"errors"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// IntentKind names the mutation a prepared intent carries (ADR-0006 §4.1).
type IntentKind uint8

const (
	// IntentNodeCreate is a genesis node create (incl. §4.1 backfill).
	IntentNodeCreate IntentKind = iota + 1
	// IntentRelCreate is a relationship create. At Lanes:1 the forward and
	// reverse adjacency halves apply together in one WriteBatch (§5.1); the
	// Half field records the day-one half-edge decomposition (§4.1.1).
	IntentRelCreate
	// IntentNodeUpdate is a node property-delta update.
	IntentNodeUpdate
	// IntentRelUpdate is a relationship property-delta update.
	IntentRelUpdate
	// IntentNodeDelete is a cascade node delete.
	IntentNodeDelete
	// IntentRelDelete is a relationship delete.
	IntentRelDelete
)

// HalfKind records the half-edge decomposition of a relationship intent
// (ADR-0006 §4.1.1). At Lanes:1 and Lanes:N both halves apply in the source
// lane's single WriteBatch (§5.1 whole-edge apply); the decomposition is a
// day-one FORMAT decision that unlocks stage-3 cross-process routing without a
// wire break, and is inert (HalfWhole) in stage 1.
type HalfKind uint8

const (
	// HalfWhole means the intent carries a complete entity (every node intent,
	// and every relationship intent applied whole in its source lane).
	HalfWhole HalfKind = iota
	// HalfForward is the content-bearing forward half of a cross-process edge
	// (stage 3 only).
	HalfForward
	// HalfReverse is the derived reverse-index half of a cross-process edge
	// (stage 3 only).
	HalfReverse
)

// IntentRecord is the day-one serializable prepared-intent format (ADR-0006
// §4.1). A prepare thread produces one on the caller thread — validation,
// property-slice construction, snowflake ID allocation, content-hash
// computation, and probe-token label resolution are all PREPARE work; the
// applier owns TxFrom, the LSN, and (for updates) PrevHash. Only the ordering
// key, the prepared payload, and the caller's valid-time claims are set here;
// txFrom / lsn / prevHash are left zero for the applier to stamp.
//
// At Lanes:1 intents pass in-process from a producer session to the single
// applier without ever hitting the wire — the in-process pipeline reuses the
// batch machinery's pendingNode/pendingRel (already an ADR-6 prepared intent).
// IntentRecord is the SERIALIZABLE projection of that intent: it exists so
// Lanes:N and the stage-3 distributed topology are a configuration change, not
// a wire-format break (§4.1 workload-exclusion test). Encode/Decode round-trip
// it; the applier can apply a decoded record just as it applies an in-process
// pendingNode.
type IntentRecord struct {
	// Ordering key (§4.1) — the total-order tuple. At Lanes:1 lane is always 0
	// and seq tracks the applier commit order == LSN order.
	Epoch uint64
	Lane  uint16
	Seq   uint64

	Kind     IntentKind
	Half     HalfKind
	EdgeID   types.RelID
	PeerLane uint16

	// Prepared payload. Labels carries canonical label STRINGS (the content
	// hash keys on strings, so the applier's token re-stamp does not invalidate
	// the precomputed hash — §4.4). TypeName carries the rel type string.
	Labels   []string
	TypeName string
	nodeWire storeutil.NodeWire
	relWire  storeutil.RelWire

	// wireBody is the prepare-side pre-encoded v2 wire of the create payload
	// with a ZERO transaction-time tail (ADR-0006 §4.5, §4.1 "wire encode MINUS
	// temporal tail"). Produced on the producer thread so the applier can patch
	// in its stamped TxFrom/TxTo (PatchWireTemporalTail) instead of a second
	// msgpack pass. Inert at Lanes:1 in-process (the applier still re-encodes via
	// the fallback path); carried day-one so apply-side consumption is a pure
	// store-capability addition, not a wire break. Equivalence is guaranteed by
	// construction: Patch(wireBody, T) == encode(payload with TxFrom=T) — proven
	// in ingest_intent_test.go and storeutil's crown property.
	wireBody []byte

	// UpdateProps carries a node/rel update delta (IntentNodeUpdate/RelUpdate).
	UpdateProps map[string]any
	// TargetID names the update/delete target (node or rel id, as int64).
	TargetID int64

	// BackfillTxFrom is a privileged §4.1 transaction-time override (0 = none).
	BackfillTxFrom types.Instant
}

// intentEnvelope is the msgpack projection of an IntentRecord. The prepared
// content-carrying creates (node, rel) serialize their full wire payload; the
// APPLY-dominant update/delete intents carry only the ordering header + target
// on the wire in stage 1 (their deltas pass in-process, never over the wire at
// Lanes:1).
type intentEnvelope struct {
	Epoch          uint64              `msgpack:"ep"`
	Lane           uint16              `msgpack:"ln"`
	Seq            uint64              `msgpack:"sq"`
	Kind           uint8               `msgpack:"k"`
	Half           uint8               `msgpack:"hf"`
	EdgeID         int64               `msgpack:"eid,omitempty"`
	PeerLane       uint16              `msgpack:"pl,omitempty"`
	Labels         []string            `msgpack:"lbl,omitempty"`
	TypeName       string              `msgpack:"tn,omitempty"`
	NodeWire       *storeutil.NodeWire `msgpack:"nw,omitempty"`
	RelWire        *storeutil.RelWire  `msgpack:"rw,omitempty"`
	WireBody       []byte              `msgpack:"wb,omitempty"`
	TargetID       int64               `msgpack:"tid,omitempty"`
	BackfillTxFrom int64               `msgpack:"bf,omitempty"`
}

// EncodeIntent serializes a prepared intent to msgpack bytes. The encoded form
// is the stage-3 wire projection; at Lanes:1 it is exercised by the round-trip
// tests and the decoded-apply path only.
func EncodeIntent(ir IntentRecord) ([]byte, error) {
	env := intentEnvelope{
		Epoch:          ir.Epoch,
		Lane:           ir.Lane,
		Seq:            ir.Seq,
		Kind:           uint8(ir.Kind),
		Half:           uint8(ir.Half),
		EdgeID:         int64(ir.EdgeID.SnowflakeID()),
		PeerLane:       ir.PeerLane,
		Labels:         ir.Labels,
		TypeName:       ir.TypeName,
		TargetID:       ir.TargetID,
		BackfillTxFrom: int64(ir.BackfillTxFrom),
		WireBody:       ir.wireBody,
	}
	switch ir.Kind {
	case IntentNodeCreate:
		nw := ir.nodeWire
		env.NodeWire = &nw
	case IntentRelCreate:
		rw := ir.relWire
		env.RelWire = &rw
	}
	return msgpack.Marshal(env)
}

// DecodeIntent reconstructs a prepared intent from msgpack bytes. It decodes
// through storeutil.SafeUnmarshal so a hostile/corrupt buffer fails closed with
// store.ErrCorruptWire rather than panicking at the trust boundary.
func DecodeIntent(data []byte) (IntentRecord, error) {
	var env intentEnvelope
	if err := storeutil.SafeUnmarshal(data, &env); err != nil {
		// The intent codec is a trust boundary — a corrupt/hostile buffer fails
		// closed with ErrCorruptWire, uniform with every other WireTo* apply
		// site (lesson 58). SafeUnmarshal already wraps panics/over-deep blobs;
		// wrap the plain decode error here too.
		if errors.Is(err, storepkg.ErrCorruptWire) {
			return IntentRecord{}, err
		}
		return IntentRecord{}, fmt.Errorf("%w: ingest intent decode: %v", storepkg.ErrCorruptWire, err)
	}
	ir := IntentRecord{
		Epoch:          env.Epoch,
		Lane:           env.Lane,
		Seq:            env.Seq,
		Kind:           IntentKind(env.Kind),
		Half:           HalfKind(env.Half),
		EdgeID:         types.RelID(env.EdgeID),
		PeerLane:       env.PeerLane,
		Labels:         env.Labels,
		TypeName:       env.TypeName,
		TargetID:       env.TargetID,
		BackfillTxFrom: types.Instant(env.BackfillTxFrom),
		wireBody:       env.WireBody,
	}
	switch ir.Kind {
	case IntentNodeCreate:
		if env.NodeWire == nil {
			return IntentRecord{}, fmt.Errorf("%w: node-create intent missing payload", storepkg.ErrCorruptWire)
		}
		ir.nodeWire = *env.NodeWire
	case IntentRelCreate:
		if env.RelWire == nil {
			return IntentRecord{}, fmt.Errorf("%w: rel-create intent missing payload", storepkg.ErrCorruptWire)
		}
		ir.relWire = *env.RelWire
	case IntentNodeUpdate, IntentRelUpdate, IntentNodeDelete, IntentRelDelete:
		// APPLY-dominant intents carry only header + target on the wire in
		// stage 1; their deltas pass in-process.
	default:
		return IntentRecord{}, fmt.Errorf("%w: unknown intent kind %d", storepkg.ErrCorruptWire, env.Kind)
	}
	return ir, nil
}

// nodeCreateIntentFrom builds a serializable node-create intent from a prepared
// pendingNode and an ordering header. Used by the wire/codec path; the
// in-process applier consumes the pendingNode directly.
func nodeCreateIntentFrom(hdr intentHeader, pn pendingNode) (IntentRecord, error) {
	// Pre-encode the wire with the queued temporal metadata applied (TxFrom/TxTo
	// zeroed for the patch) so the buffer matches the flush-path encode of the
	// finalized node modulo the tail the applier patches (batch_execute.go stamps
	// pn.temporal.TxFrom then SetTemporal). Operate on a deep copy so the shared
	// pendingNode the in-process applier consumes is untouched.
	wireNode := nodeWithZeroTailTemporal(pn.node, pn.temporal)
	w, err := storeutil.NodeToWireChecked(pn.node)
	if err != nil {
		return IntentRecord{}, err
	}
	bodyWire, err := storeutil.NodeToWireChecked(wireNode)
	if err != nil {
		return IntentRecord{}, err
	}
	body, err := storeutil.PreEncodeNodeWireV2(bodyWire)
	if err != nil {
		return IntentRecord{}, err
	}
	return IntentRecord{
		Epoch:          hdr.epoch,
		Lane:           hdr.lane,
		Seq:            hdr.seq,
		Kind:           IntentNodeCreate,
		Half:           HalfWhole,
		Labels:         append([]string(nil), pn.labels...),
		nodeWire:       w,
		wireBody:       body,
		BackfillTxFrom: pn.backfillTxFrom,
	}, nil
}

// relCreateIntentFrom builds a serializable rel-create intent from a prepared
// pendingRel and an ordering header.
func relCreateIntentFrom(hdr intentHeader, pr pendingRel) (IntentRecord, error) {
	wireRel := relWithZeroTailTemporal(pr.rel, pr.temporal)
	w, err := storeutil.RelToWireChecked(pr.rel)
	if err != nil {
		return IntentRecord{}, err
	}
	bodyWire, err := storeutil.RelToWireChecked(wireRel)
	if err != nil {
		return IntentRecord{}, err
	}
	body, err := storeutil.PreEncodeRelWireV2(bodyWire)
	if err != nil {
		return IntentRecord{}, err
	}
	return IntentRecord{
		Epoch:          hdr.epoch,
		Lane:           hdr.lane,
		Seq:            hdr.seq,
		Kind:           IntentRelCreate,
		Half:           HalfWhole,
		EdgeID:         pr.rel.ID(),
		TypeName:       pr.typeName,
		relWire:        w,
		wireBody:       body,
		BackfillTxFrom: pr.backfillTxFrom,
	}, nil
}

// nodeWithZeroTailTemporal returns a deep copy of n with tm applied but its
// transaction-time tail (TxFrom/TxTo) zeroed — the exact finalized wire state
// modulo the tail the applier patches. tm nil leaves the copy's temporal as-is.
// The copy keeps the shared pendingNode the in-process applier consumes intact.
func nodeWithZeroTailTemporal(n *types.Node, tm *types.TemporalMetadata) *types.Node {
	cp := n.DeepCopy()
	if tm != nil {
		z := *tm
		z.TxFrom = 0
		z.TxTo = 0
		cp.SetTemporal(&z)
	}
	return cp
}

// relWithZeroTailTemporal is the relationship mirror of nodeWithZeroTailTemporal.
func relWithZeroTailTemporal(r *types.Relationship, tm *types.TemporalMetadata) *types.Relationship {
	cp := r.DeepCopy()
	if tm != nil {
		z := *tm
		z.TxFrom = 0
		z.TxTo = 0
		cp.SetTemporal(&z)
	}
	return cp
}

// intentHeader is the (epoch, lane, seq) ordering key assigned in prepare.
type intentHeader struct {
	epoch uint64
	lane  uint16
	seq   uint64
}

// toPendingNode reconstructs the applier's in-process prepared intent from a
// (possibly decoded) node-create IntentRecord. The carried content hash keys on
// the label STRINGS, so re-deriving probe tokens here does not invalidate it
// (§4.4). The applier re-stamps the real tokens and TxFrom at commit.
func (ir IntentRecord) toPendingNode(c *Core) (pendingNode, error) {
	if ir.Kind != IntentNodeCreate {
		return pendingNode{}, fmt.Errorf("ingest: intent kind %d is not a node create", ir.Kind)
	}
	n, err := storeutil.WireToNodeChecked(ir.nodeWire)
	if err != nil {
		return pendingNode{}, err
	}
	labelTokens, canonicalLabels, err := c.existingLabelsOrNextProbeTokens(ir.Labels)
	if err != nil {
		return pendingNode{}, fmt.Errorf("ingest: label tokens: %w", err)
	}
	n.SetLabelTokensRaw(labelTokens.primary, labelTokens.extras)
	ig := n.Integrity()
	tm := n.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
	}
	// TxFrom is applier-owned; clear any carried value so the applier stamps it.
	tm.TxFrom = 0
	n.SetTemporal(tm)
	return pendingNode{
		node:               n,
		result:             nil,
		labels:             canonicalLabels,
		queuedPrimaryToken: labelTokens.primary,
		queuedExtraTokens:  append([]uint16(nil), labelTokens.extras...),
		nodeIntegrity:      ig,
		temporal:           tm,
		backfillTxFrom:     ir.BackfillTxFrom,
	}, nil
}
