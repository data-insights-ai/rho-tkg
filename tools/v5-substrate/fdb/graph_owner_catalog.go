package main

// Shared-KV logical bucket ownership. A bucket is a source group, NOT an SDK
// process or FDB replica. Moving its processing authority leaves source keys,
// immutable transaction epochs, cuts, CDC and retention lineage unchanged.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"slices"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphOwnerNamespace = "graph-owned-v1-"
const graphOwnerRecordBytes = 64 << 10

var errOwnerBusy = errors.New("fdb-spike: logical bucket handoff busy")

// Private application-control admission, not a native FDB/page/RSS bound.
// Every retained record reserves its maximum canonical width; no control GC,
// capacity qualification or automatic grant enlargement is implied.
type graphOwnerPolicy struct{ MaxRecords, MaxControlBytes uint64 }
type graphOwnerAuthority struct {
	ID     string
	Secret [32]byte
}
type graphOwnerPrincipal struct {
	ID         string
	SecretHash [32]byte
}
type graphOwnerRoute struct {
	Owner string
	Epoch uint64
}
type graphOwnerBinding struct {
	Version     uint8
	Config      config
	Source      graphHistoryBinding
	Authorities [2]graphOwnerPrincipal
	Policy      graphOwnerPolicy
	Transfer    *graphOwnerTransferPolicy `json:",omitempty"`
}
type graphOwnerState struct {
	Version uint64
	Routes  [2]graphOwnerRoute
	Moving  string
}
type graphOwnerLedger struct{ Records, Bytes uint64 }
type graphOwnerCatalog struct {
	h       *harness
	binding graphOwnerBinding
}
type graphOwnerView struct {
	Binding graphOwnerBinding
	State   graphOwnerState
	Records [2]graphCostStoredRecord
}

// Observe returns actual native routing bytes. A later driver must send each
// bucket's work to the process registered for this Owner, not rename a source
// SDK client. The execution gate rechecks this observation transactionally.
func (c *graphOwnerCatalog) Observe(ctx context.Context) (graphOwnerView, int, error) {
	if c == nil || ctx == nil {
		return graphOwnerView{}, 0, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return graphOwnerView{}, 0, err
	}
	ctx, cancel, err := c.operationContext(ctx)
	if err != nil {
		return graphOwnerView{}, 0, err
	}
	defer cancel()
	return graphCostParallelRead(ctx, c.h.db, func(tr fdb.ReadTransaction) (graphOwnerView, error) {
		actual, err := c.h.ownerCatalogIn(tr)
		if err != nil {
			return graphOwnerView{}, err
		}
		if !graphColumnSame(actual.binding, c.binding) {
			return graphOwnerView{}, errMismatch
		}
		state, err := actual.stateIn(tr)
		if err != nil {
			return graphOwnerView{}, err
		}
		view := graphOwnerView{Binding: actual.binding, State: state}
		for i, key := range []fdb.Key{c.key("binding", "immutable"), c.key("state", "current")} {
			raw, err := tr.Get(key).Get()
			if err != nil {
				return graphOwnerView{}, err
			}
			if raw == nil || len(raw) > graphOwnerRecordBytes {
				return graphOwnerView{}, errUnavailable
			}
			view.Records[i] = graphCostStoredRecord{bytes.Clone(key), bytes.Clone(raw)}
		}
		return view, nil
	})
}

func graphOwnerSelected(c config) bool {
	return strings.HasPrefix(c.Graph, graphFeedRetainedNamespace) || strings.HasPrefix(c.Graph, graphOwnerNamespace) || strings.HasPrefix(c.Graph, graphOwnerTransferNamespace) || strings.HasPrefix(c.Graph, graphOwnerTransportNamespace)
}
func (p graphOwnerPolicy) validate() error {
	if p.MaxRecords < 8 || p.MaxRecords > 4096 || p.MaxControlBytes < 8*graphOwnerRecordBytes || p.MaxControlBytes > 256<<20 {
		return errInvalid
	}
	return nil
}
func (b graphOwnerBinding) validate() error {
	legacy := b.Version == 1 && b.Transfer == nil && strings.HasPrefix(b.Config.Graph, graphOwnerNamespace) && b.Config.Cost.Version == 1 && b.Config.Cost.Writers == 1 && b.Policy.validate() == nil
	fresh := b.Version == 2 && b.Transfer != nil && b.Transfer.validate() == nil && b.Transfer.Config == b.Config && b.Policy == (graphOwnerPolicy{b.Transfer.MaxRecords, b.Transfer.MaxControlBytes})
	if (!legacy && !fresh) || !validGraphCostProfile(b.Config) || b.Source.Config != b.Config || b.Source.Version != 1 || b.Source.Policy.validate() != nil || !slices.Equal(b.Source.Initial.Scope, []uint8{0, 1}) || b.Source.Initial.Groups != [2]uint64{} {
		return errInvalid
	}
	for i, a := range b.Authorities {
		if !validName(a.ID) || a.SecretHash == [32]byte{} || i == 1 && a.ID == b.Authorities[0].ID {
			return errInvalid
		}
	}
	return nil
}
func (c *graphOwnerCatalog) key(kind, request string) fdb.Key {
	return c.h.key(0, "owner-catalog-v1", kind+"/"+request)
}
func (c *graphOwnerCatalog) authenticate(a graphOwnerAuthority) error {
	if a.Secret == [32]byte{} {
		return errStale
	}
	for _, p := range c.binding.Authorities {
		if p.ID == a.ID && historySecret(p.SecretHash, sha256.Sum256(a.Secret[:])) {
			return nil
		}
	}
	return errStale
}
func (c *graphOwnerCatalog) stateIn(tr fdb.ReadTransaction) (graphOwnerState, error) {
	s, found, err := get[graphOwnerState](tr, c.key("state", "current"))
	if err != nil {
		return s, err
	}
	if !found || s.Version == 0 || s.Moving != "" && !validName(s.Moving) {
		return s, errUnavailable
	}
	for _, route := range s.Routes {
		known := false
		for _, a := range c.binding.Authorities {
			known = known || route.Owner == a.ID
		}
		if !known || route.Epoch == 0 || route.Epoch > s.Version {
			return s, errMismatch
		}
	}
	return s, nil
}
func (c *graphOwnerCatalog) createIn(tr fdb.Transaction, key fdb.Key, v any) error {
	body, err := wire(v)
	if err != nil {
		return err
	}
	prior, err := tr.Get(key).Get()
	if err != nil {
		return err
	}
	if prior != nil {
		if !bytes.Equal(prior, body) {
			return errMismatch
		}
		return nil
	}
	l, found, err := get[graphOwnerLedger](tr, c.key("ledger", "controls"))
	if err != nil {
		return err
	}
	charge := c.recordCharge(key, body, v)
	p := c.binding.Policy
	if !found || l.Records >= p.MaxRecords || l.Bytes > p.MaxControlBytes || charge > p.MaxControlBytes-l.Bytes {
		return errLimit
	}
	l.Records++
	l.Bytes += charge // Mutable successors are prepaid at maximum encoded width.
	tr.Set(key, body)
	return put(tr, c.key("ledger", "controls"), l)
}
func (h *harness) ownerCatalogIn(tr fdb.ReadTransaction) (*graphOwnerCatalog, error) {
	if !graphOwnerSelected(h.config) {
		return nil, nil // Historical namespaces perform no extra native reads.
	}
	c := &graphOwnerCatalog{h: h}
	b, found, err := get[graphOwnerBinding](tr, c.key("binding", "immutable"))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errUnavailable
	}
	if b.validate() != nil || b.Config != h.config {
		return nil, errMismatch
	}
	source, err := h.historyRetentionIn(tr)
	if err != nil {
		return nil, err
	}
	if source == nil || !graphColumnSame(source.binding, b.Source) {
		return nil, errMismatch
	}
	c.binding = b
	if err := c.transportIn(tr); err != nil {
		return nil, err
	}
	if _, err := c.stateIn(tr); err != nil {
		return nil, err
	}
	l, found, err := get[graphOwnerLedger](tr, c.key("ledger", "controls"))
	if err != nil {
		return nil, err
	}
	if !found || l.Records < 3 || l.Records > b.Policy.MaxRecords || l.Bytes > b.Policy.MaxControlBytes {
		return nil, errUnavailable
	}
	return c, nil
}

// Installation binds the authentic source retention record in the SAME native
// commit, before any business original exists. It is not a later local claim
// about an earlier observation. Secrets are supplied by the caller's factory.
func newGraphOwnerCatalog(ctx context.Context, source *graphHistoryRetention, authorities [2]graphOwnerAuthority, policy graphOwnerPolicy) (*graphOwnerCatalog, error) {
	return installGraphOwnerCatalog(ctx, source, authorities, policy, nil)
}

func newGraphOwnerCatalogTransfer(ctx context.Context, source *graphHistoryRetention, authorities [2]graphOwnerAuthority, policy graphOwnerTransferPolicy) (*graphOwnerCatalog, error) {
	if policy.validate() != nil {
		return nil, errInvalid
	}
	return installGraphOwnerCatalog(ctx, source, authorities, graphOwnerPolicy{policy.MaxRecords, policy.MaxControlBytes}, &policy)
}

func installGraphOwnerCatalog(ctx context.Context, source *graphHistoryRetention, authorities [2]graphOwnerAuthority, policy graphOwnerPolicy, transfer *graphOwnerTransferPolicy) (*graphOwnerCatalog, error) {
	if ctx == nil || source == nil || source.h == nil {
		return nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c := &graphOwnerCatalog{h: source.h, binding: graphOwnerBinding{Version: 1, Config: source.h.config, Source: source.binding, Policy: policy}}
	if transfer != nil {
		c.binding.Version, c.binding.Transfer = 2, transfer
	}
	for i, a := range authorities {
		if a.Secret == [32]byte{} {
			return nil, errInvalid
		}
		c.binding.Authorities[i] = graphOwnerPrincipal{a.ID, sha256.Sum256(a.Secret[:])}
	}
	if err := c.binding.validate(); err != nil {
		return nil, err
	}
	body, err := wire(c.binding)
	if err != nil {
		return nil, err
	}
	c.binding, err = decode[graphOwnerBinding](body)
	if err != nil {
		return nil, err
	}
	ctx, cancel, err := c.operationContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	_, _, err = historyTransactContext(ctx, c.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		if err := c.transportIn(tr); err != nil {
			return false, err
		}
		actual, err := c.h.historyRetentionIn(tr)
		if err != nil {
			return false, err
		}
		if actual == nil || !graphColumnSame(actual.binding, c.binding.Source) {
			return false, errMismatch
		}
		old, found, err := get[graphOwnerBinding](tr, c.key("binding", "immutable"))
		if err != nil {
			return false, err
		}
		if found {
			if !graphColumnSame(old, c.binding) {
				return false, errMismatch
			}
			_, err := c.h.ownerCatalogIn(tr)
			return true, err
		}
		for g := uint8(0); g < 2; g++ {
			m, err := c.h.meta(tr, g)
			if err != nil {
				return false, err
			}
			if m.Transactions != 0 || m.Groups != 0 || m.Versions != 0 {
				return false, errUnavailable
			}
		}
		rows, err := tr.GetRange(prefixRange(c.h.prefix(0)+"owner-catalog-v1/"), fdb.RangeOptions{Limit: 1}).GetSliceWithError()
		if err != nil || len(rows) != 0 {
			return false, errors.Join(errUnavailable, err)
		}
		initial := graphOwnerState{Version: 1, Routes: [2]graphOwnerRoute{{authorities[0].ID, 1}, {authorities[1].ID, 1}}}
		reserved := uint64(len(c.key("binding", "immutable")) + len(c.key("state", "current")) + len(c.key("ledger", "controls")) + 3*graphOwnerRecordBytes)
		if reserved > policy.MaxControlBytes {
			return false, errLimit
		}
		for _, record := range []struct {
			key   fdb.Key
			value any
		}{{c.key("binding", "immutable"), c.binding}, {c.key("state", "current"), initial}, {c.key("ledger", "controls"), graphOwnerLedger{3, reserved}}} {
			if err := put(tr, record.key, record.value); err != nil {
				return false, err
			}
		}
		if c.binding.Version == 2 {
			if err := c.createIn(tr, c.key("allocation-state", "current"), graphOwnerAllocationState{Next: 1}); err != nil {
				return false, err
			}
		}
		return true, nil
	})
	if err != nil {
		return nil, err // Unknown installation recovers with the same binding.
	}
	actual, _, err := graphCostParallelRead(ctx, c.h.db, c.h.ownerCatalogIn)
	if err == nil && (actual == nil || !graphColumnSame(actual.binding, c.binding)) {
		return nil, errMismatch
	}
	return actual, err
}

// New cuts cannot change a frozen handoff boundary. Initial empty certificates
// are needed to install source retention BEFORE catalog publication.
func (h *harness) ownerPublicationIn(tr fdb.ReadTransaction) error {
	if !graphOwnerSelected(h.config) {
		return nil
	}
	c := &graphOwnerCatalog{h: h}
	_, found, err := get[graphOwnerBinding](tr, c.key("binding", "immutable"))
	if err != nil {
		return err
	}
	if !found {
		for g := uint8(0); g < 2; g++ {
			m, err := h.meta(tr, g)
			if err != nil {
				return err
			}
			if m.Transactions != 0 || m.Groups != 0 || m.Versions != 0 {
				return errUnavailable
			}
		}
		return nil
	}
	c, err = h.ownerCatalogIn(tr)
	if err != nil {
		return err
	}
	s, err := c.stateIn(tr)
	if err == nil && s.Moving != "" {
		return errOwnerBusy
	}
	return err
}

type graphOwnerMove struct {
	Request  string
	Bucket   uint8
	From, To string
	Before   graphOwnerState
	Owner    graphHistoryOwner
}
type graphOwnerMoveProgress struct {
	Phase    uint8 // 0 reserved; 1 frozen; 2 activated; 3 canceled.
	Cut      certificate
	Metadata [2]metadata
	Census   string `json:",omitempty"`
}

func (m graphOwnerMove) validate() error {
	if !validName(m.Request) || m.Bucket > 1 || !validName(m.From) || !validName(m.To) || m.From == m.To || m.Before.Version == 0 || m.Before.Version == math.MaxUint64 || m.Before.Moving != "" || m.Before.Routes[m.Bucket].Owner != m.From || m.Before.Routes[m.Bucket].Epoch == 0 || m.Before.Routes[m.Bucket].Epoch == math.MaxUint64 || !historyOwnerValid(m.Owner) || m.Owner.Kind != "read" || m.Owner.State != historyActive || !slices.Equal(m.Owner.Cut.Scope, []uint8{0, 1}) || !slices.Equal(m.Owner.Required.Scope, []uint8{0, 1}) {
		return errInvalid
	}
	return nil
}

func graphOwnerMoveInput(move graphOwnerMove) (graphOwnerMove, error) {
	if err := move.validate(); err != nil {
		return graphOwnerMove{}, err
	}
	body, err := wire(move)
	if err != nil {
		return graphOwnerMove{}, err
	}
	return decode[graphOwnerMove](body)
}

// Reserve persists the exact original and owner identity before acquisition.
// route is the caller's earlier capture, not a route fetched here on its behalf;
// native equality fences even a delayed never-installed reservation after ABA.
// This finite core references durable shared-KV base/tail; it does NOT prove a
// complete handoff census, allocator high-water, process restart or feed replay.
func (c *graphOwnerCatalog) ReserveMove(ctx context.Context, a graphOwnerAuthority, route graphOwnerRoute, request string, secret [32]byte, bucket uint8, target string, at cut) (graphOwnerMove, error) {
	if c == nil || ctx == nil || !validName(request) || secret == [32]byte{} || bucket > 1 || !validName(target) || !slices.Equal(at.record.Scope, []uint8{0, 1}) {
		return graphOwnerMove{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return graphOwnerMove{}, err
	}
	ctx, cancel, err := c.operationContext(ctx)
	if err != nil {
		return graphOwnerMove{}, err
	}
	defer cancel()
	at.record.Scope = slices.Clone(at.record.Scope)
	returnValue, _, err := historyTransactContext(ctx, c.h.db, func() error { return nil }, func(tr fdb.Transaction) (graphOwnerMove, error) {
		actual, err := c.h.ownerCatalogIn(tr)
		if err != nil {
			return graphOwnerMove{}, err
		}
		if err := actual.authenticate(a); err != nil {
			return graphOwnerMove{}, err
		}
		state, err := c.stateIn(tr)
		if err != nil {
			return graphOwnerMove{}, err
		}
		if err := graphOwnerCurrent(state, a, bucket, route); err != nil {
			return graphOwnerMove{}, err
		}
		prior, exists, err := get[graphOwnerMove](tr, c.key("move", request))
		if err != nil {
			return prior, err
		}
		if exists {
			if err := prior.validate(); err != nil {
				return prior, err
			}
			if state.Routes[bucket] != prior.Before.Routes[bucket] {
				return prior, errStale
			}
			if prior.Bucket != bucket || prior.From != a.ID || prior.To != target || !historySecret(prior.Owner.Secret, secret) || !sameCertificate(prior.Owner.Cut, at.record) {
				return prior, errMismatch
			}
			return prior, nil
		}
		known := false
		for _, principal := range c.binding.Authorities {
			known = known || principal.ID == target
		}
		if !known || target == a.ID || state.Routes[bucket].Owner != a.ID {
			return graphOwnerMove{}, errStale
		}
		if state.Moving != "" {
			return graphOwnerMove{}, errOwnerBusy
		}
		if state.Version == math.MaxUint64 || state.Routes[bucket].Epoch == math.MaxUint64 {
			return graphOwnerMove{}, errLimit
		}
		source, err := c.h.historyRetentionIn(tr)
		if err != nil {
			return graphOwnerMove{}, err
		}
		required, err := source.readRequiredIn(tr, at)
		if err != nil {
			return graphOwnerMove{}, err
		}
		if err := c.h.verifyCutIdentity(tr, at); err != nil {
			return graphOwnerMove{}, err
		}
		owner := graphHistoryOwner{Kind: "read", Request: "owner-move-" + request, Secret: secret, Cut: at.record, Required: required, State: historyActive}
		if !historyOwnerValid(owner) {
			return graphOwnerMove{}, errInvalid
		}
		move := graphOwnerMove{request, bucket, a.ID, target, state, owner}
		if err := move.validate(); err != nil {
			return graphOwnerMove{}, err
		}
		if err := c.createIn(tr, c.key("move", request), move); err != nil {
			return graphOwnerMove{}, err
		}
		if err := c.reserveCleanupIn(tr, "move/"+request); err != nil {
			return graphOwnerMove{}, err
		}
		return move, c.createIn(tr, c.key("move-progress", request), graphOwnerMoveProgress{})
	})
	return returnValue, err
}

func (c *graphOwnerCatalog) moveIn(tr fdb.ReadTransaction, move graphOwnerMove) (graphOwnerMoveProgress, error) {
	if err := move.validate(); err != nil {
		return graphOwnerMoveProgress{}, err
	}
	actual, found, err := get[graphOwnerMove](tr, c.key("move", move.Request))
	if err != nil {
		return graphOwnerMoveProgress{}, err
	}
	if !found || !graphColumnSame(actual, move) {
		return graphOwnerMoveProgress{}, errMismatch
	}
	p, found, err := get[graphOwnerMoveProgress](tr, c.key("move-progress", move.Request))
	if err != nil {
		return p, err
	}
	if !found || p.Phase > 3 {
		return p, errUnavailable
	}
	return p, nil
}

func (c *graphOwnerCatalog) heldIn(tr fdb.ReadTransaction, move graphOwnerMove, progress graphOwnerMoveProgress) (*graphHistoryRetention, error) {
	source, err := c.h.historyRetentionIn(tr)
	if err != nil {
		return nil, err
	}
	owner, found, err := source.owner(tr, move.Owner.Request)
	if err != nil {
		return nil, err
	}
	if !found || !graphColumnSame(owner, move.Owner) {
		return nil, errUnavailable
	}
	if err := c.h.verifyCutIdentity(tr, cut{progress.Cut}); err != nil {
		return nil, err
	}
	for g := uint8(0); g < 2; g++ {
		m, err := c.h.meta(tr, g)
		if err != nil {
			return nil, err
		}
		if m != progress.Metadata[g] {
			return nil, errMismatch
		}
	}
	return source, nil
}

// Freeze uses a conservative whole-graph NEW-write/publication barrier while
// changing one bucket's owner. Native catalog read conflicts ensure an in-flight
// commit either precedes this actual boundary or retries the SAME
// original against busy/stale authority. No prepared records are invented.
func (c *graphOwnerCatalog) Freeze(ctx context.Context, a graphOwnerAuthority, move graphOwnerMove) (graphOwnerMoveProgress, error) {
	if c == nil || ctx == nil {
		return graphOwnerMoveProgress{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return graphOwnerMoveProgress{}, err
	}
	ctx, cancel, err := c.operationContext(ctx)
	if err != nil {
		return graphOwnerMoveProgress{}, err
	}
	defer cancel()
	owned, err := graphOwnerMoveInput(move)
	if err != nil {
		return graphOwnerMoveProgress{}, err
	}
	move = owned
	p, _, err := historyTransactContext(ctx, c.h.db, func() error { return nil }, func(tr fdb.Transaction) (graphOwnerMoveProgress, error) {
		actual, err := c.h.ownerCatalogIn(tr)
		if err != nil {
			return graphOwnerMoveProgress{}, err
		}
		if err := actual.authenticate(a); err != nil {
			return graphOwnerMoveProgress{}, err
		}
		progress, err := c.moveIn(tr, move)
		if err != nil {
			return progress, err
		}
		state, err := c.stateIn(tr)
		if err != nil {
			return progress, err
		}
		if a.ID != move.From || state.Routes[move.Bucket].Owner != a.ID {
			return progress, errStale
		}
		if progress.Phase == 1 && state.Moving == move.Request {
			_, err := c.heldIn(tr, move, progress)
			return progress, err
		}
		if progress.Phase != 0 || state != move.Before {
			return progress, errStale
		}
		if err := c.allocationDrainIn(tr, move.Bucket, move.Before.Routes[move.Bucket]); err != nil {
			return progress, err
		}
		boundary, err := c.h.freshIn(tr, []uint8{0, 1}, nil)
		if err != nil {
			return progress, err
		}
		source, err := c.h.historyRetentionIn(tr)
		if err != nil {
			return progress, err
		}
		required, err := source.readRequiredIn(tr, cut{move.Owner.Cut})
		if err != nil {
			return progress, err
		}
		if !sameCertificate(required, move.Owner.Required) {
			return progress, errHistoryExpired
		}
		if err := source.acquireIn(tr, move.Owner); err != nil {
			return progress, err
		}
		progress.Phase, progress.Cut = 1, boundary.(cut).record
		for g := uint8(0); g < 2; g++ {
			progress.Metadata[g], err = c.h.meta(tr, g)
			if err != nil {
				return graphOwnerMoveProgress{}, err
			}
		}
		state.Moving = move.Request
		if err := put(tr, c.key("move-progress", move.Request), progress); err != nil {
			return progress, err
		}
		return progress, put(tr, c.key("state", "current"), state)
	})
	return p, err
}

// Activate requires the actual target's registered authority, the immutable
// predecessor reservation, its genuine held source owner and frozen metadata.
// Mutable feed/pin controls stay at the SAME stable native keys; no copied
// client hash grants lineage. Full workload census remains a separate gate.
func (c *graphOwnerCatalog) Activate(ctx context.Context, target graphOwnerAuthority, move graphOwnerMove) (graphOwnerState, error) {
	if c == nil || ctx == nil {
		return graphOwnerState{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return graphOwnerState{}, err
	}
	ctx, cancel, err := c.operationContext(ctx)
	if err != nil {
		return graphOwnerState{}, err
	}
	defer cancel()
	owned, err := graphOwnerMoveInput(move)
	if err != nil {
		return graphOwnerState{}, err
	}
	move = owned
	state, _, err := historyTransactContext(ctx, c.h.db, func() error { return nil }, func(tr fdb.Transaction) (graphOwnerState, error) {
		actual, err := c.h.ownerCatalogIn(tr)
		if err != nil {
			return graphOwnerState{}, err
		}
		if err := actual.authenticate(target); err != nil {
			return graphOwnerState{}, err
		}
		if target.ID != move.To {
			return graphOwnerState{}, errStale
		}
		progress, err := c.moveIn(tr, move)
		if err != nil {
			return graphOwnerState{}, err
		}
		state, err := c.stateIn(tr)
		if err != nil {
			return state, err
		}
		expected := move.Before
		expected.Version++
		expected.Routes[move.Bucket] = graphOwnerRoute{move.To, move.Before.Routes[move.Bucket].Epoch + 1}
		if progress.Phase == 2 {
			// Later moves must not make this exact activation ACK claim that its
			// older route is current. Its retained move result is still inspectable.
			if state != expected {
				return state, errStale
			}
			return state, nil
		}
		if progress.Phase != 1 || state.Moving != move.Request || state.Version != move.Before.Version || state.Routes != move.Before.Routes {
			return state, errStale
		}
		if graphOwnerTransferCensus(c.binding.Transfer) {
			if err := c.censusActivateIn(tr, move, progress, expected); err != nil {
				return state, err
			}
		}
		source, err := c.heldIn(tr, move, progress)
		if err != nil {
			return state, err
		}
		if err := source.terminateIn(tr, move.Owner.Request, move.Owner.Secret, historyReleased, move.Request); err != nil {
			return state, err
		}
		progress.Phase = 2
		if err := put(tr, c.key("move-progress", move.Request), progress); err != nil {
			return state, err
		}
		return expected, put(tr, c.key("state", "current"), expected)
	})
	return state, err
}

// Cancel releases a genuinely acquired source owner only with the predecessor's
// retained intent and registered authority. All mutable records were funded at
// ReserveMove, so normal intent admission cannot spend this terminal lane.
// No timeout, process death or missing original authorizes this operation.
func (c *graphOwnerCatalog) Cancel(ctx context.Context, a graphOwnerAuthority, move graphOwnerMove) error {
	if c == nil || ctx == nil {
		return errInvalid
	}
	if c.binding.Transfer == nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	owned, err := graphOwnerMoveInput(move)
	if err != nil {
		return err
	}
	move = owned
	ctx, cancel, err := c.terminalContext(ctx, "move/"+move.Request, func(tr fdb.ReadTransaction) error {
		actual, err := c.h.ownerCatalogIn(tr)
		if err != nil {
			return err
		}
		if err := actual.authenticate(a); err != nil {
			return err
		}
		if a.ID != move.From {
			return errStale
		}
		_, err = c.moveIn(tr, move)
		return err
	})
	if err != nil {
		return err
	}
	defer cancel()
	_, _, err = historyTransactContext(ctx, c.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		actual, err := c.h.ownerCatalogIn(tr)
		if err != nil {
			return false, err
		}
		if err := actual.authenticate(a); err != nil {
			return false, err
		}
		if a.ID != move.From {
			return false, errStale
		}
		p, err := c.moveIn(tr, move)
		if err != nil {
			return false, err
		}
		state, err := c.stateIn(tr)
		if err != nil {
			return false, err
		}
		if state.Routes[move.Bucket].Owner != a.ID {
			return false, errStale
		}
		if p.Phase == 3 {
			return true, nil
		}
		if p.Phase > 1 || state.Version != move.Before.Version || state.Routes != move.Before.Routes || p.Phase == 0 && state.Moving != "" || p.Phase == 1 && state.Moving != move.Request {
			return false, errStale
		}
		if p.Phase == 1 {
			source, err := c.heldIn(tr, move, p)
			if err != nil {
				return false, err
			}
			if err := source.terminateIn(tr, move.Owner.Request, move.Owner.Secret, historyReleased, move.Request); err != nil {
				return false, err
			}
		}
		state.Version++
		state.Moving = ""
		p.Phase = 3
		if err := put(tr, c.key("move-progress", move.Request), p); err != nil {
			return false, err
		}
		return true, put(tr, c.key("state", "current"), state)
	})
	return err
}
