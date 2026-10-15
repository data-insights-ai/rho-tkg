package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
)

const (
	nativeEdgeFreeze      = "GEF1"
	nativeEdgeRegister    = "GEX1"
	nativeEdgeRefusal     = "GER1"
	nativeEdgeRecipeBytes = 256 << 10
)

// A disk recipe is an exact frozen business/plan observation, NOT source or
// decision authority. Registration and every YES still traverse real peers,
// Host.Submit, Driver and Stage. At most32 recipes are owned in RAM; the parent
// funds the complete finite on-disk recipe roster before the first capture.
type nativeGraphEdgeUniquePlan struct {
	Version                        uint8
	Scope                          []byte
	Business, Manifest             []byte
	BusinessSHA256, ManifestSHA256 [32]byte
	Calls, Bytes, Rows, OutputUsed int
}

func nativeGraphEdgeUniquePlanShape(command partitionGraphCommand) bool {
	ops := command.request.operations
	return len(ops) == 1 && ops[0].Kind == graphstate.Close || len(ops) == 2 && ops[0].Kind == graphstate.CreateRelationship && ops[1].Kind == graphstate.Set && ops[0].Owner == ops[1].Owner && ops[0].Life == ops[1].Life && ops[1].Name == "p"
}

func nativeGraphEdgeUniquePlanDecode(body []byte, c *crossProcessCoordinator, arena *graphstate.OutputBudget) (nativeGraphEdgeUniquePlan, partitionGraphCommand, error) {
	var plan nativeGraphEdgeUniquePlan
	var command partitionGraphCommand
	if c == nil || c.host == nil || c.host.machine == nil || arena == nil || len(body) == 0 || len(body) > nativeEdgeRecipeBytes {
		return plan, command, errInvalid
	}
	if err := arena.Reserve(8*len(body) + 1024); err != nil {
		return plan, command, outputBudgetFailure(err)
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(&plan); err != nil {
		return plan, command, errors.Join(errInvalid, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return plan, command, errors.Join(errInvalid, err)
	}
	canonical, err := json.Marshal(plan)
	if err != nil || !bytes.Equal(canonical, body) || plan.Version != 1 || plan.BusinessSHA256 != sha256.Sum256(plan.Business) || plan.ManifestSHA256 != sha256.Sum256(plan.Manifest) || plan.Calls < 1 || plan.Bytes < 1 || plan.Rows < 0 || plan.OutputUsed < 1 {
		return plan, command, errInvalid
	}
	scope, err := decodeAllocationScope(plan.Scope)
	if err != nil || scope != c.host.machine.scope() {
		return plan, command, errors.Join(errInvalid, err)
	}
	command, err = decodePartitionGraphCommand(plan.Business, c.host.machine.limits)
	if err != nil || command.request.ns != c.host.machine.ns || !nativeGraphEdgeUniquePlanShape(command) {
		return plan, command, errors.Join(errInvalid, err)
	}
	canonical, err = encodePartitionGraphCommand(command, c.host.machine.limits)
	if err != nil || !bytes.Equal(canonical, plan.Business) {
		return plan, command, errors.Join(errInvalid, err)
	}
	wanted, err := c.business(command.request.id, command.request.operations, command.request.revision.Provenance(), command.request.claims)
	if err != nil {
		return plan, command, err
	}
	if err := c.matchBusiness(plan.Manifest, wanted, arena); err != nil {
		return plan, command, err
	}
	return plan, command, nil
}

// Capture occurs BEFORE ABA through genuine native source sessions. No R, vote
// or coordinator run is manufactured/retained. All sources close on every path.
func nativeGraphEdgeUniqueFreezePlan(ctx context.Context, c *crossProcessCoordinator, business []byte) (wire []byte, err error) {
	if ctx == nil || c == nil || c.host == nil || c.host.machine == nil || c.host.machine.SemanticContractID() != durableColumnGraphSemanticContractID() || len(business) == 0 || len(business) > 64<<10 {
		return nil, errInvalid
	}
	l := c.host.machine.limits
	command, err := decodePartitionGraphCommand(business, l)
	if err != nil || command.request.ns != c.host.machine.ns || !nativeGraphEdgeUniquePlanShape(command) {
		return nil, errors.Join(errInvalid, err)
	}
	canonical, err := encodePartitionGraphCommand(command, l)
	if err != nil || !bytes.Equal(canonical, business) {
		return nil, errors.Join(errInvalid, err)
	}
	output, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		return nil, err
	}
	arena, err := output.Child(min(l.sourceBytes, l.outputBytes))
	if err != nil {
		return nil, err
	}
	initial := arena.Remaining()
	if err := arena.Reserve(1024 + 8*len(business)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	budget := newGraphRemoteBudget(l, arena)
	endpoints, err := c.endpoints(budget)
	if err != nil {
		return nil, err
	}
	sources := make([]*remoteGraphSource, 0, c.host.machine.declaration.Len())
	defer func() {
		for _, source := range sources {
			err = errors.Join(err, source.Close())
		}
		if err != nil {
			wire = nil
		}
	}()
	captured := make([]capturedGraphSource, c.host.machine.declaration.Len())
	for i := range captured {
		part, _ := c.host.machine.declaration.PartitionAt(i)
		source, e := endpoints[part.Partition].openSource(ctx)
		if e != nil {
			return nil, e
		}
		sources = append(sources, source)
		captured[i] = capturedGraphSource{observation: source.info.observation, root: source.info.root, floor: source.info.floor, data: source}
	}
	manifest, err := c.host.planCrossCaptured(ctx, command.request.id, command.request.operations, command.request.revision.Provenance(), command.request.claims, captured, arena, graphstore.PageWork{Records: 8 * len(captured), Bytes: 4096 * len(captured)})
	if err != nil {
		return nil, err
	}
	for _, source := range sources {
		if err := source.Close(); err != nil {
			return nil, err
		}
	}
	sources = nil
	if len(manifest) > 64<<10 {
		return nil, errLimit
	}
	scope, err := encodeAllocationScope(c.host.machine.scope())
	if err != nil {
		return nil, err
	}
	if err := arena.Reserve(2048 + 4*(len(business)+len(manifest)+len(scope))); err != nil {
		return nil, outputBudgetFailure(err)
	}
	budget.mu.Lock()
	plan := nativeGraphEdgeUniquePlan{Version: 1, Scope: scope, Business: business, Manifest: manifest, BusinessSHA256: sha256.Sum256(business), ManifestSHA256: sha256.Sum256(manifest), Calls: budget.calls, Bytes: budget.bytes, Rows: budget.rows, OutputUsed: initial - arena.Remaining()}
	budget.mu.Unlock()
	wire, err = json.Marshal(plan)
	if err == nil && len(wire) > nativeEdgeRecipeBytes {
		return nil, errLimit
	}
	return wire, err
}

// Replay installs only R. A separate ordinary Run invocation must recover that
// actual original and obtain Prepare/Decide/Resolve. An uncertain submit repeats
// this SAME recipe; completed absence authorizes submission, never a decision.
func nativeGraphEdgeUniqueRegisterPlan(ctx context.Context, c *crossProcessCoordinator, body []byte) ([]byte, error) {
	if ctx == nil || c == nil || c.host == nil || c.host.machine == nil || c.host.machine.SemanticContractID() != durableColumnGraphSemanticContractID() {
		return nil, errInvalid
	}
	l := c.host.machine.limits
	output, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		return nil, err
	}
	arena, err := output.Child(min(l.sourceBytes, l.outputBytes))
	if err != nil {
		return nil, err
	}
	plan, _, err := nativeGraphEdgeUniquePlanDecode(body, c, arena)
	if err != nil {
		return nil, err
	}
	m, err := decodeCrossManifest(plan.Manifest, l, arena)
	if err != nil {
		return nil, err
	}
	budget := newGraphRemoteBudget(l, arena)
	endpoints, err := c.endpoints(budget)
	if err != nil {
		return nil, err
	}
	endpoint := endpoints[m.coordinator]
	if endpoint == nil {
		return nil, errInvalid
	}
	prior, err := endpoint.lookup(ctx, m.tx)
	if err != nil {
		return nil, errors.Join(errCrossProcessPending, err)
	}
	if prior.present {
		if prior.status.digest != plan.ManifestSHA256 || !bytes.Equal(prior.manifest, plan.Manifest) {
			return nil, errInvalid
		}
		return encodeCrossStatus(prior.status)
	}
	completion, err := budget.protectCompletion(len(m.parts), len(plan.Manifest))
	if err != nil {
		return nil, err
	}
	tx := crossTransaction{owner: c.host, manifest: plan.Manifest, digest: plan.ManifestSHA256}
	proposal, err := c.host.proposalFromAuthority(crossRegister, m.coordinator, tx, nil, completion)
	if err != nil {
		return nil, err
	}
	status, err := endpoint.submit(ctx, proposal)
	if err != nil {
		return nil, errors.Join(errCrossProcessPending, err)
	}
	if status.tx != m.tx || status.digest != m.digest {
		return nil, errInvalid
	}
	return encodeCrossStatus(status)
}

// A failed pre-registration native plan can return a nonzero resume INTENT.
// It is neither an installed original nor a durable ABORT. Only exact native
// causes, without Pending, are exported by this fresh test profile.
func nativeGraphEdgeUniqueRefusalWire(c *crossProcessCoordinator, business []byte, result crossProcessResult, cause error) ([]byte, error) {
	if c == nil || c.host == nil || c.host.machine == nil || c.host.machine.SemanticContractID() != durableColumnGraphSemanticContractID() || errors.Is(cause, errCrossProcessPending) {
		return nil, nil
	}
	kind := byte(0)
	if errors.Is(cause, graphstate.ErrUniqueOverlap) {
		kind = 1
	} else if errors.Is(cause, graphstate.ErrOwnerValidity) {
		kind = 2
	}
	if kind == 0 {
		return nil, nil
	}
	l := c.host.machine.limits
	command, err := decodePartitionGraphCommand(business, l)
	if err != nil {
		return nil, err
	}
	canonical, err := encodePartitionGraphCommand(command, l)
	if err != nil || !bytes.Equal(canonical, business) || result.decision != (crossStatus{}) {
		return nil, errors.Join(errInvalid, err)
	}
	scope, err := encodeAllocationScope(c.host.machine.scope())
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(business)
	return processEncoding(func(w *boundedWriter) {
		w.add([]byte(nativeEdgeRefusal))
		w.tag(kind)
		w.field(scope)
		w.field(business)
		w.add(hash[:])
		w.add(command.request.id[:])
		w.add(result.resume.tx[:])
		w.add(result.resume.business[:])
	})
}

func nativeGraphEdgeUniqueDecodeRefusal(wire, business []byte, target allocationScope) (byte, error) {
	c := graphCursor{b: wire}
	if !bytes.Equal(c.take(4), []byte(nativeEdgeRefusal)) {
		return 0, errInvalid
	}
	kind := c.tag()
	scope, err := decodeAllocationScope(c.field(allocationScopeBytes))
	got := c.field(64 << 10)
	var hash, intent [32]byte
	copy(hash[:], c.take(32))
	id := c.array()
	tx := c.array()
	copy(intent[:], c.take(32))
	command, de := decodePartitionGraphCommand(business, defaultMaterializerLimits())
	if err != nil || de != nil || c.err != nil || len(c.b) != 0 || kind < 1 || kind > 2 || scope != target || !bytes.Equal(got, business) || hash != sha256.Sum256(business) || id != [16]byte(command.request.id) || tx != crossTransactionID(command.request.ns, command.request.id) {
		return 0, errors.Join(errInvalid, err, de, c.err)
	}
	command.version = 0
	wanted, err := encodePartitionGraphCommand(command, defaultMaterializerLimits())
	if err != nil || intent != sha256.Sum256(wanted) {
		return 0, errors.Join(errInvalid, err)
	}
	return kind, nil
}
