package graphapply

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"reflect"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

var errGraphIdentityCollision = errors.New("graphapply: fresh identity collision")

// No graph, group or default-axis identity enters the supported creation door.
// This finite profile uses the existing fixed-three membership and command
// agreements. Schema/default-axis/routing/maxBlock are candidate Init choices:
// the pending seed persists identity/membership/declaration only; the actual
// committed Init is their immutable configuration authority. It introduces
// neither a transport nor a new storage format.
type logicalGraphCreationSpec struct {
	partitions     []graphstore.PartitionOwnership // Group must be zero.
	topology       uint64
	voters         [3]uint64
	routingVersion uint64
	bucketBits     uint8
	buckets        []graphstore.BucketOwnership
	schemas        []graphstate.PropertyDefinition
	maxBlock       uint64
	agreement      raftlog.ApplicationSemanticContractID
	limits         materializerLimits
}

type initialGraphPeer struct{ partition, voter uint64 }

// The embedding authenticates the INITIAL provisioning association, not fields
// reported by a received body. It dispatches only this live creator's fresh
// description to the selected peer, and returns nil after that peer synced its
// seed through provisionInitialGraphReplica. Network/pipe implementations may
// use their existing transport. Replaying descriptions to lost disks is not
// initial provisioning: it requires surviving-quorum replacement authority.
type initialGraphProvisioner interface {
	ProvisionInitialGraph(context.Context, initialGraphPeer, []byte) error
}

// This is owned, inert configuration, not a ready graph, commit receipt, quorum
// capability or snapshot. Voters/meaning are out-of-band authenticated settings;
// the body uses the existing canonical declared-init codec as a description.
// Successful provisioning does not durably bind its candidate Init choices.
type provisionedLogicalGraph struct {
	genesis   []byte
	voters    [3]uint64
	agreement raftlog.ApplicationSemanticContractID
}

func createLogicalGraph(ctx context.Context, spec logicalGraphCreationSpec, p initialGraphProvisioner) (provisionedLogicalGraph, error) {
	return createLogicalGraphWithEntropy(ctx, spec, p, rand.Reader)
}

func createLogicalGraphWithEntropy(ctx context.Context, spec logicalGraphCreationSpec, p initialGraphProvisioner, entropy io.Reader) (provisionedLogicalGraph, error) {
	if ctx == nil || isNilInitialProvisioner(p) || entropy == nil {
		return provisionedLogicalGraph{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return provisionedLogicalGraph{}, err
	}
	if err := spec.limits.validate(); err != nil {
		return provisionedLogicalGraph{}, err
	}
	if !partitionGraphAgreement(spec.agreement) || spec.voters[0] == 0 || spec.voters[1] <= spec.voters[0] || spec.voters[2] <= spec.voters[1] || len(spec.partitions) == 0 || len(spec.partitions) > 16 || len(spec.schemas) > 128 {
		return provisionedLogicalGraph{}, errInvalid
	}
	// Admission precedes the small bounded copies and entropy reads. None of the
	// captured descriptors share caller-mutable slice backing during dispatch.
	partitions := slices.Clone(spec.partitions)
	for _, part := range partitions {
		if part.Group != ([16]byte{}) {
			return provisionedLogicalGraph{}, errInvalid
		}
	}
	seen := make(map[[16]byte]struct{}, len(partitions)+2)
	mint := func() ([16]byte, error) {
		var id [16]byte
		if _, err := io.ReadFull(entropy, id[:]); err != nil {
			return id, err
		}
		if id == ([16]byte{}) {
			return id, errInvalid
		}
		if _, exists := seen[id]; exists {
			return id, errGraphIdentityCollision
		}
		seen[id] = struct{}{}
		return id, nil
	}
	graph, err := mint()
	if err != nil {
		return provisionedLogicalGraph{}, err
	}
	axis, err := mint()
	if err != nil {
		return provisionedLogicalGraph{}, err
	}
	for i := range partitions {
		partitions[i].Group, err = mint()
		if err != nil {
			return provisionedLogicalGraph{}, err
		}
	}
	d, err := graphstore.NewOwnershipDeclaration(graphstate.GraphID(graph), spec.topology, partitions, spec.limits.catalog)
	if err != nil {
		return provisionedLogicalGraph{}, err
	}
	binding, err := types.NewDefaultAxisBinding(types.GraphID(graph), temporal.AxisID(axis), spec.limits.catalog.Temporal)
	if err != nil {
		return provisionedLogicalGraph{}, err
	}
	routing, err := graphstore.NewPartitionRouting(d, spec.routingVersion, spec.bucketBits, spec.buckets, spec.limits.catalog)
	if err != nil {
		return provisionedLogicalGraph{}, err
	}
	first, _ := d.PartitionAt(0)
	// Placeholder command identity/authority are configuration framing only.
	// Actual Init is produced by the HOME Host and committed by its Driver.
	authority, err := idalloc.NewAuthority(graph, 1)
	if err != nil {
		return provisionedLogicalGraph{}, err
	}
	r := declaredInitCommand{ns: namespace{graph: idalloc.GraphID(graph), partition: first.Partition}, attempt: bootstrapAttemptID(graph), declaration: d, authority: authority, maxBlock: spec.maxBlock, schemas: slices.Clone(spec.schemas), defaultAxis: binding, routing: new(routing)}
	wire, err := encodeDeclaredInit(r, spec.limits)
	if err != nil {
		return provisionedLogicalGraph{}, err
	}
	for i := range d.Len() {
		part, _ := d.PartitionAt(i)
		for _, voter := range spec.voters {
			if err := ctx.Err(); err != nil {
				return provisionedLogicalGraph{}, err
			}
			// Independent borrowed-description ownership protects later peers and
			// the returned descriptor from an embedding callback's scratch reuse.
			if err := p.ProvisionInitialGraph(ctx, initialGraphPeer{part.Partition, voter}, slices.Clone(wire)); err != nil {
				// No disk deletion or synthetic rollback: remote seeds may exist.
				return provisionedLogicalGraph{}, err
			}
		}
	}
	return provisionedLogicalGraph{wire, spec.voters, spec.agreement}, nil
}

func isNilInitialProvisioner(p initialGraphProvisioner) bool {
	if p == nil {
		return true
	}
	v := reflect.ValueOf(p)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// provisionInitialGraphReplica is privileged authenticated INITIAL dispatch,
// not a decoder granting creation authority. It returns a synced pending Host;
// only the existing committed Init admits graph operations. Ordinary reopen
// never calls this function, including when its local database is missing.
func provisionInitialGraphReplica(ctx context.Context, c raftlog.Config, g provisionedLogicalGraph, peer initialGraphPeer, l materializerLimits) (*allocationHost, error) {
	return openLifecycleGraphReplica(ctx, c, g, peer, l, true)
}

// openLogicalGraphReplica restores only this voter's existing durable database.
// Constructor success grants no fresh read/issuance/cut authority. Existing Host
// operations retain their committed-Init and actual consensus/proof gates.
// Pending roots check only persisted identity/membership/declaration; callers
// may still select candidate Init choices. Once initialized, all supplied
// genesis configuration must equal the actual committed configuration.
func openLogicalGraphReplica(ctx context.Context, c raftlog.Config, g provisionedLogicalGraph, peer initialGraphPeer, l materializerLimits) (*allocationHost, error) {
	return openLifecycleGraphReplica(ctx, c, g, peer, l, false)
}

func openLifecycleGraphReplica(ctx context.Context, c raftlog.Config, g provisionedLogicalGraph, peer initialGraphPeer, l materializerLimits, initial bool) (*allocationHost, error) {
	if ctx == nil || c.Create || c.Transfer.Identity != (raftlog.ApplicationIdentity{}) || c.SemanticContractID != (raftlog.ApplicationSemanticContractID{}) || c.Replication != (raftlog.ApplicationReplicationConfig{}) || !partitionGraphAgreement(g.agreement) || g.voters[0] == 0 || g.voters[1] <= g.voters[0] || g.voters[2] <= g.voters[1] || peer.voter != c.Application.LocalVoter || !slices.Contains(g.voters[:], peer.voter) {
		return nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r, err := decodeDeclaredInit(g.genesis, l)
	if err != nil {
		return nil, err
	}
	if r.version() != 6 {
		return nil, errInvalid
	}
	part, found := r.declaration.Partition(peer.partition)
	if !found {
		return nil, errInvalid
	}
	c.Create = initial
	c.Transfer.Identity = raftlog.ApplicationIdentity{Graph: [16]byte(r.ns.graph), Partition: peer.partition, Group: part.Group}
	c.SemanticContractID = g.agreement
	c.Replication = raftlog.ApplicationReplicationConfig{Voters: g.voters}
	s, err := raftlog.Open(c)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*allocationHost, error) { return nil, errors.Join(err, s.Close()) }
	if initial {
		if err := graphstore.BootstrapBoundOwnership(s, s.ApplicationBinding(), r.declaration, g.voters); err != nil {
			return fail(err)
		}
	}
	n := namespace{graph: r.ns.graph, partition: peer.partition}
	var m *declaredMaterializer
	switch g.agreement {
	case partitionWriteSemanticContractID():
		m, err = newPartitionWriteMaterializer(s, n, r.declaration, l)
	case crossPartitionSemanticContractID():
		m, err = newCrossPartitionMaterializer(s, n, r.declaration, l)
	case certifiedGraphSemanticContractID():
		m, err = newCertifiedGraphMaterializer(s, n, r.declaration, l)
	case rangeMoveSemanticContractID():
		m, err = newRangeMoveMaterializer(s, n, r.declaration, l)
	default:
		err = errInvalid
	}
	if err != nil {
		return fail(err)
	}
	// A recovered initialized graph must match the COMPLETE supplied genesis,
	// not merely the graph/declaration prefix. Pending seeds have no installed
	// schema/default-axis/routing state and confer no readiness from this check.
	index, image, err := s.Checkpoint()
	if err != nil {
		return fail(err)
	}
	root, err := graphstore.DecodeRoot(image)
	if err != nil {
		return fail(err)
	}
	if root.SemanticEpoch() != 0 {
		view, err := s.ApplicationView(index)
		if err != nil {
			return fail(err)
		}
		q := reader{ctx: ctx, view: view, ns: n, limits: l.allocation}
		actual, err := m.readGenesisConfig(&q)
		expected, expectedErr := r.validate(l)
		err = errors.Join(err, expectedErr, view.Close())
		if err != nil {
			return fail(err)
		}
		if actual != expected {
			return fail(errors.Join(errInvalid, graphstore.ErrRebinding))
		}
	}
	h, err := openAllocationHost(m, peer.voter, replica.Config{Store: s, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
	if err != nil {
		return fail(err)
	}
	return h, nil
}
