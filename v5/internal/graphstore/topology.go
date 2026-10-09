package graphstore

import (
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

type topologyDeclaration struct{ epoch, schema, index uint64 }

var bootstrapTopology = topologyDeclaration{epoch: 1, schema: 1}

// SinglePartitionTopology records an immutable graph-wide sole-partition
// declaration from one retained root. IndexVersion zero explicitly means indexes
// are unavailable. The declaration alone supplies neither a complete ReadView
// nor a certified cut, graph writer, distributed lease or coverage proof.
type SinglePartitionTopology struct {
	Graph                                                                 graphstate.GraphID
	Partition, OwnershipEpoch, TopologyEpoch, SchemaVersion, IndexVersion uint64
}

// SinglePartition returns the persisted declaration by value. Primitive v1 roots
// have no such authority and return ErrTopologyUnsupported. Obtain the Root from
// Catalog.Root to check the borrowed application view's lifetime first.
func (r Root) SinglePartition() (SinglePartitionTopology, error) {
	if err := r.validate(); err != nil {
		return SinglePartitionTopology{}, err
	}
	if r.topology == (topologyDeclaration{}) {
		return SinglePartitionTopology{}, ErrTopologyUnsupported
	}
	return SinglePartitionTopology{r.namespace.Graph, r.namespace.Partition, r.owner, r.topology.epoch, r.topology.schema, r.topology.index}, nil
}

// BootstrapSinglePartition initializes a fresh application-mode store with an
// authoritative sole-partition declaration and unavailable indexes. It uses the
// configured local voter and Store.Initialize's synchronous atomic publication;
// initialized or recovered stores refuse, including populated primitive v1
// catalogs. It performs no promotion/migration, scan or logical-ID allocation.
// A bound durable transfer identity must match the graph and partition.
// Graphstore staging refuses this declaration until an index-aware writer exists.
// Direct raftlog installation remains a trusted opaque application seam, whose
// materializer must preserve the declared topology and maintain its real indexes.
func BootstrapSinglePartition(s *raftlog.Store, n Namespace, ownershipEpoch uint64) error {
	if s == nil {
		return ErrInvalid
	}
	r, err := NewRoot(n, ownershipEpoch)
	if err != nil {
		return err
	}
	p := s.ApplicationLimits()
	if !p.Enabled() {
		return ErrTopologyUnsupported
	}
	identity := s.ApplicationIdentity()
	if identity != (raftlog.ApplicationIdentity{}) && (identity.Graph != n.Graph || identity.Partition != n.Partition) {
		return ErrNamespace
	}
	r.topology = bootstrapTopology
	image, err := EncodeRoot(r)
	if err != nil {
		return err
	}
	if err := s.Initialize([]uint64{p.LocalVoter}, image); err != nil {
		if errors.Is(err, raftlog.ErrInvalid) {
			return errors.Join(ErrInvalid, err)
		}
		if errors.Is(err, raftlog.ErrLimit) {
			return errors.Join(ErrResourceLimit, err)
		}
		return err
	}
	return nil
}
