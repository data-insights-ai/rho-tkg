package sharded

import (
	"errors"
	"fmt"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.EntityLendBatchMeteredCapability = (*Store)(nil)
	_ storecontract.RelLendBatchMeteredCapability    = (*Store)(nil)
)

// LendNodesSortedMetered implements store.EntityLendBatchMeteredCapability
// (RH list R2): out[i] is the frozen row of ids[i], nil where the node does not
// exist, lent by the ID's slot (its entity cache serves the hits and keeps the
// rows it reads). Each row a slot decodes counts its stored bytes in meter
// before the decode (ReadWork.DecodedBytes); a cache hit counts nothing. ids
// need not be ascending. Only a metered call reads this way: an unmetered lend
// on sharded reads each row with GetNode, as before.
func (s *Store) LendNodesSortedMetered(ids []types.NodeID, out []*types.Node, meter *storecontract.ReadMeter) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if len(out) != len(ids) {
		return fmt.Errorf("graph: lend nodes: %d ids, %d out slots", len(ids), len(out))
	}
	tally := meter.Tally("LendNodesSorted")
	for i, id := range ids {
		out[i] = nil
		if err := storecontract.ValidateNodeID(id); err != nil {
			return err
		}
		shard, err := s.shardForNodeID(id)
		if err != nil {
			return err
		}
		n, err := shard.LendNodeMetered(id, tally)
		if errors.Is(err, badger.ErrNodeNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		out[i] = n
	}
	return nil
}

// LendRelationshipsSortedMetered is LendNodesSortedMetered for relationships.
func (s *Store) LendRelationshipsSortedMetered(ids []types.RelID, out []*types.Relationship, meter *storecontract.ReadMeter) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if len(out) != len(ids) {
		return fmt.Errorf("graph: lend relationships: %d ids, %d out slots", len(ids), len(out))
	}
	tally := meter.Tally("LendRelationshipsSorted")
	for i, id := range ids {
		out[i] = nil
		if err := storecontract.ValidateRelID(id); err != nil {
			return err
		}
		shard, err := s.shardForRelID(id)
		if err != nil {
			return err
		}
		r, err := shard.LendRelationshipMetered(id, tally)
		if errors.Is(err, badger.ErrRelNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		out[i] = r
	}
	return nil
}
