package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func partitionRoundFloorKey(n namespace) []byte { return recordKey(n, 8) }
func partitionRoundMapKey(n namespace, round uint64) []byte {
	return binary.BigEndian.AppendUint64(recordKey(n, 9), round)
}
func encodePartitionRoundFloor(configuration [32]byte, round uint64) []byte {
	wire := append([]byte{'P', 'L', 'F', 1}, configuration[:]...)
	wire = binary.BigEndian.AppendUint64(wire, round)
	return seal(wire)
}
func writePartitionRoundFloor(q *reader, configuration [32]byte, round uint64) error {
	if err := reserveComposer(q, 512+8*76); err != nil {
		return err
	}
	return q.put(partitionRoundFloorKey(q.ns), encodePartitionRoundFloor(configuration, round))
}
func readPartitionRoundFloor(q *reader, configuration [32]byte) (uint64, error) {
	wire, found, err := q.getBounded(partitionRoundFloorKey(q.ns), 76)
	if err != nil {
		return 0, err
	}
	if !found || len(wire) != 76 || !bytes.Equal(wire[:4], []byte{'P', 'L', 'F', 1}) || !bytes.Equal(wire[4:36], configuration[:]) {
		return 0, errCorrupt
	}
	hash := sha256.Sum256(wire[:44])
	if !bytes.Equal(hash[:], wire[44:]) {
		return 0, errCorrupt
	}
	round := binary.BigEndian.Uint64(wire[36:44])
	if round == math.MaxUint64 {
		return 0, errLimit
	}
	return round, nil
}
func (m *declaredMaterializer) finishPartitionRoundMap(q *reader, b raftlog.ApplicationBatch, configuration [32]byte, round, index uint64, budget raftlog.ApplicationBudget, outer int) (raftlog.ApplicationBatch, error) {
	// The map captures the actual graph install root/index. A later certificate
	// for an older graph round must select this retained entry, never currentroot.
	// This package supplies no certified-cut selection or retention guarantee.
	const mapBytes = 4 + 32 + 8 + 8 + 172 + 32
	retained := outer + materializerOutputCost(b) + 128*cap(q.writes)
	if err := reserveCompositionWithBudget(q, &retained, 512+8*mapBytes+128*(len(b.Writes)+1), m.limits.outputBytes); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	wire := append([]byte{'P', 'L', 'M', 1}, configuration[:]...)
	wire = binary.BigEndian.AppendUint64(wire, round)
	wire = binary.BigEndian.AppendUint64(wire, index)
	wire = append(wire, b.Image...)
	wire = seal(wire)
	if len(wire) != mapBytes {
		return raftlog.ApplicationBatch{}, errCorrupt
	}
	key := partitionRoundMapKey(m.ns, round)
	// Appending uses an exact newly admitted header array, preserving all graph
	// and outer payload ownership until the single returned batch is installed.
	merged := make([]raftlog.KV, len(b.Writes)+1)
	copy(merged, b.Writes)
	merged[len(b.Writes)] = raftlog.KV{Key: key, Value: wire}
	b.Writes = merged
	if err := sortPartitionWrites(b.Writes); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if err := m.preflight(b, budget); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	return b, nil
}

func sortPartitionWrites(writes []raftlog.KV) error {
	slices.SortFunc(writes, func(a, b raftlog.KV) int { return bytes.Compare(a.Key, b.Key) })
	for i := 1; i < len(writes); i++ {
		if bytes.Equal(writes[i-1].Key, writes[i].Key) {
			return errCorrupt
		}
	}
	return nil
}
