package graphstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

const routingBindingFixedBytes = 4 + 32 + 16 + 32 + 4 + 32

// EncodePartitionRoutingBinding co-owns the genesis routing/configuration/default
// designation fence. It is immutable configuration, never a caller grant.
func EncodePartitionRoutingBinding(r PartitionRouting, d OwnershipDeclaration, b types.DefaultAxisBinding, configuration [32]byte, l Limits) ([]byte, error) {
	l, err := l.resolve()
	if err != nil {
		return nil, err
	}
	if configuration == ([32]byte{}) || b.Check(types.GraphID(d.Graph()), b.Axis(), l.Temporal) != nil {
		return nil, ErrInvalid
	}
	size, err := r.EncodedBytes(l)
	if err != nil {
		return nil, err
	}
	if size > l.MaxRecordBytes-routingBindingFixedBytes {
		return nil, ErrResourceLimit
	}
	wire, err := EncodePartitionRouting(r, d, l)
	if err != nil {
		return nil, err
	}
	body := make([]byte, 0, routingBindingFixedBytes-32+len(wire))
	body = append(body, 'R', 'D', 'B', 1)
	body = append(body, configuration[:]...)
	id, hash := b.Axis().Descriptor().ID, b.Axis().DefinitionHash()
	body = append(body, id[:]...)
	body = append(body, hash[:]...)
	body = binary.BigEndian.AppendUint32(body, uint32(len(wire))) // #nosec G115 -- wire admitted under the<=1MiB record cap.
	body = append(body, wire...)
	return sealRecord(body), nil
}
func sealRecord(body []byte) []byte {
	hash := sha256.Sum256(body)
	return exactCopy(append(body, hash[:]...))
}

// DecodePartitionRoutingBinding checks the full supplied policy binding before
// table allocation; matching units alone never bind graph/axis/config identities.
func DecodePartitionRoutingBinding(wire []byte, d OwnershipDeclaration, b types.DefaultAxisBinding, configuration [32]byte, l Limits) (PartitionRouting, error) {
	l, err := l.resolve()
	if err != nil {
		return PartitionRouting{}, err
	}
	if configuration == ([32]byte{}) || b.Check(types.GraphID(d.Graph()), b.Axis(), l.Temporal) != nil {
		return PartitionRouting{}, ErrInvalid
	}
	if len(wire) < routingBindingFixedBytes || len(wire) > l.MaxRecordBytes || !bytes.Equal(wire[:4], []byte{'R', 'D', 'B', 1}) {
		return PartitionRouting{}, ErrCorrupt
	}
	hash := sha256.Sum256(wire[:len(wire)-32])
	id, definition := b.Axis().Descriptor().ID, b.Axis().DefinitionHash()
	if !bytes.Equal(hash[:], wire[len(wire)-32:]) || !bytes.Equal(wire[4:36], configuration[:]) || !bytes.Equal(wire[36:52], id[:]) || !bytes.Equal(wire[52:84], definition[:]) || uint64(binary.BigEndian.Uint32(wire[84:88])) != uint64(len(wire)-routingBindingFixedBytes) { // #nosec G115 -- delivered wire admitted>=fixed120 and<=1MiB; subtraction is nonnegative.
		return PartitionRouting{}, ErrCorrupt
	}
	r, err := DecodePartitionRouting(wire[88:len(wire)-32], d, l)
	if err != nil {
		return PartitionRouting{}, errors.Join(ErrCorrupt, err)
	}
	return r, nil
}

// AxisRegistrationKey identifies the immutable definition authority by AxisID.
// Definition hashes never choose placement: conflicting definitions share a key.
func AxisRegistrationKey(n Namespace, id temporal.AxisID) []byte {
	return append(routingKey(n, 5), id[:]...)
}
