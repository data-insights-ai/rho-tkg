package replica

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const applicationPacketHeaderBytes = 4 + 40 + 32 + 4
const applicationPacketMaxBytes = 256 << 20

// EncodeApplicationPacket wraps owned opaque consensus bytes in RP1's exact
// graph/partition/group/semantic binding. maxBytes bounds serialized Payload
// capacity, including the fixed envelope; caller/Packet structs are separate.
// This codec enables no Driver traffic and grants no quorum/authentication claim.
func EncodeApplicationPacket(binding raftlog.ApplicationBinding, p Packet, maxBytes int) (Packet, error) {
	if err := binding.Validate(); err != nil {
		return Packet{}, errors.Join(ErrInvalid, err)
	}
	if maxBytes < applicationPacketHeaderBytes || maxBytes > applicationPacketMaxBytes {
		return Packet{}, ErrInvalid
	}
	if len(p.Payload) > maxBytes-applicationPacketHeaderBytes {
		return Packet{}, ErrLimit
	}
	b := make([]byte, 0, applicationPacketHeaderBytes+len(p.Payload))
	b = append(b, 'R', 'P', 1, 0)
	b = append(b, binding.Identity.Graph[:]...)
	b = binary.BigEndian.AppendUint64(b, binding.Identity.Partition)
	b = append(b, binding.Identity.Group[:]...)
	b = append(b, binding.SemanticContractID[:]...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(p.Payload)))
	b = append(b, p.Payload...)
	return Packet{From: p.From, To: p.To, Snapshot: p.Snapshot, Payload: b}, nil
} // #nosec G115 -- payload length <= concrete maxBytes <=256MiB before uint32 encoding.

// DecodeApplicationPacket checks RP1 scope and lengths before copying opaque
// consensus bytes. A future Driver must call it BEFORE protobuf/RawNode. Unknown
// versions, cross-group/semantic traffic and unbound inputs fail closed.
func DecodeApplicationPacket(binding raftlog.ApplicationBinding, p Packet, maxBytes int) (Packet, error) {
	if err := binding.Validate(); err != nil {
		return Packet{}, errors.Join(ErrInvalid, err)
	}
	if maxBytes < applicationPacketHeaderBytes || maxBytes > applicationPacketMaxBytes {
		return Packet{}, ErrInvalid
	}
	b := p.Payload
	if len(b) > maxBytes {
		return Packet{}, ErrLimit
	}
	if len(b) < applicationPacketHeaderBytes || !bytes.Equal(b[:4], []byte{'R', 'P', 1, 0}) {
		return Packet{}, ErrInvalid
	}
	if !bytes.Equal(b[4:20], binding.Identity.Graph[:]) || binary.BigEndian.Uint64(b[20:28]) != binding.Identity.Partition || !bytes.Equal(b[28:44], binding.Identity.Group[:]) || !bytes.Equal(b[44:76], binding.SemanticContractID[:]) {
		return Packet{}, ErrInvalid
	}
	if uint64(binary.BigEndian.Uint32(b[76:80])) != uint64(len(b)-applicationPacketHeaderBytes) { // #nosec G115 -- validated 80 <= len(b) <= maxBytes <=256MiB before subtraction/conversion.
		return Packet{}, ErrInvalid
	}
	payload := make([]byte, len(b)-applicationPacketHeaderBytes)
	copy(payload, b[applicationPacketHeaderBytes:])
	return Packet{From: p.From, To: p.To, Snapshot: p.Snapshot, Payload: payload}, nil
}
