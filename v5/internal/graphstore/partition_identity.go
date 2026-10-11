package graphstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

func identityAuthorityKey(n Namespace, tag byte, hash [32]byte, ordinal uint64) []byte {
	key := append(routingKey(n, tag), hash[:]...)
	if tag == 7 {
		key = binary.BigEndian.AppendUint64(key, ordinal)
	}
	return key
}
func (s *partitionReadScope) identity(q *reader, key string) (graphstate.ValueID, bool, error) {
	if err := q.materialize(128 + 3*len(key)); err != nil {
		return 0, false, err
	}
	if err := s.bucket(q, IdentityRoutingBucket, []byte(key)); err != nil {
		return 0, false, err
	}
	hash := sha256.Sum256([]byte(key))
	wire, found, err := q.get(identityAuthorityKey(s.namespace, 6, hash, 0))
	if err != nil || !found {
		return 0, false, err
	}
	if len(wire) != 4+32+8+32 || !bytes.Equal(wire[:4], []byte{'V', 'I', 'C', 1}) {
		return 0, false, ErrCorrupt
	}
	digest := sha256.Sum256(wire[:44])
	if !bytes.Equal(digest[:], wire[44:]) || !bytes.Equal(wire[4:36], s.configuration[:]) {
		return 0, false, ErrCorrupt
	}
	count := binary.BigEndian.Uint64(wire[36:44])
	if count == 0 {
		return 0, false, ErrCorrupt
	}
	if count > uint64(q.c.limits.MaxBucketValues) { // #nosec G115 -- resolved catalog policy bounds this positive limit to1..65536.
		return 0, false, ErrResourceLimit
	}
	for ordinal := uint64(0); ordinal < count; ordinal++ {
		record, present, err := q.get(identityAuthorityKey(s.namespace, 7, hash, ordinal))
		if err != nil {
			return 0, false, err
		}
		if !present || len(record) < 4+32+8+4+32 || !bytes.Equal(record[:4], []byte{'V', 'I', 'D', 1}) || !bytes.Equal(record[4:36], s.configuration[:]) {
			return 0, false, ErrCorrupt
		}
		stored := record[48 : len(record)-32]
		checksum := sha256.Sum256(record[:len(record)-32])
		if uint64(binary.BigEndian.Uint32(record[44:48])) != uint64(len(stored)) || !bytes.Equal(checksum[:], record[len(record)-32:]) || sha256.Sum256(stored) != hash {
			return 0, false, ErrCorrupt
		}
		id := graphstate.ValueID(binary.BigEndian.Uint64(record[36:44]))
		if id == 0 {
			return 0, false, ErrCorrupt
		}
		if bytes.Equal(stored, []byte(key)) {
			if err := s.owner(q, uint64(id)); err != nil {
				return 0, false, err
			}
			return id, true, nil
		}
	}
	return 0, false, nil
}
func (s *partitionReadScope) registerIdentity(q *reader, key string, id graphstate.ValueID) error {
	if id == 0 {
		return ErrInvalid
	}
	prior, found, err := s.identity(q, key)
	if err != nil {
		return err
	}
	if found {
		if prior != id {
			return ErrRebinding
		}
		return nil
	}
	if err := q.materialize(768 + 6*len(key)); err != nil {
		return err
	}
	hash := sha256.Sum256([]byte(key))
	counterKey := identityAuthorityKey(s.namespace, 6, hash, 0)
	wire, present, err := q.get(counterKey)
	if err != nil {
		return err
	}
	count := uint64(0)
	if present {
		if len(wire) != 76 {
			return ErrCorrupt
		}
		count = binary.BigEndian.Uint64(wire[36:44])
	}
	if count >= uint64(q.c.limits.MaxBucketValues) { // #nosec G115 -- resolved catalog policy bounds this positive limit to1..65536.
		return ErrResourceLimit
	}
	if len(key) > q.c.limits.MaxRecordBytes-(4+32+8+4+32) {
		return ErrResourceLimit
	}
	record := append([]byte{'V', 'I', 'D', 1}, s.configuration[:]...)
	record = binary.BigEndian.AppendUint64(record, uint64(id))
	record = appendField(record, []byte(key))
	record = sealRecord(record)
	if err := q.put(identityAuthorityKey(s.namespace, 7, hash, count), record); err != nil {
		return err
	}
	next := append([]byte{'V', 'I', 'C', 1}, s.configuration[:]...)
	next = binary.BigEndian.AppendUint64(next, count+1)
	return q.put(counterKey, sealRecord(next))
}

func (q *reader) canonicalIdentity(key string) (ValueEntry, bool, error) {
	if q.route == nil {
		return q.local(key)
	}
	canonical, authoritative, err := q.route.identity(q, key)
	if err != nil {
		return ValueEntry{}, false, err
	}
	entry, found, err := q.local(key)
	if err != nil {
		return ValueEntry{}, false, err
	}
	if found != authoritative || found && entry.Ref.ID != canonical {
		return ValueEntry{}, false, ErrCorrupt
	}
	return entry, found, nil
}
