package badger

// This file is an unwired feasibility prototype. No production mutation,
// directory marker, forward posting, or row codec changes in this branch.

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

const prototypeReversePrefix byte = 0x0e

var prototypeReverseCRC = crc32.MakeTable(crc32.Castagnoli)

func prototypeReverseForwardShape(forward []byte) (byte, types.EntityID, error) {
	if len(forward) < 20 || forward[0] != diskMembershipPrefix {
		return 0, 0, fmt.Errorf("%w: reverse forward prefix/length", ErrInvalidStoreMutation)
	}
	kind, size := forward[1], 20
	switch kind {
	case diskMemberNodeLabel, diskMemberRelType:
	case diskMemberNodeProperty, diskMemberRelProperty:
		size = 52
	default:
		return 0, 0, fmt.Errorf("%w: reverse forward kind", ErrInvalidStoreMutation)
	}
	if len(forward) != size || binary.BigEndian.Uint16(forward[2:4]) == 0 {
		return 0, 0, fmt.Errorf("%w: reverse forward shape/token", ErrInvalidStoreMutation)
	}
	id, _, err := diskMembershipRecord(forward, forward[:len(forward)-16])
	return kind, id, err
}

func prototypeReverseEncode(forward []byte, strong bool) ([]byte, []byte, error) {
	kind, _, err := prototypeReverseForwardShape(forward)
	if err != nil {
		return nil, nil, err
	}
	// Reorder the existing selector into a prefix that streams one owner.
	key := make([]byte, len(forward))
	key[0], key[1] = prototypeReversePrefix, kind
	copy(key[2:10], forward[len(forward)-16:len(forward)-8])
	copy(key[10:len(key)-8], forward[2:len(forward)-16])
	copy(key[len(key)-8:], forward[len(forward)-8:])
	if strong {
		hash := sha256.Sum256(key)
		return key, bytes.Clone(hash[:]), nil
	}
	value := make([]byte, 4)
	binary.BigEndian.PutUint32(value, crc32.Checksum(key, prototypeReverseCRC))
	return key, value, nil
}

func prototypeReverseDecode(key, value []byte, kind byte, owner types.EntityID, strong bool) ([]byte, error) {
	if (len(key) != 20 && len(key) != 52) || key[0] != prototypeReversePrefix || key[1] != kind || owner <= 0 || binary.BigEndian.Uint64(key[2:10]) != uint64(owner) {
		return nil, fmt.Errorf("%w: reverse ownership/shape", ErrInvalidStoreMutation)
	}
	if strong {
		hash := sha256.Sum256(key)
		if !bytes.Equal(value, hash[:]) {
			return nil, fmt.Errorf("%w: reverse SHA mismatch", ErrInvalidStoreMutation)
		}
	} else if len(value) != 4 || binary.BigEndian.Uint32(value) != crc32.Checksum(key, prototypeReverseCRC) {
		return nil, fmt.Errorf("%w: reverse CRC mismatch", ErrInvalidStoreMutation)
	}
	forward := make([]byte, len(key))
	forward[0], forward[1] = diskMembershipPrefix, kind
	copy(forward[2:len(forward)-16], key[10:len(key)-8])
	copy(forward[len(forward)-16:len(forward)-8], key[2:10])
	copy(forward[len(forward)-8:], key[len(key)-8:])
	decodedKind, decodedOwner, err := prototypeReverseForwardShape(forward)
	if err != nil || decodedKind != kind || decodedOwner != owner {
		return nil, errors.Join(fmt.Errorf("%w: reverse reconstructed ownership", ErrInvalidStoreMutation), err)
	}
	return forward, nil
}

func TestPrototypeReverseRoundTripAndOwnerIsolation(t *testing.T) {
	for _, strong := range []bool{false, true} {
		for _, kind := range []byte{diskMemberNodeLabel, diskMemberRelType, diskMemberNodeProperty, diskMemberRelProperty} {
			for _, id := range []types.EntityID{1, math.MaxInt64} {
				for _, stamp := range []types.Instant{-1, 0, 1, math.MaxInt64} {
					scope := diskMembershipScope(kind, math.MaxUint16, "payload", "s:exact\x00\nvalue")
					old := diskMembershipPair(scope, id, stamp)
					key, value, err := prototypeReverseEncode(old[0].key, strong)
					if err != nil {
						t.Fatal(err)
					}
					forward, err := prototypeReverseDecode(key, value, kind, id, strong)
					if err != nil || !bytes.Equal(forward, old[0].key) {
						t.Fatalf("round trip kind=%d id=%d tx=%d: %x %v", kind, id, stamp, forward, err)
					}
					if _, err := prototypeReverseDecode(key, value, kind, 2, strong); !errors.Is(err, ErrInvalidStoreMutation) {
						t.Fatal("other owner accepted", err)
					}
					if _, err := prototypeReverseDecode(key, value, kind%4+1, id, strong); !errors.Is(err, ErrInvalidStoreMutation) {
						t.Fatal("other kind accepted", err)
					}
					// A valid checksum on an attacker-chosen other owner still cannot
					// make an erase for this owner reconstruct the other owner's key.
					other := diskMembershipOp(scope, 2, stamp)
					otherKey, otherValue, err := prototypeReverseEncode(other.key, strong)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := prototypeReverseDecode(otherKey, otherValue, kind, id, strong); !errors.Is(err, ErrInvalidStoreMutation) {
						t.Fatal("checksummed foreign pointer accepted", err)
					}
					for i := range key {
						corrupt := bytes.Clone(key)
						corrupt[i] ^= 1
						if _, err := prototypeReverseDecode(corrupt, value, kind, id, strong); !errors.Is(err, ErrInvalidStoreMutation) {
							t.Fatalf("key corruption byte%d accepted: %v", i, err)
						}
					}
				}
			}
		}
	}
}

func TestPrototypeReverseRefusesMalformedSelectors(t *testing.T) {
	valid := diskMembershipOp(diskMembershipScope(diskMemberNodeProperty, 1, "payload", "s:value"), 1, 10).key
	for _, change := range []func([]byte) []byte{
		func(v []byte) []byte { return nil },
		func(v []byte) []byte { return v[:20] },
		func(v []byte) []byte { v[0] = 0; return v },
		func(v []byte) []byte { v[1] = 0; return v },
		func(v []byte) []byte { v[1] = diskMemberNodeLabel; return v },
		func(v []byte) []byte { v[2], v[3] = 0, 0; return v },
		func(v []byte) []byte { clear(v[len(v)-16 : len(v)-8]); return v },
		func(v []byte) []byte { v[len(v)-16] = 0xff; return v },
		func(v []byte) []byte { v[len(v)-8] = 0xff; return v },
	} {
		if _, _, err := prototypeReverseEncode(change(bytes.Clone(valid)), false); err == nil {
			t.Fatal("malformed forward accepted")
		}
	}
	for _, strong := range []bool{false, true} {
		key, value, err := prototypeReverseEncode(valid, strong)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range [][]byte{nil, value[:len(value)-1], append(bytes.Clone(value), 0), make([]byte, len(value))} {
			if _, err := prototypeReverseDecode(key, v, diskMemberNodeProperty, 1, strong); !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatal("malformed checksum accepted", err)
			}
		}
		// These malformed selectors carry valid checksums: structural validation
		// must reject them independently of the auxiliary checksum.
		for _, change := range []func([]byte) []byte{
			func(v []byte) []byte { v[10], v[11] = 0, 0; return v },
			func(v []byte) []byte { v[len(v)-8] = 0xff; return v },
			func(v []byte) []byte { return v[:20] },
		} {
			badKey := change(bytes.Clone(key))
			var badValue []byte
			if strong {
				hash := sha256.Sum256(badKey)
				badValue = bytes.Clone(hash[:])
			} else {
				badValue = make([]byte, 4)
				binary.BigEndian.PutUint32(badValue, crc32.Checksum(badKey, prototypeReverseCRC))
			}
			if _, err := prototypeReverseDecode(badKey, badValue, diskMemberNodeProperty, 1, strong); !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatal("checksummed malformed selector accepted", err)
			}
		}
	}
}

type prototypeReverseEntry struct {
	key, value []byte
	version    uint64
}

func prototypeReverseTableBytes(entries []prototypeReverseEntry, compression options.CompressionType) int {
	slices.SortFunc(entries, func(a, b prototypeReverseEntry) int { return bytes.Compare(a.key, b.key) })
	pool := z.NewAllocatorPool(1)
	defer pool.Release()
	b := table.NewTableBuilder(table.Options{TableSize: 8 << 20, BlockSize: 4096, BloomFalsePositive: .01, Compression: compression, ZSTDCompressionLevel: 3, AllocPool: pool})
	defer b.Close()
	for _, e := range entries {
		b.Add(y.KeyWithTs(e.key, e.version), y.ValueStruct{Value: e.value}, 0)
	}
	return len(b.Finish())
}

func TestPrototypeReverseGenuineErvMeasurement(t *testing.T) {
	dir := os.Getenv("RHO_REVERSE_GENUINE_FIXTURE")
	if dir == "" {
		t.Skip("set RHO_REVERSE_GENUINE_FIXTURE to the closed Erv native derivative")
	}
	// Open the underlying database read-only; do not use a default native
	// writer or mutate the supplied format5 source fixture.
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithReadOnly(true).WithLogger(nil).WithMemTableSize(32 << 20).WithBlockCacheSize(8 << 20).WithIndexCacheSize(8 << 20))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := verifyAndStampWireFormatVersionForReader(db, true, 5); err != nil {
		t.Fatal(err)
	}
	const perKindLimit = 25000
	const limit = 2 * perKindLimit
	var legacy, compact, strong []prototypeReverseEntry
	counts := map[byte]int{}
	var rawLegacy, rawCompact, rawStrong int
	inputDigest := sha256.New()
	err = db.View(func(txn *badgerv4.Txn) error {
		for _, selectedKind := range []byte{diskMemberNodeLabel, diskMemberNodeProperty} {
			opts := badgerv4.DefaultIteratorOptions
			opts.PrefetchValues = false
			opts.Prefix = []byte{diskMembershipReversePrefix, selectedKind}
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix) && counts[selectedKind] < perKindLimit; it.Next() {
				k := it.Item().KeyCopy(nil)
				v, err := it.Item().ValueCopy(nil)
				if err != nil {
					return err
				}
				if len(k) != 42 {
					return fmt.Errorf("legacy reverse shape %d", len(k))
				}
				hash := sha256.Sum256(v)
				if !bytes.Equal(hash[:], k[10:]) {
					return errors.New("legacy reverse hash mismatch")
				}
				kind, id, err := prototypeReverseForwardShape(v)
				if err != nil {
					return err
				}
				if k[1] != kind || binary.BigEndian.Uint64(k[2:10]) != uint64(id) {
					return errors.New("legacy reverse ownership mismatch")
				}
				ck, cv, err := prototypeReverseEncode(v, false)
				if err != nil {
					return err
				}
				decoded, err := prototypeReverseDecode(ck, cv, kind, id, false)
				if err != nil || !bytes.Equal(decoded, v) {
					return errors.New("genuine CRC roundtrip differs")
				}
				sk, sv, err := prototypeReverseEncode(v, true)
				if err != nil {
					return err
				}
				decoded, err = prototypeReverseDecode(sk, sv, kind, id, true)
				if err != nil || !bytes.Equal(decoded, v) {
					return errors.New("genuine SHA roundtrip differs")
				}
				version := it.Item().Version()
				legacy = append(legacy, prototypeReverseEntry{k, v, version})
				compact = append(compact, prototypeReverseEntry{ck, cv, version})
				strong = append(strong, prototypeReverseEntry{sk, sv, version})
				counts[kind]++
				rawLegacy += len(k) + len(v)
				rawCompact += len(ck) + len(cv)
				rawStrong += len(sk) + len(sv)
				inputDigest.Write(k)
				inputDigest.Write(v)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(legacy) != limit {
		t.Fatalf("bounded fixture sample too small: %d", len(legacy))
	}
	result := map[string]any{"source": dir, "selection": "first25000 node-label and first25000 node-property legacy reverse entries in exact native key order per kind; not population representative", "sample_entries": len(legacy), "kinds": counts, "legacy_entry_sha256": fmt.Sprintf("%x", inputDigest.Sum(nil)), "raw_bytes": map[string]int{"legacy": rawLegacy, "crc32c": rawCompact, "sha256": rawStrong}}
	for _, c := range []options.CompressionType{options.Snappy, options.ZSTD} {
		result[fmt.Sprint(c)+"_sst_bytes"] = map[string]int{"legacy": prototypeReverseTableBytes(legacy, c), "crc32c": prototypeReverseTableBytes(compact, c), "sha256": prototypeReverseTableBytes(strong, c)}
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(raw))
}

func TestPrototypeReverseWriterFence(t *testing.T) {
	db, err := badgerv4.Open(badgerv4.DefaultOptions(t.TempDir()).WithLogger(nil).WithSyncWrites(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := verifyAndStampWireFormatVersionForReader(db, false, 6); err != nil {
		t.Fatal(err)
	}
	for _, readOnly := range []bool{false, true} {
		if err := verifyAndStampWireFormatVersionForReader(db, readOnly, 5); !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
			t.Fatal("format5 engine accepted writer6", err)
		}
	}
}
