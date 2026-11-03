package storeutil

import (
	"crypto/sha256"
	"encoding/binary"
)

// KeyUniqueOwner is the badger store's owner index of unique keys
// (UNIQUELOOK; the tiered store keeps the same layout in its own badger under
// DataDir/index): + 1B tag + 2B label token + ...
//
//	'D' label key            -> ""  a declaration
//	'R' label key            -> ""  seeded: lookups may trust it
//	'U' label len(key) key form value -> 8B owner ID (big-endian)
//
// form is 0 for a value key of at most UniqueOwnerVerbatimMax bytes, kept as
// is, else 1 and the value key's SHA-256: badger refuses a key above 65,000
// bytes, and a reader checks the claimant's row anyway.
const KeyUniqueOwner byte = 0x10

// UniqueOwnerVerbatimMax is the longest value key a claim keeps verbatim.
const UniqueOwnerVerbatimMax = 1024

// UniqueOwnerMarkKey is the 'D' or 'R' key of (label, key).
func UniqueOwnerMarkKey(tag byte, label uint16, key string) []byte {
	b := binary.BigEndian.AppendUint16([]byte{KeyUniqueOwner, tag}, label)
	return append(b, key...)
}

// UniqueOwnerClaimPrefix is the prefix of every claim of (label, key).
func UniqueOwnerClaimPrefix(label uint16, key string) []byte {
	b := binary.BigEndian.AppendUint16([]byte{KeyUniqueOwner, 'U'}, label)
	b = binary.BigEndian.AppendUint16(b, uint16(len(key))) // #nosec G115 -- property keys are bounded far below 64 KiB
	return append(b, key...)
}

// UniqueOwnerClaimKey is the claim key of valueKey under (label, key).
func UniqueOwnerClaimKey(label uint16, key, valueKey string) []byte {
	b := UniqueOwnerClaimPrefix(label, key)
	if len(valueKey) <= UniqueOwnerVerbatimMax {
		return append(append(b, 0), valueKey...)
	}
	sum := sha256.Sum256([]byte(valueKey))
	return append(append(b, 1), sum[:]...)
}

// UniqueOwnerStoreFormatVersion is the store-level marker of a badger
// directory with an owner index (UNIQUELOOK, KeyUniqueOwner). A binary that
// predates it would write rows of a declared unique key without claiming
// their values, so a newer binary trusting the seeded index would miss their
// holders and admit duplicates: it must refuse the store. Raised with the
// first declaration; 4 to 15 are taken, so this is 16.
const UniqueOwnerStoreFormatVersion = 16
