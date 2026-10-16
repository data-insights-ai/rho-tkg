// Package segment is the v5 port of the v4 column-segment codec (ADR-0011,
// roadmap V3): an immutable, self-contained, versioned byte format holding the
// rows of ONE relationship type in column form, with a reader over it. It is
// self-contained: it imports only v5/pkg/types and v5/pkg/temporal and touches
// no other internal package.
//
// # Contract (unchanged from v4)
//
//   - Encode accepts rows in any order and sorts them by (start, end,
//     valid_from, id, revision); a row is keyed (id, revision).
//   - Every decoded row equals its source bit for bit, property values
//     included (NaN payloads, -0, exact Go kind).
//   - Per-row hashes are never stored; Verify recomputes them from decoded
//     columns against one SHA-256 root per IntegrityBlockRows rows and a
//     segment root over those.
//   - Encode re-opens what it wrote and refuses to return a segment in which
//     any row differs from its source (ErrInvalidRow, ErrHashMismatch).
//   - Open is a trust boundary: magic, versions, every CRC32C and length are
//     checked, nothing is allocated in proportion to a count the bytes only
//     claim, and decoding never panics (FuzzOpen, FuzzOpenResealed).
//
// # Provenance (v4 = pkg/graph/internal/segment on main, 97efa1c0)
//
//	bitpack.go     verbatim from v4 bitpack.go
//	intcol.go      verbatim from v4 intcol.go (frame-of-reference pages,
//	               transforms none / minus-ref / delta-prev / delta-run)
//	strcol.go      v4 strcol.go minus the hex32 mode (hash columns) and the
//	               shared-dictionary mode; dict, plain and bytes columns stay
//	format.go      v4 format.go: header/footer/directory, new magic
//	               "TKG5SEG\x01" / "TKG5SEGF" so a v4 segment is never read as v5
//	writer.go      v4 writer.go (node table, CSR, ID index, integrity roots,
//	               page index, seal-then-verify)
//	reader.go      v4 reader.go (Open, Row, Scan, Batch, Lookup, OutRows,
//	               InRows, VerifyGroup, ForEachID, ScanRange)
//	fuzz_test.go   v4 fuzz_test.go (FuzzOpen, FuzzOpenResealed, reseal) plus
//	               the new FuzzEncodeRoundTrip
//	synthhop_test.go  v4 internal/synthhop/synthhop.go, unchanged
//
// # What changed for v5
//
//   - Row model. v4 carried *types.Relationship with TemporalMetadata and
//     RelIntegrity. v5 has Row: RelID/NodeID are int64 1..MaxInt64 (0 and
//     negatives are refused on write and on read), Revision replaces Version,
//     properties are graphstate.Scalar values of kind I64, F64, Bool or String.
//   - Time. v4 stored types.Instant where 0 meant unset/open. v5 stores Coord:
//     a finite int64 (zero is an ordinary coordinate) or an explicit -inf /
//     +inf, matching temporal.BoundKind; CoordFromBound and Coord.Bound convert
//     to and from an integer-axis temporal.Bound. Each of valid_from,
//     valid_to, tx_from, tx_to is a value column (an infinity stores the
//     valid_from slot, so the minus-reference delta is 0) plus a kind column
//     that is omitted while every row is finite. Allowed kinds: *_from finite
//     or -inf, *_to finite or +inf. The codec stores coordinates only; the
//     interval shape (inclusivity, from <= to) is the Scope layer's.
//   - Dropped as v4 transaction/integrity-chain semantics that v5 does not
//     copy: created/updated/deleted_at, base entity, created/updated_by,
//     prev_hash, author, authorization, signature, endpoint (node) hashes, the
//     per-row stored hash, TypeToken (header bytes 12..13 are reserved zero),
//     the tkg_ reserved column prefix, and the store-level NodeDict/StringDict
//     (dicts.go, an in-RAM sharing layer tied to node hashes).
//   - Row hash. v4's integrity.ComputeRelHash is replaced by RowHash, a
//     SHA-256 over a canonical encoding of the row (see model.go).
//   - Fallback column. v4 used the entity wire (msgpack) for properties
//     outside the schema and supported nested slices/maps/temporal values. v5
//     uses graphstate's kind-preserving wire (Scalar.AppendWire/DecodeWire, length
//     prefixed; model.go appendValue) and supports I64, F64, Bool, String, Bytes
//     and List (Bytes and List have no column: always fallback, so the format
//     did not change); other values are refused with ErrInvalidRow. Format 2 replaced
//     the format-1 13-width model (int8..uint64, float32).
//   - Page index entries cover valid_from, valid_to, tx_from, tx_to and rel_id
//     (min/max over finite values, flags for non-finite ones).
//
// # Layout (little-endian)
//
//	header   64 B: magic | format u16 | flags u16 (0) | reserved u16 (0) |
//	         pageRows u16 | rowCount u32 | nodeCount u32 | id min/max i64 |
//	         finite valid_from min/max i64 | header CRC32C u32 | reserved u32
//	sections contiguous, in directory order (names: format.go)
//	footer   footerVersion u16 | integrityBlockRows u16 | type name |
//	         declared schema | segment root [32] | section directory
//	trailer  16 B: footer length u32 | footer CRC32C u32 | magic
package segment
