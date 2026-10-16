// Package importmap maps the rows of a v4 io.Export stream to a v5 op stream.
//
// It reads through fixtures.ReadExport (the one door to a v4 export), writes
// nothing to a graph (write.go applies the stream) and decides every value and
// every time coordinate explicitly:
//
//   - Values (MapValue): int*/uint* up to MaxInt64 -> Int, finite float32/float64
//     -> Float (float32 widens exactly, -0 becomes +0), string, bool, nil ->
//     String, Bool, Null; bytes (hex) -> Bytes; a flat slice of mapped scalars or
//     bytes -> List. Everything else is an ambiguity row and is dropped from the
//     op, never guessed: uint above MaxInt64, NaN, +-Inf, a nested slice or a
//     slice holding an unmappable element (reported with the element's reason;
//     a nil element is null_in_list),
//     map, time, temporal values, unknown kinds, malformed text.
//   - Scope: each v4 row [valid_from, valid_to) becomes an explicit Scope.
//     valid_from 0 is v4's "unset" and resolves to the row's TxFrom (the
//     effective timelines of the fixtures show this in both untimed modes);
//     MinInt64 is -inf; valid_to 0 is +inf. In an "unbounded" store the very
//     first row of an entity with valid_from 0 is an explicit 0.
//   - Recorded is the v4 TxFrom of the row; End and Retract carry the
//     DeletedAt instant of the tombstone.
//   - Void rows (excise pieces) have no v5 op: they are ambiguity rows.
package importmap
