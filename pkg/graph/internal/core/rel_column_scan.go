package core

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// ScanRelColumns exposes the backend's typed relationship column scan when it has
// one, the sibling of ScanNodeColumns. Every batch names its type (RelType).
//
// An empty relType scans every registered relationship type, type by type in
// type-token order (each type's batches in ID order, as a typed scan; the
// order across types is not ID order), so a consumer reading the
// relationships of every type (an untyped pattern) needs no listing of the
// types. A type with no rows contributes nothing. fn returning false stops
// the whole scan.
//
// ok=false means this backend does not implement RelColumnScanCapability and the
// caller should use RelsByType — the capability is OPTIONAL, like every other one
// asserted in this package.
//
// A temporal opt (ValidAt, ValidStart+ValidEnd, TxAt or TxPin) is answered
// exactly like RelsByType answers it — each relationship's version under opts,
// history included — not from the store's current rows (scan_temporal_opts.go).
// It is validated like RelsByType (ErrConflictingTemporalOpts, compaction and
// retention watermarks).
func (c *Core) ScanRelColumns(relType string, props []string, opts storepkg.QueryOpts,
	fn func(*storepkg.RelColumnBatch) bool) (ok bool, err error) {

	if c == nil {
		return false, nil
	}
	scanner, has := c.store.(storepkg.RelColumnScanCapability)
	temporal := hasTemporalFilter(opts)
	if temporal && has {
		if err := c.validateTemporalQueryOptsScan(opts); err != nil {
			return true, err
		}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if relType == "" {
		if !has {
			return false, nil
		}
		names := c.relTypes.ExportNames() // index = token; token 0 is reserved
		if temporal {
			return true, c.scanRelColumnsTemporalLocked(names[min(1, len(names)):], props, opts, fn)
		}
		for tok := 1; tok < len(names); tok++ {
			stopped := false
			name := names[tok]
			err := scanner.ScanRelColumns(uint16(tok), props, opts, func(b *storepkg.RelColumnBatch) bool { // #nosec G115 -- tok < len(names) <= the uint16 token space
				b.RelType = name
				if !fn(b) {
					stopped = true
					return false
				}
				return true
			})
			if err != nil || stopped {
				return true, err
			}
		}
		return true, nil
	}
	// TAKES A TYPE NAME, not a token, for the reason the node door documents: every
	// consumer-facing query here names its relationship type, and handing out the
	// token would make a caller reach for an interning API that is not public.
	token, known := c.relTypes.Lookup(relType)
	if !known {
		return true, nil // known capability, no such type: zero rows
	}
	if !has {
		return false, nil
	}
	if temporal {
		return true, c.scanRelColumnsTemporalLocked([]string{relType}, props, opts, fn)
	}
	return true, scanner.ScanRelColumns(token, props, opts, func(b *storepkg.RelColumnBatch) bool {
		b.RelType = relType
		return fn(b)
	})
}

// ScanRelSegments is the columnar door over a declared bulk relationship type
// (ADR-0011 §5.3, S5): every current row of relType in batches of segment
// columns (see store.RelSegmentBatch) from one snapshot, in ascending ID
// order.
// ok=false — fn is not called — when the backend has no segments, the type
// is not declared, or a requested property is not one of its declared
// columns; the caller then uses the row doors. An unknown type is ok with
// zero rows only if it is declared (declared types always exist).
func (c *Core) ScanRelSegments(relType string, props []string, fn func(*storepkg.RelSegmentBatch) bool) (ok bool, err error) {
	if c == nil {
		return false, nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return false, ErrGraphClosed
	}
	token, known := c.relTypes.Lookup(relType)
	if !known {
		return false, nil
	}
	scanner, has := c.store.(storepkg.RelSegmentScanCapability)
	if !has {
		return false, nil
	}
	return scanner.ScanRelSegments(token, props, fn)
}
