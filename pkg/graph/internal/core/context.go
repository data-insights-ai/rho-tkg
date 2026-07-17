package core

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// nowInstant returns the current wall-clock time as a types.Instant (Unix
// milliseconds). Used by code paths that have no Core handle (notably
// the package-level test helpers and bootstrap paths). Mutation paths
// inside Core MUST use c.now() instead — that path consults c.clock,
// which tests can override.
func nowInstant() types.Instant {
	return types.Instant(time.Now().UnixMilli())
}

// now returns the current time according to c.clock, as a types.Instant (Unix
// milliseconds), with a per-Core monotonic floor. Defaults to time.Now in
// production, but still advances by at least 1ms when the wall clock repeats
// or moves backwards, so consecutive mutations cannot collapse transaction
// intervals into the same TxFrom/TxTo instant.
//
// Callers in mutation paths (TxFrom / UpdatedAt / DeletedAt) and event
// timestamp paths use this method; only a handful of test-bootstrap
// call sites still use the package-level nowInstant, where the
// per-Core clock is unavailable.
func (c *Core) now() types.Instant {
	observed := c.clock().UnixMilli()
	for {
		last := c.lastInstant.Load()
		next := observed
		if next <= last {
			next = last + 1
		}
		if c.lastInstant.CompareAndSwap(last, next) {
			return types.Instant(next)
		}
	}
}

// peekNow reads the current transaction-clock value WITHOUT reserving an instant —
// max(wall-now, the last handed-out floor), with NO CompareAndSwap. It is the
// observability read behind TempOps.PeekTx: a metrics/polling loop can sample it
// without burning instants (which c.now() would). It is NOT a sound as-of pin — the
// returned value can equal the instant a concurrent c.now() is about to reserve, so
// a read pinned here would include/exclude that write nondeterministically. Use
// NowTx() (or a value returned BY a write) for a sound pin.
func (c *Core) peekNow() types.Instant {
	observed := c.clock().UnixMilli()
	if last := c.lastInstant.Load(); last > observed {
		return types.Instant(last)
	}
	return types.Instant(observed)
}

// advanceClockFloor raises the per-Core monotonic transaction-clock floor to at
// least `to`, returning the resulting floor. It NEVER moves the clock backward:
// to <= the current floor is a no-op that returns the current floor.
//
// This is the Hybrid-Logical-Clock merge primitive for a distributed deployment.
// rho-tkg's transaction clock is per-machine; two machines' clocks are not
// globally ordered. A coordinator (sigma-tkgd) that observes a peer machine's
// transaction timestamp on an incoming message calls this before stamping any
// local write, so every subsequent local TxFrom is >= every peer timestamp seen
// — establishing a causal (happens-before) order across machines WITHOUT a
// central sequencer. rho-tkg only exposes the seam; the HLC bookkeeping (what to
// advance to, and when) is the coordinator's responsibility.
func (c *Core) advanceClockFloor(to types.Instant) types.Instant {
	target := int64(to)
	for {
		last := c.lastInstant.Load()
		if target <= last {
			return types.Instant(last)
		}
		if c.lastInstant.CompareAndSwap(last, target) {
			return types.Instant(target)
		}
	}
}

// checkCtx performs a non-blocking context cancellation check.
// Returns ctx.Err() if the context is done, nil otherwise.
// Zero overhead when the context is not cancelled.
func checkCtx(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// resolveBackfillTxFrom validates a caller-supplied transaction-time override
// from a create door (the tkg_tx_from property or AddWithTx). It returns 0 —
// "no override, stamp the system clock" — when txFrom is 0. Otherwise the value
// must be a positive instant NOT IN THE FUTURE (ErrInvalidTxFrom): a knowledge /
// transaction time is "when the DB learned a fact", which cannot exceed the
// wall clock at write — the feature is BACKfill, and a future value (e.g. from
// Unix-ms vs the snowflake layer's microsecond unit confusion) would produce an
// inverted TX interval on the version once it is superseded, invisible to every
// AS-OF query yet passing Verify*Chain (TxFrom is not hashed). A valid override
// is honored ONLY when the graph was opened with Config.AllowTxBackfill,
// otherwise ErrTxBackfillDisabled. Value validation precedes the gate check so a
// malformed value is rejected as malformed regardless of privilege. Backfill is
// create-only and TxFrom is not part of the integrity hash, so honoring a valid
// override never affects the hash chain (§4.1).
func (c *Core) resolveBackfillTxFrom(txFrom types.Instant) (types.Instant, error) {
	if txFrom == 0 {
		return 0, nil
	}
	if txFrom < 0 || txFrom > c.now() {
		return 0, ErrInvalidTxFrom
	}
	if !c.allowTxBackfill {
		return 0, ErrTxBackfillDisabled
	}
	return txFrom, nil
}

// withBackfillTxFrom returns a shallow copy of props with tkg_tx_from set to
// txFrom, so the AddWithTx ergonomic doors reuse the standard create path (the
// gate is enforced there via resolveBackfillTxFrom). The caller's map is never
// mutated; an explicit txFrom overrides any tkg_tx_from already present.
func withBackfillTxFrom(props map[string]any, txFrom types.Instant) map[string]any {
	out := make(map[string]any, len(props)+1)
	for k, v := range props {
		out[k] = v
	}
	out["tkg_tx_from"] = txFrom
	return out
}

// extractTemporal removes the reserved temporal keys (tkg_valid_from,
// tkg_valid_to, tkg_created_at, tkg_tx_from) from the props map and returns
// their values plus a filtered props map without those keys.
// If none of the reserved keys are present, the original map is returned
// unchanged (no allocation). The caller's original map is never mutated.
//
// tkg_tx_from is the transaction-time backfill override: extraction here is
// pure (no gate check — extractTemporal has no Core handle). Each create door
// funnels the returned txFrom through c.resolveBackfillTxFrom, which enforces
// Config.AllowTxBackfill and rejects it in production. A returned txFrom of 0
// means "unset — stamp the system clock" (§4.1).
func extractTemporal(props map[string]any) (validFrom, validTo, createdAt, txFrom types.Instant, filtered map[string]any, err error) {
	_, hasVF := props["tkg_valid_from"]
	_, hasVT := props["tkg_valid_to"]
	_, hasCA := props["tkg_created_at"]
	_, hasTF := props["tkg_tx_from"]
	if !hasVF && !hasVT && !hasCA && !hasTF {
		return 0, 0, 0, 0, props, nil
	}

	validFrom, err = parseInstant(props["tkg_valid_from"], "tkg_valid_from")
	if err != nil {
		return 0, 0, 0, 0, nil, err
	}
	validTo, err = parseInstant(props["tkg_valid_to"], "tkg_valid_to")
	if err != nil {
		return 0, 0, 0, 0, nil, err
	}
	createdAt, err = parseInstant(props["tkg_created_at"], "tkg_created_at")
	if err != nil {
		return 0, 0, 0, 0, nil, err
	}
	txFrom, err = parseInstant(props["tkg_tx_from"], "tkg_tx_from")
	if err != nil {
		return 0, 0, 0, 0, nil, err
	}
	if validFrom != 0 && validTo != 0 && validFrom >= validTo {
		return 0, 0, 0, 0, nil, fmt.Errorf("%w: tkg_valid_from %d must be before tkg_valid_to %d",
			ErrInvalidTimeRange, validFrom, validTo)
	}

	filtered = make(map[string]any, len(props))
	for k, v := range props {
		if k != "tkg_valid_from" && k != "tkg_valid_to" && k != "tkg_created_at" && k != "tkg_tx_from" {
			filtered[k] = v
		}
	}
	return validFrom, validTo, createdAt, txFrom, filtered, nil
}

// extractTemporalTracked is the update-path mirror of extractTemporal. Returns
// per-field presence flags so update paths can distinguish "caller did not
// supply" from "caller supplied 0". tkg_created_at is rejected on update paths
// (it's per-entity, not per-version); only tkg_valid_from and tkg_valid_to are
// accepted.
func extractTemporalTracked(props map[string]any) (updateTemporal, map[string]any, error) {
	_, hasVF := props["tkg_valid_from"]
	_, hasVT := props["tkg_valid_to"]
	if !hasVF && !hasVT {
		return updateTemporal{}, props, nil
	}

	tmp := updateTemporal{
		hasValidFrom: hasVF,
		hasValidTo:   hasVT,
		present:      true,
	}
	if hasVF {
		v, err := parseInstant(props["tkg_valid_from"], "tkg_valid_from")
		if err != nil {
			return updateTemporal{}, nil, err
		}
		tmp.validFrom = v
	}
	if hasVT {
		v, err := parseInstant(props["tkg_valid_to"], "tkg_valid_to")
		if err != nil {
			return updateTemporal{}, nil, err
		}
		tmp.validTo = v
	}
	if tmp.hasValidFrom && tmp.hasValidTo && tmp.validFrom != 0 && tmp.validTo != 0 && tmp.validFrom >= tmp.validTo {
		return updateTemporal{}, nil, fmt.Errorf("%w: tkg_valid_from %d must be before tkg_valid_to %d",
			ErrInvalidTimeRange, tmp.validFrom, tmp.validTo)
	}

	filtered := make(map[string]any, len(props))
	for k, v := range props {
		if k != "tkg_valid_from" && k != "tkg_valid_to" {
			filtered[k] = v
		}
	}
	return tmp, filtered, nil
}

// parseInstant converts a property value to types.Instant (Unix milliseconds).
func parseInstant(v any, key string) (types.Instant, error) {
	if v == nil {
		return 0, nil
	}
	switch val := v.(type) {
	case types.Instant:
		return val, nil
	case int:
		return instantFromSigned(int64(val)), nil
	case int8:
		return instantFromSigned(int64(val)), nil
	case int16:
		return instantFromSigned(int64(val)), nil
	case int32:
		return instantFromSigned(int64(val)), nil
	case int64:
		return instantFromSigned(val), nil
	case uint:
		return instantFromUnsigned(uint64(val), key)
	case uint8:
		return instantFromUnsigned(uint64(val), key)
	case uint16:
		return instantFromUnsigned(uint64(val), key)
	case uint32:
		return instantFromUnsigned(uint64(val), key)
	case uint64:
		return instantFromUnsigned(val, key)
	case float32:
		return instantFromFloat32(val, key)
	case float64:
		return instantFromFloat64(val, key)
	default:
		return 0, fmt.Errorf("graph: %s must be a number (Unix ms), got %T", key, v)
	}
}

const (
	maxInt64Value      = int64(^uint64(0) >> 1)
	maxExactFloat32Int = float64(1 << 24)
	minExactFloat32Int = -maxExactFloat32Int
	maxExactFloat64Int = float64(1 << 53)
	minExactFloat64Int = -maxExactFloat64Int
	instantRangeLabel  = "exact int64 millisecond range"
)

func instantFromSigned(v int64) types.Instant {
	return types.Instant(v)
}

func instantFromUnsigned(v uint64, key string) (types.Instant, error) {
	if v > uint64(maxInt64Value) {
		return 0, fmt.Errorf("graph: %s %d outside %s", key, v, instantRangeLabel)
	}
	return types.Instant(int64(v)), nil
}

func instantFromFloat32(v float32, key string) (types.Instant, error) {
	return instantFromFloat(float64(v), minExactFloat32Int, maxExactFloat32Int, key)
}

func instantFromFloat64(v float64, key string) (types.Instant, error) {
	return instantFromFloat(v, minExactFloat64Int, maxExactFloat64Int, key)
}

func instantFromFloat(v, minExact, maxExact float64, key string) (types.Instant, error) {
	if v != math.Trunc(v) {
		return 0, fmt.Errorf("graph: %s %g is not an integer", key, v)
	}
	if v < minExact || v > maxExact {
		return 0, fmt.Errorf("graph: %s %g outside %s", key, v, instantRangeLabel)
	}
	return types.Instant(int64(v)), nil
}

// extractProvenance removes the reserved provenance keys (tkg_author_id,
// tkg_signature, tkg_authorized_by, tkg_auth_level) from the props map and
// returns their values plus a filtered props map without those keys.
// If none of the reserved keys are present, the original map is returned
// unchanged (no allocation). The caller's original map is never mutated (B23).
// Returns an error if any reserved provenance value has an unsupported type
// or tkg_auth_level is out of [0, 255].
func extractProvenance(props map[string]any) (authorID string, sig []byte, authorizedBy string, authLevel uint8, filtered map[string]any, err error) {
	_, hasA := props["tkg_author_id"]
	_, hasS := props["tkg_signature"]
	_, hasABy := props["tkg_authorized_by"]
	_, hasAL := props["tkg_auth_level"]
	if !hasA && !hasS && !hasABy && !hasAL {
		return "", nil, "", 0, props, nil
	}
	if hasA {
		v := props["tkg_author_id"]
		if v != nil {
			s, ok := v.(string)
			if !ok {
				return "", nil, "", 0, nil, fmt.Errorf("graph: tkg_author_id must be a string, got %T", v)
			}
			authorID = s
		}
	}
	if hasS {
		v := props["tkg_signature"]
		if v != nil {
			b, ok := v.([]byte)
			if !ok {
				return "", nil, "", 0, nil, fmt.Errorf("graph: tkg_signature must be []byte, got %T", v)
			}
			sig = types.CloneBytes(b)
		}
	}
	if hasABy {
		v := props["tkg_authorized_by"]
		if v != nil {
			s, ok := v.(string)
			if !ok {
				return "", nil, "", 0, nil, fmt.Errorf("graph: tkg_authorized_by must be a string, got %T", v)
			}
			authorizedBy = s
		}
	}
	if hasAL {
		authLevel, err = parseAuthLevel(props["tkg_auth_level"])
		if err != nil {
			return "", nil, "", 0, nil, err
		}
	}
	filtered = make(map[string]any, len(props))
	for k, v := range props {
		if k != "tkg_author_id" && k != "tkg_signature" && k != "tkg_authorized_by" && k != "tkg_auth_level" {
			filtered[k] = v
		}
	}
	return authorID, sig, authorizedBy, authLevel, filtered, nil
}

// extractProvenanceTracked is like extractProvenance but returns per-field
// presence flags via updateProvenance, so update paths can merge with the
// base entity's existing integrity (B3 — preserve fields the caller did
// not restate). The legacy extractProvenance return shape is preserved
// for callers that don't need merging (e.g. node_add, where there is no
// previous integrity).
func extractProvenanceTracked(props map[string]any) (updateProvenance, map[string]any, error) {
	_, hasA := props["tkg_author_id"]
	_, hasS := props["tkg_signature"]
	_, hasABy := props["tkg_authorized_by"]
	_, hasAL := props["tkg_auth_level"]
	if !hasA && !hasS && !hasABy && !hasAL {
		return updateProvenance{}, props, nil
	}

	prov := updateProvenance{
		hasAuthorID:     hasA,
		hasSignature:    hasS,
		hasAuthorizedBy: hasABy,
		hasAuthLevel:    hasAL,
		present:         true,
	}
	if hasA {
		if v := props["tkg_author_id"]; v != nil {
			s, ok := v.(string)
			if !ok {
				return updateProvenance{}, nil, fmt.Errorf("graph: tkg_author_id must be a string, got %T", v)
			}
			prov.authorID = s
		}
	}
	if hasS {
		if v := props["tkg_signature"]; v != nil {
			b, ok := v.([]byte)
			if !ok {
				return updateProvenance{}, nil, fmt.Errorf("graph: tkg_signature must be []byte, got %T", v)
			}
			prov.signature = types.CloneBytes(b)
		}
	}
	if hasABy {
		if v := props["tkg_authorized_by"]; v != nil {
			s, ok := v.(string)
			if !ok {
				return updateProvenance{}, nil, fmt.Errorf("graph: tkg_authorized_by must be a string, got %T", v)
			}
			prov.authorizedBy = s
		}
	}
	if hasAL {
		al, err := parseAuthLevel(props["tkg_auth_level"])
		if err != nil {
			return updateProvenance{}, nil, err
		}
		prov.authLevel = al
	}

	filtered := make(map[string]any, len(props))
	for k, v := range props {
		if k != "tkg_author_id" && k != "tkg_signature" && k != "tkg_authorized_by" && k != "tkg_auth_level" {
			filtered[k] = v
		}
	}
	return prov, filtered, nil
}

func parseAuthLevel(v any) (uint8, error) {
	if v == nil {
		return 0, nil
	}
	switch val := v.(type) {
	case int:
		return authLevelFromSigned(int64(val))
	case int8:
		return authLevelFromSigned(int64(val))
	case int16:
		return authLevelFromSigned(int64(val))
	case int32:
		return authLevelFromSigned(int64(val))
	case int64:
		return authLevelFromSigned(val)
	case uint:
		return authLevelFromUnsigned(uint64(val))
	case uint8:
		return val, nil
	case uint16:
		return authLevelFromUnsigned(uint64(val))
	case uint32:
		return authLevelFromUnsigned(uint64(val))
	case uint64:
		return authLevelFromUnsigned(val)
	case float32:
		return authLevelFromFloat(float64(val))
	case float64:
		return authLevelFromFloat(val)
	default:
		return 0, fmt.Errorf("graph: tkg_auth_level must be a number, got %T", v)
	}
}

func authLevelFromSigned(v int64) (uint8, error) {
	if v < 0 || v > 255 {
		return 0, fmt.Errorf("graph: tkg_auth_level %d out of range [0, 255]", v)
	}
	return uint8(v), nil
}

func authLevelFromUnsigned(v uint64) (uint8, error) {
	if v > 255 {
		return 0, fmt.Errorf("graph: tkg_auth_level %d out of range [0, 255]", v)
	}
	return uint8(v), nil
}

func authLevelFromFloat(v float64) (uint8, error) {
	if v != math.Trunc(v) {
		return 0, fmt.Errorf("graph: tkg_auth_level %g is not an integer", v)
	}
	if v < 0 || v > 255 {
		return 0, fmt.Errorf("graph: tkg_auth_level %g out of range [0, 255]", v)
	}
	return uint8(v), nil
}
