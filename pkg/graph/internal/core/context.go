package core

import (
	"context"
	"fmt"
	"math"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// nowInstant returns the current wall-clock time as a types.Instant (Unix
// milliseconds). Used by code paths that have no Core handle (notably
// the package-level test helpers and bootstrap paths). Mutation paths
// inside Core MUST use c.now() instead — that path consults c.clock,
// which tests can override (R4-F20).
func nowInstant() types.Instant {
	return types.Instant(time.Now().UnixMilli())
}

// now returns the current time according to c.clock, as a
// types.Instant (Unix milliseconds). Defaults to time.Now in
// production. Tests swap c.clock for a deterministic counter so two
// consecutive mutations yield strictly-increasing timestamps without
// the wall-clock sleeps that flake on loaded CI hardware (R4-F20).
//
// Callers in mutation paths (TxFrom / UpdatedAt / DeletedAt) and event
// timestamp paths use this method; only a handful of test-bootstrap
// call sites still use the package-level nowInstant, where the
// per-Core clock is unavailable.
func (c *Core) now() types.Instant {
	return types.Instant(c.clock().UnixMilli())
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

// extractTemporal removes the reserved temporal keys (tkg_valid_from,
// tkg_valid_to, tkg_created_at) from the props map and returns their values
// plus a filtered props map without those keys.
// If none of the reserved keys are present, the original map is returned
// unchanged (no allocation). The caller's original map is never mutated.
func extractTemporal(props map[string]any) (validFrom, validTo, createdAt types.Instant, filtered map[string]any, err error) {
	_, hasVF := props["tkg_valid_from"]
	_, hasVT := props["tkg_valid_to"]
	_, hasCA := props["tkg_created_at"]
	if !hasVF && !hasVT && !hasCA {
		return 0, 0, 0, props, nil
	}

	validFrom, err = parseInstant(props["tkg_valid_from"], "tkg_valid_from")
	if err != nil {
		return 0, 0, 0, nil, err
	}
	validTo, err = parseInstant(props["tkg_valid_to"], "tkg_valid_to")
	if err != nil {
		return 0, 0, 0, nil, err
	}
	createdAt, err = parseInstant(props["tkg_created_at"], "tkg_created_at")
	if err != nil {
		return 0, 0, 0, nil, err
	}
	if validFrom != 0 && validTo != 0 && validFrom >= validTo {
		return 0, 0, 0, nil, fmt.Errorf("%w: tkg_valid_from %d must be before tkg_valid_to %d",
			ErrInvalidTimeRange, validFrom, validTo)
	}

	filtered = make(map[string]any, len(props))
	for k, v := range props {
		if k != "tkg_valid_from" && k != "tkg_valid_to" && k != "tkg_created_at" {
			filtered[k] = v
		}
	}
	return validFrom, validTo, createdAt, filtered, nil
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
