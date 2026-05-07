package graph

import (
	"context"
	"fmt"
	"math"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// nowInstant returns the current time as a types.Instant (Unix milliseconds).
func nowInstant() types.Instant {
	return types.Instant(time.Now().UnixMilli())
}

// checkCtx performs a non-blocking context cancellation check.
// Returns ctx.Err() if the context is done, nil otherwise.
// Zero overhead when the context is not cancelled.
func checkCtx(ctx context.Context) error {
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

	filtered = make(map[string]any, len(props))
	for k, v := range props {
		if k != "tkg_valid_from" && k != "tkg_valid_to" && k != "tkg_created_at" {
			filtered[k] = v
		}
	}
	return validFrom, validTo, createdAt, filtered, nil
}

// parseInstant converts a property value to types.Instant (Unix milliseconds).
// Accepts nil (returns 0), int64, float64, int, and types.Instant.
func parseInstant(v any, key string) (types.Instant, error) {
	if v == nil {
		return 0, nil
	}
	switch val := v.(type) {
	case types.Instant:
		return val, nil
	case int64:
		return types.Instant(val), nil
	case int:
		return types.Instant(val), nil
	case float64:
		if val != math.Trunc(val) {
			return 0, fmt.Errorf("graph: %s %g is not an integer", key, val)
		}
		return types.Instant(val), nil
	default:
		return 0, fmt.Errorf("graph: %s must be a number (Unix ms), got %T", key, v)
	}
}

// extractProvenance removes the reserved provenance keys (tkg_author_id,
// tkg_signature, tkg_authorized_by, tkg_auth_level) from the props map and
// returns their values plus a filtered props map without those keys.
// If none of the reserved keys are present, the original map is returned
// unchanged (no allocation). The caller's original map is never mutated (B23).
// Returns an error if tkg_auth_level is out of [0, 255] or has an unsupported type.
func extractProvenance(props map[string]any) (authorID string, sig []byte, authorizedBy string, authLevel uint8, filtered map[string]any, err error) {
	_, hasA := props["tkg_author_id"]
	_, hasS := props["tkg_signature"]
	_, hasABy := props["tkg_authorized_by"]
	_, hasAL := props["tkg_auth_level"]
	if !hasA && !hasS && !hasABy && !hasAL {
		return "", nil, "", 0, props, nil
	}
	authorID, _ = props["tkg_author_id"].(string)
	sig, _ = props["tkg_signature"].([]byte)
	sig = types.CloneBytes(sig)
	authorizedBy, _ = props["tkg_authorized_by"].(string)
	// Accept uint8 and all integer types for JSON round-trip safety.
	// Bounds are checked explicitly to prevent silent truncation via modulo.
	switch v := props["tkg_auth_level"].(type) {
	case uint8:
		authLevel = v
	case int:
		if v < 0 || v > 255 {
			return "", nil, "", 0, nil, fmt.Errorf("graph: tkg_auth_level %d out of range [0, 255]", v)
		}
		authLevel = uint8(v)
	case int32:
		if v < 0 || v > 255 {
			return "", nil, "", 0, nil, fmt.Errorf("graph: tkg_auth_level %d out of range [0, 255]", v)
		}
		authLevel = uint8(v)
	case int64:
		if v < 0 || v > 255 {
			return "", nil, "", 0, nil, fmt.Errorf("graph: tkg_auth_level %d out of range [0, 255]", v)
		}
		authLevel = uint8(v)
	case float64:
		if v != math.Trunc(v) {
			return "", nil, "", 0, nil, fmt.Errorf("graph: tkg_auth_level %g is not an integer", v)
		}
		if v < 0 || v > 255 {
			return "", nil, "", 0, nil, fmt.Errorf("graph: tkg_auth_level %g out of range [0, 255]", v)
		}
		authLevel = uint8(v)
	default:
		if props["tkg_auth_level"] != nil {
			return "", nil, "", 0, nil, fmt.Errorf("graph: tkg_auth_level must be a number, got %T", props["tkg_auth_level"])
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
