package storeutil

import (
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// DecodeWireRowStamps reads a FULL (non-delta) node or relationship row's
// transaction stamps and version (store.RowStamps) without the full decode:
// the scanWireTemporalMeta token walk, the SafeUnmarshal partial decode where
// the walk declines. Zero stamps where the row carries no temporal block (the
// version is still read). A row written by a newer release fails with
// ErrWireFormatVersionUnsupported, a negative version with ErrCorruptWire,
// undecodable bytes with the decoder's error. It returns a value, so the
// scanner path allocates nothing. Backlog 42: badger's current-row stamps on
// an entity-cache miss; DecodeWireTxStamps is the same read without the
// version.
func DecodeWireRowStamps(raw []byte) (storepkg.RowStamps, error) {
	w, ok := scanWireTemporalMeta(raw)
	if !ok {
		var err error
		if w, err = decodeWireTemporalMetaSlow(raw); err != nil {
			return storepkg.RowStamps{}, err
		}
	}
	if err := checkRowFormatVersion("wire row stamps", w.FormatVersion); err != nil {
		return storepkg.RowStamps{}, err
	}
	if w.Version < 0 || int64(w.Version) > maxWireUint32 {
		return storepkg.RowStamps{}, fmt.Errorf("wire row stamps: version %d out of range: %w", w.Version, storepkg.ErrCorruptWire)
	}
	s := storepkg.RowStamps{Version: uint32(w.Version)} // #nosec G115 -- range checked above
	if w.HasTemporal {
		s.TxFrom, s.TxTo, s.DeletedAt = types.Instant(w.TxFrom), types.Instant(w.TxTo), types.Instant(w.DeletedAt)
	}
	return s, nil
}
