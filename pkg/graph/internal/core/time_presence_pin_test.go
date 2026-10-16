package core

import (
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ledger row 63: a present pin at 0 reaches the tx-time and retention code
// as -1 (that code reads 0 as "no pin"); unflagged shapes keep their
// pre-v4.59.0 pins.
func TestPinInstantPresence(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name                     string
		opts                     storepkg.QueryOpts
		txAt, txPin, tx, retains types.Instant
	}{
		{"none", storepkg.QueryOpts{}, 0, 0, 0, 0},
		{"TxAt 0 unset", storepkg.QueryOpts{ValidAt: 5}, 0, 0, 0, 5},
		{"TxAt 0 set", storepkg.QueryOpts{TxAtSet: true}, -1, 0, -1, -1},
		{"TxAt -5", storepkg.QueryOpts{TxAt: -5}, -5, 0, -5, -5},
		{"TxAt 9 set", storepkg.QueryOpts{TxAt: 9, TxAtSet: true}, 9, 0, 9, 9},
		{"TxPin 0 set", storepkg.QueryOpts{TxPinSet: true}, 0, -1, -1, -1},
		{"TxPin beats TxAt", storepkg.QueryOpts{TxPin: 7, TxAt: 9}, 9, 7, 7, 7},
		{"ValidAt 0 set", storepkg.QueryOpts{ValidAtSet: true}, 0, 0, 0, -1},
		{"ValidAt -3", storepkg.QueryOpts{ValidAt: -3}, 0, 0, 0, -3},
		{"window [0, 5) set", storepkg.QueryOpts{ValidWindowSet: true, ValidEnd: 5}, 0, 0, 0, -1},
		{"window [-4, 0) set", storepkg.QueryOpts{ValidWindowSet: true, ValidStart: -4}, 0, 0, 0, -4},
		{"lone ValidStart unset", storepkg.QueryOpts{ValidStart: 4}, 0, 0, 0, 4},
		{"lone ValidEnd unset", storepkg.QueryOpts{ValidEnd: 4}, 0, 0, 0, 0},
		{"ValidAt beats the window", storepkg.QueryOpts{ValidAtSet: true, ValidStart: 3, ValidEnd: 4}, 0, 0, 0, -1},
	} {
		if got := txAtOf(c.opts); got != c.txAt {
			t.Errorf("%s: txAtOf = %d, want %d", c.name, got, c.txAt)
		}
		if got := txPinOf(c.opts); got != c.txPin {
			t.Errorf("%s: txPinOf = %d, want %d", c.name, got, c.txPin)
		}
		if got := effectiveTxPin(c.opts); got != c.tx {
			t.Errorf("%s: effectiveTxPin = %d, want %d", c.name, got, c.tx)
		}
		if got := scanRetentionPin(c.opts); got != c.retains {
			t.Errorf("%s: scanRetentionPin = %d, want %d", c.name, got, c.retains)
		}
	}
}
