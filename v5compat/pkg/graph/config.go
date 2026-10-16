package graph

import (
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
)

// UntimedValidStartMode is v4's Config.UntimedValidStart.
type UntimedValidStartMode uint8

// Untimed modes. v5 writes an untimed row from -infinity (the v4.57.0 default);
// UntimedFromTxFrom is declined and only the importer maps it.
const (
	UntimedUnbounded UntimedValidStartMode = iota
	UntimedFromTxFrom
)

// Config mirrors the v4 knobs consumers set. Knobs with a v5 meaning are mapped
// (BadgerDir -> Dir, BadgerInMemory -> Memory, DurableCommit -> DurabilityCommit,
// AllowTxBackfill and AllowReset kept); knobs without one are accepted at their
// zero value and refused otherwise with ErrNotSupportedInV5 (see Declined).
type Config struct {
	// SnowflakeNodeID is accepted and ignored: v5 IDs carry no slot (the runtime
	// caller-ID doors are declined).
	SnowflakeNodeID uint16

	BadgerDir      string
	BadgerInMemory bool
	// Store selects a v4 backend; only store/memory.New is accepted.
	Store store.Store

	AllowTxBackfill bool
	AllowReset      bool
	// DurableCommit syncs every commit (v5 DurabilityCommit on a directory graph).
	DurableCommit bool

	// UntimedValidStart: only UntimedUnbounded.
	UntimedValidStart UntimedValidStartMode

	// Declined knobs: non-zero is ErrNotSupportedInV5.
	AllowRetentionPurge bool
	SyncWrites          bool
	CacheCapacity       int
	CacheBudgetBytes    int64
	ResidentCache       bool
	ReadOnlyReplica     bool
}

// Declined lists the non-zero declined knobs.
func (c Config) Declined() []string {
	var out []string
	add := func(on bool, name string) {
		if on {
			out = append(out, name)
		}
	}
	add(c.UntimedValidStart == UntimedFromTxFrom, "UntimedValidStart=UntimedFromTxFrom")
	add(c.AllowRetentionPurge, "AllowRetentionPurge")
	add(c.SyncWrites, "SyncWrites")
	add(c.CacheCapacity != 0, "CacheCapacity")
	add(c.CacheBudgetBytes != 0, "CacheBudgetBytes")
	add(c.ResidentCache, "ResidentCache")
	add(c.ReadOnlyReplica, "ReadOnlyReplica")
	if c.Store != nil && !c.Store.IsV5CompatMemory() {
		out = append(out, "Store")
	}
	return out
}

func (c Config) validate() error {
	if d := c.Declined(); len(d) > 0 {
		return fmt.Errorf("%w: %w: %v", ErrNotSupportedInV5, ErrDeclinedConfig, d)
	}
	if c.BadgerDir != "" && (c.BadgerInMemory || c.Store != nil) {
		return fmt.Errorf("graph: BadgerDir with BadgerInMemory or a Store is contradictory")
	}
	return nil
}

func (c Config) memory() bool { return c.BadgerDir == "" }
