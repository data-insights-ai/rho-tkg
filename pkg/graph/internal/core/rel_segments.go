package core

import (
	"fmt"
	"strings"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// Column segments for declared bulk relationship types (ADR-0011 S2).
//
// Config.RelSegments names the types; New resolves each name to its type token
// (creating the token when the type is new — a deliberate registry entry, like
// an index definition) and hands the declaration to the store's
// RelSegmentCapability. A store without the capability fails New closed: a
// caller who declared a type expects its memory bound, and silently keeping
// every row in the row store would break that expectation without a word.

// declareRelSegments is called once from New after the store and the
// registries are in place.
func (c *Core) declareRelSegments(specs []storepkg.RelSegmentSpec, budget int64, dir string) error {
	if budget < 0 {
		return fmt.Errorf("graph: SegmentMemoryBudget %d: %w", budget, storepkg.ErrRelSegmentDeclaration)
	}
	if dir != "" && strings.TrimSpace(dir) == "" {
		return fmt.Errorf("graph: SegmentDir is whitespace-only: %w", storepkg.ErrRelSegmentDeclaration)
	}
	if len(specs) == 0 {
		if dir != "" {
			return fmt.Errorf("graph: SegmentDir %q without RelSegments: %w", dir, storepkg.ErrRelSegmentDeclaration)
		}
		return nil
	}
	segCap, ok := c.store.(storepkg.RelSegmentCapability)
	if !ok {
		return fmt.Errorf("graph: RelSegments: %w", storepkg.ErrCapabilityNotSupported)
	}
	seen := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		if err := storepkg.ValidateRelSegmentSpec(spec); err != nil {
			return fmt.Errorf("graph: RelSegments: %w", err)
		}
		if err := c.validateName(spec.Type); err != nil {
			return fmt.Errorf("graph: RelSegments: type %q: %w: %w", spec.Type, storepkg.ErrRelSegmentDeclaration, err)
		}
		if _, dup := seen[spec.Type]; dup {
			return fmt.Errorf("graph: RelSegments: type %q declared twice: %w", spec.Type, storepkg.ErrRelSegmentDeclaration)
		}
		seen[spec.Type] = struct{}{}
		for _, col := range spec.Columns {
			if err := c.validatePropertyKeyLength(col.Name); err != nil {
				return fmt.Errorf("graph: RelSegments: type %q column %q: %w: %w", spec.Type, col.Name, storepkg.ErrRelSegmentDeclaration, err)
			}
		}
	}
	c.registryMu.Lock()
	defer c.registryMu.Unlock()
	for _, spec := range specs {
		tok, err := c.relTypes.GetOrCreate(spec.Type)
		if err != nil {
			return fmt.Errorf("graph: RelSegments: type %q: %w", spec.Type, err)
		}
		if err := segCap.DeclareRelSegment(storepkg.RelSegmentDeclaration{
			TypeName:           spec.Type,
			TypeToken:          tok,
			Columns:            append([]storepkg.SegmentColumn(nil), spec.Columns...),
			IntegrityBlockRows: spec.IntegrityBlockRows,
			MemtableBudget:     budget,
		}); err != nil {
			return fmt.Errorf("graph: RelSegments: type %q: %w", spec.Type, err)
		}
	}
	if err := c.persistRegistries(); err != nil {
		return err
	}
	if dir == "" {
		return nil
	}
	dirCap, ok := c.store.(storepkg.RelSegmentDirCapability)
	if !ok {
		return fmt.Errorf("graph: SegmentDir: %w", storepkg.ErrCapabilityNotSupported)
	}
	if err := dirCap.OpenRelSegmentDir(dir); err != nil {
		return fmt.Errorf("graph: SegmentDir %q: %w", dir, err)
	}
	return nil
}

// relSegmentTarget resolves a declared type name for the admin doors.
func (c *Core) relSegmentTarget(relType string) (storepkg.RelSegmentCapability, uint16, error) {
	if err := c.validateRelTypeQueryName(relType); err != nil {
		return nil, 0, err
	}
	segCap, ok := c.store.(storepkg.RelSegmentCapability)
	if !ok {
		return nil, 0, fmt.Errorf("graph: relationship segments: %w", storepkg.ErrCapabilityNotSupported)
	}
	tok, ok := c.lookupRelTypeQueryToken(relType)
	if !ok {
		return nil, 0, fmt.Errorf("graph: relationship type %q: %w", relType, storepkg.ErrRelSegmentNotDeclared)
	}
	return segCap, tok, nil
}

// SealRelSegments seals every unsealed row of a declared bulk relationship
// type now, regardless of the memtable budget (ADR-0011 §3.1). A seal is a
// physical move: no read door's answer changes and no change-log record is
// written. Fails with ErrRelSegmentNotDeclared for an undeclared type and
// ErrCapabilityNotSupported on a backend without segments.
func (a *AdminOps) SealRelSegments(relType string) error {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	return c.readUnderRLock(func() error {
		segCap, tok, err := c.relSegmentTarget(relType)
		if err != nil {
			return err
		}
		return segCap.SealRelSegments(tok)
	})
}

// RelSegmentStats reports a declared bulk relationship type's physical layout
// (segments, sealed and unsealed rows, bytes). A measurement surface only.
func (a *AdminOps) RelSegmentStats(relType string) (storepkg.RelSegmentStats, error) {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return storepkg.RelSegmentStats{}, err
	}
	var st storepkg.RelSegmentStats
	err := c.readUnderRLock(func() error {
		segCap, tok, err := c.relSegmentTarget(relType)
		if err != nil {
			return err
		}
		st, err = segCap.RelSegmentStats(tok)
		return err
	})
	return st, err
}
