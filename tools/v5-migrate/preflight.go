// Package migrate maps retained v4 effective state at a source knowledge-time
// pin to the existing v5 Q/ms value contract. It writes no destination data.
package migrate

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	v4graph "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	v4types "github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	v5types "github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

var (
	// ErrInvalidSource refuses missing source identity or evidence metadata.
	ErrInvalidSource = errors.New("migrate: invalid source provenance")
	// ErrAmbiguousHistory refuses semantic admission while retaining the report
	// and source evidence. A successful source hash check does not prove that
	// history is complete or that a temporal/retraction field was preserved.
	ErrAmbiguousHistory = errors.New("migrate: ambiguous retained source history")
)

// EntityKind keeps node and relationship identity namespaces distinct.
type EntityKind uint8

// Entity kinds name the two v4 graph identity namespaces.
const (
	NodeKind EntityKind = iota + 1
	RelationshipKind
)

// Source records caller-supplied provenance, never inferred from export dates.
// ID identifies this source capture; Reader identifies the actual source-reader
// revision/build selected by the caller. Writer/Format may be unknown (empty).
// Evidence names the external capture/provenance evidence, not a v5 certificate.
type Source struct {
	ID, Reader, Writer, Format, Evidence string
}

// RowIdentity binds an attestation to one exact source row. Temporal contains
// every raw v4 temporal/audit field, including BaseEntityID and Retracted. v4's
// content hash alone cannot bind those fields; neither can a version number.
type RowIdentity struct {
	Kind           EntityKind
	ID             v4types.EntityID
	Version        uint32
	Hash, PrevHash string
	HasTemporal    bool
	Temporal       v4types.TemporalMetadata
	// Audit retains the exact non-content-hashed integrity evidence as part
	// of the attestation key, independently of Hash and raw temporal fields.
	Audit RowAuditIdentity
}

// RowAuditIdentity is a comparable exact key for the raw integrity audit fields.
// Signature is a raw byte string (not text/base64); HasSignature distinguishes a
// nil signature from a present empty byte slice. EvidenceRow retains []byte.
type RowAuditIdentity struct {
	AuthorID, AuthorizedBy   string
	AuthorizationLevel       uint8
	Signature                string
	HasSignature             bool
	FromNodeHash, ToNodeHash string
}

// SpanAttestation is evidence ALREADY VERIFIED by the caller to establish that
// the identified row's writer/format did not encode one-tick eclipse sentinels.
// This package checks exact source/row binding and presence of writer/format/
// evidence identity; it cannot authenticate a writer or verify an external
// document. A release number or a creation date alone is insufficient evidence.
type SpanAttestation struct {
	SourceID                 string
	Row                      RowIdentity
	Writer, Format, Evidence string
	ExcludesOneTickSentinels bool
}

// Options selects the explicit source pin and graph-qualified v5 default axis.
// No database cut or global transaction group is constructed from Pin.
type Options struct {
	Source        Source
	Pin           v4types.Instant
	DefaultAxis   v5types.DefaultAxisBinding
	Limits        temporal.Limits
	VerifiedSpans []SpanAttestation
}

// EvidenceRow is an independently owned frozen raw source row. Only the field
// matching Identity.Kind is populated. Labels/Type retain source registry names;
// the row retains concrete property kinds, IDs/endpoints, temporal/audit fields,
// original hashes/signatures and the belief-retraction marker. MintInstant is
// the ACTUAL v4 ID-derived Unix-millisecond start, separate from raw ValidFrom.
type EvidenceRow struct {
	Identity    RowIdentity
	MintInstant v4types.Instant
	Node        *v4types.Node
	Rel         *v4types.Relationship
	Labels      []string
	Type        string
}

// Segment retains one source oracle piece. Row is the reader-normalized state
// at Pin, distinct from Rows' raw stored evidence (v4 may undo later tombstones
// for older pins). RawFrom/RawTo are oracle bounds, with RawTo==0 open-ended.
// Scope is invalid whenever Findings is nonempty: refused state must not be
// imported accidentally. Otherwise it is the exact Q/ms half-open support.
type Segment struct {
	RawFrom, RawTo v4types.Instant
	Row            EvidenceRow
	Scope          temporal.Scope
}

// FindingKind identifies observed loss or ambiguity without inventing missing
// source history or classifying a one-tick row as an event.
type FindingKind string

// Finding kinds identify the specific observed admission blockers.
const (
	OneTickProvenanceUnknown FindingKind = "one-tick-provenance-unknown"
	VersionCollision         FindingKind = "version-collision"
	HistoryGap               FindingKind = "history-gap-or-missing-genesis"
	InvertedTransactionRange FindingKind = "inverted-transaction-range"
	IntegrityUnverified      FindingKind = "integrity-check-failed"
)

// Finding identifies the raw evidence row responsible for a refusal.
type Finding struct {
	Kind FindingKind
	// Row is the index in Report.Rows; -1 denotes the entity-level verifier.
	Row int
}

// Report is a retained-state preflight, NOT proof of complete original history,
// a destination import, certified cut, writer authenticity or durability. Rows
// retain ALL available raw evidence, including rows recorded after Pin; Segments
// alone describe state at Pin. Source and accepted attestations remain distinct.
type Report struct {
	Source              Source
	Pin                 v4types.Instant
	DefaultAxis         v5types.DefaultAxisBinding
	Rows                []EvidenceRow
	Segments            []Segment
	Findings            []Finding
	AcceptedSpans       []SpanAttestation
	SourceChainVerified bool
}

// PreflightNode maps a node's retained effective state at opts.Pin. The caller
// MUST quiesce mutation, import, retention and DDL for the source across this
// call: the v4 public reads are separate operations, not an atomic export view.
// It materializes this entity's retained history using the v4 reader; this is
// not a streaming or upstream memory/work bound. Known ambiguity returns the
// owned report plus ErrAmbiguousHistory; source/admission errors return a zero
// report and preserve source sentinel matching through errors.Is.
func PreflightNode(g *v4graph.Graph, id v4types.NodeID, opts Options) (Report, error) {
	if g == nil {
		return Report{}, v4graph.ErrNilGraph
	}
	if err := validateOptions(opts); err != nil {
		return Report{}, err
	}
	timeline, err := g.Temporal().NodeEffectiveTimeline(id, opts.Pin)
	if err != nil {
		return Report{}, fmt.Errorf("node effective timeline: %w", err)
	}
	history, err := g.Nodes().History(id)
	if err != nil {
		return Report{}, fmt.Errorf("node raw history: %w", err)
	}
	current, err := g.Nodes().Get(context.Background(), id)
	if err != nil && !errors.Is(err, v4graph.ErrNodeNotFound) {
		return Report{}, fmt.Errorf("node raw current: %w", err)
	}
	verified, err := g.Hash().VerifyNodeChain(id)
	if err != nil {
		return Report{}, fmt.Errorf("node source chain: %w", err)
	}
	r := newReport(opts, verified)
	for _, n := range history {
		r.Rows = append(r.Rows, nodeEvidence(g, n))
	}
	if current != nil {
		r.Rows = append(r.Rows, nodeEvidence(g, current))
	}
	for _, s := range timeline {
		r.Segments = append(r.Segments, Segment{RawFrom: s.ValidFrom, RawTo: s.ValidTo, Row: nodeEvidence(g, s.Node)})
	}
	return finish(r, opts)
}

// PreflightRel is PreflightNode's relationship mirror. Its state is DECLARED
// relationship validity, exactly as RelEffectiveTimeline; endpoint-life masking
// is a separate consumer view and is never silently applied here.
func PreflightRel(g *v4graph.Graph, id v4types.RelID, opts Options) (Report, error) {
	if g == nil {
		return Report{}, v4graph.ErrNilGraph
	}
	if err := validateOptions(opts); err != nil {
		return Report{}, err
	}
	timeline, err := g.Temporal().RelEffectiveTimeline(id, opts.Pin)
	if err != nil {
		return Report{}, fmt.Errorf("relationship effective timeline: %w", err)
	}
	history, err := g.Rels().History(id)
	if err != nil {
		return Report{}, fmt.Errorf("relationship raw history: %w", err)
	}
	current, err := g.Rels().Get(context.Background(), id)
	if err != nil && !errors.Is(err, v4graph.ErrRelNotFound) {
		return Report{}, fmt.Errorf("relationship raw current: %w", err)
	}
	verified, err := g.Hash().VerifyRelChain(id)
	if err != nil {
		return Report{}, fmt.Errorf("relationship source chain: %w", err)
	}
	r := newReport(opts, verified)
	for _, rel := range history {
		r.Rows = append(r.Rows, relEvidence(g, rel))
	}
	if current != nil {
		r.Rows = append(r.Rows, relEvidence(g, current))
	}
	for _, s := range timeline {
		r.Segments = append(r.Segments, Segment{RawFrom: s.ValidFrom, RawTo: s.ValidTo, Row: relEvidence(g, s.Rel)})
	}
	return finish(r, opts)
}

func validateOptions(o Options) error {
	if strings.TrimSpace(o.Source.ID) == "" || strings.TrimSpace(o.Source.Reader) == "" || strings.TrimSpace(o.Source.Evidence) == "" {
		return ErrInvalidSource
	}
	return o.DefaultAxis.Check(o.DefaultAxis.Graph(), o.DefaultAxis.Axis(), o.Limits)
}

func newReport(o Options, verified bool) Report {
	return Report{Source: o.Source, Pin: o.Pin, DefaultAxis: o.DefaultAxis, SourceChainVerified: verified}
}

func nodeEvidence(g *v4graph.Graph, n *v4types.Node) EvidenceRow {
	n = n.DeepCopy()
	n.Freeze()
	ig := n.Integrity()
	var hash, prev string
	if ig != nil {
		hash, prev = ig.Hash, ig.PrevHash
	}
	identity := rowIdentity(NodeKind, v4types.EntityID(n.ID()), n.Version(), n.Temporal(), hash, prev)
	if ig != nil {
		identity.Audit = RowAuditIdentity{AuthorID: ig.AuthorID, AuthorizedBy: ig.AuthorizedBy, AuthorizationLevel: ig.AuthorizationLevel, Signature: string(ig.Signature), HasSignature: ig.Signature != nil}
	}
	return EvidenceRow{Identity: identity, MintInstant: n.ID().MintInstant(), Node: n, Labels: g.Nodes().Labels(n)}
}

func relEvidence(g *v4graph.Graph, r *v4types.Relationship) EvidenceRow {
	r = r.DeepCopy()
	r.Freeze()
	ig := r.Integrity()
	var hash, prev string
	if ig != nil {
		hash, prev = ig.Hash, ig.PrevHash
	}
	identity := rowIdentity(RelationshipKind, v4types.EntityID(r.ID()), r.Version(), r.Temporal(), hash, prev)
	if ig != nil {
		identity.Audit = RowAuditIdentity{AuthorID: ig.AuthorID, AuthorizedBy: ig.AuthorizedBy, AuthorizationLevel: ig.AuthorizationLevel, Signature: string(ig.Signature), HasSignature: ig.Signature != nil, FromNodeHash: ig.FromNodeHash, ToNodeHash: ig.ToNodeHash}
	}
	return EvidenceRow{Identity: identity, MintInstant: r.ID().MintInstant(), Rel: r, Type: g.Rels().Type(r)}
}

func rowIdentity(kind EntityKind, id v4types.EntityID, version uint32, tm *v4types.TemporalMetadata, hash, prev string) RowIdentity {
	r := RowIdentity{Kind: kind, ID: id, Version: version, Hash: hash, PrevHash: prev, HasTemporal: tm != nil}
	if tm != nil {
		r.Temporal = *tm
	}
	return r
}

func finish(r Report, o Options) (Report, error) {
	versions := make(map[uint32]int, len(r.Rows))
	for i, row := range r.Rows {
		if _, exists := versions[row.Identity.Version]; exists {
			r.Findings = append(r.Findings, Finding{VersionCollision, i})
		}
		versions[row.Identity.Version] = i
		tm := row.Identity.Temporal
		if tm.TxTo != 0 && tm.TxTo < tm.TxFrom {
			r.Findings = append(r.Findings, Finding{InvertedTransactionRange, i})
		}
		from := tm.ValidFrom
		if from == 0 {
			from = row.MintInstant
			// Legacy non-genesis tiling starts at UpdatedAt. Actual mapped
			// support still comes ONLY from the effective timeline reader.
			if row.Identity.Version != 0 && tm.UpdatedAt != 0 {
				from = tm.UpdatedAt
			}
		}
		if tm.ValidTo != 0 && from != math.MaxInt64 && tm.ValidTo == from+1 {
			index := slices.IndexFunc(o.VerifiedSpans, func(a SpanAttestation) bool {
				return a.SourceID == o.Source.ID && a.Row == row.Identity && a.ExcludesOneTickSentinels && strings.TrimSpace(a.Writer) != "" && strings.TrimSpace(a.Format) != "" && strings.TrimSpace(a.Evidence) != ""
			})
			if index < 0 {
				r.Findings = append(r.Findings, Finding{OneTickProvenanceUnknown, i})
			} else {
				r.AcceptedSpans = append(r.AcceptedSpans, o.VerifiedSpans[index])
			}
		}
	}
	// Membership checks avoid imposing TxFrom sorting or repairing raw rows.
	// A missing version/genesis is evidence of a gap, not its cause or contents.
	for version, row := range versions {
		if version == 0 {
			continue
		}
		if _, exists := versions[version-1]; !exists {
			r.Findings = append(r.Findings, Finding{HistoryGap, row})
		}
	}
	if !r.SourceChainVerified {
		r.Findings = append(r.Findings, Finding{IntegrityUnverified, -1})
	}
	slices.SortFunc(r.Findings, func(a, b Finding) int {
		if a.Row != b.Row {
			if a.Row < b.Row {
				return -1
			}
			return 1
		}
		return strings.Compare(string(a.Kind), string(b.Kind))
	})
	if len(r.Findings) > 0 {
		return r, ErrAmbiguousHistory
	}
	for i := range r.Segments {
		scope, err := mapSpan(o.DefaultAxis.Axis(), r.Segments[i].RawFrom, r.Segments[i].RawTo, o.Limits)
		if err != nil {
			return Report{}, err
		}
		r.Segments[i].Scope = scope
	}
	return r, nil
}

func mapSpan(axis temporal.Axis, from, to v4types.Instant, l temporal.Limits) (temporal.Scope, error) {
	start, err := v5types.InstantPosition(axis, v5types.Instant(from), l)
	if err != nil {
		return temporal.Scope{}, err
	}
	lo, err := temporal.FiniteBound(start, true)
	if err != nil {
		return temporal.Scope{}, err
	}
	hi := temporal.PositiveInfinity()
	if to != 0 {
		end, err := v5types.InstantPosition(axis, v5types.Instant(to), l)
		if err != nil {
			return temporal.Scope{}, err
		}
		hi, err = temporal.FiniteBound(end, false)
		if err != nil {
			return temporal.Scope{}, err
		}
	}
	return temporal.Span(axis, lo, hi, l)
}
