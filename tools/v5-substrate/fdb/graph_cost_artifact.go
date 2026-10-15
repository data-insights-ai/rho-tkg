package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"slices"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphCostRawFormat = "fdb-graph-cost-raw-v2"

// The decoded fields never replace the actual canonical native records. Every
// header binds the same immutable config/cuts and quiescent metadata census.
type graphCostStoredRecord struct{ Key, Value []byte }
type graphCostArtifactHeader struct {
	Type, Format, Stream           string
	Config                         config
	InitialCut, OldCut, FinalCut   certificate
	Policy                         graphCostStreamPolicy
	Metadata                       [2]metadata
	ConfigRecords, MetadataRecords [2]graphCostStoredRecord
	CutRecords                     [3]graphCostStoredRecord
}
type graphCostArtifactRow struct {
	Type string
	Row  graphCostRawRow
}
type graphCostArtifactFooter struct {
	Type          string
	Rows, Groups  [2]uint64
	Metadata      [2]metadata
	PayloadSHA256 string
}

func (h *harness) graphCostArtifactHeader(initial, old, final cut, policy graphCostStreamPolicy) (graphCostArtifactHeader, error) {
	return h.graphCostArtifactHeaderContext(nil, initial, old, final, policy)
}
func (h *harness) graphCostArtifactHeaderContext(ctx context.Context, initial, old, final cut, policy graphCostStreamPolicy) (graphCostArtifactHeader, error) {
	if !slices.Equal(old.record.Scope, []uint8{0, 1}) || old.record.Round < initial.record.Round || old.record.Round > final.record.Round {
		return graphCostArtifactHeader{}, errInvalid
	}
	for g := range 2 {
		if old.record.Groups[g] < initial.record.Groups[g] || old.record.Groups[g] > final.record.Groups[g] {
			return graphCostArtifactHeader{}, errInvalid
		}
	}
	s, err := newGraphCostStreamContext(ctx, h, initial, final, policy, func(graphCostRawRow) error { return nil })
	if err != nil {
		return graphCostArtifactHeader{}, err
	}
	old.record.Scope = slices.Clone(old.record.Scope)
	result, err := graphCostReadContext(ctx, s.h.db, func(tr fdb.ReadTransaction) (any, error) {
		if _, err := s.check(tr, true); err != nil {
			return nil, err
		}
		if err := s.h.verifyCut(tr, old); err != nil {
			return nil, err
		}
		header := graphCostArtifactHeader{Type: "header", Format: graphCostRawFormat, Config: s.h.config, InitialCut: s.from.record, OldCut: old.record, FinalCut: s.to.record, Policy: policy, Metadata: s.metadata}
		stored := func(key fdb.Key, decoded any) (graphCostStoredRecord, error) {
			body, err := tr.Get(key).Get()
			if err != nil {
				return graphCostStoredRecord{}, err
			}
			canonical, err := wire(decoded)
			if err != nil || !bytes.Equal(body, canonical) {
				return graphCostStoredRecord{}, errors.Join(errMismatch, err)
			}
			return graphCostStoredRecord{bytes.Clone(key), bytes.Clone(body)}, nil
		}
		for g := uint8(0); g < 2; g++ {
			var err error
			header.ConfigRecords[g], err = stored(s.h.key(g, "meta", "config"), header.Config)
			if err != nil {
				return nil, err
			}
			header.MetadataRecords[g], err = stored(s.h.key(g, "meta", "state"), header.Metadata[g])
			if err != nil {
				return nil, err
			}
		}
		for i, c := range []certificate{header.InitialCut, header.OldCut, header.FinalCut} {
			body, err := wire(c)
			if err != nil {
				return nil, err
			}
			header.CutRecords[i], err = stored(s.h.key(c.Scope[0], "cut", hex.EncodeToString(body)), c)
			if err != nil {
				return nil, err
			}
		}
		return header, nil
	})
	if err != nil {
		return graphCostArtifactHeader{}, err
	}
	return result.(graphCostArtifactHeader), nil
}

// This writer has no SDK callback: its walk invokes IO only after successful
// native reads. A failed walk/encode/close retains partial evidence without EOF
// success. JSONL admits at most512KiB per encoded row, including base64 copies.
func writeGraphCostRawArtifact(root *os.Root, header graphCostArtifactHeader, walk func(func(graphCostRawRow) error) (graphCostStreamEnd, error)) (end graphCostStreamEnd, err error) {
	if root == nil || walk == nil || header.Type != "header" || header.Format != graphCostRawFormat || header.Policy.validate() != nil || header.Stream != "history" && header.Stream != "current" && header.Stream != "cdc" {
		return graphCostStreamEnd{}, errInvalid
	}
	file, _, err := openGraphCostArtifact(root, "fdb-cost-"+header.Stream+".jsonl")
	if err != nil {
		return graphCostStreamEnd{}, err
	}
	defer func() {
		err = errors.Join(err, file.Close())
		if err != nil {
			end = graphCostStreamEnd{}
		}
	}()
	hash := sha256.New()
	encoder := json.NewEncoder(io.MultiWriter(file, hash))
	if err := encoder.Encode(header); err != nil {
		return graphCostStreamEnd{}, err
	}
	var counts [2]uint64
	verified, err := walk(func(row graphCostRawRow) error {
		if row.Group > 1 || counts[row.Group] >= header.Config.Limits.Versions {
			return errLimit
		}
		body, err := json.Marshal(graphCostArtifactRow{"row", row})
		if err != nil {
			return err
		}
		if len(body)+1 > 512<<10 {
			return errLimit
		}
		if _, err := io.MultiWriter(file, hash).Write(append(body, '\n')); err != nil {
			return err
		}
		counts[row.Group]++
		return nil
	})
	if err != nil {
		return graphCostStreamEnd{}, err
	}
	if verified.Rows != counts || verified.Groups != header.FinalCut.Groups || verified.Metadata != header.Metadata {
		return graphCostStreamEnd{}, errMismatch
	}
	footer := graphCostArtifactFooter{"footer", verified.Rows, verified.Groups, verified.Metadata, hex.EncodeToString(hash.Sum(nil))}
	if err := json.NewEncoder(file).Encode(footer); err != nil {
		return graphCostStreamEnd{}, err
	}
	return verified, nil
}

func (h *harness) graphCostArtifacts(root *os.Root, initial, old, final cut, policy graphCostStreamPolicy) ([3]graphCostStreamEnd, error) {
	var complete [3]graphCostStreamEnd
	header, err := h.graphCostArtifactHeader(initial, old, final, policy)
	if err != nil {
		return complete, err
	}
	for i, stream := range []struct {
		name string
		walk func(func(graphCostRawRow) error) (graphCostStreamEnd, error)
	}{
		{"history", func(emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
			return h.graphCostWalkHistory(old, final, policy, emit)
		}},
		{"current", func(emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
			return h.graphCostWalkCurrent(final, policy, emit)
		}},
		{"cdc", func(emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
			return h.graphCostWalkChanges(initial, final, policy, emit)
		}},
	} {
		header.Stream = stream.name
		complete[i], err = writeGraphCostRawArtifact(root, header, stream.walk)
		if err != nil {
			return [3]graphCostStreamEnd{}, err
		}
	}
	return complete, nil
}

// Link publishes only the closed exclusive file and refuses an existing result.
// The launcher must still prove exit0, owned cleanup and every artifact hash.
func publishGraphCostResult(root *os.Root, result graphCostResult) error {
	retained := result.Version == 6 && result.OracleFormat == graphCostRetentionFormat && result.Retention != nil
	if root == nil || !((result.Retention == nil && ((result.Version == 2 && result.OracleFormat == graphCostRawFormat && result.Columns == nil && result.Parallel == nil) ||
		(result.Version == 3 && result.OracleFormat == graphCostColumnFormat && result.Columns != nil && result.Columns.Format == graphCostColumnFormat && result.Columns.complete() && result.Parallel == nil) ||
		(result.Version == 4 && result.OracleFormat == graphCostParallelFormat && result.Columns == nil && result.Parallel != nil && result.Parallel.Format == graphCostParallelFormat && result.Parallel.complete(result.Spec)) ||
		(result.Version == 5 && result.OracleFormat == graphCostParallelColumnFormat && result.Columns != nil && result.Columns.Format == graphCostColumnFormat && result.Columns.complete() && result.Parallel != nil && result.Parallel.Format == graphCostParallelFormat && result.Parallel.complete(result.Spec)))) ||
		(retained && result.Retention.complete(result.Spec) && result.Retention.Binding.Config == result.Config && result.Retention.Sources == result.StreamEnds &&
			(result.Columns == nil || result.Columns.Format == graphCostRetentionColumnFormat && result.Columns.complete()) &&
			(result.Parallel == nil && result.Spec.Writers == 1 || result.Parallel != nil && result.Parallel.Format == graphCostRetentionParallelFormat && result.Parallel.complete(result.Spec)))) {
		return errInvalid
	}
	if retained && (!sameCertificate(result.Retention.Binding.Initial, result.InitialCut) || !sameCertificate(result.Retention.OldCut, result.OldCut) || !sameCertificate(result.Retention.FinalCut, result.FinalCut)) {
		return errMismatch
	}
	columnGraph := "graph-cost-columns-v1"
	if result.Parallel != nil {
		columnGraph = "graph-cost-parallel-columns-v1"
		if result.Config != result.Parallel.Binding.Config || !sameCertificate(result.Parallel.Old.Cut, result.OldCut) || !sameCertificate(result.Parallel.Final.Cut, result.FinalCut) {
			return errMismatch
		}
	}
	if retained {
		columnGraph = graphCostRetentionGraph("columns", result.Spec.Writers)
	}
	if result.Columns != nil && (result.Config.Graph != columnGraph ||
		!sameCertificate(result.Columns.Old.Generation.Initial, result.InitialCut) || !sameCertificate(result.Columns.Final.Generation.Initial, result.InitialCut) ||
		!sameCertificate(result.Columns.Old.Generation.Cut, result.OldCut) || !sameCertificate(result.Columns.Final.Generation.Cut, result.FinalCut)) {
		return errMismatch
	}
	file, encoder, err := openGraphCostArtifact(root, "fdb-cost-result.pending.json")
	if err != nil {
		return err
	}
	if err := errors.Join(encoder.Encode(result), file.Close()); err != nil {
		return err
	}
	if err := root.Link("fdb-cost-result.pending.json", "fdb-cost-result.json"); err != nil {
		return err
	}
	return root.Remove("fdb-cost-result.pending.json")
}
