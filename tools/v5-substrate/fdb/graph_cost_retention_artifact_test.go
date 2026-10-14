package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// These pure IO/selection guards grant no SDK, graph or capacity acceptance.
func TestGraphCostRetentionFormatAndExclusiveEOFGuards(t *testing.T) {
	if err := graphCostRetentionRow(nil, graphCostRawRow{}, 5); !errors.Is(err, errInvalid) {
		t.Fatal("nil typed source admitted", err)
	}
	t.Setenv("RHO_GRAPH_COST_RETENTION", "foreign")
	if _, err := graphCostRetentionSelected(); !errors.Is(err, errInvalid) {
		t.Fatal("foreign retention selected", err)
	}
	t.Setenv("RHO_GRAPH_COST_RETENTION", "source-v1")
	if selected, err := graphCostRetentionSelected(); err != nil || !selected {
		t.Fatal("explicit fresh selector", selected, err)
	}
	for _, writers := range []int{1, 8, 32} {
		p := graphCostProfile{Version: 1, Facts: 2, CrossPercent: 100, Writers: writers}
		for _, layout := range []string{"kv", "columns"} {
			fresh := graphCostConfig(graphCostRetentionGraph(layout, writers), p)
			if !validGraphCostProfile(fresh) || graphCostHistoryPolicy(p).validate() != nil {
				t.Fatal("fresh retained profile refused", fresh)
			}
			if writers != 1 && validGraphCostProfile(graphCostConfig("graph-cost-v1", p)) {
				t.Fatal("legacy writer1 namespace widened")
			}
		}
	}
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	p := graphCostProfile{Version: 1, Facts: 2, Writers: 1}
	header := graphCostArtifactHeader{Type: "header", Format: graphCostRetentionFormat, Stream: "history", Config: graphCostConfig(graphCostRetentionGraph("kv", 1), p), Policy: graphCostStreamPolicy{1, 4096}}
	stop := errors.New("consumer refused")
	walk := func(emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
		if err := emit(graphCostRawRow{Key: []byte{0, 255}, Value: []byte{255, 0}}); err != nil {
			return graphCostStreamEnd{}, err
		}
		return graphCostStreamEnd{}, stop
	}
	if end, err := writeGraphCostRetainedRaw(root, header, walk); !errors.Is(err, stop) || end != (graphCostStreamEnd{}) {
		t.Fatal("partial retained stream published EOF", end, err)
	}
	raw, err := root.ReadFile("fdb-cost-history.jsonl")
	if err != nil || bytes.Contains(raw, []byte(`"Type":"footer"`)) {
		t.Fatal("partial retained footer", err)
	}
	if _, err := writeGraphCostRetainedRaw(root, header, walk); !errors.Is(err, fs.ErrExist) {
		t.Fatal("retained original overwritten", err)
	}
	legacy := header
	legacy.Format = graphCostRawFormat
	if _, err := writeGraphCostRetainedRaw(root, legacy, walk); !errors.Is(err, errInvalid) {
		t.Fatal("legacy evidence relabelled", err)
	}
	if err := publishGraphCostResult(root, graphCostResult{Version: 6, OracleFormat: graphCostRetentionFormat}); !errors.Is(err, errInvalid) {
		t.Fatal("missing retention EOF published result", err)
	}
	if _, err := root.Stat("fdb-cost-result.json"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("partial retention result exists", err)
	}
	cause := errors.Join(errUnknown, errHistoryBusy)
	pending := &graphCostRetentionCleanupPending{Cause: cause}
	if !errors.Is(pending, errUnknown) || !errors.Is(pending, errHistoryBusy) {
		t.Fatal("cleanup cause classification lost")
	}
}

// Authored real-SDK composition, not executed at this implementation stage.
// Each case owns its fresh graph prefix before and after the run; a failed case
// cannot silently resume a previous persisted profile on the next invocation.
func TestGraphCostRetentionActualHeldReadResumeOriginalsAndEOF(t *testing.T) {
	db := fixture(t, defaults()).db
	for _, tc := range []struct {
		writers int
		layout  string
		cross   uint64
	}{
		{1, "kv", 0}, {1, "columns", 100}, {8, "kv", 100}, {32, "columns", 100},
	} {
		t.Run(fmt.Sprintf("writers%d-%s-cross%d", tc.writers, tc.layout, tc.cross), func(t *testing.T) {
			facts := uint64(2)
			if tc.writers != 1 {
				facts = 33
			}
			p := graphCostProfile{Version: 1, Facts: facts, CrossPercent: tc.cross, Writers: tc.writers}
			directory := t.TempDir()
			t.Setenv("FDB_COST_ARTIFACT", directory)
			t.Setenv("RHO_GRAPH_COST_LAYOUT", tc.layout)
			t.Setenv("RHO_GRAPH_COST_RETENTION", "source-v1")
			config := graphCostConfig(graphCostRetentionGraph(tc.layout, tc.writers), p)
			cleanup := func() {
				_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
					for g := uint8(0); g < 2; g++ {
						tr.ClearRange(prefixRange((&harness{config: config}).prefix(g)))
					}
					return nil, nil
				})
				if err != nil {
					t.Error("owned retained prefix cleanup", err)
				}
			}
			t.Cleanup(cleanup)
			cleanup()
			if t.Failed() {
				return
			}
			if err := runGraphCost(db, p); err != nil {
				t.Fatal(err)
			}
			root, err := os.OpenRoot(directory)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := root.Close(); err != nil {
					t.Error(err)
				}
			})
			body, err := root.ReadFile("fdb-cost-result.json")
			if err != nil {
				t.Fatal(err)
			}
			result, err := decode[graphCostResult](body)
			if err != nil || result.Version != 6 || result.OracleFormat != graphCostRetentionFormat || result.Retention == nil || !result.Retention.complete(p) || result.Config != config || result.CompletedCreates != facts || result.CompletedUpdates != facts || result.CDCGroups != 3+2*facts || result.PhysicalGraphChanges != 3+6*facts {
				t.Fatal("retention profile lacks exact completion", result, err)
			}
			artifact, err := root.ReadFile("fdb-cost-retention.jsonl")
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(artifact)
			if fmt.Sprintf("%x", digest) != result.Retention.ArtifactSHA256 {
				t.Fatal("closed retention artifact hash changed")
			}
			var guard, oldRows, resume, originals uint64
			var request string
			var proposal []byte
			for line := range bytes.SplitSeq(bytes.TrimSuffix(artifact, []byte{'\n'}), []byte{'\n'}) {
				var tag struct{ Type string }
				if err := json.Unmarshal(line, &tag); err != nil {
					t.Fatal(err)
				}
				switch tag.Type {
				case "row":
					oldRows++
				case "durable-resume":
					resume++
				case "retained-original":
					originals++
				case "guard-submission":
					var frame struct {
						Body           []byte
						Classification string
						Diagnostic     bool
						Submission     graphCostParallelSubmission
						NativeCode     *int
						Before, After  graphCostRetentionObservation
					}
					if err := json.Unmarshal(line, &frame); err != nil {
						t.Fatal(err)
					}
					x, err := decode[transaction](frame.Body)
					if err != nil {
						t.Fatal(err)
					}
					if guard == 0 {
						request = x.Request
						proposal = bytes.Clone(frame.Body)
					}
					if x.Request != request || !bytes.Equal(proposal, frame.Body) || !frame.Diagnostic || frame.Classification != "retirement-busy" || frame.NativeCode != nil || frame.Submission.Retryable || frame.Submission.MaybeCommitted || frame.Submission.Outcome != (outcome{}) || !graphColumnSame(frame.Before, frame.After) || frame.Before.Originals[0].Value != nil || frame.Before.Originals[1].Value != nil {
						t.Fatal("diagnostic invented authority/completion", frame)
					}
					guard++
				}
			}
			if guard != 2 || resume != 1 || oldRows != 3+3*facts || originals != result.Retention.Replays {
				t.Fatal("missing actual ownership/original census", guard, resume, oldRows, originals)
			}
			for i, name := range []string{"history", "current", "cdc"} {
				raw, err := root.ReadFile("fdb-cost-" + name + ".jsonl")
				if err != nil {
					t.Fatal(err)
				}
				lines := bytes.Split(bytes.TrimSuffix(raw, []byte{'\n'}), []byte{'\n'})
				var header graphCostArtifactHeader
				var footer graphCostArtifactFooter
				if len(lines) < 2 || json.Unmarshal(lines[0], &header) != nil || json.Unmarshal(lines[len(lines)-1], &footer) != nil || header.Format != graphCostRetentionFormat || header.Metadata != result.Retention.Metadata || footer.Rows != result.StreamEnds[i].Rows || footer.Metadata != header.Metadata || footer.Type != "footer" {
					t.Fatal("mixed/truncated post-GC EOF", name)
				}
			}
			if result.Columns != nil && (result.Columns.Format != graphCostRetentionColumnFormat || result.Columns.SourceFormat != graphCostRetentionFormat || result.Columns.Final.History[0]+result.Columns.Final.History[1] != 3+6*facts) {
				t.Fatal("full column publication census was replaced by retained physical count")
			}
		})
	}
}
