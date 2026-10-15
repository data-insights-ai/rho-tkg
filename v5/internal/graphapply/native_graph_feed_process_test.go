package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

type nativeFeedResult struct {
	Version                                                         int            `json:"version"`
	Format                                                          string         `json:"format"`
	Engine                                                          string         `json:"engine"`
	Spec                                                            nativeFeedSpec `json:"spec"`
	Admission                                                       nativeFeedAdmission
	AdmissionSHA256                                                 [32]byte
	Initial, Snapshot, Final, Reopened                              []byte
	Original, Token                                                 []byte
	Directory, ArtifactDirectory                                    string
	Completed                                                       [32]uint64
	LogicalLocal, LogicalCross                                      uint64
	FramesCeiling, PageCeiling, Pages, BusinessPages, ProgressPages uint64
	SourceRows                                                      [2]uint64
	ExtraInvocations, FeedExtraInvocations                          int
	OriginalCaptures                                                uint64
	ColumnManifests                                                 [2][2]nativeColumnManifest
	ColumnBindings                                                  [2][2]nativeColumnBinding
	ColumnFailed                                                    [2]uint64
	ProcessInstances, Reaped                                        int
	Transport                                                       []nativeWorkloadTransportReceipt
	PersistedFileBytes, AllocatedFileBytes                          uint64
	SetupNanos, BusinessNanos, DiagnosticNanos, TotalNanos          int64
	RetentionContract, Scope                                        string
}

func nativeFeedRequest(digest [32]byte, domain string, sequence uint64) [16]byte {
	h := sha256.New()
	_, _ = h.Write(digest[:])
	_, _ = h.Write([]byte(domain))
	_, _ = h.Write(binary.BigEndian.AppendUint64(nil, sequence))
	var id [16]byte
	copy(id[:], h.Sum(nil))
	return id
}
func nativeFeedCheckPoint(s nativeFeedSpec, p nativeFeedPoint, value int64) error {
	id, life, target, _ := s.cost().edge(p.Fact)
	r := p.Entity
	if p.Fact >= s.Facts || r.ID != id || !r.Found || !r.Effective || r.Kind != byte(graphstate.Relationship) || r.Type != "edge" || r.Mode != byte(graphstate.LifeBound) || r.Source != 1 || r.Target != target || r.EffectiveLife != life || len(r.Labels) != 0 || !r.Presence.Present || r.Presence.Null || r.Presence.Kind != "life" || r.Presence.Handle != life || r.Presence.I64 != 0 || r.Presence.Revision == 0 || r.Presence.Provenance != 4+p.Fact || !r.Property.Present || r.Property.Null || r.Property.Kind != "i64" || r.Property.I64 != value || r.Property.Handle != uint64(value) || r.Property.Revision == 0 || r.Property.Provenance != 4+p.Fact+boolUint(value == 6)*s.Facts {
		return errCorrupt
	}
	return nil
}
func nativeFeedSnapshot(t *testing.T, c *nativeFeedClient, s nativeFeedSpec, from uint64) uint64 {
	t.Helper()
	r := c.call(nativeFeedInput{Action: "snapshot", From: from})
	if r.Next != min(from+16, s.Facts) || r.EOF != (r.Next == s.Facts) || len(r.Points) != int(r.Next-from) {
		t.Fatal("complete snapshot page")
	}
	for i, p := range r.Points {
		if p.Fact != from+uint64(i) || nativeFeedCheckPoint(s, p, 5) != nil {
			t.Fatal("snapshot C changed after update", p)
		}
	}
	return r.Next
}

// Explicit source-authored workload, never a routine unit-suite substitute.
// Snapshot C remains retained throughout feed advancement; this is not the FDB
// feed-only retention profile and does not claim equal-retention GC cost.
func TestNativeGraphSnapshotFeedWorkloadCell(t *testing.T) {
	if testing.Short() || os.Getenv("RHO_GRAPH_NATIVE_WORKLOAD") != nativeFeedFormat {
		t.Skip("explicit native snapshot-to-feed workload required")
	}
	spec, err := nativeFeedSpecFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	admission, digest, err := nativeFeedAdmissionFromEnv(spec)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, admission.DeadlineUnixNanos))
	defer cancel()
	started := time.Now()
	output := os.Getenv("RHO_GRAPH_WORKLOAD_OUTPUT")
	if !filepath.IsAbs(output) {
		t.Fatal("absolute artifact root required")
	}
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RHO_GRAPH_PROCESS_ARTIFACT", output)
	open := func(name string) *json.Encoder {
		f, e := os.OpenFile(filepath.Join(output, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() {
			if e := f.Close(); e != nil {
				t.Error(e)
			}
		})
		return json.NewEncoder(f)
	}
	grants, attempts, operations := open("grants.jsonl"), open("attempts.jsonl"), open("operations.jsonl")
	traces, points, sources := open("feed.jsonl"), open("points.jsonl"), open("sources.jsonl")
	processes, columns := open("processes.jsonl"), open("columns.jsonl")
	n := nativeFeedNetwork(t, spec, admission, grants)
	initial := nativeFeedCut(t, ctx, n, graphReadFresh, graphCut{})
	initialWire, err := nativeFeedCutWire(initial)
	if err != nil {
		t.Fatal(err)
	}
	result := nativeFeedResult{Version: 1, Format: nativeFeedFormat, Engine: "native-raft", Spec: spec, Admission: admission, AdmissionSHA256: digest, Initial: initialWire, Directory: n.dir, ArtifactDirectory: output, ProcessInstances: 13, RetentionContract: "snapshot-C-plus-feed-v1"}
	result.FramesCeiling, err = nativeFeedFrames(spec.cost())
	if err != nil {
		t.Fatal(err)
	}
	result.PageCeiling = nativeFeedPages(spec.cost())
	driver := &nativeFeedDriver{ctx: ctx, n: n, spec: spec, started: started}
	driver.admissions = newNativeGraphCostAdmissions()
	driver.admissions.originals = nativeFeedMaxOriginals
	driver.admissions.same = nativeFeedSameInvocations
	driver.admissions.extra = driver.retry
	driver.admissions.beginOriginal = driver.beginOriginal
	result.SetupNanos = time.Since(started).Nanoseconds()
	businessStarted := time.Now()
	for phase := uint64(1); phase <= 3; phase++ {
		driver.phase(t, phase, phase-1, attempts, operations, &result.Completed)
	}
	driver.phase(t, 4, 0, attempts, operations, &result.Completed)
	result.BusinessNanos = time.Since(businessStarted).Nanoseconds()
	extra := 0
	client := &nativeFeedClient{t: t, ctx: ctx, n: n, member: 2, stream: traces, stage: "capture", extra: &extra}
	begin := client.call(nativeFeedInput{Action: "begin", Request: nativeFeedRequest(digest, "feed-owner", 0)})
	if len(begin.Original) == 0 {
		t.Fatal("missing genuine preparing original")
	}
	result.Original, result.Snapshot = begin.Original, begin.Initial
	old, err := nativeFeedCutDecode(begin.Initial)
	if err != nil {
		t.Fatal(err)
	}
	// Original was streamed before first Issue/Publish. The journal is descriptive;
	// only actual native issuance/publication and keeper observations authorize Open.
	published := client.call(nativeFeedInput{Action: "publish"})
	result.Token = published.Token
	if _, err := nativeFeedTokenDecode(result.Token); err != nil {
		t.Fatal(err)
	}
	var pairs [2]nativeGraphCostColumnPair
	if spec.Layout == "columns" {
		pairs[0] = nativeFeedPublishColumns(t, ctx, n, spec.cost(), initial, old, nativeFeedRequest(digest, "old-columns", 0), "old-seal", false, &result.ColumnFailed[0], columns)
	}
	client.stage = "held-snapshot-before-update"
	next := nativeFeedSnapshot(t, client, spec, 0)
	businessStarted = time.Now()
	driver.phase(t, 5, 0, attempts, operations, &result.Completed)
	result.BusinessNanos += time.Since(businessStarted).Nanoseconds()
	client.stage = "held-snapshot-after-update"
	for next < spec.Facts {
		next = nativeFeedSnapshot(t, client, spec, next)
	}
	client.call(nativeFeedInput{Action: "snapshot-close"})
	final := nativeFeedCut(t, ctx, n, graphReadFresh, graphCut{})
	result.Final, err = nativeFeedCutWire(final)
	if err != nil {
		t.Fatal(err)
	}
	nativeFeedPoints(t, ctx, n, spec, old, "old-after-update", points)
	nativeFeedPoints(t, ctx, n, spec, final, "final", points)
	var firstPage nativeFeedReply
	var firstRequest [16]byte
	for sequence := uint64(1); ; sequence++ {
		if sequence > result.PageCeiling {
			t.Fatal("declared feed page ceiling exhausted")
		}
		request := nativeFeedRequest(digest, "page", sequence)
		client.stage = "page"
		page := client.call(nativeFeedInput{Action: "page", Request: request, Boundary: result.Final})
		arena, e := graphstate.NewOutputBudget(2 << 20)
		if e != nil {
			t.Fatal(e)
		}
		decoded, e := decodeRetainedFeedPage(page.Page, 64<<10, arena)
		if e != nil || page.Digest != sha256.Sum256(page.Page) || decoded.request != request {
			t.Fatal("actual complete feed page", e)
		}
		result.Pages++
		if decoded.kind == 0 {
			result.ProgressPages++
		} else {
			result.BusinessPages++
		}
		if sequence == 1 {
			firstPage, firstRequest = page, request
			nativeFeedReplaceClient(t, ctx, n, spec, admission, processes)
			client.stage = "same-page-after-client-loss"
			client.call(nativeFeedInput{Action: "open", Token: result.Token})
			replay := client.call(nativeFeedInput{Action: "page", Request: request, Boundary: result.Final})
			if !replay.Replay || !bytes.Equal(replay.Page, page.Page) || replay.Digest != page.Digest {
				t.Fatal("pending page changed after native client loss")
			}
		}
		client.stage = "ack"
		ack := client.call(nativeFeedInput{Action: "ack", Request: request, Digest: page.Digest})
		complete, e := nativeFeedCutDecode(ack.Completed)
		if e != nil {
			t.Fatal(e)
		}
		if page.EOF {
			if !feedCutEqual(complete, final) {
				t.Fatal("half transaction advertised completed cut")
			}
			break
		}
		if feedCutEqual(complete, final) {
			t.Fatal("non-EOF page advanced completed cut")
		}
	}
	client.stage = "original-page-after-ack"
	replay := client.call(nativeFeedInput{Action: "page", Request: firstRequest, Boundary: result.Final})
	if !replay.Replay || replay.Digest != firstPage.Digest || !bytes.Equal(replay.Page, firstPage.Page) {
		t.Fatal("acknowledged original changed result")
	}
	client.call(nativeFeedInput{Action: "ack", Request: firstRequest, Digest: firstPage.Digest})
	if spec.Layout == "columns" {
		pairs[1] = nativeFeedPublishColumns(t, ctx, n, spec.cost(), initial, final, nativeFeedRequest(digest, "final-columns", 0), "final-seal", true, &result.ColumnFailed[1], columns)
		clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, &pairs[0], "old-after-update", false, &result.ColumnFailed[0], columns)
		nativeFeedReadColumns(t, clients, &pairs[0], spec.cost(), old, false, false)
		nativeGraphEdgeUniqueColumnCloseClients(clients)
	}
	client.stage = "before-cohort-close"
	client.call(nativeFeedInput{Action: "close"})
	result.SourceRows = nativeFeedSources(t, ctx, n, initial, final, "before-stop", sources)
	if result.SourceRows[0]+result.SourceRows[1] != 3+3*spec.Facts {
		t.Fatal("complete real typed before/after census", result.SourceRows)
	}
	result.Transport = append(result.Transport, nativeFeedRestart(t, ctx, n, spec, admission, processes))
	client.stage = "reopen-same-native-pin"
	client.call(nativeFeedInput{Action: "open", Token: result.Token})
	nativeFeedPoints(t, ctx, n, spec, old, "reopened-old", points)
	nativeFeedPoints(t, ctx, n, spec, final, "reopened-final", points)
	client.stage = "reopened-original-page"
	replay = client.call(nativeFeedInput{Action: "page", Request: firstRequest, Boundary: result.Final})
	if !replay.Replay || !bytes.Equal(replay.Page, firstPage.Page) {
		t.Fatal("physical restart changed feed original")
	}
	client.call(nativeFeedInput{Action: "ack", Request: firstRequest, Digest: firstPage.Digest})
	if spec.Layout == "columns" {
		for generation := range pairs {
			pair := &pairs[generation]
			for _, cached := range []bool{false, true} {
				clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, pair, fmt.Sprintf("reopened-%d-%t", generation, cached), false, &result.ColumnFailed[generation], columns)
				nativeFeedReadColumns(t, clients, pair, spec.cost(), pair.Final, cached, false)
				nativeGraphEdgeUniqueColumnCloseClients(clients)
			}
			clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, pair, fmt.Sprintf("retire-%d", generation), false, &result.ColumnFailed[generation], columns)
			for _, c := range clients {
				r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsRetire})
				if r.Retention == nil || r.Retention.State != nativeColumnRetired || !r.Closed {
					t.Fatal("durable column lifecycle incomplete")
				}
				c.terminalProbe(pair.ID, r.Retention.Token)
			}
			result.ColumnBindings[generation], result.ColumnManifests[generation] = pair.Bindings, pair.Manifests
		}
	}
	client.stage = "native-terminal-release"
	client.call(nativeFeedInput{Action: "release"})
	client.call(nativeFeedInput{Action: "close"})
	question := nativeFeedInput{Version: 1, Action: "open", Token: result.Token}
	wire, err := nativeFeedInputWire(question)
	if err != nil {
		t.Fatal(err)
	}
	refused, err := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processCostObserve, data: wire})
	if !errors.Is(err, errGraphPinReleased) {
		t.Fatal("actual original keepers did not classify terminal release", err)
	}
	var refusal nativeFeedReply
	if err := nativeFeedDecodeJSON(refused.aux, &refusal); err != nil || refusal.Failure != 3 {
		t.Fatal("terminal response provenance", err)
	}
	if err := traces.Encode(nativeFeedTrace{Stage: "actual-keeper-released-token", Member: 2, Input: question, Reply: refusal, Error: err.Error()}); err != nil {
		t.Fatal(err)
	}
	reopened := nativeFeedCut(t, ctx, n, graphReadFresh, graphCut{})
	result.Reopened, err = nativeFeedCutWire(reopened)
	if err != nil {
		t.Fatal(err)
	}
	if !feedCutEqual(reopened, final) {
		t.Fatal("protocol-only lifecycle changed graph cut")
	}
	if rows := nativeFeedSources(t, ctx, n, initial, reopened, "reopened-replayed", sources); rows != result.SourceRows {
		t.Fatal("retry or cleanup invented business CDC")
	}
	result.Reaped = 7 + nativeGraphCostStop(t, n, 2, processes)
	result.Transport = append(result.Transport, n.all[1].mux.hub.workload.receipt(n.all[1].mux.hub))
	for id := uint64(1); id <= 6; id++ {
		logical, allocated, e := nativeGraphCostFileBytes(filepath.Join(n.dir, fmt.Sprint(id)))
		if e != nil || !nativeColumnAdd(result.PersistedFileBytes, logical) || !nativeColumnAdd(result.AllocatedFileBytes, allocated) {
			t.Fatal("actual all-voter/derived/cache byte accounting", e)
		}
		result.PersistedFileBytes += logical
		result.AllocatedFileBytes += allocated
	}
	result.ExtraInvocations, result.FeedExtraInvocations = driver.extra, extra
	result.OriginalCaptures = driver.originals
	result.LogicalCross = 2 * (spec.Facts * spec.CrossPercent / 100)
	result.LogicalLocal = 2*spec.Facts + 3 - result.LogicalCross
	result.TotalNanos = time.Since(started).Nanoseconds()
	result.DiagnosticNanos = result.TotalNanos - result.SetupNanos - result.BusinessNanos
	if result.DiagnosticNanos < 0 || ctx.Err() != nil || result.Reaped != 13 {
		t.Fatal("owned terminal/timing accounting", ctx.Err())
	}
	for client := 0; client < spec.Writers; client++ {
		if result.Completed[client] == 0 {
			t.Fatal("declared client completed no useful operation")
		}
	}
	result.Scope = "Actual native snapshot-C plus complete grouped feed, bounded SAME originals/ACKs and real independent writers; source-authored OS-child, durable pin/cache/generation and same-directory recovery path. Source integration is not execution, full matrix, GC-retention equivalence or performance acceptance."
	if err := open("result.json").Encode(result); err != nil {
		t.Fatal(err)
	}
}
