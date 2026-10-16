package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

type nativeFeedRetainedResult struct {
	Version                                                  int                    `json:"version"`
	Format                                                   string                 `json:"format"`
	Engine                                                   string                 `json:"engine"`
	Spec                                                     nativeFeedRetainedSpec `json:"spec"`
	Admission                                                nativeFeedRetainedAdmission
	AdmissionSHA256                                          [32]byte
	Initial, Snapshot, BeforeGC, Checkpoint, Final, Reopened []byte
	Original, Token                                          []byte
	Directory, ArtifactDirectory                             string
	Completed                                                [32]uint64
	LogicalLocal, LogicalCross, FramesCeiling, PageCeiling   uint64
	SetupTransactions                                        uint64
	Pages, BusinessPages, ProgressPages, OriginalCaptures    uint64
	SourceRows                                               [3][2]uint64
	SparseFrom                                               [2]uint64
	GC                                                       [6]nativeRetentionReceipt
	ExtraInvocations, FeedExtraInvocations                   int
	ColumnManifests                                          [2][2]nativeColumnManifest
	ColumnBindings                                           [2][2]nativeColumnBinding
	ColumnFailed                                             [2]uint64
	CheckpointFirst                                          uint64
	ProcessInstances, Reaped                                 int
	Transport                                                []nativeWorkloadTransportReceipt
	PersistedFileBytes, AllocatedFileBytes                   uint64
	SetupNanos, BusinessNanos, DiagnosticNanos, TotalNanos   int64
	RetentionContract, Scope                                 string
}

func nativeFeedRetainedSnapshot(t *testing.T, c *nativeFeedRetainedClient, s nativeFeedRetainedSpec, from uint64) uint64 {
	t.Helper()
	r := c.call(nativeFeedInput{Action: "snapshot", From: from})
	if r.Next != min(from+16, s.Facts) || r.EOF != (r.Next == s.Facts) || len(r.Points) != int(r.Next-from) {
		t.Fatal("complete original snapshot page")
	}
	for i, p := range r.Points {
		if p.Fact != from+uint64(i) || nativeFeedRetainedCheckPoint(s, p, 5) != nil {
			t.Fatal("snapshot C scalar/presence/life", p)
		}
	}
	return r.Next
}
func nativeFeedRetainedCheckPoint(s nativeFeedRetainedSpec, p nativeFeedPoint, value int64) error {
	// This helper checks shape only; the independent raw oracle joins the exact
	// actual revisions/presence cells at this cut. No guessed revision formula.
	return nativeCreateUpdateCheckPoint(nativeFeedSpec{Facts: s.Facts, CrossPercent: s.CrossPercent, Writers: s.Writers}, p, value)
}
func nativeFeedRetainedPoints(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, s nativeFeedRetainedSpec, at graphCut, stage string, stream *json.Encoder) {
	t.Helper()
	wire, err := nativeFeedCutWire(at)
	if err != nil {
		t.Fatal(err)
	}
	for first := uint64(0); first < s.Facts; first += 16 {
		r, err := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processSourceCertifiedOpen, data: wire})
		if err != nil {
			t.Fatal(err)
		}
		handle, actual, err := decodeGraphE15ReadToken(r.aux)
		if err != nil || handle == 0 || !feedCutEqual(actual, at) {
			t.Fatal("genuine retained exact cut", err)
		}
		for fact := first; fact < min(first+16, s.Facts); fact++ {
			id, _, _, _ := s.cost().edge(fact)
			r, err := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processCostObserve, index: handle, entity: id})
			if err != nil {
				t.Fatal(err)
			}
			var point nativeLifecycleEntity
			if err := nativeFeedDecodeJSON(r.aux, &point); err != nil {
				t.Fatal(err)
			}
			value := int64(6)
			if stage == "old-before-gc" {
				value = 5
			}
			if nativeFeedRetainedCheckPoint(s, nativeFeedPoint{fact, point}, value) != nil || point.Property.Revision > at.certificates[0].sourceRound || point.Presence.Revision > at.certificates[0].sourceRound {
				t.Fatal("actual retained point cut/value/life", fact)
			}
			if err := stream.Encode(struct {
				Stage string
				Cut   []byte
				Point nativeFeedPoint
			}{stage, wire, nativeFeedPoint{fact, point}}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processSourceCertifiedClose, index: handle}); err != nil {
			t.Fatal(err)
		}
	}
}

// Explicit full workload family. Its pre-GC raw evidence is immutable; the
// post-GC exporter promises only the genuine retained suffix. The native rolling
// ACK predecessor/unread prefix remains more conservative than detached FDB CDC.
func TestNativeGraphSnapshotFeedRetainedOnlyWorkloadCell(t *testing.T) {
	if testing.Short() || os.Getenv("RHO_GRAPH_NATIVE_WORKLOAD") != nativeFeedRetainedFormat {
		t.Skip("explicit native retained-only feed workload required")
	}
	spec, err := nativeFeedRetainedSpecFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	admission, digest, err := nativeFeedRetainedAdmissionFromEnv(spec)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, admission.DeadlineUnixNanos))
	defer cancel()
	started := time.Now()
	output := os.Getenv("RHO_GRAPH_WORKLOAD_OUTPUT")
	if !filepath.IsAbs(output) {
		t.Fatal("absolute owned artifact root required")
	}
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RHO_GRAPH_PROCESS_ARTIFACT", output)
	open := func(name string) *json.Encoder {
		f, err := os.OpenFile(filepath.Join(output, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := f.Close(); err != nil {
				t.Error(err)
			}
		})
		return json.NewEncoder(f)
	}
	grants, attempts, operations := open("grants.jsonl"), open("attempts.jsonl"), open("operations.jsonl")
	traces, points, sources := open("feed.jsonl"), open("points.jsonl"), open("sources.jsonl")
	processes, columns, gc := open("processes.jsonl"), open("columns.jsonl"), open("gc.jsonl")
	n := nativeFeedRetainedNetwork(t, spec, admission, grants)
	initial := nativeScalarWorkloadCut(t, ctx, n, graphReadFresh, graphCut{}, retainedOnlyFeedSemanticContractID())
	wire := func(c graphCut) []byte {
		w, err := nativeFeedCutWire(c)
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	result := nativeFeedRetainedResult{Version: 1, Format: nativeFeedRetainedFormat, Engine: "native-raft", Spec: spec, Admission: admission, AdmissionSHA256: digest, Initial: wire(initial), Directory: n.dir, ArtifactDirectory: output, ProcessInstances: 13, RetentionContract: nativeFeedRetainedContract}
	result.FramesCeiling, err = nativeFeedRetainedFrames(spec.cost())
	if err != nil {
		t.Fatal(err)
	}
	result.PageCeiling = nativeFeedRetainedPages(spec.cost())
	driver := &nativeFeedRetainedDriver{ctx: ctx, n: n, spec: spec, started: started, admissions: newNativeGraphCostAdmissions()}
	driver.admissions.originals, driver.admissions.same = 64, 8
	driver.admissions.extra, driver.admissions.beginOriginal = driver.retry, driver.beginOriginal
	for phase := uint64(1); phase <= 3; phase++ {
		driver.phase(t, phase, phase-1, phase, attempts, operations, &result.Completed)
	}
	result.SetupTransactions = 3
	result.SetupNanos = time.Since(started).Nanoseconds()
	business := time.Now()
	driver.phase(t, 4, 0, spec.Facts, attempts, operations, &result.Completed)
	result.BusinessNanos = time.Since(business).Nanoseconds()
	client := &nativeFeedRetainedClient{t: t, ctx: ctx, n: n, member: 2, stream: traces, stage: "capture"}
	begin := client.call(nativeFeedInput{Action: "begin", Request: nativeFeedRequest(digest, "feed-owner", 0)})
	if len(begin.Original) == 0 {
		t.Fatal("missing original before any native Issue/Publish")
	}
	result.Original, result.Snapshot = begin.Original, begin.Initial
	old, err := nativeFeedRetainedCutDecode(begin.Initial)
	if err != nil {
		t.Fatal(err)
	}
	result.Token = client.call(nativeFeedInput{Action: "publish"}).Token
	if _, err := nativeFeedTokenDecode(result.Token); err != nil {
		t.Fatal(err)
	}
	client.stage = "complete-snapshot-before-updates"
	for next := uint64(0); next < spec.Facts; {
		next = nativeFeedRetainedSnapshot(t, client, spec, next)
	}
	client.call(nativeFeedInput{Action: "snapshot-close"})
	var pairs [2]nativeGraphCostColumnPair
	if spec.Layout == "columns" {
		pairs[0] = nativeFeedPublishColumns(t, ctx, n, spec.cost(), initial, old, nativeFeedRequest(digest, "old-columns", 0), "old-seal", false, &result.ColumnFailed[0], columns)
	}
	half := spec.Facts / 2
	business = time.Now()
	driver.phase(t, 5, 0, half, attempts, operations, &result.Completed)
	result.BusinessNanos += time.Since(business).Nanoseconds()
	mid := nativeScalarWorkloadCut(t, ctx, n, graphReadFresh, graphCut{}, retainedOnlyFeedSemanticContractID())
	result.BeforeGC = wire(mid)
	nativeFeedRetainedPoints(t, ctx, n, spec, old, "old-before-gc", points)
	if spec.Layout == "columns" {
		clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, &pairs[0], "old-after-first-update-wave", false, &result.ColumnFailed[0], columns)
		nativeFeedReadColumns(t, clients, &pairs[0], spec.cost(), old, false, false)
		nativeGraphEdgeUniqueColumnCloseClients(clients)
		nativeFeedRetainedRetireColumns(t, ctx, n, &pairs[0], "retire-all-C-dependent-columns", &result.ColumnFailed[0], columns)
	}
	sequence := uint64(0)
	replaced := false
	var savedPage nativeFeedReply
	var savedRequest [16]byte
	deliver := func(boundary graphCut, stopAfterBusiness, replace bool) {
		boundaryWire := wire(boundary)
		for {
			sequence++
			if sequence > result.PageCeiling {
				t.Fatal("finite complete feed page grant exhausted")
			}
			request := nativeFeedRequest(digest, "page", sequence)
			client.stage = "page"
			page := client.call(nativeFeedInput{Action: "page", Request: request, Boundary: boundaryWire})
			arena, err := graphstate.NewOutputBudget(2 << 20)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeRetainedFeedPage(page.Page, 64<<10, arena)
			if err != nil || page.Digest != sha256.Sum256(page.Page) || decoded.request != request {
				t.Fatal("actual complete retained-only page", err)
			}
			result.Pages++
			if decoded.kind == 0 {
				result.ProgressPages++
			} else {
				result.BusinessPages++
			}
			if replace && !replaced {
				savedPage, savedRequest = page, request
				nativeScalarWorkloadReplaceClient(t, ctx, n, processes, func() ([]string, error) { return nativeFeedRetainedChildEnv(spec, admission, true) })
				client.stage = "same-page-after-GC-client-loss"
				client.call(nativeFeedInput{Action: "open", Token: result.Token})
				replay := client.call(nativeFeedInput{Action: "page", Request: request, Boundary: boundaryWire})
				if !replay.Replay || !bytes.Equal(replay.Page, page.Page) || replay.Digest != page.Digest {
					t.Fatal("pending page changed after actual GC/client loss")
				}
				replaced = true
			}
			client.stage = "ack"
			ack := client.call(nativeFeedInput{Action: "ack", Request: request, Digest: page.Digest})
			complete, err := nativeFeedRetainedCutDecode(ack.Completed)
			if err != nil {
				t.Fatal(err)
			}
			if decoded.eof {
				if !feedCutEqual(complete, boundary) {
					t.Fatal("half transaction completed boundary")
				}
				if stopAfterBusiness {
					t.Fatal("GC schedule accidentally drained all unread work")
				}
				return
			}
			if feedCutEqual(complete, boundary) {
				t.Fatal("partial page advanced completed cut")
			}
			if stopAfterBusiness && decoded.kind != 0 {
				return
			}
		}
	}
	// Consume/ACK one genuine useful envelope, leaving first-wave business work
	// unread. Mode cannot cross a pending page, and it installs on every keeper.
	deliver(mid, true, false)
	if result.BusinessPages != 1 || half <= 1 {
		t.Fatal("missing actual unread pre-GC business suffix")
	}
	client.stage = "irreversible-native-retained-only"
	client.call(nativeFeedInput{Action: "retain", Request: nativeFeedRequest(digest, "retained-only", 0)})
	client.stage = "snapshot-token-expired-before-physical-collection"
	if _, err := client.exchange(nativeFeedInput{Action: "snapshot"}); !errors.Is(err, errGraphHistoryExpired) || !errors.Is(err, raftlog.ErrApplicationExpired) {
		t.Fatal("retained-only snapshot token was not genuinely expired", err)
	}
	client.stage = "actual-ACK-vector-before-GC"
	state := client.call(nativeFeedInput{Action: "state"})
	cursor, err := decodeRetainedFeedCursor(state.Cursor)
	if err != nil || cursor.pending != ([32]byte{}) {
		t.Fatal("actual acknowledged cursor", err)
	}
	from := [2]uint64{initial.certificates[0].sourceIndex, initial.certificates[1].sourceIndex}
	result.SourceRows[0] = nativeFeedRetainedSources(t, ctx, n, from, mid, result.Original, "before-gc-complete-prefix", sources)
	result.GC = nativeFeedRetainedCollect(t, ctx, n, digest, cursor, gc)
	result.SparseFrom = [2]uint64{result.GC[0].Plan.RootFloor, result.GC[3].Plan.RootFloor}
	if result.SparseFrom[0] <= old.certificates[0].sourceIndex {
		t.Fatal("actual collection did not pass old snapshot source")
	}
	// The saved immutable recipe is not a new pin/At capability. Genuine token
	// reopen after collection still refuses Snapshot C, while feed Open works.
	client.stage = "snapshot-token-expired-after-physical-GC"
	if _, err := client.exchange(nativeFeedInput{Action: "snapshot"}); !errors.Is(err, errGraphHistoryExpired) || !errors.Is(err, raftlog.ErrApplicationExpired) {
		t.Fatal("collected C snapshot reopened", err)
	}
	// One real next update creates the actual retained checkpoint; no fake PLM,
	// invented business round or genesis substitution creates column authority.
	business = time.Now()
	driver.phase(t, 5, half, half+1, attempts, operations, &result.Completed)
	result.BusinessNanos += time.Since(business).Nanoseconds()
	checkpoint := nativeScalarWorkloadCut(t, ctx, n, graphReadFresh, graphCut{}, retainedOnlyFeedSemanticContractID())
	result.Checkpoint = wire(checkpoint)
	result.CheckpointFirst = half + 1
	deliver(mid, false, true)
	business = time.Now()
	driver.phase(t, 5, half+1, spec.Facts, attempts, operations, &result.Completed)
	result.BusinessNanos += time.Since(business).Nanoseconds()
	final := nativeScalarWorkloadCut(t, ctx, n, graphReadFresh, graphCut{}, retainedOnlyFeedSemanticContractID())
	result.Final = wire(final)
	deliver(final, false, false)
	if result.BusinessPages != spec.Facts || !replaced {
		t.Fatal("incomplete genuine unread update feed")
	}
	client.stage = "same-ack-after-GC"
	client.call(nativeFeedInput{Action: "ack", Request: savedRequest, Digest: savedPage.Digest})
	if spec.Layout == "columns" {
		pairs[1] = nativeFeedRetainedPublishColumns(t, ctx, n, spec.cost(), checkpoint, final, nativeFeedRequest(digest, "checkpoint-columns", 0), "actual-post-GC-checkpoint-seal", &result.ColumnFailed[1], columns, result.CheckpointFirst)
	}
	nativeFeedRetainedPoints(t, ctx, n, spec, final, "final", points)
	result.SourceRows[1] = nativeFeedRetainedSources(t, ctx, n, result.SparseFrom, final, result.Original, "post-gc-retained-suffix", sources)
	client.stage = "before-cohort-close"
	client.call(nativeFeedInput{Action: "close"})
	result.Transport = append(result.Transport, nativeScalarWorkloadRestart(t, ctx, n, processes, func() ([]string, error) { return nativeFeedRetainedChildEnv(spec, admission, true) }, func() { nativeFeedRetainedAttach(t, n, spec, admission) }))
	client.stage = "reopen-same-native-feed"
	client.call(nativeFeedInput{Action: "open", Token: result.Token})
	client.stage = "reopened-same-page-after-GC"
	replay := client.call(nativeFeedInput{Action: "page", Request: savedRequest, Boundary: result.BeforeGC})
	if !replay.Replay || !bytes.Equal(replay.Page, savedPage.Page) || replay.Digest != savedPage.Digest {
		t.Fatal("physical restart changed original page")
	}
	client.call(nativeFeedInput{Action: "ack", Request: savedRequest, Digest: savedPage.Digest})
	nativeFeedRetainedPoints(t, ctx, n, spec, final, "reopened-final", points)
	if spec.Layout == "columns" {
		for _, cached := range []bool{false, true} {
			clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, &pairs[1], fmt.Sprintf("reopened-checkpoint-%t", cached), false, &result.ColumnFailed[1], columns)
			nativeFeedRetainedReadColumns(t, clients, &pairs[1], spec.cost(), final, cached, false, result.CheckpointFirst)
			nativeGraphEdgeUniqueColumnCloseClients(clients)
		}
		nativeFeedRetainedRetireColumns(t, ctx, n, &pairs[1], "retire-checkpoint-owner", &result.ColumnFailed[1], columns)
		for generation := range pairs {
			result.ColumnBindings[generation], result.ColumnManifests[generation] = pairs[generation].Bindings, pairs[generation].Manifests
		}
	}
	client.stage = "terminal-release"
	client.call(nativeFeedInput{Action: "release"})
	client.call(nativeFeedInput{Action: "close"})
	client.stage = "actual-keeper-released-token"
	if reply, err := client.exchange(nativeFeedInput{Action: "open", Token: result.Token}); !errors.Is(err, errGraphPinReleased) || reply.Failure != 3 {
		t.Fatal("actual keeper release classification", err)
	}
	reopened := nativeScalarWorkloadCut(t, ctx, n, graphReadFresh, graphCut{}, retainedOnlyFeedSemanticContractID())
	result.Reopened = wire(reopened)
	if !feedCutEqual(reopened, final) {
		t.Fatal("retention protocol invented graph round/CDC")
	}
	result.SourceRows[2] = nativeFeedRetainedSources(t, ctx, n, result.SparseFrom, final, result.Original, "reopened-retained-suffix", sources)
	result.Reaped = 7 + nativeGraphCostStop(t, n, 2, processes)
	result.Transport = append(result.Transport, n.all[1].mux.hub.workload.receipt(n.all[1].mux.hub))
	for member := uint64(1); member <= 6; member++ {
		logical, allocated, err := nativeGraphCostFileBytes(filepath.Join(n.dir, fmt.Sprint(member)))
		if err != nil || !nativeColumnAdd(result.PersistedFileBytes, logical) || !nativeColumnAdd(result.AllocatedFileBytes, allocated) {
			t.Fatal("all native/derived/cache physical bytes", err)
		}
		result.PersistedFileBytes += logical
		result.AllocatedFileBytes += allocated
	}
	result.OriginalCaptures, result.ExtraInvocations, result.FeedExtraInvocations = driver.originals, driver.extra, client.extra
	result.LogicalCross = 2 * (spec.Facts * spec.CrossPercent / 100)
	result.LogicalLocal = 2*spec.Facts - result.LogicalCross
	result.TotalNanos = time.Since(started).Nanoseconds()
	result.DiagnosticNanos = result.TotalNanos - result.SetupNanos - result.BusinessNanos
	if result.Reaped != 13 || result.DiagnosticNanos < 0 || ctx.Err() != nil {
		t.Fatal("native owned lifetime/timing", ctx.Err())
	}
	for caller := 0; caller < spec.Writers; caller++ {
		if result.Completed[caller] == 0 {
			t.Fatal("declared caller completed no useful operation")
		}
	}
	result.Scope = "Source-authored actual rolling-ACK retention/physical-GC/unread-feed/SAME-page/ACK/process and checkpoint-column path. Planned IDs use genuine installed-grant references, not Recipient.Next. Native protects an ACK predecessor plus unread prefix; detached FDB retention/availability and allocator costs remain separately labelled. No execution, capacity, paired retained-only or full V2 acceptance."
	if err := open("result.json").Encode(result); err != nil {
		t.Fatal(err)
	}
}
