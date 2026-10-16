package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

type nativeProtocolPredecessorArtifact struct {
	Version    uint8
	Profile    string
	Evidence   nativeProtocolPredecessorEvidence
	BaseSHA256 [32]byte
	BaseBytes  int64
}

func TestNativeProtocolPredecessorProgramsKeepExactReadyInventory(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	rows := nativeProtocolPredecessorInventory()
	counts := map[string]int{}
	names := map[string]bool{}
	for _, r := range rows {
		if !r.valid() || r.name() == "" || names[r.name()] {
			t.Fatal("missing/duplicate real local Ready exclusion", r)
		}
		names[r.name()] = true
		counts[r.Family]++
		for _, layout := range []string{"original", "rotated"} {
			for _, mode := range []string{"delay", "reorder", "duplicate"} {
				s := nativeProtocolPredecessorSpec{1, r, layout, mode, filepath.Join(t.TempDir(), "predecessor.json")}
				if !s.valid() {
					t.Fatal("finite actual mechanism refused", s)
				}
				if r.Fence != nil && s.mechanism() != "emitted-predecessor-tick-packets" {
					t.Fatal("local fence invented a source Submit")
				}
				if mode == "reorder" && s.mechanism() != "emitted-predecessor-tick-packets" {
					t.Fatal("same-command pre-emission frame invented")
				}
			}
		}
	}
	if len(rows) != 20 || counts["p02-local-fence"] != 2 || counts["p06-first-live"] != 12 || counts["p03-recovery"] != 4 || counts["partial-recovery"] != 2 {
		t.Fatal("exact120 source inventory changed", counts)
	}
	for _, bad := range []nativeProtocolPredecessorRecipe{{}, {Family: "p03-recovery", Coordinator: &nativeProtocolCoordinatorRecipe{3, crossPrepare, 3, raftlog.StableStorageReady, "before"}}, {Family: "partial-recovery", Partial: &nativeProtocolPartialRecipe{crossResolve, 8, raftlog.StableStorageApplication, "after"}}} {
		if bad.valid() {
			t.Fatal("foreign native occurrence acquired companion", bad)
		}
	}
	t.Setenv(nativeProtocolPredecessorEnv, "")
	if owner, err := nativeProtocolPredecessorFromEnv(nil); owner != nil || err != nil {
		t.Fatal("literal old program changed", owner, err)
	}
	t.Setenv(nativeProtocolPredecessorEnv, "{}")
	if _, err := nativeProtocolPredecessorFromEnv(nil); !errors.Is(err, errInvalid) {
		t.Fatal("unbound companion admitted", err)
	}
}

// All semantic bodies/observers/original and CDC assertions are the existing
// literal program adapters. Only the private, explicitly admitted companion
// owner changes delivery. No case asserts selected-command Raft emission.
func TestNativeProtocolReadyLossWithServiceAndPredecessorMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, layout := range []string{"original", "rotated"} {
		for _, mode := range []string{"delay", "reorder", "duplicate"} {
			for index, recipe := range nativeProtocolPredecessorInventory() {
				if testing.Short() && index != 0 && index != 2 && index != 14 && index != 18 {
					continue
				}
				t.Run(layout+"/"+mode+"/"+recipe.name(), func(t *testing.T) {
					output := filepath.Join(t.TempDir(), "predecessor.json")
					s := nativeProtocolPredecessorSpec{1, recipe, layout, mode, output}
					wire, err := json.Marshal(s)
					if err != nil {
						t.Fatal(err)
					}
					t.Setenv(nativeProtocolPredecessorEnv, string(wire))
					literal := nativeProtocolProgramSpec{1, layout, "literal"}
					switch recipe.Family {
					case "p02-local-fence":
						nativeProtocolFenceCase(t, *recipe.Fence, literal)
					case "p06-first-live":
						nativeProtocolLiveP06Case(t, *recipe.Live, literal)
					case "p03-recovery":
						nativeProtocolCoordinatorCase(t, *recipe.Coordinator, literal)
					case "partial-recovery":
						nativeFaultPartialCaseOwned(t, &literal, *recipe.Partial)
					default:
						t.Fatal("unknown finite companion family")
					}
					// Each original adapter already completed its unchanged full oracle.
					// Bind the final raw EOF/sidecars, never a producer success summary.
					body, err := nativeProtocolPredecessorRead(output, nativeProtocolPredecessorOutput)
					if err != nil {
						t.Fatal(err)
					}
					var evidence nativeProtocolPredecessorEvidence
					if err := nativeFeedDecodeJSON(body, &evidence); err != nil {
						t.Fatal(err)
					}
					actual, encoded := json.Marshal(evidence.Spec)
					if encoded != nil || !bytes.Equal(actual, wire) {
						t.Fatal("immutable companion admission changed", encoded)
					}
					base, err := os.Open(evidence.BasePath)
					if err != nil {
						t.Fatal(err)
					}
					hash := sha256.New()
					count, copied := io.Copy(hash, io.LimitReader(base, (16<<20)+1))
					closed := base.Close()
					if copied != nil || closed != nil || count <= 0 || count > 16<<20 {
						t.Fatal("complete bounded semantic artifact", copied, closed)
					}
					artifact := nativeProtocolPredecessorArtifact{Version: 1, Profile: nativeProtocolPredecessorProfile, Evidence: evidence, BaseBytes: count}
					copy(artifact.BaseSHA256[:], hash.Sum(nil))
					final := filepath.Join(filepath.Dir(output), "complete.json")
					file, err := os.OpenFile(final, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
					if err != nil {
						t.Fatal(err)
					}
					if err := errors.Join(json.NewEncoder(file).Encode(artifact), file.Close()); err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, evidence.DeadlineUnixNanos))
					defer cancel()
					command := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "..", "tools", "v5-substrate", "native_protocol_campaign_predecessor_check.py"), final)
					var diagnostic processStderr
					command.Stdout, command.Stderr = &diagnostic, &diagnostic
					if err := command.Run(); err != nil {
						message, _ := diagnostic.state()
						t.Fatal("independent original semantic then companion joins", err, message)
					}
					if _, overflow := diagnostic.state(); overflow {
						t.Fatal("bounded companion checker output")
					}
				})
			}
		}
	}
}
