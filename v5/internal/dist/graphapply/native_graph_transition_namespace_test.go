package graphapply

import (
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestNativeNamespaceSelectorsRequireActualMarkerLineageAndNativeCaller(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	seen := make(map[string]bool)
	for _, kind := range nativeTransitionNamespaceInventory() {
		if !nativeFaultKind(kind) || seen[kind] {
			t.Fatal("duplicate/foreign namespace selector")
		}
		seen[kind] = true
	}
	if len(seen) != 8 || nativeTransitionManifestFrame() != "" {
		t.Fatal("finite namespace inventory or invented native caller")
	}
	owner, err := newNativeFaultFS(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	arm, err := json.Marshal(nativeFaultControl{Version: 1, Action: "arm", Cookie: owner.state.Cookie, Sequence: 1, Kind: "manifest-directory-sync-after", Nth: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.control(arm); err != nil {
		t.Fatal(err)
	}
	if _, matched, err := owner.match(owner.dir, true, "sync", nil); err != nil || matched || owner.state.Seen != 0 {
		t.Fatal("unrelated directory Sync qualified marker publication", err)
	}
	// Even a recorded closed file cannot invent a native Marker.Move stack.
	owner.markerFile, owner.markerClosed = new(nativeFaultFile), true
	owner.markerLength = copy(owner.markerPath[:], "marker.manifest.000002.MANIFEST-000005")
	if _, matched, err := owner.match(owner.dir, true, "sync", nil); err != nil || matched || owner.state.Seen != 0 {
		t.Fatal("earlier unrelated close granted directory publication", err)
	}
}

func TestNativeNamespacePublicationBeforeAfterReopensActualOldAndCurrentSources(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, kind := range nativeTransitionNamespaceInventory() {
		if testing.Short() && kind != "options-rename-before" && kind != "manifest-directory-sync-after" {
			continue
		}
		t.Run(kind, func(t *testing.T) {
			artifact := nativeTransitionNamespaceCase(t, kind)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "..", "tools", "v5-substrate", "native_transition_namespace_check.py"), artifact)
			var diagnostics processStderr
			command.Stdout, command.Stderr = &diagnostics, &diagnostics
			if err := command.Run(); err != nil {
				body, _ := diagnostics.state()
				t.Fatal("independent namespace/original/old-source check", err, body)
			}
			if _, full := diagnostics.state(); full {
				t.Fatal("bounded checker diagnostics")
			}
		})
	}
}
