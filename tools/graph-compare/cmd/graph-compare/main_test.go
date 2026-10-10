package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, errors.New("injected output failure") }
func TestCLIClaimsOnlySupportedInventoryAndRefusesErrors(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run(t.Context(), []string{"capabilities"}, &out, &errOut); code != 0 {
		t.Fatal(code)
	}
	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil || got["native_v5"] != false || got["performance_acceptance"] != false || got["current_answers"] != float64(92) {
		t.Fatal("incorrect capabilities", got, err)
	}
	for _, args := range [][]string{nil, {"vendor"}, {"run", "--unknown"}, {"run", "unexpected"}, {"run", "--out", filepath.Join(t.TempDir(), "refused"), "--max-rows", "0"}} {
		out.Reset()
		errOut.Reset()
		if code := run(t.Context(), args, &out, &errOut); code == 0 || out.Len() != 0 {
			t.Fatal("CLI mislabeled refusal", args, code, out.String())
		}
	}
	if code := run(t.Context(), []string{"capabilities"}, failedWriter{}, &errOut); code != 1 {
		t.Fatal("output error", code)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	out.Reset()
	if code := run(ctx, []string{"run", "--out", filepath.Join(t.TempDir(), "cancelled")}, &out, &errOut); code != 1 || out.Len() != 0 {
		t.Fatal("cancel published success", code)
	}
}
func TestCLIActualMemoryFixture(t *testing.T) {
	var out, errOut bytes.Buffer
	path := filepath.Join(t.TempDir(), "published")
	args := []string{"run", "--dataset", "../../../reference/fixtures/basic-small", "--reference", "../../../reference", "--pin", "../../../reference-pin.json", "--out", path}
	if code := run(t.Context(), args, &out, &errOut); code != 0 {
		t.Fatal(code, errOut.String())
	}
	if !bytes.Contains(out.Bytes(), []byte(`"validated_current":92`)) || !bytes.Contains(out.Bytes(), []byte(`"pending_history":1`)) {
		t.Fatal("wrong complete inventory", out.String())
	}
	if _, err := os.Stat(filepath.Join(path, "report.json")); err != nil {
		t.Fatal(err)
	}
}
