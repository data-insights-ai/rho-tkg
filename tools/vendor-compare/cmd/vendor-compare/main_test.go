package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCapabilitiesAndUsageNeverRunVendor(t *testing.T) {
	for _, test := range []struct {
		args []string
		want int
	}{
		{[]string{"capabilities"}, 0}, {nil, 2}, {[]string{"unknown"}, 2}, {[]string{"run", "--unknown"}, 2}, {[]string{"run", "--timeout", "0s"}, 2},
	} {
		var out, errOut bytes.Buffer
		if result := run(t.Context(), test.args, &out, &errOut); result != test.want {
			t.Fatal(result, out.String(), errOut.String())
		}
	}
	var out, errOut bytes.Buffer
	if result := run(nil, []string{"capabilities"}, &out, &errOut); result != 2 {
		t.Fatal(result)
	}
}

type refusingWriter struct{ err error }

func (w refusingWriter) Write([]byte) (int, error) { return 0, w.err }
func TestCLIOutputFailureBoundsAndPreVendorArtifactRefusals(t *testing.T) {
	var errOut bytes.Buffer
	if code := run(t.Context(), []string{"capabilities"}, refusingWriter{errors.New("writer refused")}, &errOut); code != 1 {
		t.Fatal(code)
	}
	for _, args := range [][]string{{"run", "--timeout", "31m"}, {"run", "unexpected-position"}, {"run", "--timeout", "invalid"}} {
		var out, errOut bytes.Buffer
		if code := run(t.Context(), args, &out, &errOut); code != 2 || out.Len() != 0 {
			t.Fatal(args, code, out.String(), errOut.String())
		}
	}
	base := t.TempDir()
	trap := filepath.Join(base, "bin")
	if err := os.Mkdir(trap, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(base, "vendor-called")
	if err := os.WriteFile(filepath.Join(trap, "docker"), []byte("#!/bin/sh\nprintf invoked > \"$VENDOR_COMPARE_TRAP_MARKER\"\nexit 99\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VENDOR_COMPARE_TRAP_MARKER", marker)
	t.Setenv("PATH", trap+string(os.PathListSeparator)+os.Getenv("PATH"))
	reference := os.Getenv("VENDOR_COMPARE_REFERENCE")
	if reference == "" {
		t.Fatal("VENDOR_COMPARE_REFERENCE required for valid guard controls")
	}
	for _, fault := range []string{"missing-reference", "cancelled", "existing-file", "existing-directory", "dangling", "repository", "unavailable-git"} {
		t.Run(fault, func(t *testing.T) {
			parent := t.TempDir()
			output := filepath.Join(parent, "output")
			ctx := t.Context()
			args := []string{"run", "--reference", reference, "--out", output}
			switch fault {
			case "missing-reference":
				args = []string{"run", "--out", output}
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "existing-file":
				if err := os.WriteFile(output, []byte("caller data"), 0600); err != nil {
					t.Fatal(err)
				}
			case "existing-directory":
				if err := os.Mkdir(output, 0700); err != nil {
					t.Fatal(err)
				}
			case "dangling":
				if err := os.Symlink(filepath.Join(parent, "absent"), output); err != nil {
					t.Fatal(err)
				}
			case "repository":
				if err := os.Mkdir(filepath.Join(parent, ".git"), 0700); err != nil {
					t.Fatal(err)
				}
			case "unavailable-git":
				t.Setenv("PATH", trap)
			}
			var out, errOut bytes.Buffer
			if code := run(ctx, args, &out, &errOut); code != 1 || out.Len() != 0 || !strings.Contains(errOut.String(), "adapter run refused:") {
				t.Fatal(fault, code, out.String(), errOut.String())
			}
			if _, err := os.Lstat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("CLI reached vendor subprocess", err)
			}
			if fault == "existing-file" {
				if data, err := os.ReadFile(output); err != nil || string(data) != "caller data" {
					t.Fatal("caller data changed", string(data), err)
				}
			}
			if _, err := os.Lstat(filepath.Join(output, "completion.json")); !errors.Is(err, os.ErrNotExist) && fault != "existing-file" {
				t.Fatal("refusal completion appeared", err)
			}
		})
	}
}
