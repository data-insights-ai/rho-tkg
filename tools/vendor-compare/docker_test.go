package vendorcompare

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func ownedFakeCommand(t *testing.T, badOwner bool) commandFunc {
	t.Helper()
	return func(_ context.Context, args ...string) ([]byte, error) {
		command := strings.Join(args, " ")
		switch {
		case strings.Contains(command, "docker inspect"):
			project := ProjectName
			if badOwner {
				project = "foreign"
			}
			data := map[string]any{"Id": strings.Repeat("a", 64), "Image": "sha256:" + strings.Repeat("b", 64), "SizeRw": 1234,
				"Config":     map[string]any{"Image": ImageReference, "Labels": map[string]string{"com.docker.compose.project": project, "io.rho.vendor-compare.scope": "tg20261010"}},
				"State":      map[string]bool{"Running": true},
				"HostConfig": map[string]any{"NanoCpus": int64(4_000_000_000), "Memory": int64(12 << 30), "MemorySwap": int64(12 << 30), "NetworkMode": ProjectName + "_default", "PortBindings": map[string]any{"14240/tcp": []any{map[string]string{"HostIp": "127.0.0.1", "HostPort": "19240"}}}},
				"Mounts":     []any{map[string]any{"Type": "volume", "Name": "rho-vendor-tg-20261010-home", "Source": "/daemon/owned-volume", "Destination": "/home/tigergraph", "RW": true}, map[string]any{"Type": "bind", "Destination": "/opt/vendor-compare/schema.gsql", "RW": false}}}
			return json.Marshal(data)
		case strings.Contains(command, "docker volume inspect"):
			return json.Marshal(map[string]any{"Name": "rho-vendor-tg-20261010-home", "Driver": "local", "Mountpoint": "/daemon/owned-volume", "CreatedAt": "fixture-generation1", "Labels": map[string]string{"com.docker.compose.project": ProjectName, "com.docker.compose.volume": "tigergraph-home", "io.rho.vendor-compare.scope": "tg20261010"}})
		case strings.Contains(command, "docker image inspect"):
			return json.Marshal(map[string]any{"Id": "sha256:" + strings.Repeat("b", 64), "Architecture": "amd64", "Os": "linux", "RepoDigests": []string{"tigergraph/tigergraph@sha256:78a3d62604527ba8686930465554fc3a419bf4b4de5c3d2d50202825a5308d49"}})
		case strings.Contains(command, "docker info"):
			return []byte("aarch64\n"), nil
		case strings.Contains(command, "gsql version"):
			return []byte("GSQL version 4.2.5\n"), nil
		case strings.Contains(command, "sha256sum"):
			return []byte(checksum(schema) + "  /opt/vendor-compare/schema.gsql\n"), nil
		case strings.Contains(command, "memory.current"):
			return []byte("123456\n"), nil
		case strings.Contains(command, "memory.peak"):
			return []byte("234567\n"), nil
		case strings.Contains(command, "memory.stat"):
			return []byte("anon 100\nfile 200\n"), nil
		case strings.Contains(command, "cpu.stat"):
			return []byte("usage_usec 555\nnr_periods 4\n"), nil
		case strings.Contains(command, "find /home/tigergraph"):
			return []byte("data/a\t17\t8\t1:1\ndata/hardlink\t17\t8\t1:1\ndata/zero\t0\t0\t1:2\n"), nil
		case strings.Contains(command, "gadmin start all"), strings.Contains(command, "gadmin stop all"), strings.Contains(command, "docker restart"), strings.Contains(command, "gsql /opt/vendor-compare/schema.gsql"):
			return []byte("OK"), nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return nil, ErrContract
		}
	}
}
func isMutation(args []string) bool {
	text := strings.Join(args, " ")
	return strings.Contains(text, "gadmin start") || strings.Contains(text, "gadmin stop") || strings.Contains(text, "docker restart") || strings.Contains(text, "gsql /opt")
}
func TestDockerExactOwnershipRefusalsBeforeMutation(t *testing.T) {
	for _, fault := range []string{"extra-mount", "duplicate-home", "duplicate-schema", "writable-schema", "wrong-schema-type", "wrong-volume", "wrong-volume-source", "readonly-home", "volume-label", "volume-project", "volume-name", "volume-driver", "volume-mountpoint", "volume-created", "extra-port", "public-port", "missing-port", "duplicate-port", "cpu", "memory", "swap", "privileged", "network", "image", "short-container-id", "short-image-id", "schema-hash", "schema-path", "schema-extra"} {
		t.Run(fault, func(t *testing.T) {
			base := ownedFakeCommand(t, false)
			mutations := 0
			controller := &DockerController{waitReady: func(context.Context) error { return nil }}
			faultCommand := func(ctx context.Context, args ...string) ([]byte, error) {
				if isMutation(args) {
					mutations++
				}
				data, err := base(ctx, args...)
				if err != nil {
					return nil, err
				}
				text := strings.Join(args, " ")
				if strings.Contains(text, "sha256sum") {
					switch fault {
					case "schema-hash":
						return []byte(strings.Repeat("0", 64) + " /opt/vendor-compare/schema.gsql"), nil
					case "schema-path":
						return []byte(checksum(schema) + " /other/schema.gsql"), nil
					case "schema-extra":
						return append(data, []byte("extra\n")...), nil
					}
				}
				if strings.Contains(text, "docker inspect") {
					var object map[string]any
					if err := json.Unmarshal(data, &object); err != nil {
						t.Fatal(err)
					}
					mounts := object["Mounts"].([]any)
					host := object["HostConfig"].(map[string]any)
					ports := host["PortBindings"].(map[string]any)
					switch fault {
					case "extra-mount":
						object["Mounts"] = append(mounts, map[string]any{"Type": "bind", "Destination": "/shared", "RW": true})
					case "duplicate-home":
						object["Mounts"] = []any{mounts[0], mounts[0]}
					case "duplicate-schema":
						object["Mounts"] = []any{mounts[1], mounts[1]}
					case "writable-schema":
						mounts[1].(map[string]any)["RW"] = true
					case "wrong-schema-type":
						mounts[1].(map[string]any)["Type"] = "volume"
					case "wrong-volume":
						mounts[0].(map[string]any)["Name"] = "foreign-volume"
					case "wrong-volume-source":
						mounts[0].(map[string]any)["Source"] = "/daemon/foreign-volume"
					case "readonly-home":
						mounts[0].(map[string]any)["RW"] = false
					case "extra-port":
						ports["22/tcp"] = []any{map[string]string{"HostIp": "127.0.0.1", "HostPort": "19222"}}
					case "public-port":
						ports["14240/tcp"].([]any)[0].(map[string]any)["HostIp"] = "0.0.0.0"
					case "missing-port":
						delete(ports, "14240/tcp")
					case "duplicate-port":
						ports["14240/tcp"] = append(ports["14240/tcp"].([]any), map[string]any{"HostIp": "127.0.0.1", "HostPort": "19241"})
					case "cpu":
						host["NanoCpus"] = float64(2_000_000_000)
					case "memory":
						host["Memory"] = float64(8 << 30)
					case "swap":
						host["MemorySwap"] = float64(16 << 30)
					case "privileged":
						host["Privileged"] = true
					case "network":
						host["NetworkMode"] = "host"
					case "image":
						object["Config"].(map[string]any)["Image"] = "tigergraph/tigergraph:latest"
					case "short-container-id":
						object["Id"] = "aaa"
					case "short-image-id":
						object["Image"] = "sha256:bbb"
					}
					return json.Marshal(object)
				}
				if strings.Contains(text, "docker volume inspect") {
					var object map[string]any
					if err := json.Unmarshal(data, &object); err != nil {
						t.Fatal(err)
					}
					switch fault {
					case "volume-label":
						object["Labels"].(map[string]any)["io.rho.vendor-compare.scope"] = "foreign"
					case "volume-project":
						object["Labels"].(map[string]any)["com.docker.compose.project"] = "foreign"
					case "volume-name":
						object["Name"] = "foreign-volume"
					case "volume-driver":
						object["Driver"] = "nfs"
					case "volume-mountpoint":
						object["Mountpoint"] = ""
					case "volume-created":
						object["CreatedAt"] = ""
					}
					return json.Marshal(object)
				}
				return data, nil
			}
			controller.command = faultCommand
			if _, err := controller.Prepare(t.Context()); !errors.Is(err, ErrContract) || mutations != 0 {
				t.Fatal(fault, err, mutations)
			}
			// The same preflight must guard the restart path, including schema bytes.
			controller.command = base
			if _, err := controller.Prepare(t.Context()); err != nil {
				t.Fatal(err)
			}
			mutations = 0
			controller.command = faultCommand
			if err := controller.Reopen(t.Context()); !errors.Is(err, ErrContract) || mutations != 0 {
				t.Fatal("reopen", fault, err, mutations)
			}
		})
	}
}
func TestDockerImmutableIDReplacementAndObservationIdentity(t *testing.T) {
	base := ownedFakeCommand(t, false)
	replaced := false
	mutations := 0
	execIDs := []string{}
	restartIDs := []string{}
	controller := &DockerController{waitReady: func(context.Context) error { return nil }}
	controller.command = func(ctx context.Context, args ...string) ([]byte, error) {
		if isMutation(args) {
			mutations++
		}
		if len(args) > 4 && args[0] == "docker" && args[1] == "exec" {
			execIDs = append(execIDs, args[4])
		}
		if len(args) == 3 && args[0] == "docker" && args[1] == "restart" {
			restartIDs = append(restartIDs, args[2])
		}
		data, err := base(ctx, args...)
		if err != nil {
			return nil, err
		}
		text := strings.Join(args, " ")
		if replaced && strings.Contains(text, "docker inspect") {
			var object map[string]any
			if err := json.Unmarshal(data, &object); err != nil {
				t.Fatal(err)
			}
			object["Id"] = strings.Repeat("c", 64)
			return json.Marshal(object)
		}
		return data, nil
	}
	if _, err := controller.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := controller.Reopen(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(restartIDs, []string{strings.Repeat("a", 64)}) {
		t.Fatal("mutable-name restart", restartIDs)
	}
	for _, id := range execIDs {
		if id != strings.Repeat("a", 64) {
			t.Fatal("mutable-name exec", id)
		}
	}
	mutations = 0
	replaced = true
	if err := controller.Reopen(t.Context()); !errors.Is(err, ErrContract) || mutations != 0 {
		t.Fatal("copied-label replacement", err, mutations)
	}
	replaced = false
	original := controller.command
	inspections := 0
	controller.command = func(ctx context.Context, args ...string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "docker inspect") {
			inspections++
			if inspections == 2 {
				replaced = true
			}
		}
		return original(ctx, args...)
	}
	if _, err := controller.Observe(t.Context()); !errors.Is(err, ErrContract) {
		t.Fatal("observation identity changed", err)
	}
}
func TestDockerReplacementDuringReadOnlyPreflightAndVolumeRecreation(t *testing.T) {
	for _, fault := range []string{"container-prestart", "recreated-volume"} {
		t.Run(fault, func(t *testing.T) {
			base := ownedFakeCommand(t, false)
			inspections, mutations := 0, 0
			controller := &DockerController{waitReady: func(context.Context) error { return nil }}
			controller.command = func(ctx context.Context, args ...string) ([]byte, error) {
				if isMutation(args) {
					mutations++
				}
				data, err := base(ctx, args...)
				if err != nil {
					return nil, err
				}
				text := strings.Join(args, " ")
				if strings.Contains(text, "docker inspect") {
					inspections++
				}
				if inspections == 2 && ((fault == "container-prestart" && strings.Contains(text, "docker inspect")) || (fault == "recreated-volume" && strings.Contains(text, "docker volume inspect"))) {
					var object map[string]any
					if err := json.Unmarshal(data, &object); err != nil {
						t.Fatal(err)
					}
					if fault == "container-prestart" {
						object["Id"] = strings.Repeat("c", 64)
					} else {
						object["CreatedAt"] = "fixture-generation2"
					}
					return json.Marshal(object)
				}
				return data, nil
			}
			if _, err := controller.Prepare(t.Context()); !errors.Is(err, ErrContract) || mutations != 0 {
				t.Fatal(fault, err, mutations)
			}
		})
	}
}
func TestDockerControllerOwnsOnlyFixedDeployment(t *testing.T) {
	controller := NewDockerController()
	if controller == nil || controller.command == nil {
		t.Fatal("inert constructor")
	}
	controller.command = ownedFakeCommand(t, false)
	controller.waitReady = func(context.Context) error { return nil }
	identity, err := controller.Prepare(t.Context())
	if err != nil || identity.Version != "4.2.5" || identity.Project != ProjectName || identity.HostArchitecture != "aarch64" || !strings.Contains(identity.ExecutionMode, "emulation") {
		t.Fatal(identity, err)
	}
	if err := controller.Reopen(t.Context()); err != nil {
		t.Fatal(err)
	}
	costs, err := controller.Observe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if costs.Complete || costs.CgroupMemoryCurrent.Value == nil || *costs.CgroupMemoryCurrent.Value != 123456 || costs.CgroupCPUUsec.Value == nil || *costs.CgroupCPUUsec.Value != 555 {
		t.Fatal(costs)
	}
	if costs.HomeVolumeAllocated.Status != "partial" || costs.HomeVolumeAllocated.Value == nil || *costs.HomeVolumeAllocated.Value != 4096 || *costs.HomeVolumeApparent.Value != 34 {
		t.Fatal("hardlink/default/partial accounting", costs)
	}
	if costs.ProcessTreePSS.Value != nil || costs.AllReplicaTotal.Value != nil || costs.ImageProvisioning.Value != nil {
		t.Fatal("invented unavailable costs")
	}
	if !slices.Contains(costs.Unmeasured, "Docker stdout logs") {
		t.Fatal(costs.Unmeasured)
	}
	controller.command = ownedFakeCommand(t, true)
	if _, err := controller.Prepare(t.Context()); !errors.Is(err, ErrContract) {
		t.Fatal("foreign owner", err)
	}
	if err := controller.Reopen(t.Context()); !errors.Is(err, ErrContract) {
		t.Fatal("foreign restart", err)
	}
	if _, err := controller.Observe(t.Context()); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
}
func TestDockerNilAndUnavailableMetricRefusals(t *testing.T) {
	var nilController *DockerController
	if _, err := nilController.Prepare(t.Context()); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	if err := nilController.Reopen(t.Context()); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	if _, err := nilController.Observe(t.Context()); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	controller := &DockerController{command: ownedFakeCommand(t, false), waitReady: func(context.Context) error { return nil }}
	if _, err := controller.Prepare(nil); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	base := controller.command
	controller.command = func(ctx context.Context, args ...string) ([]byte, error) {
		command := strings.Join(args, " ")
		if strings.Contains(command, "cat /sys/") || strings.Contains(command, "find /home/") {
			return nil, ErrUnsupported
		}
		return base(ctx, args...)
	}
	costs, err := controller.Observe(t.Context())
	if err != nil || costs.CgroupMemoryCurrent.Value != nil || costs.HomeVolumeAllocated.Value != nil || costs.Complete {
		t.Fatal(costs, err)
	}
	for _, data := range []string{"anon -1", "anon 1\nanon 2", "broken", "anon 18446744073709551616"} {
		if _, err := parseCounters([]byte(data)); !errors.Is(err, ErrContract) {
			t.Fatal(data, err)
		}
	}
}
func TestCommandSubprocessHelper(t *testing.T) {
	if os.Getenv("VENDOR_COMPARE_COMMAND_HELPER") != "1" {
		return
	}
	args := os.Args
	for i, arg := range args {
		if arg == "--" {
			args = args[i+1:]
			break
		}
	}
	switch args[0] {
	case "both":
		fmt.Fprint(os.Stdout, "stdout")
		fmt.Fprint(os.Stderr, "stderr")
	case "exit":
		os.Exit(7)
	case "cap":
		for range 34 {
			_, _ = os.Stdout.Write(bytes.Repeat([]byte("x"), 1<<20))
		}
	case "wait":
		if err := os.WriteFile(args[1], []byte("started"), 0600); err != nil {
			os.Exit(8)
		}
		fmt.Fprint(os.Stdout, "started")
		time.Sleep(time.Minute)
	default:
		os.Exit(9)
	}
	os.Exit(0)
}
func TestBoundedCommandHelperOutputErrorsAndCancellation(t *testing.T) {
	var output cappedOutput
	data := bytes.Repeat([]byte("x"), 32<<20)
	if n, err := output.Write(data); err != nil || n != len(data) {
		t.Fatal(n, err)
	}
	if n, err := output.Write([]byte("overflow")); !errors.Is(err, ErrLimit) || n != 0 || output.buffer.Len() != len(data) {
		t.Fatal(n, err, output.buffer.Len())
	}
	for _, args := range [][]string{nil, {"unused"}} {
		ctx := t.Context()
		if len(args) > 0 {
			ctx = nil
		}
		if _, err := execute(ctx, args...); !errors.Is(err, ErrContract) {
			t.Fatal(err)
		}
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("VENDOR_COMPARE_COMMAND_HELPER", "1")
	helper := func(mode string, extra ...string) []string {
		return append([]string{binary, "-test.run=^TestCommandSubprocessHelper$", "--", mode}, extra...)
	}
	result, err := execute(t.Context(), helper("both")...)
	if err != nil || !bytes.Contains(result, []byte("stdout")) || !bytes.Contains(result, []byte("stderr")) {
		t.Fatal(string(result), err)
	}
	if _, err := execute(t.Context(), helper("exit")...); err == nil {
		t.Fatal("nonzero child accepted")
	} else if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 7 {
		t.Fatal(err)
	}
	if _, err := execute(t.Context(), helper("cap")...); !errors.Is(err, ErrLimit) {
		t.Error("child exit masked output limit", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	marker := filepath.Join(t.TempDir(), "started")
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.Tick(10 * time.Millisecond)
		for {
			if _, err := os.Stat(marker); err == nil {
				cancel()
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker:
			}
		}
	}()
	_, err = execute(ctx, helper("wait", marker)...)
	<-done
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Fatal("cancellation never exercised running child", statErr)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatal("child cancellation identity", err)
	}
}

type readinessTransport func(*http.Request) (*http.Response, error)

func (f readinessTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type failingReadCloser struct{ readErr, closeErr error }

func (b failingReadCloser) Read([]byte) (int, error) { return 0, b.readErr }
func (b failingReadCloser) Close() error             { return b.closeErr }
func TestReadinessAcceptsCompleteEchoAndRefusesMalformedOrCancelledResponses(t *testing.T) {
	original := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = original })
	for _, test := range []struct {
		name, body                      string
		status                          int
		transportErr, readErr, closeErr error
		valid                           bool
	}{
		{"valid-extra-fields", `{"error":false,"message":"Hello","version":{"api":"v2"}}`, 200, nil, nil, nil, true},
		{"missing-error", `{"message":"Hello"}`, 200, nil, nil, nil, false},
		{"true-error", `{"error":true}`, 200, nil, nil, nil, false},
		{"duplicate-error", `{"error":true,"error":false}`, 200, nil, nil, nil, false},
		{"oversized", `{"error":false}` + strings.Repeat(" ", 1<<20), 200, nil, nil, nil, false},
		{"bad-http", `{"error":false}`, 503, nil, nil, nil, false},
		{"malformed", `{"error":false`, 200, nil, nil, nil, false},
		{"transport", "", 200, ErrContract, nil, nil, false},
		{"read", "", 200, nil, ErrContract, nil, false},
		{"close", "", 200, nil, io.EOF, ErrContract, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			http.DefaultTransport = readinessTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Method != http.MethodGet || r.URL.String() != Endpoint+"/echo" {
					t.Fatal(r.Method, r.URL)
				}
				if !test.valid {
					cancel()
				}
				if test.transportErr != nil {
					return nil, test.transportErr
				}
				var body io.ReadCloser = io.NopCloser(strings.NewReader(test.body))
				if test.readErr != nil || test.closeErr != nil {
					body = failingReadCloser{test.readErr, test.closeErr}
				}
				return &http.Response{StatusCode: test.status, Body: body, Header: make(http.Header), Request: r}, nil
			})
			err := new(DockerController).ready(ctx)
			if test.valid {
				if err != nil || calls != 1 {
					t.Fatal(err, calls)
				}
			} else if !errors.Is(err, context.Canceled) || calls != 1 {
				t.Error("unsafe ready acceptance", err, calls)
			}
		})
	}
}
