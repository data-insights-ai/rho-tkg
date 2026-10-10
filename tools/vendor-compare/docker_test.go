package vendorcompare

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
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
			data := map[string]any{"Id": "test-owned", "Image": "sha256:test-image", "SizeRw": 1234,
				"Config":     map[string]any{"Image": ImageReference, "Labels": map[string]string{"com.docker.compose.project": project, "io.rho.vendor-compare.scope": "tg20261010"}},
				"State":      map[string]bool{"Running": true},
				"HostConfig": map[string]any{"PortBindings": map[string]any{"14240/tcp": []any{map[string]string{"HostIp": "127.0.0.1", "HostPort": "19240"}}}},
				"Mounts":     []any{map[string]any{"Type": "volume", "Name": "rho-vendor-tg-20261010-home", "Destination": "/home/tigergraph", "RW": true}}}
			return json.Marshal(data)
		case strings.Contains(command, "docker image inspect"):
			return json.Marshal(map[string]any{"Id": "sha256:test-image", "Architecture": "amd64", "Os": "linux", "RepoDigests": []string{"tigergraph/tigergraph@sha256:78a3d62604527ba8686930465554fc3a419bf4b4de5c3d2d50202825a5308d49"}})
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
