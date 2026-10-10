package vendorcompare

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed deploy/tigergraph-schema.gsql
var schema []byte

const (
	gadminExecutable = "/home/tigergraph/tigergraph/app/4.2.5/cmd/gadmin"
	gsqlExecutable   = "/home/tigergraph/tigergraph/app/4.2.5/cmd/gsql"
)

type commandFunc func(context.Context, ...string) ([]byte, error)
type cappedOutput struct {
	buffer   bytes.Buffer
	limitErr error
}

func (w *cappedOutput) Write(data []byte) (int, error) {
	if len(data) > 32<<20-w.buffer.Len() {
		w.limitErr = ErrLimit
		return 0, ErrLimit
	}
	return w.buffer.Write(data)
}
func execute(ctx context.Context, args ...string) ([]byte, error) {
	if ctx == nil || len(args) == 0 {
		return nil, ErrContract
	}
	// #nosec G204 -- Call sites supply fixed Docker commands and owned names; no shell interpolation.
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	var output cappedOutput
	cmd.Stdout = &output
	cmd.Stderr = &output
	err := cmd.Run()
	if err != nil || output.limitErr != nil {
		return nil, fmt.Errorf("%w: command refused output_sha256=%s", errors.Join(err, ctx.Err(), output.limitErr), checksum(output.buffer.Bytes()))
	}
	return output.buffer.Bytes(), nil
}

// DockerController never pulls or creates containers/volumes. It only admits the
// fixed labeled deployment configured by the bundled Compose definition.
type DockerController struct {
	mu                                      sync.Mutex
	command                                 commandFunc
	waitReady                               func(context.Context) error
	containerID, imageID, volumeFingerprint string
}

// NewDockerController creates an inert controller; deployment commands are explicit.
func NewDockerController() *DockerController { return &DockerController{command: execute} }

type containerInspect struct {
	ID     string  `json:"Id"`
	Image  string  `json:"Image"`
	SizeRW *uint64 `json:"SizeRw"`
	Config struct {
		Image  string            `json:"Image"`
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	State struct {
		Running bool `json:"Running"`
	} `json:"State"`
	HostConfig struct {
		PortBindings map[string][]struct{ HostIP, HostPort string } `json:"PortBindings"`
		NanoCPUs     int64                                          `json:"NanoCpus"`
		Memory       int64                                          `json:"Memory"`
		MemorySwap   int64                                          `json:"MemorySwap"`
		Privileged   bool                                           `json:"Privileged"`
		NetworkMode  string                                         `json:"NetworkMode"`
	} `json:"HostConfig"`
	Mounts []struct {
		Type, Name, Source, Destination string
		RW                              bool
	} `json:"Mounts"`
}

func fullID(id string) bool {
	if len(id) != 64 || strings.ToLower(id) != id {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}
func (d *DockerController) owned(ctx context.Context) (containerInspect, Identity, error) {
	if d == nil || d.command == nil || ctx == nil {
		return containerInspect{}, Identity{}, ErrContract
	}
	data, err := d.command(ctx, "docker", "inspect", "--size", "--format", "{{json .}}", ContainerName)
	if err != nil {
		return containerInspect{}, Identity{}, err
	}
	var container containerInspect
	if err := json.Unmarshal(data, &container); err != nil {
		return containerInspect{}, Identity{}, err
	}
	if !fullID(container.ID) || !strings.HasPrefix(container.Image, "sha256:") || !fullID(strings.TrimPrefix(container.Image, "sha256:")) || !container.State.Running || container.Config.Image != ImageReference || container.Config.Labels["com.docker.compose.project"] != ProjectName || container.Config.Labels["io.rho.vendor-compare.scope"] != "tg20261010" {
		return containerInspect{}, Identity{}, ErrContract
	}
	if container.HostConfig.NanoCPUs != 4_000_000_000 || container.HostConfig.Memory != 12<<30 || container.HostConfig.MemorySwap != 12<<30 || container.HostConfig.Privileged || container.HostConfig.NetworkMode != ProjectName+"_default" {
		return containerInspect{}, Identity{}, ErrContract
	}
	ports := container.HostConfig.PortBindings["14240/tcp"]
	if len(container.HostConfig.PortBindings) != 1 || len(ports) != 1 || ports[0].HostIP != "127.0.0.1" || ports[0].HostPort != "19240" {
		return containerInspect{}, Identity{}, ErrContract
	}
	if len(container.Mounts) != 2 {
		return containerInspect{}, Identity{}, ErrContract
	}
	home, schemaMount := false, false
	var homeSource string
	for _, mount := range container.Mounts {
		if mount.Destination == "/home/tigergraph" {
			if mount.Type != "volume" || mount.Name != "rho-vendor-tg-20261010-home" || !mount.RW {
				return containerInspect{}, Identity{}, ErrContract
			}
			if home {
				return containerInspect{}, Identity{}, ErrContract
			}
			home = true
			homeSource = mount.Source
		} else if mount.Destination == "/opt/vendor-compare/schema.gsql" {
			if schemaMount || mount.Type != "bind" || mount.RW {
				return containerInspect{}, Identity{}, ErrContract
			}
			schemaMount = true
		} else {
			return containerInspect{}, Identity{}, ErrContract
		}
	}
	if !home || !schemaMount {
		return containerInspect{}, Identity{}, ErrContract
	}
	data, err = d.command(ctx, "docker", "volume", "inspect", "--format", "{{json .}}", "rho-vendor-tg-20261010-home")
	if err != nil {
		return containerInspect{}, Identity{}, err
	}
	var volume struct {
		Name, Driver, Mountpoint, CreatedAt string
		Labels                              map[string]string
	}
	if err := json.Unmarshal(data, &volume); err != nil {
		return containerInspect{}, Identity{}, err
	}
	if volume.Name != "rho-vendor-tg-20261010-home" || volume.Driver != "local" || volume.Mountpoint == "" || volume.Mountpoint != homeSource || volume.CreatedAt == "" || volume.Labels["com.docker.compose.project"] != ProjectName || volume.Labels["com.docker.compose.volume"] != "tigergraph-home" || volume.Labels["io.rho.vendor-compare.scope"] != "tg20261010" {
		return containerInspect{}, Identity{}, ErrContract
	}
	volumeIdentity, err := json.Marshal(volume)
	if err != nil {
		return containerInspect{}, Identity{}, err
	}
	fingerprint := checksum(volumeIdentity)
	if d.containerID != "" && (d.containerID != container.ID || d.imageID != container.Image || d.volumeFingerprint != fingerprint) {
		return containerInspect{}, Identity{}, ErrContract
	}
	data, err = d.command(ctx, "docker", "image", "inspect", "--format", "{{json .}}", container.Image)
	if err != nil {
		return containerInspect{}, Identity{}, err
	}
	var image struct {
		ID               string `json:"Id"`
		Architecture, OS string
		RepoDigests      []string
	}
	if err := json.Unmarshal(data, &image); err != nil {
		return containerInspect{}, Identity{}, err
	}
	if image.ID != container.Image || image.Architecture != "amd64" || image.OS != "linux" || !slices.Contains(image.RepoDigests, "tigergraph/tigergraph@sha256:78a3d62604527ba8686930465554fc3a419bf4b4de5c3d2d50202825a5308d49") {
		return containerInspect{}, Identity{}, ErrContract
	}
	data, err = d.command(ctx, "docker", "info", "--format", "{{.Architecture}}")
	if err != nil {
		return containerInspect{}, Identity{}, err
	}
	arch := strings.TrimSpace(string(data))
	mode := "unknown architecture; no performance acceptance"
	if arch == "aarch64" || arch == "arm64" {
		mode = "amd64 emulation on ARM Docker host; functional evidence only"
	}
	if arch == "x86_64" || arch == "amd64" {
		mode = "native amd64; functional evidence only"
	}
	d.containerID = container.ID
	d.imageID = container.Image
	d.volumeFingerprint = fingerprint
	id := Identity{Vendor: "TigerGraph", Version: "4.2.5", ImageReference: ImageReference, ImageID: container.Image, Platform: "linux/amd64", HostArchitecture: arch, ExecutionMode: mode, Project: ProjectName, ContainerID: container.ID, DeclaredCopies: 1, DeclaredPartitions: 1}
	return container, id, nil
}
func (d *DockerController) commandIn(ctx context.Context, script string) ([]byte, error) {
	if d == nil || d.command == nil || ctx == nil || !fullID(d.containerID) {
		return nil, ErrContract
	}
	return d.command(ctx, "docker", "exec", "--user", "tigergraph", d.containerID, "bash", "-lc", script)
}
func (d *DockerController) checkSchema(ctx context.Context) error {
	data, err := d.commandIn(ctx, "sha256sum /opt/vendor-compare/schema.gsql")
	if err != nil {
		return err
	}
	parts := strings.Fields(string(data))
	if len(parts) != 2 || parts[0] != checksum(schema) || parts[1] != "/opt/vendor-compare/schema.gsql" {
		return ErrContract
	}
	return nil
}
func (d *DockerController) ready(ctx context.Context) error {
	if d.waitReady != nil {
		return d.waitReady(ctx)
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	ticker := time.Tick(time.Second)
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, Endpoint+"/echo", nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err == nil {
			data, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20+1))
			closeErr := response.Body.Close()
			var envelope struct {
				Error *bool `json:"error"`
			}
			if readErr == nil && closeErr == nil && len(data) <= 1<<20 && response.StatusCode == http.StatusOK {
				decoder := json.NewDecoder(bytes.NewReader(data))
				decoder.UseNumber()
				if walkJSON(decoder, 0) == nil && json.Unmarshal(data, &envelope) == nil && envelope.Error != nil && !*envelope.Error {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker:
		}
	}
}

// Prepare starts services and creates the namespaced schema in the owned instance.
// It refuses a different version/image/owner and never drops an existing schema.
func (d *DockerController) Prepare(ctx context.Context) (Identity, error) {
	if d == nil || ctx == nil {
		return Identity{}, ErrContract
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	_, identity, err := d.owned(ctx)
	if err != nil {
		return Identity{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := d.checkSchema(ctx); err != nil {
		return Identity{}, err
	}
	if _, _, err := d.owned(ctx); err != nil {
		return Identity{}, err
	}
	if _, err := d.commandIn(ctx, gadminExecutable+" start all"); err != nil {
		return Identity{}, err
	}
	data, err := d.commandIn(ctx, gsqlExecutable+" version")
	if err != nil {
		return Identity{}, err
	}
	if !strings.Contains(string(data), "4.2.5") {
		return Identity{}, ErrContract
	}
	identity.VersionOutputSHA256 = checksum(data)
	if err := d.checkSchema(ctx); err != nil {
		return Identity{}, err
	}
	data, err = d.commandIn(ctx, gsqlExecutable+" /opt/vendor-compare/schema.gsql")
	if err != nil {
		return Identity{}, err
	}
	if strings.Contains(string(data), "Semantic Check Fails") || strings.Contains(string(data), "Failed") || strings.Contains(string(data), "ERROR") {
		return Identity{}, ErrContract
	}
	if err := d.ready(ctx); err != nil {
		return Identity{}, err
	}
	return identity, nil
}

// Reopen gracefully stops services, restarts only the owned container and waits
// for reads. This is not SIGKILL, multi-host or power-loss durability evidence.
func (d *DockerController) Reopen(ctx context.Context) error {
	if d == nil || ctx == nil {
		return ErrContract
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, _, err := d.owned(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := d.checkSchema(ctx); err != nil {
		return err
	}
	if _, _, err := d.owned(ctx); err != nil {
		return err
	}
	if _, err := d.commandIn(ctx, gadminExecutable+" stop all -y"); err != nil {
		return err
	}
	if _, err := d.command(ctx, "docker", "restart", d.containerID); err != nil {
		return err
	}
	if _, _, err := d.owned(ctx); err != nil {
		return err
	}
	if err := d.checkSchema(ctx); err != nil {
		return err
	}
	if _, err := d.commandIn(ctx, gadminExecutable+" start all"); err != nil {
		return err
	}
	return d.ready(ctx)
}
func parseCounters(data []byte) (map[string]uint64, error) {
	out := map[string]uint64{}
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, ErrContract
		}
		if _, ok := out[fields[0]]; ok {
			return nil, ErrContract
		}
		n, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return nil, ErrContract
		}
		out[fields[0]] = n
	}
	return out, nil
}

// Observe samples whole-container cgroup and whole-home volume costs. Every
// missing category remains unavailable; Complete never silently becomes true.
func (d *DockerController) Observe(ctx context.Context) (Costs, error) {
	if d == nil || ctx == nil {
		return Costs{}, ErrContract
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	container, _, err := d.owned(ctx)
	if err != nil {
		return Costs{}, err
	}
	out := unavailableCosts("metric not observed")
	sample := func(script string) ([]byte, bool) {
		data, err := d.commandIn(ctx, script)
		if err != nil {
			out.ObservationErrors = append(out.ObservationErrors, "command observation failed: "+checksum([]byte(script)))
			return nil, false
		}
		return data, true
	}
	for _, metric := range []struct {
		path, scope string
		target      *Metric
	}{
		{"/sys/fs/cgroup/memory.current", "one sample; all owned services including file cache", &out.CgroupMemoryCurrent},
		{"/sys/fs/cgroup/memory.peak", "owned container lifecycle high-water, including startup and sampler", &out.CgroupMemoryPeak},
	} {
		if data, ok := sample("cat " + metric.path); ok {
			if value, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64); err == nil {
				*metric.target = measured(value, "bytes", metric.scope)
			} else {
				out.ObservationErrors = append(out.ObservationErrors, "invalid cgroup scalar")
			}
		}
	}
	if data, ok := sample("cat /sys/fs/cgroup/memory.stat"); ok {
		if values, err := parseCounters(data); err == nil {
			out.CgroupMemoryStat = values
		} else {
			out.ObservationErrors = append(out.ObservationErrors, "invalid cgroup memory.stat")
		}
	}
	if data, ok := sample("cat /sys/fs/cgroup/cpu.stat"); ok {
		if values, err := parseCounters(data); err == nil {
			if value, ok := values["usage_usec"]; ok {
				out.CgroupCPUUsec = measured(value, "microseconds", "container lifecycle CPU, includes all services and observer")
			}
		}
	}
	// GNU find returns metadata only, never file contents. Whole /home/tigergraph
	// is the declared mounted volume; traversal stays on its filesystem.
	if data, ok := sample("find /home/tigergraph -xdev -type f -printf '%P\t%s\t%b\t%D:%i\n'"); ok {
		valid := true
		var apparent, allocated uint64
		seen := map[string]bool{}
		for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
			fields := strings.Split(line, "\t")
			if len(fields) != 4 || fields[0] == "" || fields[3] == "" || strings.HasPrefix(fields[0], "/") || strings.Contains(fields[0], "../") {
				valid = false
				break
			}
			size, e1 := strconv.ParseUint(fields[1], 10, 64)
			blocks, e2 := strconv.ParseUint(fields[2], 10, 64)
			if e1 != nil || e2 != nil || blocks > ^uint64(0)/512 || size > ^uint64(0)-apparent || blocks*512 > ^uint64(0)-allocated {
				valid = false
				break
			}
			apparent += size
			if !seen[fields[3]] {
				allocated += blocks * 512
				seen[fields[3]] = true
			}
			out.HomeVolumeFiles = append(out.HomeVolumeFiles, DiskFile{Name: fields[0], ApparentBytes: size, AllocatedBytes: blocks * 512, DeviceInode: fields[3]})
		}
		if valid {
			out.HomeVolumeApparent = measured(apparent, "bytes", "regular-file path bytes under whole home volume, installation/data/logs/config included; directory/symlink metadata and other mounts excluded")
			out.HomeVolumeApparent.Status = "partial"
			out.HomeVolumeAllocated = measured(allocated, "bytes", "unique device/inode regular-file st_blocks*512; hard links counted once; directories/symlinks/filesystem overhead excluded")
			out.HomeVolumeAllocated.Status = "partial"
		} else {
			out.HomeVolumeFiles = nil
			out.ObservationErrors = append(out.ObservationErrors, "incomplete/invalid whole-home file inventory")
		}
	}
	if container.SizeRW != nil {
		out.ContainerWritableLayer = measured(*container.SizeRW, "bytes", "Docker inspect SizeRw; apparent writable-layer snapshot, allocation unmeasured")
	}
	out.ObservationErrors = append(out.ObservationErrors, "metrics are samples; observer runs inside container and is included")
	if _, _, err := d.owned(ctx); err != nil {
		return Costs{}, err
	}
	return out, nil
}

var _ Lifecycle = (*DockerController)(nil)
var _ Backend = (*TigerGraph)(nil)
var _ Validator = PythonValidator{}
