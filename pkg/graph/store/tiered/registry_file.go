package tiered

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/vmihailenco/msgpack/v5"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
)

// RegistryFileData is the msgpack wire format for a registry file.
type RegistryFileData struct {
	Labels   []string `msgpack:"labels"`
	RelTypes []string `msgpack:"reltypes"`
}

// saveRegistryFile writes label and reltype name slices to a flat msgpack file.
// Uses write-tmp + fsync + atomic rename for crash safety.
func saveRegistryFile(path string, labels, relTypes []string) error {
	data, err := msgpack.Marshal(&RegistryFileData{
		Labels:   labels,
		RelTypes: relTypes,
	})
	if err != nil {
		return fmt.Errorf("registry file: marshal: %w", err)
	}
	snapshot, err := snapshotRegistryFile(path)
	if err != nil {
		return err
	}
	if err := atomicWriteFile(path, data, "registry file"); err != nil {
		if restoreErr := restoreRegistryFile(snapshot); restoreErr != nil {
			return fmt.Errorf("registry file: %w (rollback failed: %v)", err, restoreErr)
		}
		return err
	}
	return nil
}

type registryFileSnapshot struct {
	path   string
	data   []byte
	exists bool
}

func snapshotRegistryFile(path string) (registryFileSnapshot, error) {
	if path == "" {
		return registryFileSnapshot{}, nil
	}
	data, err := os.ReadFile(path) // #nosec G304 — path derived from trusted Store config
	if err != nil {
		if os.IsNotExist(err) {
			return registryFileSnapshot{path: path}, nil
		}
		return registryFileSnapshot{}, fmt.Errorf("registry file: snapshot: %w", err)
	}
	return registryFileSnapshot{path: path, data: data, exists: true}, nil
}

func restoreRegistryFile(snapshot registryFileSnapshot) error {
	if snapshot.path == "" {
		return nil
	}
	if snapshot.exists {
		return atomicWriteFile(snapshot.path, snapshot.data, "registry file rollback")
	}
	if err := os.Remove(snapshot.path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("registry file rollback: remove: %w", err)
	}
	if err := syncParentDir(snapshot.path, "registry file rollback"); err != nil {
		return err
	}
	return nil
}

// atomicWriteFile writes data to path using write-tmp + fsync + rename.
// The fsync ensures data reaches stable storage before the rename makes it
// visible, preventing corruption if the OS crashes between write and rename.
// The prefix is used in error messages for context.
func atomicWriteFile(path string, data []byte, prefix string) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "atomic_*.tmp")
	if err != nil {
		return fmt.Errorf("%s: create temp: %w", prefix, err)
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()        // best-effort cleanup; returning primary error
		_ = os.Remove(tmpName) // #nosec G703 — tmpName from os.CreateTemp, no traversal risk
		return fmt.Errorf("%s: write temp: %w", prefix, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()        // best-effort cleanup; returning primary error
		_ = os.Remove(tmpName) // #nosec G703 — tmpName from os.CreateTemp, no traversal risk
		return fmt.Errorf("%s: sync temp: %w", prefix, err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName) // #nosec G703 — tmpName from os.CreateTemp, no traversal risk
		return fmt.Errorf("%s: close temp: %w", prefix, err)
	}
	if err := os.Rename(tmpName, path); err != nil { // #nosec G703 — tmpName from os.CreateTemp, path from trusted config
		_ = os.Remove(tmpName) // #nosec G703 — tmpName from os.CreateTemp, no traversal risk
		return fmt.Errorf("%s: rename: %w", prefix, err)
	}
	// Fsync the directory to ensure the rename is durable on crash.
	if err := syncParentDir(path, prefix); err != nil {
		return err
	}
	return nil
}

func syncParentDir(path, prefix string) error {
	dir := filepath.Dir(path)
	d, err := os.Open(dir) // #nosec G304 — dir derived from caller-provided Config.DataDir (trusted config, not end-user input)
	if err != nil {
		return fmt.Errorf("%s: open dir for sync: %w", prefix, err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close() // best-effort cleanup; returning primary error
		return fmt.Errorf("%s: sync dir: %w", prefix, err)
	}
	_ = d.Close() // fsync already completed; Close error is non-fatal
	return nil
}

// loadRegistryFile reads a flat msgpack registry file. Returns nil slices
// (not error) when the file does not exist.
func loadRegistryFile(path string) (labels, relTypes []string, err error) {
	data, err := os.ReadFile(path) // #nosec G304 — path derived from caller-provided Config.DataDir (trusted config, not end-user input)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("registry file: read: %w", err)
	}

	var rfd RegistryFileData
	if err := msgpack.Unmarshal(data, &rfd); err != nil {
		return nil, nil, fmt.Errorf("registry file: unmarshal: %w", err)
	}
	if err := validateRegistryFileNames("label", rfd.Labels); err != nil {
		return nil, nil, err
	}
	if err := validateRegistryFileNames("reltype", rfd.RelTypes); err != nil {
		return nil, nil, err
	}
	return rfd.Labels, rfd.RelTypes, nil
}

func validateRegistryFileNames(kind string, names []string) error {
	if names == nil {
		return nil
	}
	switch kind {
	case "label":
		if err := registrypkg.NewLabelRegistry().ImportNames(names); err != nil {
			return fmt.Errorf("registry file: invalid label registry: %w", err)
		}
	case "reltype":
		if err := registrypkg.NewRelTypeRegistry().ImportNames(names); err != nil {
			return fmt.Errorf("registry file: invalid reltype registry: %w", err)
		}
	default:
		return fmt.Errorf("registry file: invalid registry kind %q", kind)
	}
	return nil
}
