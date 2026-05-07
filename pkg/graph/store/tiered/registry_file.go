package tiered

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/vmihailenco/msgpack/v5"
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
	return atomicWriteFile(path, data, "registry file")
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
	return rfd.Labels, rfd.RelTypes, nil
}
