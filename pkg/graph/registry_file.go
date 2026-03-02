package graph

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/vmihailenco/msgpack/v5"
)

// registryFileData is the msgpack wire format for a registry file.
type registryFileData struct {
	Labels   []string `msgpack:"labels"`
	RelTypes []string `msgpack:"reltypes"`
}

// saveRegistryFile writes label and reltype name slices to a flat msgpack file.
// Uses write-tmp + atomic rename for crash safety.
func saveRegistryFile(path string, labels, relTypes []string) error {
	data, err := msgpack.Marshal(&registryFileData{
		Labels:   labels,
		RelTypes: relTypes,
	})
	if err != nil {
		return fmt.Errorf("registry file: marshal: %w", err)
	}

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "registry_*.tmp")
	if err != nil {
		return fmt.Errorf("registry file: create temp: %w", err)
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("registry file: write temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("registry file: close temp: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("registry file: rename: %w", err)
	}
	return nil
}

// loadRegistryFile reads a flat msgpack registry file. Returns nil slices
// (not error) when the file does not exist.
func loadRegistryFile(path string) (labels, relTypes []string, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("registry file: read: %w", err)
	}

	var rfd registryFileData
	if err := msgpack.Unmarshal(data, &rfd); err != nil {
		return nil, nil, fmt.Errorf("registry file: unmarshal: %w", err)
	}
	return rfd.Labels, rfd.RelTypes, nil
}
