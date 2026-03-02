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
// Uses write-tmp + fsync + atomic rename for crash safety.
func saveRegistryFile(path string, labels, relTypes []string) error {
	data, err := msgpack.Marshal(&registryFileData{
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
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("%s: write temp: %w", prefix, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("%s: sync temp: %w", prefix, err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("%s: close temp: %w", prefix, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("%s: rename: %w", prefix, err)
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
