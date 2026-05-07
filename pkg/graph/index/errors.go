package index

import "errors"

// ErrIndexProviderExists is returned by Graph.RegisterIndexProvider /
// RegisterLegacyIndexProvider when a provider with the same Name is
// already registered.
var ErrIndexProviderExists = errors.New("graph: index provider already registered")

// ErrIndexProviderNotFound is returned by Graph.UnregisterIndexProvider
// when no provider with the given name is registered.
var ErrIndexProviderNotFound = errors.New("graph: index provider not found")

// ErrIndexProviderEmptyName is returned by Graph.RegisterIndexProvider /
// RegisterLegacyIndexProvider when the provider's Name() is the empty
// string. Names are the registry key.
var ErrIndexProviderEmptyName = errors.New("graph: index provider Name must be non-empty")
