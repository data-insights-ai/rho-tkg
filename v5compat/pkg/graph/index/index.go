// Package index mirrors the v4 import path pkg/graph/index: index DDL and the vector index. Every method keeps its
// v4 signature and reports ErrNotSupportedInV5 (declined or not in the first
// batch of WP-11); the types are placeholders so consumers compile.
package index

import (
	"fmt"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/types"
	"time"
)

// ErrNotSupportedInV5 is the class of every declined door.
var ErrNotSupportedInV5 = store.ErrNotSupportedInV5

func declined(door string) error { return fmt.Errorf("%w: "+door, ErrNotSupportedInV5) }

// API is the sub-API handle returned by the graph accessor.
type API struct{}

// VectorHit is a placeholder for the v4 type of the same name.
type VectorHit struct{}

// IndexProvider is a placeholder for the v4 type of the same name.
type IndexProvider struct{}

// CreateProperty is declined.
func (a *API) CreateProperty(label, propertyKey string) error {
	return declined("index.CreateProperty")
}

// DeleteProperty is declined.
func (a *API) DeleteProperty(label, propertyKey string) error {
	return declined("index.DeleteProperty")
}

// HasProperty is declined.
func (a *API) HasProperty(label, propertyKey string) (r0 bool, err error) {
	err = declined("index.HasProperty")
	return
}

// CreateRelProperty is declined.
func (a *API) CreateRelProperty(typeName, propertyKey string) error {
	return declined("index.CreateRelProperty")
}

// DeleteRelProperty is declined.
func (a *API) DeleteRelProperty(typeName, propertyKey string) error {
	return declined("index.DeleteRelProperty")
}

// HasRelProperty is declined.
func (a *API) HasRelProperty(typeName, propertyKey string) (r0 bool, err error) {
	err = declined("index.HasRelProperty")
	return
}

// CreateComposite is declined.
func (a *API) CreateComposite(label string, keys []string) error {
	return declined("index.CreateComposite")
}

// DeleteComposite is declined.
func (a *API) DeleteComposite(label string, keys []string) error {
	return declined("index.DeleteComposite")
}

// HasComposite is declined.
func (a *API) HasComposite(label string, keys []string) (r0 bool, err error) {
	err = declined("index.HasComposite")
	return
}

// ListComposites is declined.
func (a *API) ListComposites(label string) (r0 [][]string, err error) {
	err = declined("index.ListComposites")
	return
}

// CreateHighFrequency is declined.
func (a *API) CreateHighFrequency(label string, bucketSize time.Duration) error {
	return declined("index.CreateHighFrequency")
}

// DeleteHighFrequency is declined.
func (a *API) DeleteHighFrequency(label string) error {
	return declined("index.DeleteHighFrequency")
}

// CreateTemporal is declined.
func (a *API) CreateTemporal(label string) error {
	return declined("index.CreateTemporal")
}

// DeleteTemporal is declined.
func (a *API) DeleteTemporal(label string) error {
	return declined("index.DeleteTemporal")
}

// HasTemporal is declined.
func (a *API) HasTemporal(label string) (r0 bool, err error) {
	err = declined("index.HasTemporal")
	return
}

// ListTemporal is declined.
func (a *API) ListTemporal() (r0 []string, err error) {
	err = declined("index.ListTemporal")
	return
}

// ListRelTemporal is declined.
func (a *API) ListRelTemporal() (r0 []string, err error) {
	err = declined("index.ListRelTemporal")
	return
}

// HasRelTemporal is declined.
func (a *API) HasRelTemporal(typeName string) (r0 bool, err error) {
	err = declined("index.HasRelTemporal")
	return
}

// CreateRelTemporal is declined.
func (a *API) CreateRelTemporal(typeName string) error {
	return declined("index.CreateRelTemporal")
}

// DeleteRelTemporal is declined.
func (a *API) DeleteRelTemporal(typeName string) error {
	return declined("index.DeleteRelTemporal")
}

// CreateVector is declined.
func (a *API) CreateVector(label, propertyKey string, dims int, metric store.DistanceMetric) error {
	return declined("index.CreateVector")
}

// CreateVectorWithOptions is declined.
func (a *API) CreateVectorWithOptions(label, propertyKey string, dims int, metric store.DistanceMetric, opts store.VectorIndexOptions) error {
	return declined("index.CreateVectorWithOptions")
}

// DeleteVector is declined.
func (a *API) DeleteVector(label, propertyKey string) error {
	return declined("index.DeleteVector")
}

// VectorIndexInfo is declined.
func (a *API) VectorIndexInfo(label, propertyKey string) (r0 store.VectorIndexInfo, r1 bool, err error) {
	err = declined("index.VectorIndexInfo")
	return
}

// SearchNearest is declined.
func (a *API) SearchNearest(label, propertyKey string, query []float32, k int, opts store.QueryOpts) (r0 []*types.Node, err error) {
	err = declined("index.SearchNearest")
	return
}

// SearchNearestScored is declined.
func (a *API) SearchNearestScored(label, propertyKey string, query []float32, k int, opts store.QueryOpts) (r0 []VectorHit, err error) {
	err = declined("index.SearchNearestScored")
	return
}

// RegisterProvider is declined.
func (a *API) RegisterProvider(p IndexProvider) error {
	return declined("index.RegisterProvider")
}

// UnregisterProvider is declined.
func (a *API) UnregisterProvider(name string) error {
	return declined("index.UnregisterProvider")
}

// Providers is declined.
func (a *API) Providers() (r0 []string) {
	return
}

// InventoryEpoch is declined.
func (a *API) InventoryEpoch() (r0 uint64) {
	return
}

// CreatePropertyWithOptions is declined.
func (a *API) CreatePropertyWithOptions(label, propertyKey string, opts store.PropertyIndexOptions) error {
	return declined("index.CreatePropertyWithOptions")
}

// CreateRelPropertyWithOptions is declined.
func (a *API) CreateRelPropertyWithOptions(typeName, propertyKey string, opts store.PropertyIndexOptions) error {
	return declined("index.CreateRelPropertyWithOptions")
}

// PropertyOptions is declined.
func (a *API) PropertyOptions(label, propertyKey string) (r0 store.PropertyIndexOptions, r1 bool, err error) {
	err = declined("index.PropertyOptions")
	return
}

// RelPropertyOptions is declined.
func (a *API) RelPropertyOptions(typeName, propertyKey string) (r0 store.PropertyIndexOptions, r1 bool, err error) {
	err = declined("index.RelPropertyOptions")
	return
}
