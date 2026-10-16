// Package stats mirrors the v4 import path pkg/graph/stats: statistics counters (NodeCount, ...). Every method keeps its
// v4 signature and reports ErrNotSupportedInV5 (declined or not in the first
// batch of WP-11); the types are placeholders so consumers compile.
package stats

import (
	"fmt"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
)

// ErrNotSupportedInV5 is the class of every declined door.
var ErrNotSupportedInV5 = store.ErrNotSupportedInV5

func declined(door string) error { return fmt.Errorf("%w: "+door, ErrNotSupportedInV5) }

// API is the sub-API handle returned by the graph accessor.
type API struct{}

// GraphStats is a placeholder for the v4 type of the same name.
type GraphStats struct{}

// Get is declined.
func (a *API) Get() (r0 GraphStats, err error) {
	err = declined("stats.Get")
	return
}

// NodeCount is declined.
func (a *API) NodeCount() (r0 int, err error) {
	err = declined("stats.NodeCount")
	return
}

// RelCount is declined.
func (a *API) RelCount() (r0 int, err error) {
	err = declined("stats.RelCount")
	return
}

// NodeCountByLabel is declined.
func (a *API) NodeCountByLabel(label string) (r0 int, err error) {
	err = declined("stats.NodeCountByLabel")
	return
}

// NodeCountByLabelAndPropertyKey is declined.
func (a *API) NodeCountByLabelAndPropertyKey(label, propertyKey string) (r0 int, err error) {
	err = declined("stats.NodeCountByLabelAndPropertyKey")
	return
}

// PropertyTypeClassCounts is declined.
func (a *API) PropertyTypeClassCounts(label, propertyKey string) (r0 store.PropertyTypeClassCounts, err error) {
	err = declined("stats.PropertyTypeClassCounts")
	return
}

// RelPropertyTypeClassCounts is declined.
func (a *API) RelPropertyTypeClassCounts(typeName, propertyKey string) (r0 store.PropertyTypeClassCounts, err error) {
	err = declined("stats.RelPropertyTypeClassCounts")
	return
}

// RelTypeDegreeStats is declined.
func (a *API) RelTypeDegreeStats(typeName string) (r0 store.RelTypeDegreeStats, err error) {
	err = declined("stats.RelTypeDegreeStats")
	return
}

// TrackRelTypeDegrees is declined.
func (a *API) TrackRelTypeDegrees(typeName string) error {
	return declined("stats.TrackRelTypeDegrees")
}

// UntrackRelTypeDegrees is declined.
func (a *API) UntrackRelTypeDegrees(typeName string) error {
	return declined("stats.UntrackRelTypeDegrees")
}

// ReadCosts is declined.
func (a *API) ReadCosts() (r0 store.ReadCosts, r1 bool, err error) {
	err = declined("stats.ReadCosts")
	return
}

// HistoryCounts is declined.
func (a *API) HistoryCounts() (r0 store.HistoryCounts, r1 bool, err error) {
	err = declined("stats.HistoryCounts")
	return
}

// RelHistoryCountByType is declined.
func (a *API) RelHistoryCountByType(typeName string) (r0 int, r1 bool, err error) {
	err = declined("stats.RelHistoryCountByType")
	return
}

// PropertyStats is declined.
func (a *API) PropertyStats(label, propertyKey string) (r0 store.PropertyStats, err error) {
	err = declined("stats.PropertyStats")
	return
}

// RelPropertyStats is declined.
func (a *API) RelPropertyStats(typeName, propertyKey string) (r0 store.PropertyStats, err error) {
	err = declined("stats.RelPropertyStats")
	return
}

// PropertyNDV is declined.
func (a *API) PropertyNDV(label, propertyKey string) (r0 store.PropertyNDV, err error) {
	err = declined("stats.PropertyNDV")
	return
}

// RelPropertyNDV is declined.
func (a *API) RelPropertyNDV(typeName, propertyKey string) (r0 store.PropertyNDV, err error) {
	err = declined("stats.RelPropertyNDV")
	return
}

// PropertyBounds is declined.
func (a *API) PropertyBounds(label, propertyKey string, opts store.QueryOpts) (r0 store.PropertyBounds, r1 bool, err error) {
	err = declined("stats.PropertyBounds")
	return
}

// RelPropertyBounds is declined.
func (a *API) RelPropertyBounds(typeName, propertyKey string, opts store.QueryOpts) (r0 store.PropertyBounds, r1 bool, err error) {
	err = declined("stats.RelPropertyBounds")
	return
}

// LabelTimeStats is declined.
func (a *API) LabelTimeStats(label string) (r0 store.MemberTimeStats, r1 bool, err error) {
	err = declined("stats.LabelTimeStats")
	return
}

// RelTypeTimeStats is declined.
func (a *API) RelTypeTimeStats(typeName string) (r0 store.MemberTimeStats, r1 bool, err error) {
	err = declined("stats.RelTypeTimeStats")
	return
}

// RelCountByType is declined.
func (a *API) RelCountByType(typeName string) (r0 int, err error) {
	err = declined("stats.RelCountByType")
	return
}

// RangeCardinality is declined.
func (a *API) RangeCardinality(label, propKey string, min, max float64, inclMin, inclMax bool, opts store.QueryOpts) (r0 int64, r1 bool, err error) {
	err = declined("stats.RangeCardinality")
	return
}

// RelRangeCardinality is declined.
func (a *API) RelRangeCardinality(typeName, propKey string, min, max float64, inclMin, inclMax bool, opts store.QueryOpts) (r0 int64, r1 bool, err error) {
	err = declined("stats.RelRangeCardinality")
	return
}

// AllLabelCounts is declined.
func (a *API) AllLabelCounts() (r0 map[string]int, err error) {
	err = declined("stats.AllLabelCounts")
	return
}

// AllRelTypeCounts is declined.
func (a *API) AllRelTypeCounts() (r0 map[string]int, err error) {
	err = declined("stats.AllRelTypeCounts")
	return
}
