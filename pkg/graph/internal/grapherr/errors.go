package grapherr

import (
	"errors"
	"reflect"
)

// ErrNilGraph is returned when a public graph or sub-API wrapper is nil or unwired.
var ErrNilGraph = errors.New("graph: graph must not be nil")

// ErrNilCallback is returned when a streaming/iteration method receives a
// nil callback function.
var ErrNilCallback = errors.New("graph: iteration callback must not be nil")

// IsNil reports whether v is nil, including typed nil interface values.
func IsNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
