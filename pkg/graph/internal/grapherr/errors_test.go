package grapherr

import "testing"

func TestIsNil(t *testing.T) {
	var ptr *int
	var s []string
	var m map[string]int
	var ch chan int
	var fn func()
	var iface any

	for _, tc := range []struct {
		name string
		v    any
		want bool
	}{
		{name: "nil interface", v: nil, want: true},
		{name: "typed nil pointer", v: ptr, want: true},
		{name: "typed nil slice", v: s, want: true},
		{name: "typed nil map", v: m, want: true},
		{name: "typed nil channel", v: ch, want: true},
		{name: "typed nil func", v: fn, want: true},
		{name: "nil any variable", v: iface, want: true},
		{name: "non-nil pointer", v: new(int), want: false},
		{name: "non-nil slice", v: []string{}, want: false},
		{name: "non-nil map", v: map[string]int{}, want: false},
		{name: "non-nil channel", v: make(chan int), want: false},
		{name: "non-nil func", v: func() {}, want: false},
		{name: "zero int", v: 0, want: false},
		{name: "empty string", v: "", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsNil(tc.v); got != tc.want {
				t.Fatalf("IsNil(%T) = %v, want %v", tc.v, got, tc.want)
			}
		})
	}
}
