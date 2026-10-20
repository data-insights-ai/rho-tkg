package types

import (
	"math"
	"testing"
)

func TestSpanExceedsMax(t *testing.T) {
	cases := []struct {
		name     string
		from, to Instant
		want     bool
	}{
		{"unbounded to infinite", UnboundedValidFrom, InfiniteValidTo, true},
		{"minint to maxint", math.MinInt64, math.MaxInt64, true},
		{"zero to maxint", 0, math.MaxInt64, true},
		{"minint to zero", math.MinInt64, 0, true},
		{"exactly max", 0, maxExpandSpan, false},
		{"max plus one", 0, maxExpandSpan + 1, true},
		{"negative from exactly max", -5, maxExpandSpan - 5, false},
		{"negative from max plus one", -5, maxExpandSpan - 4, true},
		{"small", 0, 1, false},
		{"from near maxint", math.MaxInt64 - 10, math.MaxInt64, false},
		{"from at maxint minus max", math.MaxInt64 - maxExpandSpan, math.MaxInt64, false},
		{"from below maxint minus max", math.MaxInt64 - maxExpandSpan - 1, math.MaxInt64, true},
		{"minint to minint plus max", math.MinInt64, math.MinInt64 + maxExpandSpan, false},
		{"minint to minint plus max plus one", math.MinInt64, math.MinInt64 + maxExpandSpan + 1, true},
	}
	for _, c := range cases {
		if got := spanExceedsMax(c.from, c.to); got != c.want {
			t.Errorf("%s: spanExceedsMax(%d,%d)=%v want %v", c.name, c.from, c.to, got, c.want)
		}
	}
}

func TestExpandHugeSpanRejected(t *testing.T) {
	p := RecurrencePattern{Frequency: RecurrenceYearly, DayEnd: 3600e9}
	if _, err := p.Expand(UnboundedValidFrom, InfiniteValidTo); err == nil {
		t.Fatal("want span error")
	}
	if _, err := p.Expand(5, 5); err == nil {
		t.Fatal("from==to must error")
	}
	if _, err := p.Expand(9, 5); err == nil {
		t.Fatal("from>to must error")
	}
}
