package main

import (
	"bytes"
	"testing"
)

func TestCapabilitiesAndUsageNeverRunVendor(t *testing.T) {
	for _, test := range []struct {
		args []string
		want int
	}{
		{[]string{"capabilities"}, 0}, {nil, 2}, {[]string{"unknown"}, 2}, {[]string{"run", "--unknown"}, 2}, {[]string{"run", "--timeout", "0s"}, 2},
	} {
		var out, errOut bytes.Buffer
		if result := run(t.Context(), test.args, &out, &errOut); result != test.want {
			t.Fatal(result, out.String(), errOut.String())
		}
	}
	var out, errOut bytes.Buffer
	if result := run(nil, []string{"capabilities"}, &out, &errOut); result != 2 {
		t.Fatal(result)
	}
}
