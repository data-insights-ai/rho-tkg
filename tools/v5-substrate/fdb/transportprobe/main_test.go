package main

import "testing"

// Authored guards only, no socket/probe/RTT evidence.
func TestOriginalAndCommandAdmission(t *testing.T) {
	for _, input := range []string{"", "00", "0000000000000000000000000000000000000000000000000000000000000000"} {
		if _, err := original(input); err == nil {
			t.Fatal("foreign token admitted")
		}
	}
	const token = "0102030405060708091011121314151617181920212223242526272829303132"
	if value, err := original(token); err != nil || string(value[:]) != token {
		t.Fatal("original bytes", err)
	}
	if err := run([]string{"unknown"}); err == nil {
		t.Fatal("unknown command")
	}
	if err := run([]string{"serve", "127.0.0.1", "127.0.0.2", token, "1"}); err == nil {
		t.Fatal("expired deadline opened socket")
	}
}
