package badger

import "testing"

// ValidateMemTableSize is MemTableSize's own check (tiered.Config.WarmMemTableSize
// uses it): 0 for badger's stock, else [8 MB, 1 GB].
func TestValidateMemTableSize(t *testing.T) {
	for _, tc := range []struct {
		size int64
		ok   bool
	}{
		{0, true}, {8 << 20, true}, {64 << 20, true}, {1 << 30, true},
		{8<<20 - 1, false}, {1<<30 + 1, false}, {-1, false},
	} {
		if err := ValidateMemTableSize(tc.size); (err == nil) != tc.ok {
			t.Errorf("ValidateMemTableSize(%d) = %v, want ok %v", tc.size, err, tc.ok)
		}
		// New refuses exactly what the exported check refuses; a size that
		// is accepted and large is not opened (its arena is the point).
		if tc.ok && tc.size != 8<<20 {
			continue
		}
		s, err := New(Config{InMemory: true, MemTableSize: tc.size})
		if (err == nil) != tc.ok {
			t.Errorf("New(MemTableSize %d) = %v, want ok %v", tc.size, err, tc.ok)
		}
		if err == nil {
			_ = s.Close()
		}
	}
}
