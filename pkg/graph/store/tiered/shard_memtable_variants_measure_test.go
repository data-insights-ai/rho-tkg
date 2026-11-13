//go:build measure

package tiered

// memVariants are the configurations TestShardMemTableStates compares. The
// lifecycle variants measured for R3 (a read-only reopen at demotion, a
// writable reopen at the hot-to-warm rotation) were throwaway prototypes and
// are not in the tree; their numbers are in tasks/evidence/R3/measure.txt.
func memVariants() []memVariant {
	return []memVariant{
		{"baseline-64", func(c *Config) {}},
		{"warm-16", func(c *Config) { c.WarmMemTableSize = 16 * mib }},
		{"warm-8", func(c *Config) { c.WarmMemTableSize = 8 * mib }},
	}
}
