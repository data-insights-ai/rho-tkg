package store

// ValueDictionary is one named value dictionary (DICT): a store keeps each
// string value of its members once and stores a token in every row that
// carries it. Reads, filters and property indexes see the plain value. Each
// badger store (each tiered shard) holds the entries its own rows use.
//
// Only string values of at least 4 bytes are coded; a value arriving after the
// store's dictionary holds Cap entries stays inline and is counted
// (ValueDictionaryStats.InlineFull). Rows written before a member was listed
// stay inline and read the same.
//
// A store holding a coded row is store format 11: a release before DICT
// refuses to open it.
type ValueDictionary struct {
	// Name identifies the dictionary; it is stored with each entry, so members
	// can be added or regrouped later without misreading old rows.
	Name string
	// Cap is the most entries one store (one tiered shard) keeps for this
	// dictionary. Must be positive.
	Cap int
	// Members are the properties coded with this dictionary. A (label or
	// relationship type, property) pair belongs to at most one dictionary.
	Members []ValueDictionaryMember
}

// ValueDictionaryMember is one property coded with a value dictionary: the
// property of nodes carrying Label, or of relationships of type RelType
// (exactly one of the two is set).
type ValueDictionaryMember struct {
	Label    string
	RelType  string
	Property string
}

// ValueDictionaryStats counts one value dictionary's work since the store
// opened, summed over a tiered store's shards, and the entries it holds in RAM,
// summed over a tiered store's open shards.
type ValueDictionaryStats struct {
	Name string
	// Coded counts property values written as a token.
	Coded uint64
	// InlineFull counts string values written inline because the
	// dictionary was at its cap in that store: bytes a larger cap would save.
	InlineFull uint64
	// NewEntries counts values added to a dictionary.
	NewEntries uint64
	// Entries counts the entries resident now: every entry of a store whose
	// dictionaries are loaded (the first coded row read or written loads them),
	// at most Cap a store.
	Entries int
	// Bytes estimates those entries' heap: each value's length plus a fixed
	// per-entry cost (its token and lookup slots and allocation rounding).
	Bytes int64
}
