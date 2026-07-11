package graph

// oneMiB is a byte-unit helper for expressing the [4.8.0] Badger footprint
// knobs (ValueLogFileSize, MemTableSize, BlockCacheSize) in MB without
// hand-computed literals in every profile below.
const oneMiB = 1 << 20

// Option configures a Config in place before it is handed to New. Options
// are applied in the order given to Open / OpenInMemory, so a later Option
// that touches the same field as an earlier one wins (see
// WithSnowflakeNodeID / WithProfile* for the fields each one sets).
//
// Config and New remain the primary, fully-documented public constructor —
// Option and Open/OpenInMemory are additive convenience sugar layered on
// top; nothing below does anything New itself could not already do via a
// Config literal.
type Option func(*Config)

// Open is a convenience constructor for a graph rooted at dir. It is
// equivalent to:
//
//	cfg := Config{}
//	if dir != "" { cfg.BadgerDir = dir }
//	for _, opt := range opts { opt(&cfg) }
//	return New(cfg)
//
// An empty dir leaves Config.BadgerDir at its zero value "", which is
// exactly what a zero Config already does — New's backend-selection branch
// (Store nil, BadgerDir "", BadgerInMemory false) falls through to an
// in-memory MemoryStore. That is precisely OpenInMemory's definition: it
// calls Open("", opts...), so the two constructors can never disagree about
// what "no directory" means.
//
// A whitespace-only dir is NOT special-cased or pre-validated here — it is
// forwarded to New exactly like a whitespace-only Config.BadgerDir, and
// fails with the identical error New itself returns
// ("graph: BadgerDir is whitespace-only; use a valid path or omit for
// MemoryStore").
//
// Options are applied AFTER dir is copied into cfg.BadgerDir, so an Option
// that itself sets BadgerDir would take precedence over the dir argument.
// None of the Option constructors in this file touch BadgerDir.
func Open(dir string, opts ...Option) (*Graph, error) {
	cfg := Config{}
	if dir != "" {
		cfg.BadgerDir = dir
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return New(cfg)
}

// OpenInMemory is the in-memory counterpart of Open. It is defined purely as
// Open("", opts...) — not as a separate reimplementation of New's zero-Config
// defaulting logic — so the "does a zero Config default to memory?" question
// only has one answer to keep in sync (see New's backend-selection branch:
// Store nil, BadgerDir "", BadgerInMemory false => memory.New()).
func OpenInMemory(opts ...Option) (*Graph, error) {
	return Open("", opts...)
}

// WithSnowflakeNodeID sets Config.SnowflakeNodeID (valid range 0-15; each
// concurrent graph instance must use a distinct value — see CLAUDE.md
// "Snowflake Configuration").
func WithSnowflakeNodeID(id int) Option {
	return func(cfg *Config) {
		cfg.SnowflakeNodeID = int64(id)
	}
}

// WithValidation sets Config.Validation (the property/label/name limits
// applied to every mutation).
func WithValidation(v ValidationLimits) Option {
	return func(cfg *Config) {
		cfg.Validation = v
	}
}

// WithProfileSmall bounds the badger-backed store's per-instance footprint
// for many-instance or embedded deployments — e.g. one graph per
// tenant/shard/test inside a single process — where CHANGELOG.md [4.8.0]'s
// stock defaults (1GB vlog / 64MB memtable / 256MB block cache / 4
// compactors) would multiply out of proportion to the number of open
// instances. Every value stays inside the [4.8.0] validated bounds (vlog
// [1MB,2GB), memtable [8MB,1GB], cache >=0, compactors 0 or >=2).
func WithProfileSmall() Option {
	return func(cfg *Config) {
		// 64MB: well above the 1MB validation floor, far below the 1GB
		// stock default — bounds the on-disk value-log segment size per
		// instance (CHANGELOG.md [4.8.0]).
		cfg.ValueLogFileSize = 64 * oneMiB
		// 8MB: the validated floor itself ([4.8.0]: memtable [8MB,1GB]) —
		// the smallest write buffer Badger accepts, minimizing per-instance
		// RAM for an embedded/many-instance deployment.
		cfg.MemTableSize = 8 * oneMiB
		// 32MB: a small but non-zero block cache — keeps hot index blocks
		// resident without the 256MB stock default multiplying per
		// instance.
		cfg.BlockCacheSize = 32 * oneMiB
		// 2: the validated floor for a nonzero NumCompactors ([4.8.0]: 0 or
		// >=2) — fewer background compaction goroutines per instance.
		cfg.NumCompactors = 2
	}
}

// WithProfileServer explicitly resets the footprint knobs to zero, i.e.
// Badger's own stock defaults (1GB vlog / 64MB memtable / 256MB block cache
// / 4 compactors per CHANGELOG.md [4.8.0]) — the right choice for a single
// graph instance with a whole process/host to itself, where per-instance
// footprint never has to be divided across many open stores. Zero is
// already Config's zero value, so applying this Option to a zero Config is
// a no-op; it exists so "yes, stock Badger defaults, deliberately" is
// visible and discoverable at the call site instead of a silently-unset
// field, and so it can override an earlier profile Option in the same
// Open/OpenInMemory call (see Option's "later wins" ordering).
func WithProfileServer() Option {
	return func(cfg *Config) {
		cfg.ValueLogFileSize = 0
		cfg.MemTableSize = 0
		cfg.BlockCacheSize = 0
		cfg.NumCompactors = 0
	}
}

// WithProfileBulkLoad raises the badger-backed store's write-path footprint
// for a one-shot bulk-ingest run: a larger MemTableSize/ValueLogFileSize
// absorbs more writes before a flush/segment rotation, and more compactors
// keep up with the resulting write volume. Every value stays inside the
// [4.8.0] validated bounds. Pair this profile with
// BatchBuilder.AddNodes (pkg/graph/internal/core — the write-only bulk node
// create door) on the write side, and QueryOpts.NoSort on any read
// interleaved with the load (bulk-ingest reads rarely need a sorted scan,
// and NoSort avoids paying for one).
func WithProfileBulkLoad() Option {
	return func(cfg *Config) {
		// 512MB: within the validated [1MB,2GB) range; a larger vlog
		// segment than WithProfileSmall's 64MB so a high-throughput ingest
		// rotates value-log segments less often.
		cfg.ValueLogFileSize = 512 * oneMiB
		// 256MB: within the validated [8MB,1GB] range; larger than the 64MB
		// stock memtable so more of the bulk load's writes land in RAM
		// before a flush, trading peak memory for fewer flushes during the
		// load window only.
		cfg.MemTableSize = 256 * oneMiB
		// 4: matches Badger's own stock NumCompactors, kept explicit here
		// so bulk-load's higher write volume is never bottlenecked by a
		// lower profile's compactor count when this Option is applied after
		// one.
		cfg.NumCompactors = 4
	}
}
