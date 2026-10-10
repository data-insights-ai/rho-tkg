# Pinned native-v4 correctness reproduction

This standalone tool reproduces the accepted basic-current functional slice against shipped rho v4.43.0 commit `4126bb108cd3fae2f0818052d08a31f0b5466c6e`. It validates 92 Memory answers, 92 isolated synchronous Badger answers and 23 r3 answers after Badger close/reopen. One retained-history query remains pending. No vendor, native-v5, benchmark or phase acceptance is claimed.

Use an explicitly available native Go1.26.9 compiler. The runner refuses other versions and ambient GOFLAGS, GOWORK, GOOS, GOARCH, GOEXPERIMENT, GOROOT or automatic toolchain overrides. It sets GOWORK=off, GOTOOLCHAIN=local, GOENV=off and readonly module flags. Root/v5/tool CI jobs each select their own go.mod version; dispatch flags prevent duplicate nested gates. Root formatting excludes the tool, whose formatter checks its own compiler.

From the repository root, with `go` resolving to Go1.26.9:

```sh
make compare-ci COMPARE_GO=go
make compare-ci-docker COMPARE_GO=go
```

The second command uses only an already cached Go1.26.9 Docker image and pinned scanners; it never pulls an image. Both commands require the declared scanner versions. Provisioning is a separate explicit `make -C tools/graph-compare tools` action; normal correctness tests never install tools or contact vendor services. Local reproduction is offline by default. CI explicitly permits checksum-verified module dependency downloads after provisioning its compiler/tools.

The runner reads trusted source/reference lock digests embedded in its reviewed code. It obtains the actual pinned commit and tree from local Git objects, safely extracts a transient archive, checks all 1,438 filenames and file hashes against the compact aggregate content pin, and compiles only that extracted dependency through a controlled relative replacement in scratch go.mod. There is no current-head/tag fallback. The shipped go.mod remains Go1.26.7; the reviewed compiler is Go1.26.9. Original source-manifest and tar hashes are historical evidence; newly generated raw tar bytes need not equal the old tar if exact commit/tree/content match.

All 114 existing comparison-input files are copied and checked against the unchanged trusted reference pin, including missing/extra/symlink refusals. Nothing regenerates or edits that reference package. Native answer generation does not read oracle outputs. The existing normalizer validates every complete native answer afterward. Dependency checksums and `go mod verify` precede readonly builds; effective module Dir/version/relative replacement and all scratch source bytes are checked.

Default runs use disposable owned workspaces. To retain outputs, create a new artifact parent and pass a new destination with REPRO_FLAGS, for example `--out artifacts/local-run`. Existing destinations refuse. The source archive/workspace is removed; completed reports, exports, native store files and coverage may be retained as caller-owned artifacts. Publication uses exclusive destination creation and moves the completion manifest last; this is not a crash-atomic whole-directory or power-loss guarantee. Cancellation/refusal publishes no success and cleanup never deletes preexisting caller artifacts. No raw source archive, native store or host paths are checked in.

Resource evidence remains limited: instrumented graph/store calls exclude row/sub-API accessors and initial graph.New (once/run, recorded separately); history counts identify IDs with history, not version totals. All native Badger files after close are counted; heap is a whole-process snapshot. MaxVisited limits delivered callbacks only: native range/full/adjacency may precompute before callbacks. No internal-work/engine-memory/disk quota, peak/RSS/cgroup, performance or complete-profile claim follows.

The accepted v4 request/scalar/native-operation contracts remain intact. The separately reviewed future Backend/streamQuery seam and vendor adapters are not included here. All three vendors, native-v5, retained/temporal/distributed lanes, eligible licenses, comparable native hardware and the full comparative resource/performance profiles remain open.
