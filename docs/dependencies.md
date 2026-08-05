# Dependencies

This document lists all dependencies of `github.com/data-insights-ai/rho-tkg/v4` and their licenses.

`go list -m all` reports 36 dependency modules (plus the main module itself); the
table below has 36 rows, one per dependency, kept in sync with that output
(refreshed 2026-08-05 for the v4.28.1 line).

**License Assertion:** All dependencies use licenses from the set {Apache-2.0, BSD-2-Clause, BSD-3-Clause, MIT, ISC}. Every row in the table below carries one of these five licenses.

## Dependency Table

| Module | Version | License |
|--------|---------|---------|
| github.com/bds421/rho-mclock | v0.2.1 | Apache-2.0 |
| github.com/bds421/rho-snowflake-2026 | v1.3.2 | Apache-2.0 |
| github.com/cespare/xxhash/v2 | v2.3.0 | MIT |
| github.com/davecgh/go-spew | v1.1.1 | ISC |
| github.com/dgraph-io/badger/v4 | v4.9.2 | Apache-2.0 |
| github.com/dgraph-io/ristretto/v2 | v2.2.0 | Apache-2.0 |
| github.com/dgryski/go-farm | v0.0.0-20240924180020-3414d57e47da | MIT |
| github.com/dustin/go-humanize | v1.0.1 | MIT |
| github.com/go-logr/logr | v1.4.3 | Apache-2.0 |
| github.com/go-logr/stdr | v1.2.2 | Apache-2.0 |
| github.com/golang/protobuf | v1.5.0 | BSD-3-Clause |
| github.com/google/flatbuffers | v25.2.10+incompatible | Apache-2.0 |
| github.com/google/go-cmp | v0.7.0 | BSD-3-Clause |
| github.com/google/uuid | v1.6.0 | BSD-3-Clause |
| github.com/inconshreveable/mousetrap | v1.1.0 | Apache-2.0 |
| github.com/klauspost/compress | v1.18.0 | BSD-3-Clause |
| github.com/kr/pretty | v0.3.1 | MIT |
| github.com/kr/text | v0.2.0 | MIT |
| github.com/pmezard/go-difflib | v1.0.0 | BSD-3-Clause |
| github.com/rogpeppe/go-internal | v1.14.1 | BSD-3-Clause |
| github.com/spf13/cobra | v1.9.1 | Apache-2.0 |
| github.com/spf13/pflag | v1.0.6 | BSD-3-Clause |
| github.com/stretchr/testify | v1.11.1 | MIT |
| github.com/vmihailenco/msgpack/v5 | v5.4.1 | BSD-3-Clause |
| github.com/vmihailenco/tagparser/v2 | v2.0.0 | MIT |
| go.opentelemetry.io/auto/sdk | v1.2.1 | Apache-2.0 |
| go.opentelemetry.io/contrib/zpages | v0.62.0 | Apache-2.0 |
| go.opentelemetry.io/otel | v1.41.0 | Apache-2.0 |
| go.opentelemetry.io/otel/metric | v1.41.0 | Apache-2.0 |
| go.opentelemetry.io/otel/sdk | v1.37.0 | Apache-2.0 |
| go.opentelemetry.io/otel/trace | v1.41.0 | Apache-2.0 |
| golang.org/x/sys | v0.35.0 | BSD-3-Clause |
| golang.org/x/xerrors | v0.0.0-20191204190536-9bdfabe68543 | BSD-3-Clause |
| google.golang.org/protobuf | v1.36.7 | BSD-3-Clause |
| gopkg.in/check.v1 | v1.0.0-20201130134442-10cb98267c6c | BSD-2-Clause |
| gopkg.in/yaml.v3 | v3.0.1 | Apache-2.0 |

## Notes

- All licenses have been verified by reading the LICENSE file in the module cache.
- ISC (used by `github.com/davecgh/go-spew`) is a permissive, MIT-equivalent license (no copyleft, no additional redistribution obligations) — included in the license allowlist above.
- Direct dependencies are few (`rho-snowflake-2026`, `msgpack/v5`, `badger/v4`); the OpenTelemetry modules arrive transitively via Badger and are not used by this library's own call graph.
