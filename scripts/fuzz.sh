#!/usr/bin/env bash
# The trust-boundary fuzz harnesses (docs/SECURITY.md), FUZZTIME each (default 20s). Part of the major gate.
set -euo pipefail
t=${FUZZTIME:-20s}
for f in FuzzImport FuzzApplyChange; do go test -run '^$' -fuzz=$f -fuzztime="$t" ./pkg/graph/internal/core/; done
for f in FuzzWireToNodeChecked FuzzWireToRelChecked FuzzUnmarshalNodeWireWithKeys; do
  go test -run '^$' -fuzz=$f -fuzztime="$t" ./pkg/graph/internal/storeutil/
done
