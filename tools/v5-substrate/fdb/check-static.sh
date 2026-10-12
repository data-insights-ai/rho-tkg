#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
exec ./run-local.sh static "${1:-small}"
