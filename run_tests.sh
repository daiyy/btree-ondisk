#!/usr/bin/env bash
# Run the full test suite across the supported feature matrix.
set -euo pipefail

run() {
    echo "=== $* ==="
    "$@"
}

# default (rc + tokio-runtime), async API
run cargo test --test bmap_tests --test coverage_boost

# arc + tokio-runtime, async API
run cargo test --no-default-features --features arc,value-check,tokio-runtime --test bmap_tests --test coverage_boost

# rc + sync-api
run cargo test --features sync-api --test sync_smoke

# arc + sync-api
run cargo test --no-default-features --features arc,value-check,tokio-runtime,sync-api --test sync_smoke
