#!/usr/bin/env bash
# Unsafe audit: runs Miri on the unit tests and invokes cargo-fuzz
# for each registered target.
#
# Prereqs (one-time):
#   rustup +nightly component add miri rust-src
#   cargo install cargo-fuzz
#
# Usage:
#   ./run_audit.sh miri          # run Miri on unit + integration tests
#   ./run_audit.sh fuzz [secs]   # run each fuzz target for N seconds (default 60)

set -euo pipefail

cmd="${1:-miri}"

case "$cmd" in
    miri)
        MIRIFLAGS="${MIRIFLAGS:--Zmiri-disable-isolation}" \
            cargo +nightly miri test --lib
        MIRIFLAGS="${MIRIFLAGS:--Zmiri-disable-isolation}" \
            cargo +nightly miri test --test coverage_boost btree_node_from_slice_errors
        ;;
    fuzz)
        secs="${2:-60}"
        for t in btree_node_from_slice direct_node_from_slice bmap_read; do
            echo "=== fuzz $t (${secs}s) ==="
            cargo +nightly fuzz run "$t" -- -max_total_time="$secs" || true
        done
        ;;
    *)
        echo "usage: $0 {miri|fuzz [secs]}" >&2
        exit 2
        ;;
esac
