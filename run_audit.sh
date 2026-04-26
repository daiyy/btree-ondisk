#!/usr/bin/env bash
# Unsafe audit: runs Miri on the unit tests and invokes cargo-fuzz
# for each registered target.
#
# Prereqs (one-time):
#   rustup +nightly component add miri rust-src
#   cargo install cargo-fuzz
#
# Usage:
#   ./run_audit.sh miri          # run Miri on the default (rc) feature set
#   ./run_audit.sh miri-arc      # run Miri on arc+tokio-runtime
#   ./run_audit.sh miri-all      # run both
#   ./run_audit.sh fuzz [secs]   # run each fuzz target for N seconds (default 60)

set -euo pipefail

cmd="${1:-miri}"
MIRIFLAGS="${MIRIFLAGS:--Zmiri-disable-isolation}"
export MIRIFLAGS

miri_rc() {
    cargo +nightly miri test --lib
    cargo +nightly miri test --test coverage_boost
    cargo +nightly miri test --test bmap_tests
}

miri_arc() {
    local feats='arc,value-check,tokio-runtime'
    cargo +nightly miri test --no-default-features --features "$feats" --lib
    cargo +nightly miri test --no-default-features --features "$feats" --test coverage_boost
    cargo +nightly miri test --no-default-features --features "$feats" --test bmap_tests
}

case "$cmd" in
    miri)     miri_rc ;;
    miri-arc) miri_arc ;;
    miri-all) miri_rc; miri_arc ;;
    fuzz)
        secs="${2:-60}"
        for t in btree_node_from_slice direct_node_from_slice bmap_read; do
            echo "=== fuzz $t (${secs}s) ==="
            cargo +nightly fuzz run "$t" -- -max_total_time="$secs" || true
        done
        ;;
    *)
        echo "usage: $0 {miri|miri-arc|miri-all|fuzz [secs]}" >&2
        exit 2
        ;;
esac
