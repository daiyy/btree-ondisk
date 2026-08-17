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
#   ./run_audit.sh fuzz-arc [secs]   # same but under arc feature
#   ./run_audit.sh fuzz-all [secs]   # run both rc and arc
#   ./run_audit.sh fuzz-quick    # CI-friendly: seed from fuzz/seeds/* + 30s each
#   ./run_audit.sh seed          # just (re)seed corpus from fuzz/seeds/*

set -euo pipefail

cmd="${1:-miri}"
MIRIFLAGS="${MIRIFLAGS:--Zmiri-disable-isolation}"
export MIRIFLAGS

miri_rc() {
    cargo +nightly miri test --lib
    cargo +nightly miri test --test coverage_boost
    cargo +nightly miri test --test bmap_tests
    cargo +nightly miri test --test lookup_batch
    cargo +nightly miri test --test bigvalue
    cargo +nightly miri test --test node_id
}

miri_arc() {
    local feats='arc,value-check,tokio-runtime'
    cargo +nightly miri test --no-default-features --features "$feats" --lib
    cargo +nightly miri test --no-default-features --features "$feats" --test coverage_boost
    cargo +nightly miri test --no-default-features --features "$feats" --test bmap_tests
    cargo +nightly miri test --no-default-features --features "$feats" --test lookup_batch
    cargo +nightly miri test --no-default-features --features "$feats" --test bigvalue
    cargo +nightly miri test --no-default-features --features "$feats" --test node_id
}

seed_corpus() {
    # Copy any tracked regression seeds into the live corpus so libFuzzer
    # picks them up at startup. fuzz/corpus/ is gitignored.
    if [[ -d fuzz/seeds ]]; then
        for dir in fuzz/seeds/*/; do
            local target="$(basename "$dir")"
            mkdir -p "fuzz/corpus/$target"
            cp -n "$dir"* "fuzz/corpus/$target/" 2>/dev/null || true
        done
    fi
}

# $1: seconds, $2: feature set ("rc" or "arc")
fuzz_run() {
    local secs="$1"
    local feat="${2:-rc}"
    seed_corpus
    local flags=()
    if [[ "$feat" == "arc" ]]; then
        flags=(--no-default-features --features arc)
    fi
    for t in btree_node_from_slice direct_node_from_slice bmap_read bmap_ops bmap_lookup_batch; do
        echo "=== fuzz $t ($feat, ${secs}s) ==="
        cargo +nightly fuzz run "${flags[@]}" "$t" -- -max_total_time="$secs" || return 1
    done
}

case "$cmd" in
    miri)       miri_rc ;;
    miri-arc)   miri_arc ;;
    miri-all)   miri_rc; miri_arc ;;
    fuzz)       fuzz_run "${2:-60}" rc ;;
    fuzz-arc)   fuzz_run "${2:-60}" arc ;;
    fuzz-all)   fuzz_run "${2:-60}" rc; fuzz_run "${2:-60}" arc ;;
    fuzz-quick) fuzz_run 30 rc ;;
    seed)       seed_corpus ;;
    *)
        echo "usage: $0 {miri|miri-arc|miri-all|fuzz [secs]|fuzz-arc [secs]|fuzz-all [secs]|fuzz-quick|seed}" >&2
        exit 2
        ;;
esac
