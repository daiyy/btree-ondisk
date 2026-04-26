# Fuzz regression seeds

Tracked inputs that previously triggered a crash. `./run_audit.sh seed`
copies them into `fuzz/corpus/<target>/` before each fuzz run so any
regression of a fixed issue is caught immediately.

Each subdirectory matches a target name under `fuzz/fuzz_targets/`.
