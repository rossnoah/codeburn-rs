#!/usr/bin/env bash
#
# Benchmark the Rust `cburn` binary against the published JS version
# (via `npx codeburn`). Uses hyperfine and reports means + the speedup ratio.
#
# Each run starts with the cursor disk cache cleared (`--prepare`) so both
# implementations do the full cursor SQLite scan — the "cold" path, which
# matches what the Rust `--no-cache` flag forces.
#
# Usage:
#   ./bench.sh                      # all providers
#   ./bench.sh --provider cursor    # single provider
#   ./bench.sh --runs 10 --warmup 3
#   ./bench.sh --no-js              # skip JS benchmark for fast iteration
#   ./bench.sh --no-output-cache    # bypass the static-report output cache
#                                   # (forces every run through the parse pipeline)

set -euo pipefail

RUST_BIN="$(cd "$(dirname "$0")" && pwd)/target/release/cburn"

if [ ! -x "$RUST_BIN" ]; then
  echo "building release binary..." >&2
  (cd "$(dirname "$0")" && cargo build --release >/dev/null 2>&1)
fi

if ! command -v hyperfine >/dev/null; then
  echo "hyperfine is required (brew install hyperfine)" >&2
  exit 1
fi

PROVIDER="all"
PERIOD="week"
RUNS=5
WARMUP=2
MODE="cache"   # "nocache" = wipe cursor disk cache + pass --no-cache
                 # "cache"   = let both sides use their on-disk caches
NO_JS=0
NO_OUTPUT_CACHE=0
EXTRA=()

while [ $# -gt 0 ]; do
  case "$1" in
    --provider)        PROVIDER="$2";        shift 2 ;;
    --period)          PERIOD="$2";          shift 2 ;;
    --runs)            RUNS="$2";            shift 2 ;;
    --warmup)          WARMUP="$2";          shift 2 ;;
    --mode)            MODE="$2";            shift 2 ;;
    --no-js)           NO_JS=1;              shift   ;;
    --no-output-cache) NO_OUTPUT_CACHE=1;    shift   ;;
    *)                 EXTRA+=("$1");        shift   ;;
  esac
done

if [ "$NO_JS" -eq 0 ] && ! command -v npx >/dev/null; then
  echo "npx is required (or pass --no-js to skip the JS benchmark)" >&2
  exit 1
fi

RUST_EXTRA_FLAGS=""
RUST_LABEL_SUFFIX=""
if [ "$NO_OUTPUT_CACHE" -eq 1 ]; then
  RUST_EXTRA_FLAGS="--no-output-cache"
  RUST_LABEL_SUFFIX=" no-out-cache"
fi

case "$MODE" in
  nocache)
    # Wipe both sides' cursor result caches before every run so the full
    # SQLite scan actually happens. Rust still gets `--no-cache` so its
    # in-memory LRU in `parse_all_sessions` can't short-circuit either.
    PREPARE="rm -f $HOME/.cache/codeburn/cursor-results.json $HOME/.cache/codeburn/cursor-full-cache.json"
    RUST_CMD="$RUST_BIN report --no-cache $RUST_EXTRA_FLAGS --provider $PROVIDER --period $PERIOD"
    JS_CMD="npx --yes codeburn report --provider $PROVIDER --period $PERIOD"
    RUST_LABEL="rust (--no-cache$RUST_LABEL_SUFFIX)"
    JS_LABEL="js  (npx codeburn)"
    ;;
  cache)
    # Both sides keep their on-disk caches. Warmup populates the caches; the
    # measured runs read from them. This is closer to a user's second-and-
    # later invocations of `codeburn report` against unchanged session data.
    PREPARE=""
    RUST_CMD="$RUST_BIN report $RUST_EXTRA_FLAGS --provider $PROVIDER --period $PERIOD"
    JS_CMD="npx --yes codeburn report --provider $PROVIDER --period $PERIOD"
    RUST_LABEL="rust (cache on$RUST_LABEL_SUFFIX)"
    JS_LABEL="js  (cache on)"
    ;;
  *)
    echo "unknown --mode '$MODE' (use 'cache' or 'nocache')" >&2
    exit 1
    ;;
esac

echo "mode: $MODE"
echo "rust: $RUST_CMD"
if [ "$NO_JS" -eq 0 ]; then
  echo "js:   $JS_CMD"
fi
echo

# --shell=none avoids the ~1-2 ms `sh -c` startup that hyperfine warns about
# at sub-5 ms results. --input null fills in for the `< /dev/null` shell
# redirection we used to inline in the command string.
#
# CODEBURN_STATIC_OUTPUT=1 forces the compact text aggregate (the bench's
# original target) instead of the rich ratatui dashboard that's now the
# default for non-TTY stdin — keeps cached numbers comparable to historical
# baselines.
HF_ARGS=(--shell=none --input null --warmup "$WARMUP" --runs "$RUNS")
if [ -n "$PREPARE" ]; then
  HF_ARGS+=(--prepare "$PREPARE")
fi

if [ "$NO_JS" -eq 1 ]; then
  exec hyperfine \
    "${HF_ARGS[@]}" \
    -n "$RUST_LABEL" "$RUST_CMD" \
    "${EXTRA[@]}"
else
  exec hyperfine \
    "${HF_ARGS[@]}" \
    -n "$RUST_LABEL" "$RUST_CMD" \
    -n "$JS_LABEL"   "$JS_CMD" \
    "${EXTRA[@]}"
fi
