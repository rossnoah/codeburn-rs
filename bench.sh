#!/usr/bin/env bash
#
# Benchmark the Rust `codeburn` binary against the published JS version
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

set -euo pipefail

RUST_BIN="$(cd "$(dirname "$0")" && pwd)/target/release/codeburn"

if [ ! -x "$RUST_BIN" ]; then
  echo "building release binary..." >&2
  (cd "$(dirname "$0")" && cargo build --release >/dev/null 2>&1)
fi

if ! command -v hyperfine >/dev/null; then
  echo "hyperfine is required (brew install hyperfine)" >&2
  exit 1
fi

if ! command -v npx >/dev/null; then
  echo "npx is required" >&2
  exit 1
fi

PROVIDER="all"
PERIOD="week"
RUNS=5
WARMUP=2
MODE="cache"   # "nocache" = wipe cursor disk cache + pass --no-cache
                 # "cache"   = let both sides use their on-disk caches
EXTRA=()

while [ $# -gt 0 ]; do
  case "$1" in
    --provider) PROVIDER="$2"; shift 2 ;;
    --period)   PERIOD="$2";   shift 2 ;;
    --runs)     RUNS="$2";     shift 2 ;;
    --warmup)   WARMUP="$2";   shift 2 ;;
    --mode)     MODE="$2";     shift 2 ;;
    *)          EXTRA+=("$1"); shift   ;;
  esac
done

case "$MODE" in
  nocache)
    # Wipe both sides' cursor result caches before every run so the full
    # SQLite scan actually happens. Rust still gets `--no-cache` so its
    # in-memory LRU in `parse_all_sessions` can't short-circuit either.
    PREPARE='rm -f ~/.cache/codeburn/cursor-results.json ~/.cache/codeburn/cursor-full-cache.json'
    RUST_CMD="$RUST_BIN report --no-cache --provider $PROVIDER --period $PERIOD < /dev/null"
    JS_CMD="npx --yes codeburn report --provider $PROVIDER --period $PERIOD < /dev/null"
    RUST_LABEL="rust (--no-cache)"
    JS_LABEL="js  (npx codeburn)"
    ;;
  cache)
    # Both sides keep their on-disk caches. Warmup populates the caches; the
    # measured runs read from them. This is closer to a user's second-and-
    # later invocations of `codeburn report` against unchanged session data.
    PREPARE=""
    RUST_CMD="$RUST_BIN report --provider $PROVIDER --period $PERIOD < /dev/null"
    JS_CMD="npx --yes codeburn report --provider $PROVIDER --period $PERIOD < /dev/null"
    RUST_LABEL="rust (cache on)"
    JS_LABEL="js  (cache on)"
    ;;
  *)
    echo "unknown --mode '$MODE' (use 'cache' or 'nocache')" >&2
    exit 1
    ;;
esac

echo "mode: $MODE"
echo "rust: $RUST_CMD"
echo "js:   $JS_CMD"
echo

HF_ARGS=(--warmup "$WARMUP" --runs "$RUNS")
if [ -n "$PREPARE" ]; then
  HF_ARGS+=(--prepare "$PREPARE")
fi

exec hyperfine \
  "${HF_ARGS[@]}" \
  -n "$RUST_LABEL" "$RUST_CMD" \
  -n "$JS_LABEL"   "$JS_CMD" \
  "${EXTRA[@]}"
