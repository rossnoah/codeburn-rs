#!/usr/bin/env bash
#
# perf-bench.sh — A/B performance benchmark for cburn.
#
# Builds two binaries:
#   baseline  — from the current git HEAD (or a ref you specify)
#   candidate — from your working tree (uncommitted changes and all)
#
# For each cache mode (normal, --no-output-cache, --no-cache), it first
# checks that both binaries produce identical output, then benchmarks
# them head-to-head with hyperfine.
#
# Usage:
#   ./perf-bench.sh                             # 50 warmup, 100 runs
#   ./perf-bench.sh --warmup 5 --runs 20        # quick iteration
#   ./perf-bench.sh --providers "claude codex"   # just those two
#   ./perf-bench.sh --label "simd-json"          # tag the results
#   ./perf-bench.sh --baseline-ref v1.2.0        # compare against a tag

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── defaults ──────────────────────────────────────────────────────────
WARMUP=50
RUNS=100
PROVIDERS="all claude codex opencode cursor copilot pi"
PERIOD="30days"
LABEL=""
BASELINE_REF="HEAD"
SKIP_BUILD=0

# ── parse args ────────────────────────────────────────────────────────
while [ $# -gt 0 ]; do
  case "$1" in
    --warmup)       WARMUP="$2";       shift 2 ;;
    --runs)         RUNS="$2";         shift 2 ;;
    --providers)    PROVIDERS="$2";    shift 2 ;;
    --period)       PERIOD="$2";       shift 2 ;;
    --label)        LABEL="$2";        shift 2 ;;
    --baseline-ref) BASELINE_REF="$2"; shift 2 ;;
    --skip-build)   SKIP_BUILD=1;      shift   ;;
    -h|--help)
      sed -n '3,/^$/{ s/^# //; s/^#//; p }' "$0"
      exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 1 ;;
  esac
done

# ── preflight ─────────────────────────────────────────────────────────
if ! command -v hyperfine >/dev/null; then
  echo "error: hyperfine is required (brew install hyperfine)" >&2
  exit 1
fi

# ── paths ─────────────────────────────────────────────────────────────
BENCH_DIR="$SCRIPT_DIR/target/bench"
mkdir -p "$BENCH_DIR"

BASELINE_BIN="$BENCH_DIR/cburn-baseline"
CANDIDATE_BIN="$BENCH_DIR/cburn-candidate"

WORKTREE_DIR="$BENCH_DIR/_worktree_baseline"

# ── git info ──────────────────────────────────────────────────────────
HEAD_SHA="$(git -C "$SCRIPT_DIR" rev-parse --short HEAD)"
BASELINE_SHA="$(git -C "$SCRIPT_DIR" rev-parse --short "$BASELINE_REF")"
DIRTY=""
if ! git -C "$SCRIPT_DIR" diff --quiet 2>/dev/null; then
  DIRTY=" (dirty)"
fi

# ── build both binaries ──────────────────────────────────────────────
if [ "$SKIP_BUILD" -eq 0 ]; then
  echo "Building two binaries for A/B comparison"
  echo "========================================="
  echo

  # --- candidate: build from working tree ---
  echo "[candidate] building from working tree (${HEAD_SHA}${DIRTY})..."
  (cd "$SCRIPT_DIR" && cargo build --release 2>&1 | tail -3)
  cp "$SCRIPT_DIR/target/release/cburn" "$CANDIDATE_BIN"
  echo "  -> $CANDIDATE_BIN ($(du -h "$CANDIDATE_BIN" | cut -f1 | xargs))"
  echo

  # --- baseline: build from BASELINE_REF via git worktree ---
  echo "[baseline] building from ${BASELINE_REF} (${BASELINE_SHA})..."

  # clean up any leftover worktree
  if [ -d "$WORKTREE_DIR" ]; then
    git -C "$SCRIPT_DIR" worktree remove --force "$WORKTREE_DIR" 2>/dev/null || rm -rf "$WORKTREE_DIR"
  fi

  git -C "$SCRIPT_DIR" worktree add --detach "$WORKTREE_DIR" "$BASELINE_REF" 2>/dev/null
  (cd "$WORKTREE_DIR" && cargo build --release 2>&1 | tail -3)
  cp "$WORKTREE_DIR/target/release/cburn" "$BASELINE_BIN"
  echo "  -> $BASELINE_BIN ($(du -h "$BASELINE_BIN" | cut -f1 | xargs))"

  # clean up worktree
  git -C "$SCRIPT_DIR" worktree remove --force "$WORKTREE_DIR" 2>/dev/null || true
  echo
else
  echo "Skipping build, reusing existing binaries..."
  [ ! -x "$BASELINE_BIN" ] && echo "error: $BASELINE_BIN not found" >&2 && exit 1
  [ ! -x "$CANDIDATE_BIN" ] && echo "error: $CANDIDATE_BIN not found" >&2 && exit 1
  echo
fi

# ── output setup ──────────────────────────────────────────────────────
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
RESULTS_DIR="$SCRIPT_DIR/.notes/bench"
mkdir -p "$RESULTS_DIR"

SUFFIX=""
[ -n "$LABEL" ] && SUFFIX="-${LABEL}"
OUT_FILE="$RESULTS_DIR/perf-${TIMESTAMP}${SUFFIX}.txt"
JSON_DIR="$RESULTS_DIR/json-${TIMESTAMP}${SUFFIX}"
mkdir -p "$JSON_DIR"

CURSOR_CACHE_WIPE="rm -f $HOME/.cache/codeburn/cursor-results.json $HOME/.cache/codeburn/cursor-full-cache.json"

# ── correctness tracking ─────────────────────────────────────────────
OUTPUT_MISMATCHES=0
DIFF_DIR="$BENCH_DIR/diffs"
rm -rf "$DIFF_DIR"
mkdir -p "$DIFF_DIR"

# ── header ────────────────────────────────────────────────────────────
{
  echo "A/B Performance Benchmark  $(date '+%Y-%m-%d %H:%M')"
  echo
  echo "  baseline:   ${BASELINE_REF} (${BASELINE_SHA})"
  echo "  candidate:  working tree (${HEAD_SHA}${DIRTY})"
  [ -n "$LABEL" ] && echo "  label:      ${LABEL}"
  echo "  warmup:     ${WARMUP}"
  echo "  runs:       ${RUNS}"
  echo "  period:     ${PERIOD}"
  echo "  providers:  ${PROVIDERS}"
  echo "  machine:    $(uname -ms), $(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo '?') cores"
  echo
} | tee "$OUT_FILE"

# ── verify + bench runner ────────────────────────────────────────────
# run_ab <mode_name> <extra_flags> <prepare_cmd>
run_ab() {
  local mode_name="$1"
  local extra_flags="$2"
  local prepare_cmd="$3"

  {
    echo "-----------------------------------------------------------"
    echo "  ${mode_name}"
    echo "-----------------------------------------------------------"
    echo
  } | tee -a "$OUT_FILE"

  for provider in $PROVIDERS; do
    local base_cmd="$BASELINE_BIN report ${extra_flags} --provider ${provider} --period ${PERIOD}"
    local cand_cmd="$CANDIDATE_BIN report ${extra_flags} --provider ${provider} --period ${PERIOD}"
    local slug="${mode_name//[ \/()]/_}-${provider}"
    local json_file="${JSON_DIR}/${slug}.json"

    echo "  ${provider}:" | tee -a "$OUT_FILE"

    # -- correctness check --
    [ -n "$prepare_cmd" ] && eval "$prepare_cmd" 2>/dev/null || true

    local base_out="$DIFF_DIR/${slug}-baseline.txt"
    local cand_out="$DIFF_DIR/${slug}-candidate.txt"

    CODEBURN_STATIC_OUTPUT=1 $BASELINE_BIN report ${extra_flags} --provider "${provider}" --period "${PERIOD}" > "$base_out" 2>/dev/null || true
    CODEBURN_STATIC_OUTPUT=1 $CANDIDATE_BIN report ${extra_flags} --provider "${provider}" --period "${PERIOD}" > "$cand_out" 2>/dev/null || true

    local match="ok"
    if ! diff -q "$base_out" "$cand_out" >/dev/null 2>&1; then
      match="MISMATCH"
      OUTPUT_MISMATCHES=$((OUTPUT_MISMATCHES + 1))
      diff -u "$base_out" "$cand_out" > "$DIFF_DIR/${slug}.diff" 2>/dev/null || true
      echo "    !! output mismatch -- diff saved to $DIFF_DIR/${slug}.diff" | tee -a "$OUT_FILE"
    else
      echo "    output: identical" | tee -a "$OUT_FILE"
    fi

    # -- benchmark both --
    HF_ARGS=(
      --shell=none
      --input null
      --warmup "$WARMUP"
      --runs "$RUNS"
      --export-json "$json_file"
    )
    [ -n "$prepare_cmd" ] && HF_ARGS+=(--prepare "$prepare_cmd")

    hyperfine "${HF_ARGS[@]}" \
      -n "baseline" "$base_cmd" \
      -n "candidate" "$cand_cmd" \
      2>&1 | grep -E '(Time|Range|faster|slower)' | sed 's/^/    /' | tee -a "$OUT_FILE"

    # -- extract numbers from the json --
    if [ -f "$json_file" ]; then
      local summary
      summary="$(python3 -c "
import json
rs = json.load(open('$json_file'))['results']
def fmt(r):
    s = r['stddev'] or 0
    return f\"{r['mean']*1000:.1f} ms +/- {s*1000:.1f}\"
b, c = rs[0]['mean'], rs[1]['mean']
d = (c - b) / b * 100
sign = '+' if d > 0 else ''
print(f'    baseline {fmt(rs[0])}  candidate {fmt(rs[1])}  delta {sign}{d:.1f}%  output {\"$match\"}')")"
      echo "$summary" | tee -a "$OUT_FILE"
    fi

    echo | tee -a "$OUT_FILE"
  done
}

# ── run the three modes ───────────────────────────────────────────────

run_ab "normal (all caches)" "" ""
run_ab "no output cache (--no-output-cache)" "--no-output-cache" ""
run_ab "no cache / cold (--no-cache)" "--no-cache" "$CURSOR_CACHE_WIPE"

# ── summary ──────────────────────────────────────────────────────────
{
  echo "==========================================="
  if [ "$OUTPUT_MISMATCHES" -gt 0 ]; then
    echo "  ${OUTPUT_MISMATCHES} output mismatch(es)"
    echo "  diffs: target/bench/diffs/"
  else
    echo "  all outputs match"
  fi
  echo "  results: ${OUT_FILE}"
  echo "  json:    ${JSON_DIR}/"
  echo "==========================================="
} | tee -a "$OUT_FILE"

exit $OUTPUT_MISMATCHES
