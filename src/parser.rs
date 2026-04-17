// Copyright (C) 2026  Noah Ross
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::collections::HashMap;
use std::time::{Instant, SystemTime};

use anyhow::Result;
use chrono::Local;
use dashmap::DashSet;
use rayon::prelude::*;

use crate::classifier::classify_turn;
use crate::models::get_short_model_name;
use crate::providers::{get_all_providers, Provider};
use crate::report_cache::{CacheSnapshot, EntryHeader, NewEntry};
use crate::types::{
    CategoryStats, ClassifiedTurn, DateRange, ModelStats, ParsedApiCall, ParsedProviderCall,
    ParsedTurn, ProjectSummary, SessionSource, SessionSummary, StatusAggregate, StatusBounds,
    TokenUsage, ToolStats,
};

/// Process-wide cache bypass flag. Set once at startup by --no-cache and
/// consulted by every cache-lookup site (cursor disk cache, status SQLite
/// cache, and the report-cache.bin mmap).
static CACHE_BYPASS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn set_cache_bypass(bypass: bool) {
    CACHE_BYPASS.store(bypass, std::sync::atomic::Ordering::Release);
}

pub fn is_cache_bypassed() -> bool {
    CACHE_BYPASS.load(std::sync::atomic::Ordering::Acquire)
}

/// Process-wide flag set by --no-output-cache. When set the static report
/// path skips both reading and writing the per-(period, provider) output
/// memoization file; the underlying `report-cache.bin` parse cache is
/// unaffected. Mostly useful for benchmarking the full parse pipeline.
static OUTPUT_CACHE_BYPASS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub fn set_output_cache_bypass(bypass: bool) {
    OUTPUT_CACHE_BYPASS.store(bypass, std::sync::atomic::Ordering::Release);
}

pub fn is_output_cache_bypassed() -> bool {
    OUTPUT_CACHE_BYPASS.load(std::sync::atomic::Ordering::Acquire)
}

fn unsanitize_path(dir_name: &str) -> String {
    dir_name.replace('-', "/")
}

/// Compute cache-invalidation metadata for every known source and fold
/// the results into a single `u64` hash plus a per-path `(mtime, size)`
/// map. The hash is the output cache's "session-files signature" — any
/// change to any source file flips one of the mtimes, flips the hash,
/// invalidates the memoized output. The per-path map is reused as
/// `pre_stats` in `parse_all_sessions` when the output cache misses so
/// we don't pay for the stat fanout twice.
///
/// This dispatches through each provider's `cache_metadata` — same logic
/// the parse cache uses in `partition_cache`, so the invalidation rule is
/// symmetric with the parse cache:
///   - jsonl providers (claude / codex / pi / copilot): stat the jsonl
///   - cursor: stat the cursor SQLite DB
///   - opencode: stat the opencode SQLite DB (186 sessions collapse to
///     one real stat via `DB_META_CACHE`)
///
/// Any file the parse cache would consider "changed" is one this
/// signature also catches — there's no correctness gap between the two
/// layers.
pub fn stat_all_sources() -> (HashMap<String, (u64, u64)>, u64) {
    use std::hash::Hasher;
    let cache = match crate::discovery_cache::load() {
        Some(c) => c,
        None => return (HashMap::new(), 0),
    };
    let providers = get_all_providers();
    let provider_by_name: HashMap<&str, &Box<dyn Provider>> =
        providers.iter().map(|p| (p.name(), p)).collect();

    let all_sources: Vec<SessionSource> = cache
        .sources
        .into_values()
        .flat_map(|srcs| srcs.into_iter())
        .collect();

    let results: Vec<(String, (u64, u64))> = all_sources
        .par_iter()
        .filter_map(|s| {
            let provider = provider_by_name.get(s.provider.as_str())?;
            let meta = provider.cache_metadata(s)?;
            Some((s.path.clone(), meta))
        })
        .collect();

    // Sort for stable hash regardless of rayon collect order.
    let mut sorted = results.clone();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let mut h = rustc_hash::FxHasher::default();
    for (path, (mtime, size)) in &sorted {
        h.write(path.as_bytes());
        h.write_u64(*mtime);
        h.write_u64(*size);
    }
    let sig = h.finish();
    let map: HashMap<String, (u64, u64)> = results.into_iter().collect();
    (map, sig)
}

fn provider_call_to_api_call(call: &ParsedProviderCall) -> ParsedApiCall {
    let usage = TokenUsage {
        input_tokens: call.input_tokens,
        output_tokens: call.output_tokens,
        cache_creation_input_tokens: call.cache_creation_input_tokens,
        cache_read_input_tokens: call.cache_read_input_tokens,
        cached_input_tokens: call.cached_input_tokens,
        reasoning_tokens: call.reasoning_tokens,
        web_search_requests: call.web_search_requests,
    };

    let mcp_tools: Vec<String> = call
        .tools
        .iter()
        .filter(|t| t.starts_with("mcp__"))
        .cloned()
        .collect();

    ParsedApiCall {
        model: call.model.clone(),
        usage,
        cost_usd: call.cost_usd,
        tools: call.tools.clone(),
        mcp_tools,
        has_agent_spawn: call.tools.contains(&"Agent".to_string()),
        has_plan_mode: call.tools.contains(&"EnterPlanMode".to_string()),
        timestamp: call.timestamp.clone(),
        bash_commands: call.bash_commands.clone(),
    }
}

/// Group calls into turns. Behavior differs by provider, matching the JS
/// reference:
///
/// - Claude: calls that share a `user_message_timestamp` belong to one
///   turn (one user prompt + its assistant bursts). Retries only fire
///   when a turn has multiple calls.
/// - Everything else: one call = one turn, same as JS
///   `providerCallToTurn`. Those providers don't preserve prompt grouping
///   the way the Claude JSONL does, so don't invent it.
fn group_calls_into_turns(calls: &[&ParsedProviderCall]) -> Vec<ParsedTurn> {
    let mut turns: Vec<ParsedTurn> = Vec::new();
    let mut current_key: Option<String> = None;
    let mut current_user_msg: String = String::new();
    let mut current_calls: Vec<ParsedApiCall> = Vec::new();
    let mut current_ts: String = String::new();

    let flush = |turns: &mut Vec<ParsedTurn>,
                 user_msg: &mut String,
                 calls: &mut Vec<ParsedApiCall>,
                 ts: &mut String| {
        if !calls.is_empty() {
            turns.push(ParsedTurn {
                user_message: std::mem::take(user_msg),
                assistant_calls: std::mem::take(calls),
                timestamp: std::mem::take(ts),
            });
        }
    };

    for call in calls {
        // Non-Claude providers (or Claude calls missing the user-entry
        // timestamp) emit one turn per call — don't fold them together.
        if call.user_message_timestamp.is_empty() {
            flush(&mut turns, &mut current_user_msg, &mut current_calls, &mut current_ts);
            current_key = None;
            turns.push(ParsedTurn {
                user_message: call.user_message.clone(),
                assistant_calls: vec![provider_call_to_api_call(call)],
                timestamp: call.timestamp.clone(),
            });
            continue;
        }

        let key = &call.user_message_timestamp;
        if current_key.as_deref() != Some(key.as_str()) {
            flush(&mut turns, &mut current_user_msg, &mut current_calls, &mut current_ts);
            current_key = Some(key.clone());
            current_user_msg = call.user_message.clone();
            current_ts = call.timestamp.clone();
        }
        current_calls.push(provider_call_to_api_call(call));
    }
    flush(&mut turns, &mut current_user_msg, &mut current_calls, &mut current_ts);

    turns
}

fn build_session_summary(
    session_id: &str,
    project: &str,
    turns: &[ClassifiedTurn],
) -> SessionSummary {
    let mut model_breakdown: HashMap<String, ModelStats> = HashMap::new();
    let mut tool_breakdown: HashMap<String, ToolStats> = HashMap::new();
    let mut mcp_breakdown: HashMap<String, ToolStats> = HashMap::new();
    let mut bash_breakdown: HashMap<String, ToolStats> = HashMap::new();
    let mut category_breakdown: HashMap<crate::types::TaskCategory, CategoryStats> = HashMap::new();
    let mut daily_map: HashMap<String, crate::types::DailyCostEntry> = HashMap::new();

    let mut total_cost = 0.0;
    let mut total_input = 0u64;
    let mut total_output = 0u64;
    let mut total_cache_read = 0u64;
    let mut total_cache_write = 0u64;
    let mut api_calls = 0u64;
    let mut first_ts = String::new();
    let mut last_ts = String::new();

    for ct in turns {
        let turn_cost: f64 = ct.turn.assistant_calls.iter().map(|c| c.cost_usd).sum();

        let cat_entry = category_breakdown
            .entry(ct.category)
            .or_insert_with(CategoryStats::default);
        cat_entry.turns += 1;
        cat_entry.cost_usd += turn_cost;
        if ct.has_edits {
            cat_entry.edit_turns += 1;
            cat_entry.retries += ct.retries as u64;
            if ct.retries == 0 {
                cat_entry.one_shot_turns += 1;
            }
        }

        for call in &ct.turn.assistant_calls {
            total_cost += call.cost_usd;
            total_input += call.usage.input_tokens;
            total_output += call.usage.output_tokens;
            total_cache_read += call.usage.cache_read_input_tokens;
            total_cache_write += call.usage.cache_creation_input_tokens;
            api_calls += 1;

            // Accumulate daily costs
            if call.timestamp.len() >= 10 {
                let day = &call.timestamp[..10];
                let daily = daily_map
                    .entry(day.to_string())
                    .or_insert_with(|| crate::types::DailyCostEntry {
                        day: day.to_string(),
                        ..Default::default()
                    });
                daily.cost_usd += call.cost_usd;
                daily.call_count += 1;
                daily.input_tokens += call.usage.input_tokens;
                daily.output_tokens += call.usage.output_tokens;
                daily.cache_read_tokens += call.usage.cache_read_input_tokens;
                daily.cache_write_tokens += call.usage.cache_creation_input_tokens;
            }

            let model_key = get_short_model_name(&call.model);
            let model_entry = model_breakdown
                .entry(model_key)
                .or_insert_with(ModelStats::default);
            model_entry.calls += 1;
            model_entry.cost_usd += call.cost_usd;
            model_entry.tokens.add(&call.usage);

            for tool in call.tools.iter().filter(|t| !t.starts_with("mcp__")) {
                tool_breakdown
                    .entry(tool.clone())
                    .or_insert_with(ToolStats::default)
                    .calls += 1;
            }
            for mcp in &call.mcp_tools {
                let server = mcp.split("__").nth(1).unwrap_or(mcp);
                mcp_breakdown
                    .entry(server.to_string())
                    .or_insert_with(ToolStats::default)
                    .calls += 1;
            }
            for cmd in &call.bash_commands {
                bash_breakdown
                    .entry(cmd.clone())
                    .or_insert_with(ToolStats::default)
                    .calls += 1;
            }

            if first_ts.is_empty() || (!call.timestamp.is_empty() && call.timestamp < first_ts) {
                first_ts = call.timestamp.clone();
            }
            if last_ts.is_empty() || (!call.timestamp.is_empty() && call.timestamp > last_ts) {
                last_ts = call.timestamp.clone();
            }
        }
    }

    // Convert daily map to sorted vec
    let mut daily_costs: Vec<crate::types::DailyCostEntry> =
        daily_map.into_values().collect();
    daily_costs.sort_by(|a, b| a.day.cmp(&b.day));

    SessionSummary {
        session_id: session_id.to_string(),
        project: project.to_string(),
        first_timestamp: if first_ts.is_empty() {
            turns
                .first()
                .map(|t| t.turn.timestamp.clone())
                .unwrap_or_default()
        } else {
            first_ts
        },
        last_timestamp: if last_ts.is_empty() {
            turns
                .last()
                .map(|t| t.turn.timestamp.clone())
                .unwrap_or_default()
        } else {
            last_ts
        },
        total_cost_usd: total_cost,
        total_input_tokens: total_input,
        total_output_tokens: total_output,
        total_cache_read_tokens: total_cache_read,
        total_cache_write_tokens: total_cache_write,
        api_calls,
        daily_costs,
        model_breakdown,
        tool_breakdown,
        mcp_breakdown,
        bash_breakdown,
        category_breakdown,
    }
}


// ── parse_all_sessions pipeline ────────────────────────────────────────
//
// Staged as a small number of helpers so the orchestrator reads top-down:
//   partition_cache  → decode_cache_hits  → parse_misses  → spawn_persist
//                    → classify_and_summarize  → finalize_projects
//
// Each helper preserves the exact rayon / DashSet / clone shape from when
// this lived as one function. Perf guardrail: changes here must not regress
// the numbers in `notes/bench/BASELINE.md`.

static PROF: std::sync::LazyLock<bool> =
    std::sync::LazyLock::new(|| std::env::var_os("CODEBURN_PROFILE").is_some());

fn prof_log(label: &str, t: Instant) {
    if *PROF {
        eprintln!("[prof] {:<28} {:>8.1} ms", label, t.elapsed().as_secs_f64() * 1000.0);
    }
}

fn compute_date_filters(
    date_range: Option<&DateRange>,
) -> (Option<SystemTime>, Option<String>, Option<String>) {
    // JSONL timestamps are UTC, so any caller-provided date range must be
    // converted to UTC for both the mtime filter and the string prefix
    // comparison providers use to skip non-matching lines.
    let since = date_range.map(|dr| {
        // 1-day buffer protects against TZ differences between the shell
        // user and the files being scanned.
        let buffered = dr.start - chrono::Duration::days(1);
        let secs = buffered.timestamp() as u64;
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs)
    });
    let start = date_range.map(|dr| {
        dr.start.with_timezone(&chrono::Utc).format("%Y-%m-%dT%H:%M:%S").to_string()
    });
    let end = date_range.map(|dr| {
        dr.end.with_timezone(&chrono::Utc).format("%Y-%m-%dT%H:%M:%S").to_string()
    });
    (since, start, end)
}

struct CachePartition<'a> {
    /// `(entry, project)` for every source whose cache row is still valid.
    hits: Vec<(&'a EntryHeader, String)>,
    /// Misses grouped by `Provider::name()` — matches the key
    /// `parse_sources` needs.
    misses_by_provider: HashMap<String, Vec<SessionSource>>,
    /// Per-miss `(mtime, size, project)` so we don't re-stat on the write
    /// path. Keyed on source path (the cache-row primary key).
    miss_meta: HashMap<String, (u64, u64, String)>,
    /// Total misses (including sources with no cache_metadata). Used for
    /// `CODEBURN_PROFILE=1` telemetry only.
    miss_count: usize,
}

fn partition_cache<'a>(
    snapshot: &'a CacheSnapshot,
    all_sources: &[SessionSource],
    provider_by_name: &HashMap<&str, &Box<dyn Provider>>,
    pre_stats: &HashMap<String, (u64, u64)>,
    bypass: bool,
) -> CachePartition<'a> {
    // Most sources are in the snapshot from the last run, so their stat
    // results may already have been computed (e.g. by `stat_all_sources`
    // for the output-cache fingerprint). Look those up from `pre_stats`
    // first; fall through to the provider's `cache_metadata` dispatch
    // only for fresh sources or ones the caller didn't pre-stat.
    // Callers that don't have pre-stats pass `&HashMap::new()`.
    let per_source: Vec<(usize, Option<(u64, u64)>)> = all_sources
        .par_iter()
        .enumerate()
        .map(|(i, source)| {
            let meta = if bypass {
                None
            } else if let Some(m) = pre_stats.get(source.path.as_str()) {
                Some(*m)
            } else {
                provider_by_name
                    .get(source.provider.as_str())
                    .and_then(|p| p.cache_metadata(source))
            };
            (i, meta)
        })
        .collect();

    let mut hits: Vec<(&'a EntryHeader, String)> = Vec::new();
    let mut misses_by_provider: HashMap<String, Vec<SessionSource>> = HashMap::new();
    let mut miss_meta: HashMap<String, (u64, u64, String)> = HashMap::new();
    let mut miss_count = 0usize;

    for (i, meta) in per_source {
        let source = &all_sources[i];
        if !provider_by_name.contains_key(source.provider.as_str()) {
            continue;
        }
        if let Some((mtime, size)) = meta {
            if let Some(entry) = snapshot.lookup(&source.path, mtime, size) {
                hits.push((entry, source.project.clone()));
                continue;
            }
            miss_meta.insert(source.path.clone(), (mtime, size, source.project.clone()));
        }
        miss_count += 1;
        misses_by_provider
            .entry(source.provider.clone())
            .or_default()
            .push(source.clone());
    }

    CachePartition { hits, misses_by_provider, miss_meta, miss_count }
}

fn decode_cache_hits(
    snapshot: &CacheSnapshot,
    hits: &[(&EntryHeader, String)],
) -> Vec<(String, Vec<SessionSummary>)> {
    hits.par_iter()
        .filter_map(|(entry, project)| {
            snapshot.decode(entry).map(|sessions| (project.clone(), sessions))
        })
        .collect()
}

fn parse_misses(
    providers: &[Box<dyn Provider>],
    misses_by_provider: &HashMap<String, Vec<SessionSource>>,
    seen_keys: &DashSet<String>,
    since: Option<SystemTime>,
    date_start: Option<&str>,
    date_end: Option<&str>,
) -> Vec<(String, String, Vec<ParsedProviderCall>)> {
    let prof = *PROF;
    providers
        .par_iter()
        .flat_map(|provider| {
            let sources_for_provider = misses_by_provider
                .get(provider.name())
                .cloned()
                .unwrap_or_default();
            if sources_for_provider.is_empty() {
                return Vec::new();
            }
            let p_start = if prof { Some(Instant::now()) } else { None };
            let out = provider.parse_sources(
                &sources_for_provider,
                seen_keys,
                since,
                date_start,
                date_end,
            );
            if let Some(s) = p_start {
                eprintln!(
                    "[prof]   {:<10} wall: {:.1} ms ({} sources)",
                    provider.name(),
                    s.elapsed().as_secs_f64() * 1000.0,
                    sources_for_provider.len(),
                );
            }
            out
        })
        .collect()
}

/// Cache *every* miss source — including ones that produced no calls —
/// otherwise "empty" sources (user-only jsonl, subagent stubs, files with no
/// assistant turns) miss forever and we reparse them every run. Encoding an
/// empty-Vec blob is ~2 bytes, so this is free.
///
/// The snapshot is cheap to clone (it's a pair of `Arc`s). We move the clone
/// + the fresh entries into a detached thread so compose + write + rename
/// all run off the hot path.
fn spawn_persist(
    snapshot: CacheSnapshot,
    miss_meta: &HashMap<String, (u64, u64, String)>,
    miss_summaries_by_source: &HashMap<String, Vec<SessionSummary>>,
    miss_calls_by_source: &HashMap<String, Vec<ParsedProviderCall>>,
) {
    if miss_meta.is_empty() {
        return;
    }
    let fresh: Vec<NewEntry> = miss_meta
        .par_iter()
        .map(|(source_path, (mtime, size, project))| {
            let empty_calls: Vec<ParsedProviderCall> = Vec::new();
            let empty_sessions: Vec<SessionSummary> = Vec::new();
            let calls = miss_calls_by_source
                .get(source_path.as_str())
                .unwrap_or(&empty_calls);
            let sessions = miss_summaries_by_source
                .get(source_path.as_str())
                .unwrap_or(&empty_sessions);
            NewEntry {
                key: source_path.clone(),
                mtime: *mtime,
                size: *size,
                project: project.clone(),
                summary: crate::report_cache::encode_summary(calls),
                blob: crate::report_cache::encode_session_summaries(sessions),
            }
        })
        .collect();

    let handle = std::thread::spawn(move || {
        let bytes = crate::report_cache::compose_from_snapshot(&snapshot, &fresh);
        drop(snapshot);
        let _ = crate::report_cache::persist_bytes(bytes);
    });
    // Stash the handle in a process-global so `main` can join it before
    // exit. Without this, `std::thread::spawn`'d writer threads get killed
    // when main returns and the cache never gets written — a silent
    // regression where every run looks like a fresh start.
    PENDING_PERSIST.lock().unwrap().push(handle);
}

/// Detached-but-rejoinable persist threads. `main` drains + joins this at
/// the end of the non-TTY path so the on-disk cache is actually written.
pub static PENDING_PERSIST: std::sync::LazyLock<
    std::sync::Mutex<Vec<std::thread::JoinHandle<()>>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(Vec::new()));

/// Drain & join any outstanding persist threads. Call at the end of a
/// one-shot (non-TTY) command so all disk writes finish before we exit.
pub fn join_pending_persists() {
    let handles: Vec<_> = std::mem::take(&mut *PENDING_PERSIST.lock().unwrap());
    for h in handles {
        let _ = h.join();
    }
}

/// Classify + aggregate one source's raw calls into `Vec<SessionSummary>`.
/// Runs without any date filter — the result is what gets bincode'd into the
/// persistent cache. Downstream callers filter by date range at render time
/// via [`filter_session_for_range`].
fn build_source_summaries(
    project: &str,
    calls: &[ParsedProviderCall],
) -> Vec<SessionSummary> {
    let mut sess_calls: HashMap<String, Vec<&ParsedProviderCall>> = HashMap::new();
    for call in calls {
        sess_calls
            .entry(call.session_id.clone())
            .or_default()
            .push(call);
    }

    let mut summaries: Vec<SessionSummary> = Vec::with_capacity(sess_calls.len());
    for (session_id, mut session_calls) in sess_calls {
        session_calls.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        let turns = group_calls_into_turns(&session_calls);
        let classified: Vec<ClassifiedTurn> =
            turns.into_iter().map(classify_turn).collect();
        let s = build_session_summary(&session_id, project, &classified);
        if s.api_calls > 0 {
            summaries.push(s);
        }
    }
    summaries
}

/// Trim a cached `SessionSummary` down to the calls that fall within
/// `date_range`. The returned summary has:
/// - `daily_costs` restricted to in-range UTC days.
/// - totals derived from those filtered days (cost / tokens / call count).
/// Session-level breakdowns (`model_breakdown`, `tool_breakdown`,
/// `bash_breakdown`, `mcp_breakdown`, `category_breakdown`) are carried
/// through unchanged — for sessions that straddle the period boundary this
/// can include contributions from out-of-range calls. In practice sessions
/// are short-lived so the overlap is tiny; the dashboard totals (which come
/// from the filtered daily_costs) stay exact.
///
/// Returns `None` when no day of the session falls in range. Consumes `s`
/// so the HashMap breakdowns can be moved (not cloned) into the output.
fn filter_session_for_range(
    s: SessionSummary,
    start_day: &str,
    end_day: &str,
) -> Option<SessionSummary> {
    // Quick path: whole session is outside the range. `first_timestamp` /
    // `last_timestamp` are UTC 19-char prefixes; start/end are UTC "YYYY-MM-DD"
    // days extracted from the caller's range.
    if !s.last_timestamp.is_empty() && s.last_timestamp.as_str() < start_day {
        return None;
    }
    let end_upper = end_day_upper_bound(end_day);
    if !s.first_timestamp.is_empty() && s.first_timestamp.as_str() > end_upper.as_str() {
        return None;
    }

    // Retain in-range days in place; bail if none match so we skip the move
    // cost of breakdown HashMaps below.
    let mut s = s;
    s.daily_costs.retain(|d| d.day.as_str() >= start_day && d.day.as_str() <= end_day);
    if s.daily_costs.is_empty() {
        return None;
    }
    s.daily_costs.sort_by(|a, b| a.day.cmp(&b.day));

    let mut total_cost = 0.0f64;
    let mut api_calls = 0u64;
    let mut total_input = 0u64;
    let mut total_output = 0u64;
    let mut total_cache_read = 0u64;
    let mut total_cache_write = 0u64;
    for d in &s.daily_costs {
        total_cost += d.cost_usd;
        api_calls += d.call_count;
        total_input += d.input_tokens;
        total_output += d.output_tokens;
        total_cache_read += d.cache_read_tokens;
        total_cache_write += d.cache_write_tokens;
    }
    if api_calls == 0 {
        return None;
    }

    s.total_cost_usd = total_cost;
    s.api_calls = api_calls;
    s.total_input_tokens = total_input;
    s.total_output_tokens = total_output;
    s.total_cache_read_tokens = total_cache_read;
    s.total_cache_write_tokens = total_cache_write;
    Some(s)
}

/// `end_day` is a "YYYY-MM-DD" string — to compare against a 19-char timestamp
/// prefix we need "YYYY-MM-DDT23:59:59" (or anything lexically >= the max
/// in-day prefix). Returning "YYYY-MM-DDZ" works since 'Z' > 'T' > digits so
/// any in-day ts prefix sorts below it.
fn end_day_upper_bound(end_day: &str) -> String {
    // "2026-04-17" → "2026-04-17Z"  (Z sorts above any "T…" suffix)
    let mut s = String::with_capacity(end_day.len() + 1);
    s.push_str(end_day);
    s.push('Z');
    s
}

/// Group cached session summaries (and fresh miss summaries) by project,
/// filtering each by the requested `date_range` — the read-time replacement
/// for the old `classify_and_summarize` pass. No bincode decode of raw calls,
/// no `classify_turn` on previously-seen turns.
fn merge_and_filter_sessions(
    hit_sessions: Vec<(String, Vec<SessionSummary>)>,
    miss_sessions: Vec<(String, Vec<SessionSummary>)>,
    date_range: Option<&DateRange>,
) -> HashMap<String, Vec<SessionSummary>> {
    let (start_day, end_day) = match date_range {
        Some(dr) => (
            dr.start
                .with_timezone(&chrono::Utc)
                .format("%Y-%m-%d")
                .to_string(),
            dr.end
                .with_timezone(&chrono::Utc)
                .format("%Y-%m-%d")
                .to_string(),
        ),
        None => (String::new(), String::new()),
    };
    let filter_active = !start_day.is_empty();

    let per_source: Vec<(String, Vec<SessionSummary>)> = hit_sessions
        .into_par_iter()
        .chain(miss_sessions.into_par_iter())
        .map(|(project, sessions)| {
            if !filter_active {
                return (project, sessions);
            }
            let filtered: Vec<SessionSummary> = sessions
                .into_iter()
                .filter_map(|s| filter_session_for_range(s, &start_day, &end_day))
                .collect();
            (project, filtered)
        })
        .collect();

    let mut project_map: HashMap<String, Vec<SessionSummary>> = HashMap::new();
    for (project, summaries) in per_source {
        if summaries.is_empty() {
            continue;
        }
        project_map.entry(project).or_default().extend(summaries);
    }
    project_map
}

fn finalize_projects(
    project_map: HashMap<String, Vec<SessionSummary>>,
) -> Vec<ProjectSummary> {
    let mut result: Vec<ProjectSummary> = project_map
        .into_iter()
        .map(|(project, sessions)| {
            let total_cost: f64 = sessions.iter().map(|s| s.total_cost_usd).sum();
            let total_calls: u64 = sessions.iter().map(|s| s.api_calls).sum();
            ProjectSummary {
                project_path: unsanitize_path(&project),
                sessions,
                total_cost_usd: total_cost,
                total_api_calls: total_calls,
            }
        })
        .collect();

    // Stable tiebreak on project path so two projects with equal cost never
    // swap between renders (visible as UI jitter in the TUI).
    result.sort_by(|a, b| {
        b.total_cost_usd
            .partial_cmp(&a.total_cost_usd)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.project_path.cmp(&b.project_path))
    });
    result
}

/// Lightweight per-project aggregate used by the non-TTY `report` output.
/// Skips `classify_turn` + full `SessionSummary` construction — those are
/// ~60% of the cached-hit wall, and the static renderer only reads cost,
/// call count, a handful of token sums, and unique session count.
#[derive(Debug, Clone)]
pub struct StaticProjectAggregate {
    pub project_path: String,
    pub total_cost_usd: f64,
    pub total_api_calls: u64,
}

#[derive(Debug, Default, Clone)]
pub struct StaticAggregate {
    pub projects: Vec<StaticProjectAggregate>,
    pub total_sessions: usize,
    pub total_input: u64,
    pub total_output: u64,
    pub total_cache_read: u64,
    pub total_cache_write: u64,
}

/// Fast-path entry for the non-TTY report command. Does everything
/// `parse_all_sessions` does up through cache decode + miss parse, but
/// skips classification + the per-session HashMap build. Returns only the
/// numbers `render_static` actually reads.
///
/// For cache hits this skips bincode decoding entirely — it iterates the
/// per-entry compact summary blob (added in cache v3) instead. The summary
/// is an order of magnitude smaller than the full call list and contains
/// exactly the fields `aggregate_static` needs (cost, call count, the four
/// token totals, and session ID for global de-dup).
pub fn parse_all_sessions_static(
    date_range: Option<&DateRange>,
    provider_filter: Option<&str>,
) -> Result<StaticAggregate> {
    let bypass = is_cache_bypassed();
    let t0 = Instant::now();

    let t_cache_load = Instant::now();
    let snapshot = if bypass {
        CacheSnapshot::empty()
    } else {
        CacheSnapshot::load()
    };
    prof_log("cache snapshot load", t_cache_load);

    let discover_hints: HashMap<String, String> = snapshot
        .iter()
        .map(|(k, h)| (k.to_string(), h.project.clone()))
        .collect();
    let all_sources = crate::providers::discover_all_sessions_with_hints(
        provider_filter,
        Some(&discover_hints),
    )?;
    prof_log("discover_all_sessions", t0);

    let providers = get_all_providers();
    let (since, date_start_str, date_end_str) = compute_date_filters(date_range);

    let t_partition = Instant::now();
    let provider_by_name: HashMap<&str, &Box<dyn Provider>> =
        providers.iter().map(|p| (p.name(), p)).collect();
    let CachePartition { hits, misses_by_provider, miss_meta, miss_count } =
        partition_cache(&snapshot, &all_sources, &provider_by_name, &HashMap::new(), bypass);
    prof_log("cache partition", t_partition);
    if *PROF {
        eprintln!(
            "[prof] cache hits={} misses={} (of {} sources)",
            hits.len(),
            miss_count,
            all_sources.len()
        );
    }

    let seen_keys: DashSet<String> = DashSet::new();
    let t_parse = Instant::now();
    // Pass date filters to providers ONLY when --no-cache is set. In
    // cached mode the parse result feeds `spawn_persist`, and storing a
    // date-shaped subset would poison the cache: a later run with a
    // wider period would get a hit and see truncated data.
    //
    // The poisoning was visible in pi/codex/claude when populating the
    // cache via a "today" run and then querying "30days" — those
    // providers all use `since` to bail out early on stale files.
    let (parse_since, parse_ds, parse_de) = if bypass {
        (since, date_start_str.as_deref(), date_end_str.as_deref())
    } else {
        (None, None, None)
    };
    let miss_results = parse_misses(
        &providers,
        &misses_by_provider,
        &seen_keys,
        parse_since,
        parse_ds,
        parse_de,
    );
    prof_log("parallel provider parse", t_parse);

    let t_persist = Instant::now();
    if !bypass {
        // Build per-source SessionSummary list for each miss so the cache
        // write path stores pre-classified rows (matches parse_all_sessions).
        let miss_summaries: HashMap<String, Vec<SessionSummary>> = miss_results
            .par_iter()
            .map(|(source_path, project, calls)| {
                (source_path.clone(), build_source_summaries(project, calls))
            })
            .collect();
        let mut by_calls: HashMap<String, Vec<ParsedProviderCall>> =
            HashMap::with_capacity(miss_results.len());
        for (source_path, _project, calls) in &miss_results {
            by_calls.insert(source_path.clone(), calls.clone());
        }
        spawn_persist(snapshot.clone(), &miss_meta, &miss_summaries, &by_calls);
    }
    prof_log("cache persist compose", t_persist);

    let t_agg = Instant::now();
    let agg = aggregate_static_from_summary(&snapshot, &hits, &miss_results, date_range);
    prof_log("static aggregate", t_agg);

    // Drop the ~30 k ParsedProviderCall vectors off the hot path — their
    // combined String fields take ~4 ms to deallocate, which we don't want
    // the user's `codeburn report` to wait on. We've already extracted every
    // number we need into `agg`.
    std::thread::spawn(move || {
        drop(miss_results);
    });

    prof_log("TOTAL parse_all_sessions", t0);
    Ok(agg)
}

/// Aggregate cache hits straight from their packed summary blobs (no
/// bincode decode), plus parse-miss results (still per-call). For the warm
/// cached path this skips the ~10 ms `cache decode hits` step entirely.
fn aggregate_static_from_summary(
    snapshot: &CacheSnapshot,
    hits: &[(&EntryHeader, String)],
    miss_results: &[(String, String, Vec<ParsedProviderCall>)],
    date_range: Option<&DateRange>,
) -> StaticAggregate {
    let (start_str, end_str) = match date_range {
        Some(dr) => (
            dr.start
                .with_timezone(&chrono::Utc)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string(),
            dr.end
                .with_timezone(&chrono::Utc)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string(),
        ),
        None => (String::new(), String::new()),
    };
    let start_bytes = start_str.as_bytes();
    let end_bytes = end_str.as_bytes();
    let date_filter_active = !start_str.is_empty();

    #[derive(Default)]
    struct ProjectAcc {
        cost: f64,
        calls: u64,
        input: u64,
        output: u64,
        cache_read: u64,
        cache_write: u64,
        sessions: rustc_hash::FxHashSet<String>,
    }

    // Hit aggregation: parallel over sources, iterate the packed summary
    // blob inline. No String allocations beyond the unique session-id ones
    // that survive the HashSet check.
    let hit_acc: Vec<(&str, ProjectAcc)> = hits
        .par_iter()
        .map(|(entry, project)| {
            let mut acc = ProjectAcc::default();
            for b in crate::report_cache::SummaryIter::new(snapshot.summary_bytes(entry)) {
                // 19-byte ASCII prefix compare, matching the original
                // per-call `aggregate_static`. Buckets without a real
                // timestamp (zero prefix) are always included — same
                // fall-through behaviour the original loop had.
                if date_filter_active && b.has_timestamp() {
                    if b.ts_prefix < start_bytes || b.ts_prefix > end_bytes {
                        continue;
                    }
                }
                acc.cost += b.cost;
                acc.calls += b.calls as u64;
                acc.input += b.input;
                acc.output += b.output;
                acc.cache_read += b.cache_read;
                acc.cache_write += b.cache_write;
                if !b.session_id.is_empty() {
                    if let Ok(s) = std::str::from_utf8(b.session_id) {
                        acc.sessions.insert(s.to_string());
                    }
                }
            }
            (project.as_str(), acc)
        })
        .collect();

    // Miss aggregation: still iterate the per-call list since we don't have
    // a summary in memory yet — it's being composed for persist on a side
    // thread. Same shape as the hit loop.
    let miss_acc: Vec<(&str, ProjectAcc)> = miss_results
        .par_iter()
        .map(|(_, project, calls)| {
            let mut acc = ProjectAcc::default();
            for call in calls {
                // Match original `aggregate_static`: only filter when both
                // the date range is set AND the timestamp is parseable.
                // Calls without a 19-char timestamp prefix fall through
                // and get aggregated unconditionally.
                if date_filter_active && call.timestamp.len() >= 19 {
                    let prefix = &call.timestamp.as_bytes()[..19];
                    if prefix < start_bytes || prefix > end_bytes {
                        continue;
                    }
                }
                acc.cost += call.cost_usd;
                acc.calls += 1;
                acc.input += call.input_tokens;
                acc.output += call.output_tokens;
                acc.cache_read += call.cache_read_input_tokens;
                acc.cache_write += call.cache_creation_input_tokens;
                if !call.session_id.is_empty() {
                    acc.sessions.insert(call.session_id.clone());
                }
            }
            (project.as_str(), acc)
        })
        .collect();

    let mut by_project: HashMap<String, ProjectAcc> = HashMap::new();
    let mut global_sessions: rustc_hash::FxHashSet<String> = rustc_hash::FxHashSet::default();
    let mut total_input = 0u64;
    let mut total_output = 0u64;
    let mut total_cache_read = 0u64;
    let mut total_cache_write = 0u64;

    for (project, acc) in hit_acc.into_iter().chain(miss_acc.into_iter()) {
        total_input += acc.input;
        total_output += acc.output;
        total_cache_read += acc.cache_read;
        total_cache_write += acc.cache_write;
        for s in &acc.sessions {
            global_sessions.insert(s.clone());
        }
        let entry = by_project.entry(project.to_string()).or_default();
        entry.cost += acc.cost;
        entry.calls += acc.calls;
        for s in acc.sessions {
            entry.sessions.insert(s);
        }
    }

    let mut projects: Vec<StaticProjectAggregate> = by_project
        .into_iter()
        .filter(|(_, a)| a.calls > 0)
        .map(|(p, a)| StaticProjectAggregate {
            project_path: unsanitize_path(&p),
            total_cost_usd: a.cost,
            total_api_calls: a.calls,
        })
        .collect();
    projects.sort_by(|a, b| {
        b.total_cost_usd
            .partial_cmp(&a.total_cost_usd)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.project_path.cmp(&b.project_path))
    });

    StaticAggregate {
        projects,
        total_sessions: global_sessions.len(),
        total_input,
        total_output,
        total_cache_read,
        total_cache_write,
    }
}

/// Parse every discovered session into `Vec<ProjectSummary>`, using the
/// cache for any source whose `(mtime, size)` matches.
///
/// `pre_stats` is an optional pre-computed `source_path → (mtime, size)`
/// map that the caller may have built already (see `stat_all_sources`
/// for the output-cache path). Pass `&HashMap::new()` when you don't
/// have one — `partition_cache` falls through to the provider's
/// `cache_metadata` dispatch for any source missing from the map.
pub fn parse_all_sessions(
    date_range: Option<&DateRange>,
    provider_filter: Option<&str>,
    pre_stats: &HashMap<String, (u64, u64)>,
) -> Result<Vec<ProjectSummary>> {
    let bypass = is_cache_bypassed();

    let t0 = Instant::now();
    let seen_keys: DashSet<String> = DashSet::new();

    // Snapshot load and discovery walk are independent — running them on
    // two rayon threads shaves a few hundred µs off the sequential version
    // on a warm cache. (Hints for codex discovery are derived from the
    // on-disk discovery cache when present; passing None here is fine
    // because that path is already short-circuited by the discovery cache.)
    let t_cache_load = Instant::now();
    let provider_filter_owned = provider_filter.map(|s| s.to_string());
    let (snapshot, discovered_pre) = rayon::join(
        || if bypass { CacheSnapshot::empty() } else { CacheSnapshot::load() },
        || {
            crate::providers::discover_all_sessions_with_hints(
                provider_filter_owned.as_deref(),
                None,
            )
        },
    );
    prof_log("cache snapshot load", t_cache_load);

    let all_sources = discovered_pre?;
    prof_log("discover_all_sessions", t0);

    let t1 = Instant::now();
    let providers = get_all_providers();
    prof_log("get_all_providers", t1);

    let (since, date_start_str, date_end_str) = compute_date_filters(date_range);

    // ── disk cache: partition hits/misses ──
    // Each cache row holds one source's parsed `Vec<ParsedProviderCall>`
    // (bincode'd) keyed on source path + (mtime, size). A row stays valid
    // as long as the underlying file hasn't changed. `--no-cache` bypasses
    // this entirely.
    let t_partition = Instant::now();
    let provider_by_name: HashMap<&str, &Box<dyn Provider>> =
        providers.iter().map(|p| (p.name(), p)).collect();
    let CachePartition { hits, misses_by_provider, miss_meta, miss_count } =
        partition_cache(&snapshot, &all_sources, &provider_by_name, pre_stats, bypass);
    prof_log("cache partition", t_partition);
    if *PROF {
        eprintln!(
            "[prof] cache hits={} misses={} (of {} sources)",
            hits.len(),
            miss_count,
            all_sources.len()
        );
    }

    let t_decode = Instant::now();
    let hit_sessions = decode_cache_hits(&snapshot, &hits);
    prof_log("cache decode hits", t_decode);

    // Providers return `(source_path, project, calls)` so each miss maps
    // cleanly to a cache entry. Same caveat as `parse_all_sessions_static`:
    // we only push date filters into the providers when --no-cache is set.
    // In cached mode the result feeds `spawn_persist`, and a date-shaped
    // subset would poison the cache for any subsequent wider-period query.
    let t_parse = Instant::now();
    let (parse_since, parse_ds, parse_de) = if bypass {
        (since, date_start_str.as_deref(), date_end_str.as_deref())
    } else {
        (None, None, None)
    };
    let miss_results = parse_misses(
        &providers,
        &misses_by_provider,
        &seen_keys,
        parse_since,
        parse_ds,
        parse_de,
    );
    prof_log("parallel provider parse", t_parse);

    // Classify + aggregate miss results per source so we can (a) cache the
    // pre-classified summaries and (b) feed them into the same downstream
    // pipeline as cache hits. Misses are typically 1 source (the active
    // session being written), so this stays cheap.
    let t_miss_cls = Instant::now();
    let miss_summaries_per_source: Vec<(String, String, Vec<SessionSummary>)> = miss_results
        .par_iter()
        .map(|(source_path, project, calls)| {
            let summaries = build_source_summaries(project, calls);
            (source_path.clone(), project.clone(), summaries)
        })
        .collect();
    prof_log("miss classify", t_miss_cls);

    let t_persist_compose = Instant::now();
    if !bypass {
        let mut by_summary: HashMap<String, Vec<SessionSummary>> =
            HashMap::with_capacity(miss_summaries_per_source.len());
        for (source_path, _project, summaries) in &miss_summaries_per_source {
            by_summary.insert(source_path.clone(), summaries.clone());
        }
        let mut by_calls: HashMap<String, Vec<ParsedProviderCall>> =
            HashMap::with_capacity(miss_results.len());
        for (source_path, _project, calls) in &miss_results {
            by_calls.insert(source_path.clone(), calls.clone());
        }
        spawn_persist(snapshot.clone(), &miss_meta, &by_summary, &by_calls);
    }
    prof_log("cache persist compose", t_persist_compose);

    if *PROF {
        let total_sessions: usize = hit_sessions.iter().map(|(_, s)| s.len()).sum::<usize>()
            + miss_summaries_per_source.iter().map(|(_, _, s)| s.len()).sum::<usize>();
        eprintln!("[prof] sources={} total_sessions={}", hits.len() + miss_meta.len(), total_sessions);
    }

    let miss_sessions: Vec<(String, Vec<SessionSummary>)> = miss_summaries_per_source
        .into_iter()
        .map(|(_, project, sessions)| (project, sessions))
        .collect();

    let t_post = Instant::now();
    let project_map = merge_and_filter_sessions(hit_sessions, miss_sessions, date_range);
    prof_log("parallel post-processing", t_post);

    let t_finalize = Instant::now();
    let result = finalize_projects(project_map);
    prof_log("finalize/sort", t_finalize);

    // Drop the miss_results Vec<ParsedProviderCall> off the hot path — the
    // per-call Strings take a few ms to free. We've already extracted what we
    // need (summaries + encoded blobs).
    std::thread::spawn(move || {
        drop(miss_results);
    });

    prof_log("TOTAL parse_all_sessions", t0);

    Ok(result)
}

/// Fast path for `codeburn status`: computes only aggregate cost/call data.
/// Skips classification, tool extraction, user messages, session grouping,
/// and all breakdown building. Uses mmap, simd-json pre-filtering, persistent
/// SQLite cache, and flat parallelism across all files.
///
/// All providers run in parallel via rayon. Caching is handled *inside*
/// each provider's `parse_session_status` method at the individual file level.
pub fn parse_status_fast(provider_filter: Option<&str>) -> Result<StatusAggregate> {
    let now = Local::now();

    // Compute UTC timestamp boundaries for each period.
    // JSONL timestamps are UTC, so all comparisons must be in UTC.
    let today_start_local = now.date_naive().and_hms_opt(0, 0, 0).unwrap()
        .and_local_timezone(Local).unwrap();
    let today_end_local = now.date_naive().and_hms_milli_opt(23, 59, 59, 999).unwrap()
        .and_local_timezone(Local).unwrap();
    let week_start_local = (now - chrono::Duration::days(7))
        .date_naive().and_hms_opt(0, 0, 0).unwrap()
        .and_local_timezone(Local).unwrap();
    let month_start_local = {
        use chrono::Datelike;
        let d = now.date_naive();
        chrono::NaiveDate::from_ymd_opt(d.year(), d.month(), 1)
            .unwrap().and_hms_opt(0, 0, 0).unwrap()
            .and_local_timezone(Local).unwrap()
    };

    let bounds = StatusBounds {
        today_start: today_start_local.with_timezone(&chrono::Utc)
            .format("%Y-%m-%dT%H:%M:%S").to_string(),
        today_end: today_end_local.with_timezone(&chrono::Utc)
            .format("%Y-%m-%dT%H:%M:%S").to_string(),
        week_start: week_start_local.with_timezone(&chrono::Utc)
            .format("%Y-%m-%dT%H:%M:%S").to_string(),
        month_start: month_start_local.with_timezone(&chrono::Utc)
            .format("%Y-%m-%dT%H:%M:%S").to_string(),
        month_end: today_end_local.with_timezone(&chrono::Utc)
            .format("%Y-%m-%dT%H:%M:%S").to_string(),
    };

    let providers = get_all_providers();
    let seen_keys: DashSet<u64> = DashSet::new();

    let results: Vec<StatusAggregate> = providers
        .par_iter()
        .filter_map(|provider| {
            if let Some(filter) = provider_filter {
                if filter != "all" && filter != provider.name() {
                    return None;
                }
            }

            let sources = provider.discover_sessions().ok()?;
            // Parallelise per-source: after the per-file source refactor,
            // providers like Claude return hundreds of sources each. A
            // serial loop here made status O(n) files.
            let provider_agg = sources
                .par_iter()
                .filter_map(|source| {
                    provider
                        .parse_session_status(source, &seen_keys, &bounds)
                        .ok()
                        .map(|(agg, _)| agg)
                })
                .reduce(StatusAggregate::default, |mut a, b| {
                    a.merge(&b);
                    a
                });
            Some(provider_agg)
        })
        .collect();

    let mut total = StatusAggregate::default();
    for agg in &results {
        total.merge(agg);
    }

    Ok(total)
}
