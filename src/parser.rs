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

fn unsanitize_path(dir_name: &str) -> String {
    dir_name.replace('-', "/")
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

fn filter_by_date_range<'a>(calls: &'a [ParsedProviderCall], date_range: Option<&DateRange>) -> Vec<&'a ParsedProviderCall> {
    match date_range {
        None => calls.iter().collect(),
        Some(dr) => {
            // JSONL timestamps are UTC, so convert date range to UTC for comparison.
            let start_str = dr.start.with_timezone(&chrono::Utc)
                .format("%Y-%m-%dT%H:%M:%S").to_string();
            let end_str = dr.end.with_timezone(&chrono::Utc)
                .format("%Y-%m-%dT%H:%M:%S").to_string();
            calls
                .iter()
                .filter(|c| {
                    if c.timestamp.is_empty() {
                        return true;
                    }
                    let ts = &c.timestamp;
                    if ts.len() < 19 {
                        return true;
                    }
                    let prefix = &ts[..19];
                    prefix >= start_str.as_str() && prefix <= end_str.as_str()
                })
                .collect()
        }
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
    bypass: bool,
) -> CachePartition<'a> {
    // Kick every source's `cache_metadata` stat call in parallel — it's
    // ~1443 independent `stat(2)`s on the hot path, and the serial version
    // was eating 3-4 ms. Results are pairs of (source idx, optional meta).
    // `miss_count` is the total number of source indices that didn't hit
    // cache, including sources with no metadata (which we can't cache).
    let per_source: Vec<(usize, Option<(u64, u64)>)> = all_sources
        .par_iter()
        .enumerate()
        .map(|(i, source)| {
            let meta = if bypass {
                None
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
) -> Vec<(String, Vec<ParsedProviderCall>)> {
    hits.par_iter()
        .filter_map(|(entry, project)| {
            snapshot.decode(entry).map(|calls| (project.clone(), calls))
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
    miss_results: &[(String, String, Vec<ParsedProviderCall>)],
) {
    if miss_meta.is_empty() {
        return;
    }
    let mut by_source: HashMap<&str, &[ParsedProviderCall]> =
        HashMap::with_capacity(miss_results.len());
    for (source_path, _project, calls) in miss_results {
        by_source.insert(source_path.as_str(), calls.as_slice());
    }
    let fresh: Vec<NewEntry> = miss_meta
        .par_iter()
        .map(|(source_path, (mtime, size, project))| {
            let empty: &[ParsedProviderCall] = &[];
            let calls = by_source.get(source_path.as_str()).copied().unwrap_or(empty);
            NewEntry {
                key: source_path.clone(),
                mtime: *mtime,
                size: *size,
                project: project.clone(),
                summary: crate::report_cache::encode_summary(calls),
                blob: crate::report_cache::encode_calls(calls),
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

fn classify_and_summarize(
    source_calls: &[(String, Vec<ParsedProviderCall>)],
    date_range: Option<&DateRange>,
) -> HashMap<String, Vec<SessionSummary>> {
    // Each source's calls are independent once date-filtered, so map in
    // parallel: source → Vec<SessionSummary>, then merge by project. With
    // the disk cache on, this loop is the last thing gating the TUI.
    let per_source: Vec<(String, Vec<SessionSummary>)> = source_calls
        .par_iter()
        .map(|(project, calls)| {
            let filtered = filter_by_date_range(calls, date_range);

            // Bucket by session so each session's calls can be sorted
            // chronologically before we group consecutive same-user-message
            // calls into turns (matches JS `groupIntoTurns`).
            let mut sess_calls: HashMap<String, Vec<&ParsedProviderCall>> = HashMap::new();
            for call in filtered {
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
            (project.clone(), summaries)
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
        partition_cache(&snapshot, &all_sources, &provider_by_name, bypass);
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
        spawn_persist(snapshot.clone(), &miss_meta, &miss_results);
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

#[allow(dead_code)]
fn aggregate_static(
    source_views: &[(&str, &[ParsedProviderCall])],
    date_range: Option<&DateRange>,
) -> StaticAggregate {
    let (start, end) = match date_range {
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

    #[derive(Default)]
    struct ProjectAcc {
        cost: f64,
        calls: u64,
        input: u64,
        output: u64,
        cache_read: u64,
        cache_write: u64,
        sessions: std::collections::HashSet<String>,
    }

    // Map per source → its (project_path, ProjectAcc). Parallel over sources,
    // then reduce by project at the end.
    let per_source: Vec<(String, ProjectAcc)> = source_views
        .par_iter()
        .map(|(project, calls)| {
            let mut acc = ProjectAcc::default();
            for call in *calls {
                if !start.is_empty() && !call.timestamp.is_empty() && call.timestamp.len() >= 19 {
                    let prefix = &call.timestamp[..19];
                    if prefix < start.as_str() || prefix > end.as_str() {
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
            (project.to_string(), acc)
        })
        .collect();

    let mut by_project: HashMap<String, ProjectAcc> = HashMap::new();
    let mut global_sessions: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    let mut total_input = 0u64;
    let mut total_output = 0u64;
    let mut total_cache_read = 0u64;
    let mut total_cache_write = 0u64;
    for (project, acc) in per_source {
        total_input += acc.input;
        total_output += acc.output;
        total_cache_read += acc.cache_read;
        total_cache_write += acc.cache_write;
        for s in &acc.sessions {
            global_sessions.insert(s.clone());
        }
        let entry = by_project.entry(project).or_default();
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

pub fn parse_all_sessions(
    date_range: Option<&DateRange>,
    provider_filter: Option<&str>,
) -> Result<Vec<ProjectSummary>> {
    let bypass = is_cache_bypassed();

    let t0 = Instant::now();
    let seen_keys: DashSet<String> = DashSet::new();

    // Load the cache snapshot *before* discovery so providers can use its
    // path→project map to skip expensive per-file validation on already-seen
    // files (see codex's `discover_sessions_with_hints`).
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
        partition_cache(&snapshot, &all_sources, &provider_by_name, bypass);
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
    let hit_calls = decode_cache_hits(&snapshot, &hits);
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

    // Combine hits + misses into the flat `(project, calls)` shape the
    // downstream post-processing expects.
    let source_calls: Vec<(String, Vec<ParsedProviderCall>)> = hit_calls
        .into_iter()
        .chain(
            miss_results
                .iter()
                .map(|(_, project, calls)| (project.clone(), calls.clone())),
        )
        .collect();

    let t_persist_compose = Instant::now();
    if !bypass {
        spawn_persist(snapshot.clone(), &miss_meta, &miss_results);
    }
    prof_log("cache persist compose", t_persist_compose);

    if *PROF {
        let total_calls: usize = source_calls.iter().map(|(_, c)| c.len()).sum();
        eprintln!("[prof] sources={} total_calls={}", source_calls.len(), total_calls);
    }

    let t_post = Instant::now();
    let project_map = classify_and_summarize(&source_calls, date_range);
    prof_log("parallel post-processing", t_post);

    let t_finalize = Instant::now();
    let result = finalize_projects(project_map);
    prof_log("finalize/sort", t_finalize);

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
