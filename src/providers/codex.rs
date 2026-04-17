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
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use anyhow::Result;
use dashmap::DashSet;
use serde::Deserialize;

use crate::models::calculate_cost;
use crate::providers::Provider;
use crate::types::{ParsedProviderCall, SessionSource, Speed, StatusAggregate};

pub struct CodexProvider;

static TOOL_NAME_MAP: &[(&str, &str)] = &[
    ("exec_command", "Bash"),
    ("read_file", "Read"),
    ("write_file", "Edit"),
    ("apply_diff", "Edit"),
    ("apply_patch", "Edit"),
    ("spawn_agent", "Agent"),
    ("close_agent", "Agent"),
    ("wait_agent", "Agent"),
    ("read_dir", "Glob"),
];

pub fn get_codex_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("CODEX_HOME") {
        PathBuf::from(dir)
    } else {
        dirs::home_dir().unwrap_or_default().join(".codex")
    }
}

fn sanitize_project(cwd: &str) -> String {
    cwd.strip_prefix('/').unwrap_or(cwd).replace('/', "-")
}

#[derive(Deserialize)]
struct CodexEntry {
    #[serde(rename = "type")]
    entry_type: Option<String>,
    timestamp: Option<String>,
    payload: Option<CodexPayload>,
}

#[derive(Deserialize)]
struct CodexPayload {
    #[serde(rename = "type")]
    payload_type: Option<String>,
    role: Option<String>,
    cwd: Option<String>,
    originator: Option<String>,
    session_id: Option<String>,
    model: Option<String>,
    name: Option<String>,
    content: Option<Vec<ContentItem>>,
    info: Option<CodexInfo>,
}

#[derive(Deserialize)]
struct ContentItem {
    #[serde(rename = "type")]
    content_type: Option<String>,
    text: Option<String>,
}

#[derive(Deserialize)]
struct CodexInfo {
    model: Option<String>,
    model_name: Option<String>,
    last_token_usage: Option<CodexTokenUsage>,
    total_token_usage: Option<CodexTokenUsage>,
}

#[derive(Deserialize)]
struct CodexTokenUsage {
    input_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    reasoning_output_tokens: Option<u64>,
    total_tokens: Option<u64>,
}

fn is_valid_codex_session(file_path: &Path) -> Option<CodexEntry> {
    // Only the first line is needed to check session_meta + originator.
    // Reading the entire file (rollouts can be many MB) just to slice off
    // the first line wastes significant I/O during discovery.
    use std::io::{BufRead, BufReader};
    let file = fs::File::open(file_path).ok()?;
    let mut reader = BufReader::new(file);
    let mut first_line = String::new();
    reader.read_line(&mut first_line).ok()?;
    if first_line.is_empty() {
        return None;
    }
    let entry: CodexEntry = serde_json::from_str(first_line.trim_end()).ok()?;
    if entry.entry_type.as_deref() != Some("session_meta") {
        return None;
    }
    let originator = entry.payload.as_ref()?.originator.as_deref()?;
    if originator.to_lowercase().starts_with("codex") {
        Some(entry)
    } else {
        None
    }
}

fn discover_sessions_in_dir(
    codex_dir: &Path,
    known: Option<&std::collections::HashMap<String, String>>,
) -> Vec<SessionSource> {
    use rayon::prelude::*;
    let sessions_dir = codex_dir.join("sessions");

    // Gather the day-level directories serially (YYYY/MM/DD is only ~30-60
    // dirs on even heavy codex users), then par_iter over the days so the
    // per-day read_dir + file filtering happens in parallel — the old code
    // walked every day serially.
    let mut day_dirs: Vec<PathBuf> = Vec::new();
    let years = match fs::read_dir(&sessions_dir) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };
    for year_entry in years.flatten() {
        // OsStr::len counts bytes without allocating.
        if year_entry.file_name().len() != 4 {
            continue;
        }
        let months = match fs::read_dir(year_entry.path()) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for month_entry in months.flatten() {
            if month_entry.file_name().len() != 2 {
                continue;
            }
            let days = match fs::read_dir(month_entry.path()) {
                Ok(e) => e,
                Err(_) => continue,
            };
            for day_entry in days.flatten() {
                if day_entry.file_name().len() != 2 {
                    continue;
                }
                // Use the readdir d_type result rather than a fresh stat.
                if day_entry
                    .file_type()
                    .map_or(true, |t| !t.is_dir())
                {
                    continue;
                }
                day_dirs.push(day_entry.path());
            }
        }
    }

    let candidates: Vec<PathBuf> = day_dirs
        .par_iter()
        .flat_map_iter(|day_path| {
            let entries = match fs::read_dir(day_path) {
                Ok(e) => e,
                Err(_) => return Vec::new().into_iter(),
            };
            let mut out = Vec::new();
            for file_entry in entries.flatten() {
                let fname = file_entry.file_name();
                let bytes = fname.as_encoded_bytes();
                // Filename format: "rollout-<uuid>.jsonl". Skip anything that
                // doesn't match without allocating.
                if !bytes.starts_with(b"rollout-") || !bytes.ends_with(b".jsonl") {
                    continue;
                }
                // readdir d_type: skip stat unless the filesystem didn't
                // return a type (rare; we fall through in that case).
                match file_entry.file_type() {
                    Ok(t) if !t.is_file() => continue,
                    _ => {}
                }
                out.push(file_entry.path());
            }
            out.into_iter()
        })
        .collect();

    candidates
        .par_iter()
        .filter_map(|path| {
            let path_str = path.to_string_lossy().to_string();
            // Cache-fast-path: if the caller handed us this file's project
            // from the last run's cache, trust it. Saves ~60 µs per file
            // (first-line read + JSON parse) × 249 files ≈ 15 ms.
            if let Some(hints) = known {
                if let Some(project) = hints.get(&path_str) {
                    return Some(SessionSource {
                        path: path_str,
                        project: project.clone(),
                        provider: "codex".to_string(),
                    });
                }
            }
            let meta = is_valid_codex_session(path)?;
            let cwd = meta
                .payload
                .as_ref()
                .and_then(|p| p.cwd.as_deref())
                .unwrap_or("unknown");
            Some(SessionSource {
                path: path_str,
                project: sanitize_project(cwd),
                provider: "codex".to_string(),
            })
        })
        .collect()
}

fn resolve_model(payload: Option<&CodexPayload>, session_model: Option<&str>) -> String {
    if let Some(p) = payload {
        if let Some(info) = &p.info {
            if let Some(m) = &info.model {
                return m.clone();
            }
            if let Some(m) = &info.model_name {
                return m.clone();
            }
        }
    }
    session_model
        .unwrap_or("gpt-5")
        .to_string()
}

fn parse_codex_file(
    source: &SessionSource,
    seen_keys: &DashSet<String>,
) -> Vec<ParsedProviderCall> {
    let file = match fs::File::open(&source.path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };
    let mmap = match unsafe { memmap2::Mmap::map(&file) } {
        Ok(m) => m,
        Err(_) => return Vec::new(),
    };
    if mmap.is_empty() {
        return Vec::new();
    }

    let map = crate::providers::common::build_tool_map(TOOL_NAME_MAP);
    let mut results = Vec::new();
    let mut session_model: Option<String> = None;
    let mut session_id = String::new();
    let mut prev_cumulative_total: u64 = 0;
    let mut prev_input: u64 = 0;
    let mut prev_cached: u64 = 0;
    let mut prev_output: u64 = 0;
    let mut prev_reasoning: u64 = 0;
    let mut pending_tools: Vec<String> = Vec::new();
    let mut pending_user_message = String::new();

    let mut start = 0;
    let len = mmap.len();
    while start < len {
        let end = memchr::memchr(b'\n', &mmap[start..])
            .map(|i| start + i)
            .unwrap_or(len);
        let line_bytes = &mmap[start..end];
        start = end + 1;

        if line_bytes.iter().all(|b| b.is_ascii_whitespace()) {
            continue;
        }
        let mut line_buf = line_bytes.to_vec();
        let entry: CodexEntry = match simd_json::serde::from_slice(&mut line_buf) {
            Ok(e) => e,
            Err(_) => continue,
        };

        match entry.entry_type.as_deref() {
            Some("session_meta") => {
                if let Some(payload) = &entry.payload {
                    session_id = payload
                        .session_id
                        .clone()
                        .unwrap_or_else(|| {
                            Path::new(&source.path)
                                .file_stem()
                                .map(|s| s.to_string_lossy().to_string())
                                .unwrap_or_default()
                        });
                    session_model = payload.model.clone();
                }
            }
            Some("response_item") => {
                if let Some(payload) = &entry.payload {
                    if payload.payload_type.as_deref() == Some("function_call") {
                        let raw_name = payload.name.as_deref().unwrap_or("");
                        let mapped = map.get(raw_name).copied().unwrap_or(raw_name);
                        pending_tools.push(mapped.to_string());
                    } else if payload.payload_type.as_deref() == Some("message")
                        && payload.role.as_deref() == Some("user")
                    {
                        if let Some(content_items) = &payload.content {
                            let texts: Vec<&str> = content_items
                                .iter()
                                .filter(|c| c.content_type.as_deref() == Some("input_text"))
                                .filter_map(|c| c.text.as_deref())
                                .filter(|t| !t.is_empty())
                                .collect();
                            if !texts.is_empty() {
                                pending_user_message = texts.join(" ");
                            }
                        }
                    }
                }
            }
            Some("event_msg") => {
                let payload = match &entry.payload {
                    Some(p) if p.payload_type.as_deref() == Some("token_count") => p,
                    _ => continue,
                };
                let info = match &payload.info {
                    Some(i) => i,
                    None => continue,
                };

                let cumulative_total = info
                    .total_token_usage
                    .as_ref()
                    .and_then(|t| t.total_tokens)
                    .unwrap_or(0);
                if cumulative_total > 0 && cumulative_total == prev_cumulative_total {
                    continue;
                }
                prev_cumulative_total = cumulative_total;

                let (input_tokens, cached_input_tokens, output_tokens, reasoning_tokens) =
                    if let Some(last) = &info.last_token_usage {
                        (
                            last.input_tokens.unwrap_or(0),
                            last.cached_input_tokens.unwrap_or(0),
                            last.output_tokens.unwrap_or(0),
                            last.reasoning_output_tokens.unwrap_or(0),
                        )
                    } else if cumulative_total > 0 {
                        let total = match &info.total_token_usage {
                            Some(t) => t,
                            None => continue,
                        };
                        let it = total.input_tokens.unwrap_or(0).saturating_sub(prev_input);
                        let ct = total.cached_input_tokens.unwrap_or(0).saturating_sub(prev_cached);
                        let ot = total.output_tokens.unwrap_or(0).saturating_sub(prev_output);
                        let rt = total
                            .reasoning_output_tokens
                            .unwrap_or(0)
                            .saturating_sub(prev_reasoning);
                        (it, ct, ot, rt)
                    } else {
                        continue;
                    };

                if info.last_token_usage.is_none() {
                    if let Some(total) = &info.total_token_usage {
                        prev_input = total.input_tokens.unwrap_or(0);
                        prev_cached = total.cached_input_tokens.unwrap_or(0);
                        prev_output = total.output_tokens.unwrap_or(0);
                        prev_reasoning = total.reasoning_output_tokens.unwrap_or(0);
                    }
                }

                let total_tokens =
                    input_tokens + cached_input_tokens + output_tokens + reasoning_tokens;
                if total_tokens == 0 {
                    continue;
                }

                let uncached_input = input_tokens.saturating_sub(cached_input_tokens);
                let model = resolve_model(Some(payload), session_model.as_deref());
                let timestamp = entry.timestamp.clone().unwrap_or_default();
                let dedup_key =
                    format!("codex:{}:{}:{}", source.path, timestamp, cumulative_total);

                if seen_keys.contains(&dedup_key) {
                    continue;
                }
                seen_keys.insert(dedup_key.clone());

                let cost_usd = calculate_cost(
                    &model,
                    uncached_input,
                    output_tokens + reasoning_tokens,
                    0,
                    cached_input_tokens,
                    0,
                    Speed::Standard,
                );

                results.push(ParsedProviderCall {
                    provider: "codex".to_string(),
                    model,
                    input_tokens: uncached_input,
                    output_tokens,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: cached_input_tokens,
                    cached_input_tokens,
                    reasoning_tokens,
                    web_search_requests: 0,
                    cost_usd,
                    tools: std::mem::take(&mut pending_tools),
                    bash_commands: Vec::new(),
                    timestamp,
                    speed: Speed::Standard,
                    deduplication_key: dedup_key,
                    user_message: std::mem::replace(&mut pending_user_message, String::new()),
                    session_id: session_id.clone(),
                    user_message_timestamp: String::new(),
                });
            }
            _ => {}
        }
    }

    results
}

impl Provider for CodexProvider {
    fn name(&self) -> &str {
        "codex"
    }

    fn discovery_fingerprint(&self) -> Vec<(String, u64)> {
        // Stat every day-level dir under sessions/YYYY/MM/DD. New rollout
        // files bump the day dir's mtime. ~30-60 stats for a heavy user.
        let sessions_dir = get_codex_dir().join("sessions");
        let mut out: Vec<(String, u64)> = Vec::with_capacity(64);
        out.push((
            sessions_dir.to_string_lossy().to_string(),
            crate::discovery_cache::mtime_secs(&sessions_dir),
        ));
        let years = match fs::read_dir(&sessions_dir) {
            Ok(e) => e,
            Err(_) => return out,
        };
        for year_entry in years.flatten() {
            if year_entry.file_name().len() != 4 {
                continue;
            }
            let year_path = year_entry.path();
            out.push((
                year_path.to_string_lossy().to_string(),
                crate::discovery_cache::mtime_secs(&year_path),
            ));
            let months = match fs::read_dir(&year_path) {
                Ok(e) => e,
                Err(_) => continue,
            };
            for month_entry in months.flatten() {
                if month_entry.file_name().len() != 2 {
                    continue;
                }
                let month_path = month_entry.path();
                out.push((
                    month_path.to_string_lossy().to_string(),
                    crate::discovery_cache::mtime_secs(&month_path),
                ));
                let days = match fs::read_dir(&month_path) {
                    Ok(e) => e,
                    Err(_) => continue,
                };
                for day_entry in days.flatten() {
                    if day_entry.file_name().len() != 2 {
                        continue;
                    }
                    let day_path = day_entry.path();
                    out.push((
                        day_path.to_string_lossy().to_string(),
                        crate::discovery_cache::mtime_secs(&day_path),
                    ));
                }
            }
        }
        out
    }

    fn discover_sessions(&self) -> Result<Vec<SessionSource>> {
        Ok(discover_sessions_in_dir(&get_codex_dir(), None))
    }

    fn discover_sessions_with_hints(
        &self,
        known: Option<&std::collections::HashMap<String, String>>,
    ) -> Result<Vec<SessionSource>> {
        Ok(discover_sessions_in_dir(&get_codex_dir(), known))
    }

    fn parse_session(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<String>,
    ) -> Result<Vec<ParsedProviderCall>> {
        Ok(parse_codex_file(source, seen_keys))
    }

    fn parse_session_filtered(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<String>,
        since: Option<std::time::SystemTime>,
        _date_start: Option<&str>,
        _date_end: Option<&str>,
    ) -> Result<Vec<ParsedProviderCall>> {
        // Cheap mtime gate: rollouts are append-only files. If the whole
        // file is older than the requested date range start, no row inside
        // can be in range. Skip without opening — saves the mmap + line
        // scan for the bulk of historical rollouts on a "today" or "week"
        // query.
        if let Some(since_t) = since {
            if let Ok(meta) = fs::metadata(&source.path) {
                if let Ok(mtime) = meta.modified() {
                    if mtime < since_t {
                        return Ok(Vec::new());
                    }
                }
            }
        }
        Ok(parse_codex_file(source, seen_keys))
    }

    fn parse_session_status(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<u64>,
        bounds: &crate::types::StatusBounds,
    ) -> Result<(StatusAggregate, HashMap<String, (f64, u64)>)> {
        let content = match fs::read_to_string(&source.path) {
            Ok(c) => c,
            Err(_) => return Ok((StatusAggregate::default(), HashMap::new())),
        };

        let mut agg = StatusAggregate::default();
        let mut by_day: HashMap<String, (f64, u64)> = HashMap::new();
        let mut session_model: Option<String> = None;
        let mut prev_cumulative_total: u64 = 0;
        let mut prev_input: u64 = 0;
        let mut prev_cached: u64 = 0;
        let mut prev_output: u64 = 0;
        let mut prev_reasoning: u64 = 0;

        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let entry: CodexEntry = match serde_json::from_str(line) {
                Ok(e) => e,
                Err(_) => continue,
            };

            match entry.entry_type.as_deref() {
                Some("session_meta") => {
                    if let Some(payload) = &entry.payload {
                        session_model = payload.model.clone();
                    }
                }
                Some("event_msg") => {
                    let payload = match &entry.payload {
                        Some(p) if p.payload_type.as_deref() == Some("token_count") => p,
                        _ => continue,
                    };
                    let info = match &payload.info {
                        Some(i) => i,
                        None => continue,
                    };

                    let cumulative_total = info
                        .total_token_usage
                        .as_ref()
                        .and_then(|t| t.total_tokens)
                        .unwrap_or(0);
                    if cumulative_total > 0 && cumulative_total == prev_cumulative_total {
                        continue;
                    }
                    prev_cumulative_total = cumulative_total;

                    let (input_tokens, cached_input_tokens, output_tokens, reasoning_tokens) =
                        if let Some(last) = &info.last_token_usage {
                            (
                                last.input_tokens.unwrap_or(0),
                                last.cached_input_tokens.unwrap_or(0),
                                last.output_tokens.unwrap_or(0),
                                last.reasoning_output_tokens.unwrap_or(0),
                            )
                        } else if cumulative_total > 0 {
                            let total = match &info.total_token_usage {
                                Some(t) => t,
                                None => continue,
                            };
                            let it = total.input_tokens.unwrap_or(0).saturating_sub(prev_input);
                            let ct = total.cached_input_tokens.unwrap_or(0).saturating_sub(prev_cached);
                            let ot = total.output_tokens.unwrap_or(0).saturating_sub(prev_output);
                            let rt = total.reasoning_output_tokens.unwrap_or(0).saturating_sub(prev_reasoning);
                            (it, ct, ot, rt)
                        } else {
                            continue;
                        };

                    if info.last_token_usage.is_none() {
                        if let Some(total) = &info.total_token_usage {
                            prev_input = total.input_tokens.unwrap_or(0);
                            prev_cached = total.cached_input_tokens.unwrap_or(0);
                            prev_output = total.output_tokens.unwrap_or(0);
                            prev_reasoning = total.reasoning_output_tokens.unwrap_or(0);
                        }
                    }

                    let total_tokens = input_tokens + cached_input_tokens + output_tokens + reasoning_tokens;
                    if total_tokens == 0 {
                        continue;
                    }

                    let uncached_input = input_tokens.saturating_sub(cached_input_tokens);
                    let model = resolve_model(Some(payload), session_model.as_deref());
                    let timestamp = entry.timestamp.clone().unwrap_or_default();
                    let dedup_key = format!("codex:{}:{}:{}", source.path, timestamp, cumulative_total);

                    let h = {
                        let mut hasher = DefaultHasher::new();
                        dedup_key.hash(&mut hasher);
                        hasher.finish()
                    };
                    if seen_keys.contains(&h) {
                        continue;
                    }
                    seen_keys.insert(h);

                    let cost = calculate_cost(
                        &model,
                        uncached_input,
                        output_tokens + reasoning_tokens,
                        0,
                        cached_input_tokens,
                        0,
                        Speed::Standard,
                    );

                    if timestamp.len() >= 19 {
                        let ts_prefix = &timestamp[..19];
                        let day = &timestamp[..10];
                        let e = by_day.entry(day.to_string()).or_insert((0.0, 0));
                        e.0 += cost;
                        e.1 += 1;

                        if ts_prefix >= bounds.today_start.as_str() && ts_prefix <= bounds.today_end.as_str() {
                            agg.today_cost += cost;
                            agg.today_calls += 1;
                        }
                        if ts_prefix >= bounds.week_start.as_str() && ts_prefix <= bounds.month_end.as_str() {
                            agg.week_cost += cost;
                            agg.week_calls += 1;
                        }
                        if ts_prefix >= bounds.month_start.as_str() && ts_prefix <= bounds.month_end.as_str() {
                            agg.month_cost += cost;
                            agg.month_calls += 1;
                        }
                    }
                }
                _ => {}
            }
        }

        Ok((agg, by_day))
    }
}
