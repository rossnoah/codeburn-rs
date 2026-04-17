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
use std::time::SystemTime;

use anyhow::Result;
use dashmap::DashSet;
use memchr::memmem;
use rayon::prelude::*;
use serde::Deserialize;

use crate::bash_utils::extract_bash_commands;
use crate::classifier::BASH_TOOLS;
use crate::models::calculate_cost;
use crate::providers::Provider;
use crate::types::{ParsedProviderCall, SessionSource, Speed, StatusAggregate};

pub struct ClaudeProvider;

fn get_claude_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("CLAUDE_CONFIG_DIR") {
        PathBuf::from(dir)
    } else {
        dirs::home_dir().unwrap_or_default().join(".claude")
    }
}

pub fn get_projects_dir() -> PathBuf {
    get_claude_dir().join("projects")
}

pub fn get_desktop_sessions_dir() -> PathBuf {
    let home = dirs::home_dir().unwrap_or_default();
    if cfg!(target_os = "macos") {
        home.join("Library/Application Support/Claude/local-agent-mode-sessions")
    } else if cfg!(target_os = "windows") {
        home.join("AppData/Roaming/Claude/local-agent-mode-sessions")
    } else {
        home.join(".config/Claude/local-agent-mode-sessions")
    }
}

fn find_desktop_project_dirs(base: &Path) -> Vec<PathBuf> {
    let mut results = Vec::new();
    fn walk(dir: &Path, depth: u32, results: &mut Vec<PathBuf>) {
        if depth > 8 {
            return;
        }
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str == "node_modules" || name_str == ".git" {
                continue;
            }
            let full = entry.path();
            if !full.is_dir() {
                continue;
            }
            if name_str == "projects" {
                if let Ok(project_dirs) = fs::read_dir(&full) {
                    for pd in project_dirs.flatten() {
                        if pd.path().is_dir() {
                            results.push(pd.path());
                        }
                    }
                }
            } else {
                walk(&full, depth + 1, results);
            }
        }
    }
    walk(base, 0, &mut results);
    results
}

#[derive(Deserialize)]
struct ApiUsage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cache_creation_input_tokens: Option<u64>,
    cache_read_input_tokens: Option<u64>,
    server_tool_use: Option<ServerToolUse>,
    speed: Option<String>,
}

#[derive(Deserialize)]
struct ServerToolUse {
    web_search_requests: Option<u64>,
}

// ── Status fast-path structs (skip content deserialization) ──

#[derive(Deserialize)]
struct StatusJournalEntry {
    #[serde(rename = "type")]
    entry_type: Option<String>,
    timestamp: Option<String>,
    message: Option<StatusMessage>,
}

#[derive(Deserialize)]
struct StatusMessage {
    model: Option<String>,
    id: Option<String>,
    usage: Option<ApiUsage>,
    // `content` is intentionally NOT deserialized — saves 40-80% of parse work
}

static ASSISTANT_NEEDLE: std::sync::LazyLock<memmem::Finder<'static>> =
    std::sync::LazyLock::new(|| memmem::Finder::new(b"\"type\":\"assistant\""));

static USER_NEEDLE: std::sync::LazyLock<memmem::Finder<'static>> =
    std::sync::LazyLock::new(|| memmem::Finder::new(b"\"type\":\"user\""));

fn hash_str(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

// ── TUI full-path structs (lightweight but includes tools/content names) ──

#[derive(Deserialize)]
struct TuiJournalEntry {
    timestamp: Option<String>,
    #[serde(rename = "sessionId")]
    session_id: Option<String>,
    message: Option<TuiMessage>,
}

#[derive(Deserialize)]
struct TuiMessage {
    model: Option<String>,
    id: Option<String>,
    usage: Option<ApiUsage>,
    content: Option<TuiContent>,
}

/// Claude JSONL stores `content` as EITHER a plain string (common for user
/// messages the user typed) OR an array of typed blocks (assistant messages,
/// or user messages with attachments). Both forms must deserialize or we
/// silently drop the entry's text — which breaks turn grouping and
/// classification downstream.
#[derive(Deserialize)]
#[serde(untagged)]
enum TuiContent {
    Text(String),
    Blocks(Vec<TuiContentBlock>),
}

#[derive(Deserialize)]
struct TuiContentBlock {
    #[serde(rename = "type")]
    block_type: Option<String>,
    name: Option<String>,
    text: Option<String>,
    input: Option<TuiToolInput>,
}

#[derive(Deserialize)]
struct TuiToolInput {
    command: Option<String>,
}

static TIMESTAMP_NEEDLE: std::sync::LazyLock<memmem::Finder<'static>> =
    std::sync::LazyLock::new(|| memmem::Finder::new(b"\"timestamp\":\""));

/// Extract timestamp from raw bytes without JSON parsing.
/// Looks for "timestamp":"YYYY-MM-DDThh:mm:ss" and returns the 19-char prefix.
fn extract_timestamp_fast(line: &[u8]) -> Option<&[u8]> {
    let pos = TIMESTAMP_NEEDLE.find(line)?;
    let ts_start = pos + 13; // length of "timestamp":"
    if ts_start + 19 <= line.len() {
        Some(&line[ts_start..ts_start + 19])
    } else {
        None
    }
}

/// 1BRC-style single-pass JSONL parser for the full TUI path.
/// Uses mmap + memchr line scanning + SIMD pre-filter + simd-json.
/// Directly produces ParsedProviderCall objects without intermediate structs.
/// Optional date_start/date_end for pre-filtering lines by timestamp BEFORE JSON parse.
fn parse_jsonl_mmap_full(
    file_bytes: &[u8],
    seen_ids: &DashSet<String>,
    date_start: Option<&[u8]>,
    date_end: Option<&[u8]>,
) -> Vec<ParsedProviderCall> {
    let mut results: Vec<ParsedProviderCall> = Vec::new();
    let mut current_user_message = String::new();
    let mut current_user_message_ts = String::new();
    let mut current_session_id = String::new();

    let mut start = 0;
    let len = file_bytes.len();

    while start < len {
        let end = memchr::memchr(b'\n', &file_bytes[start..])
            .map(|i| start + i)
            .unwrap_or(len);
        let line = &file_bytes[start..end];
        start = end + 1;

        if line.iter().all(|b| b.is_ascii_whitespace()) {
            continue;
        }

        // SIMD pre-filter: only parse user and assistant lines
        let is_assistant = ASSISTANT_NEEDLE.find(line).is_some();
        let is_user = !is_assistant && USER_NEEDLE.find(line).is_some();

        if !is_assistant && !is_user {
            continue;
        }

        // Date-range pre-filter: extract timestamp from raw bytes BEFORE JSON parse.
        // For assistant lines, skip entries clearly outside the date range.
        // Only check the DATE portion (first 10 bytes: YYYY-MM-DD) to avoid timezone issues.
        if is_assistant {
            if let Some(ts_bytes) = extract_timestamp_fast(line) {
                if let Some(ds) = date_start {
                    if ts_bytes.len() >= 10 && ds.len() >= 10 && &ts_bytes[..10] < &ds[..10] {
                        continue; // Definitely before date range start day
                    }
                }
                if let Some(de) = date_end {
                    if ts_bytes.len() >= 10 && de.len() >= 10 && &ts_bytes[..10] > &de[..10] {
                        continue; // Definitely after date range end day
                    }
                }
            }
        }

        let mut line_buf = line.to_vec();
        let entry: TuiJournalEntry = match simd_json::from_slice(&mut line_buf) {
            Ok(e) => e,
            Err(_) => continue,
        };

        if is_user {
            if let Some(msg) = &entry.message {
                // Content can be a plain string OR a blocks array — matches
                // JS `getUserMessageText` which handles both forms.
                let text = match &msg.content {
                    Some(TuiContent::Text(s)) => s.clone(),
                    Some(TuiContent::Blocks(blocks)) => blocks
                        .iter()
                        .filter(|b| b.block_type.as_deref() == Some("text"))
                        .filter_map(|b| b.text.as_deref())
                        .collect::<Vec<_>>()
                        .join(" "),
                    None => String::new(),
                };
                if !text.trim().is_empty() {
                    current_user_message = text;
                    // Capture the user-entry's own timestamp so downstream
                    // turn grouping can distinguish two distinct turns that
                    // share identical text.
                    current_user_message_ts = entry.timestamp.clone().unwrap_or_default();
                }
            }
            if let Some(sid) = &entry.session_id {
                if !sid.is_empty() {
                    current_session_id = sid.clone();
                }
            }
            continue;
        }

        // Assistant entry — extract API call data
        let msg = match &entry.message {
            Some(m) => m,
            None => continue,
        };

        // Dedup by message id
        if let Some(id) = msg.id.as_deref() {
            if !id.is_empty() {
                if seen_ids.contains(id) {
                    continue;
                }
                seen_ids.insert(id.to_string());
            }
        }

        let model = match msg.model.as_deref() {
            Some(m) => m,
            None => continue,
        };
        let usage = match &msg.usage {
            Some(u) => u,
            None => continue,
        };

        let input = usage.input_tokens.unwrap_or(0);
        let output = usage.output_tokens.unwrap_or(0);
        let cache_write = usage.cache_creation_input_tokens.unwrap_or(0);
        let cache_read = usage.cache_read_input_tokens.unwrap_or(0);
        let web_search = usage
            .server_tool_use
            .as_ref()
            .and_then(|s| s.web_search_requests)
            .unwrap_or(0);
        let speed = match usage.speed.as_deref() {
            Some("fast") => Speed::Fast,
            _ => Speed::Standard,
        };

        // Extract tool names and bash commands from content blocks.
        // Assistant messages always use the blocks form; treat the rare
        // string form as "no tools".
        let mut tools: Vec<String> = Vec::new();
        let mut bash_commands: Vec<String> = Vec::new();
        if let Some(TuiContent::Blocks(blocks)) = &msg.content {
            for block in blocks {
                if block.block_type.as_deref() == Some("tool_use") {
                    if let Some(name) = &block.name {
                        tools.push(name.clone());
                        if BASH_TOOLS.contains(name.as_str()) {
                            if let Some(input) = &block.input {
                                if let Some(cmd) = &input.command {
                                    bash_commands.extend(extract_bash_commands(cmd));
                                }
                            }
                        }
                    }
                }
            }
        }

        let cost = calculate_cost(model, input, output, cache_write, cache_read, web_search, speed);
        let timestamp = entry.timestamp.unwrap_or_default();
        let dedup_key = msg.id.clone().unwrap_or_else(|| format!("claude:{}", timestamp));

        results.push(ParsedProviderCall {
            provider: "claude".to_string(),
            model: model.to_string(),
            input_tokens: input,
            output_tokens: output,
            cache_creation_input_tokens: cache_write,
            cache_read_input_tokens: cache_read,
            cached_input_tokens: 0,
            reasoning_tokens: 0,
            web_search_requests: web_search,
            cost_usd: cost,
            tools,
            bash_commands,
            timestamp,
            speed,
            deduplication_key: dedup_key,
            user_message: current_user_message.clone(),
            session_id: current_session_id.clone(),
            user_message_timestamp: current_user_message_ts.clone(),
        });
    }

    results
}

fn parse_jsonl_file_status(
    file_bytes: &[u8],
    seen_ids: &DashSet<u64>,
    bounds: &crate::types::StatusBounds,
) -> (StatusAggregate, HashMap<String, (f64, u64)>) {
    let mut agg = StatusAggregate::default();
    let mut by_day: HashMap<String, (f64, u64)> = HashMap::new();

    let mut start = 0;
    let len = file_bytes.len();

    while start < len {
        let end = memchr::memchr(b'\n', &file_bytes[start..])
            .map(|i| start + i)
            .unwrap_or(len);

        let line = &file_bytes[start..end];
        start = end + 1;

        if line.iter().all(|b| b.is_ascii_whitespace()) {
            continue;
        }

        if ASSISTANT_NEEDLE.find(line).is_none() {
            continue;
        }

        let mut line_buf = line.to_vec();
        let entry: StatusJournalEntry = match simd_json::from_slice(&mut line_buf) {
            Ok(e) => e,
            Err(_) => continue,
        };

        if entry.entry_type.as_deref() != Some("assistant") {
            continue;
        }

        let msg = match &entry.message {
            Some(m) => m,
            None => continue,
        };

        if let Some(id) = msg.id.as_deref() {
            if !id.is_empty() {
                let h = hash_str(id);
                if seen_ids.contains(&h) {
                    continue;
                }
                seen_ids.insert(h);
            }
        }

        let model = match msg.model.as_deref() {
            Some(m) => m,
            None => continue,
        };
        let usage = match &msg.usage {
            Some(u) => u,
            None => continue,
        };

        let input = usage.input_tokens.unwrap_or(0);
        let output = usage.output_tokens.unwrap_or(0);
        let cache_write = usage.cache_creation_input_tokens.unwrap_or(0);
        let cache_read = usage.cache_read_input_tokens.unwrap_or(0);
        let web_search = usage
            .server_tool_use
            .as_ref()
            .and_then(|s| s.web_search_requests)
            .unwrap_or(0);
        let speed = match usage.speed.as_deref() {
            Some("fast") => Speed::Fast,
            _ => Speed::Standard,
        };

        let cost = calculate_cost(model, input, output, cache_write, cache_read, web_search, speed);

        // Compare full UTC timestamp against UTC boundaries
        let ts = match entry.timestamp.as_deref() {
            Some(t) if t.len() >= 19 => t,
            _ => continue,
        };
        let ts_prefix = &ts[..19];

        // Accumulate into per-day map (UTC day, for caching)
        let day = &ts[..10];
        let entry_ref = by_day.entry(day.to_string()).or_insert((0.0, 0));
        entry_ref.0 += cost;
        entry_ref.1 += 1;

        // Accumulate into aggregates using full UTC timestamp comparison
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

    (agg, by_day)
}

fn collect_jsonl_files_with_mtime(dir_path: &Path, since: Option<SystemTime>) -> Vec<PathBuf> {
    let entries = match fs::read_dir(dir_path) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };

    let mut jsonl_files = Vec::new();
    let mut dirs_to_check = Vec::new();

    for entry in entries.flatten() {
        let name = entry.file_name();
        let bytes = name.as_encoded_bytes();

        if bytes.ends_with(b".jsonl") {
            let path = entry.path();
            if let Some(since_time) = since {
                if let Ok(meta) = fs::metadata(&path) {
                    if let Ok(mtime) = meta.modified() {
                        if mtime < since_time {
                            continue; // Skip files not modified since date range start
                        }
                    }
                }
            }
            jsonl_files.push(path);
        } else {
            // Use readdir's d_type rather than a fresh stat via
            // `path.is_dir()` — saves ~1 µs per non-jsonl entry.
            match entry.file_type() {
                Ok(t) if t.is_dir() => dirs_to_check.push(entry.path()),
                _ => {}
            }
        }
    }

    for dir in dirs_to_check {
        let subagents_path = dir.join("subagents");
        if let Ok(sub_entries) = fs::read_dir(&subagents_path) {
            for sub_entry in sub_entries.flatten() {
                let sub_name = sub_entry.file_name();
                if sub_name.as_encoded_bytes().ends_with(b".jsonl") {
                    let sub_path = sub_entry.path();
                    if let Some(since_time) = since {
                        if let Ok(meta) = fs::metadata(&sub_path) {
                            if let Ok(mtime) = meta.modified() {
                                if mtime < since_time {
                                    continue;
                                }
                            }
                        }
                    }
                    jsonl_files.push(sub_path);
                }
            }
        }
    }

    jsonl_files
}

impl Provider for ClaudeProvider {
    fn name(&self) -> &str {
        "claude"
    }

    fn discovery_fingerprint(&self) -> Vec<(String, u64)> {
        // Stat the projects root + every project subdir + the desktop
        // sessions dir. Any new jsonl file bumps its containing subdir's
        // mtime; any new project bumps the projects root's mtime. Per-file
        // mtime changes (append) don't bump these, which is exactly what
        // we want — partition_cache handles those.
        //
        // Stats on 100+ dirs are the bulk of the fingerprint cost, so the
        // per-subdir stat fans out via rayon.
        let projects_dir = get_projects_dir();
        let mut subdirs: Vec<PathBuf> = Vec::with_capacity(128);
        if let Ok(entries) = fs::read_dir(&projects_dir) {
            for entry in entries.flatten() {
                match entry.file_type() {
                    Ok(t) if t.is_dir() => subdirs.push(entry.path()),
                    _ => {}
                }
            }
        }
        let desktop = get_desktop_sessions_dir();
        let desktop_exists = desktop.exists();
        let mut desktop_dirs: Vec<PathBuf> = Vec::new();
        if desktop_exists {
            desktop_dirs = find_desktop_project_dirs(&desktop);
        }

        let mut all_paths: Vec<PathBuf> = Vec::with_capacity(subdirs.len() + desktop_dirs.len() + 2);
        all_paths.push(projects_dir);
        if desktop_exists {
            all_paths.push(desktop);
        }
        all_paths.extend(subdirs);
        all_paths.extend(desktop_dirs);

        all_paths
            .par_iter()
            .map(|p| {
                let m = crate::discovery_cache::mtime_secs(p);
                (p.to_string_lossy().to_string(), m)
            })
            .collect()
    }

    fn discover_sessions(&self) -> Result<Vec<SessionSource>> {
        // One SessionSource per jsonl file. This makes the disk cache
        // granular: a single project dir with 50 jsonl files produces 50
        // cache entries, each invalidated independently when its own mtime
        // changes. We enumerate project dirs serially (small number) but
        // walk each project's jsonl files in parallel across rayon — the
        // per-project walks are independent and the read_dir+subagents
        // recursion is disk-bound, so overlapping them helps.
        let mut project_dirs: Vec<(std::path::PathBuf, String)> = Vec::new();

        let projects_dir = get_projects_dir();
        if let Ok(entries) = fs::read_dir(&projects_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                let project_name = entry.file_name().to_string_lossy().to_string();
                project_dirs.push((path, project_name));
            }
        }

        for dir_path in find_desktop_project_dirs(&get_desktop_sessions_dir()) {
            let project_name = dir_path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            project_dirs.push((dir_path, project_name));
        }

        let sources: Vec<SessionSource> = project_dirs
            .par_iter()
            .flat_map(|(path, project_name)| {
                collect_jsonl_files_with_mtime(path, None)
                    .into_iter()
                    .map(|f| SessionSource {
                        path: f.to_string_lossy().to_string(),
                        project: project_name.clone(),
                        provider: "claude".to_string(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect();

        Ok(sources)
    }

    fn parse_session(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<String>,
    ) -> Result<Vec<ParsedProviderCall>> {
        self.parse_session_filtered(source, seen_keys, None, None, None)
    }

    fn parse_session_filtered(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<String>,
        since: Option<SystemTime>,
        date_start: Option<&str>,
        date_end: Option<&str>,
    ) -> Result<Vec<ParsedProviderCall>> {
        // Post-refactor: `source.path` is a single jsonl file.
        let path = Path::new(&source.path);

        if let Some(since_t) = since {
            if let Ok(meta) = fs::metadata(path) {
                if let Ok(mtime) = meta.modified() {
                    if mtime < since_t {
                        return Ok(Vec::new());
                    }
                }
            }
        }

        let file = match fs::File::open(path) {
            Ok(f) => f,
            Err(_) => return Ok(Vec::new()),
        };
        let mmap = match unsafe { memmap2::Mmap::map(&file) } {
            Ok(m) => m,
            Err(_) => return Ok(Vec::new()),
        };
        if mmap.is_empty() {
            return Ok(Vec::new());
        }

        let ds_bytes: Option<Vec<u8>> = date_start.map(|s| s.as_bytes().to_vec());
        let de_bytes: Option<Vec<u8>> = date_end.map(|s| s.as_bytes().to_vec());
        let file_session_id = path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();

        let mut calls = parse_jsonl_mmap_full(
            &mmap,
            seen_keys,
            ds_bytes.as_deref(),
            de_bytes.as_deref(),
        );
        for call in &mut calls {
            call.session_id = file_session_id.clone();
        }
        Ok(calls)
    }

    fn parse_session_status(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<u64>,
        bounds: &crate::types::StatusBounds,
    ) -> Result<(StatusAggregate, HashMap<String, (f64, u64)>)> {
        // `source.path` is a single jsonl file (post per-file refactor). Apply
        // the month-start mtime filter, cache lookup, then parse if miss.
        let path = Path::new(&source.path);

        let month_prefix = &bounds.month_start[..7]; // "YYYY-MM"
        let since = month_start_as_system_time(month_prefix);

        let meta = match fs::metadata(path) {
            Ok(m) => m,
            Err(_) => return Ok((StatusAggregate::default(), HashMap::new())),
        };
        if let Some(since_t) = since {
            if let Ok(mtime) = meta.modified() {
                if mtime < since_t {
                    return Ok((StatusAggregate::default(), HashMap::new()));
                }
            }
        }

        let mtime_secs = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let file_size = meta.len();
        let file_path_str = path.to_string_lossy().to_string();

        // Batch-load the entire status cache on first access; subsequent
        // calls reuse the in-memory map.
        static CACHE_SNAPSHOT: std::sync::LazyLock<
            HashMap<String, crate::session_cache::CachedFileEntry>,
        > = std::sync::LazyLock::new(crate::session_cache::load_all_cached);

        if !crate::parser::is_cache_bypassed() {
            if let Some(entry) = CACHE_SNAPSHOT.get(&file_path_str) {
                if entry.mtime_secs == mtime_secs
                    && entry.file_size == file_size
                    && !crate::session_cache::has_boundary_day(entry, bounds)
                {
                    let agg = crate::session_cache::aggregate_cached_entry(entry, bounds);
                    return Ok((agg, HashMap::new()));
                }
            }
        }

        let file = match fs::File::open(path) {
            Ok(f) => f,
            Err(_) => return Ok((StatusAggregate::default(), HashMap::new())),
        };
        let mmap = match unsafe { memmap2::Mmap::map(&file) } {
            Ok(m) => m,
            Err(_) => return Ok((StatusAggregate::default(), HashMap::new())),
        };
        if mmap.is_empty() {
            return Ok((StatusAggregate::default(), HashMap::new()));
        }

        let (agg, by_day) = parse_jsonl_file_status(&mmap, seen_keys, bounds);

        if !by_day.is_empty() && !crate::parser::is_cache_bypassed() {
            crate::session_cache::store_batch(&[(
                file_path_str,
                mtime_secs,
                file_size,
                by_day.clone(),
            )]);
        }

        Ok((agg, by_day))
    }
}

fn month_start_as_system_time(month_prefix: &str) -> Option<SystemTime> {
    // month_prefix is "YYYY-MM"
    let date_str = format!("{}-01T00:00:00Z", month_prefix);
    let dt = chrono::DateTime::parse_from_rfc3339(&date_str).ok()?;
    let secs = dt.timestamp();
    if secs < 0 {
        return None;
    }
    Some(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(secs as u64))
}
