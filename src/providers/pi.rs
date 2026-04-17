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
use serde::Deserialize;

use crate::bash_utils::extract_bash_commands;
use crate::models::calculate_cost;
use crate::providers::Provider;
use crate::types::{ParsedProviderCall, SessionSource, Speed, StatusAggregate, StatusBounds};

pub struct PiProvider;

static TOOL_NAME_MAP: &[(&str, &str)] = &[
    ("bash", "Bash"),
    ("read", "Read"),
    ("edit", "Edit"),
    ("write", "Write"),
    ("glob", "Glob"),
    ("grep", "Grep"),
    ("task", "Agent"),
    ("dispatch_agent", "Agent"),
    ("fetch", "WebFetch"),
    ("search", "WebSearch"),
    ("todo", "TodoWrite"),
    ("patch", "Patch"),
];

fn tool_name(raw: &str) -> String {
    crate::providers::common::lookup_tool(TOOL_NAME_MAP, raw)
}

pub fn get_pi_sessions_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join(".pi/agent/sessions")
}

#[derive(Deserialize)]
struct PiEntry {
    #[serde(rename = "type")]
    entry_type: Option<String>,
    id: Option<String>,
    timestamp: Option<String>,
    cwd: Option<String>,
    message: Option<PiMessage>,
}

#[derive(Deserialize)]
struct PiMessage {
    role: Option<String>,
    content: Option<Vec<PiContent>>,
    model: Option<String>,
    #[serde(rename = "responseId")]
    response_id: Option<String>,
    usage: Option<PiUsage>,
}

#[derive(Deserialize)]
struct PiContent {
    #[serde(rename = "type")]
    content_type: Option<String>,
    text: Option<String>,
    name: Option<String>,
    arguments: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct PiUsage {
    input: u64,
    output: u64,
    #[serde(rename = "cacheRead")]
    cache_read: u64,
    #[serde(rename = "cacheWrite")]
    cache_write: u64,
}

fn read_first_entry(path: &Path) -> Option<PiEntry> {
    let content = fs::read_to_string(path).ok()?;
    let first_line = content.lines().find(|l| !l.trim().is_empty())?;
    serde_json::from_str(first_line).ok()
}

fn basename_str(s: &str) -> String {
    Path::new(s)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| s.to_string())
}

fn parse_pi_file(
    path: &Path,
    seen_keys: &DashSet<String>,
) -> Vec<ParsedProviderCall> {
    let content = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    let mut results = Vec::new();
    let mut session_id = Path::new(path)
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let mut pending_user = String::new();
    let path_str = path.to_string_lossy().to_string();

    for (idx, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let entry: PiEntry = match serde_json::from_str(line) {
            Ok(e) => e,
            Err(_) => continue,
        };

        let et = entry.entry_type.as_deref().unwrap_or("");
        if et == "session" {
            if let Some(id) = entry.id {
                session_id = id;
            }
            continue;
        }
        if et != "message" {
            continue;
        }
        let msg = match entry.message {
            Some(m) => m,
            None => continue,
        };

        let role = msg.role.as_deref().unwrap_or("");
        if role == "user" {
            if let Some(content) = &msg.content {
                let texts: Vec<&str> = content
                    .iter()
                    .filter(|c| c.content_type.as_deref() == Some("text"))
                    .filter_map(|c| c.text.as_deref())
                    .filter(|t| !t.is_empty())
                    .collect();
                if !texts.is_empty() {
                    pending_user = texts.join(" ");
                }
            }
            continue;
        }
        if role != "assistant" {
            continue;
        }
        let usage = match msg.usage {
            Some(u) => u,
            None => continue,
        };
        if usage.input == 0 && usage.output == 0 {
            continue;
        }

        let model = msg.model.unwrap_or_else(|| "gpt-5".to_string());
        let response_id = msg.response_id.unwrap_or_default();
        let dedup_key = format!(
            "pi:{}:{}",
            path_str,
            if !response_id.is_empty() {
                response_id.clone()
            } else if entry.id.as_deref().is_some() {
                entry.id.clone().unwrap()
            } else if entry.timestamp.as_deref().is_some() {
                entry.timestamp.clone().unwrap()
            } else {
                idx.to_string()
            }
        );
        if seen_keys.contains(&dedup_key) {
            continue;
        }
        seen_keys.insert(dedup_key.clone());

        let mut tools: Vec<String> = Vec::new();
        let mut bash_commands: Vec<String> = Vec::new();
        if let Some(content) = &msg.content {
            for c in content {
                if c.content_type.as_deref() == Some("toolCall") {
                    if let Some(name) = &c.name {
                        tools.push(tool_name(name));
                        if name == "bash" {
                            if let Some(args) = &c.arguments {
                                if let Some(cmd) = args.get("command").and_then(|v| v.as_str()) {
                                    bash_commands.extend(extract_bash_commands(cmd));
                                }
                            }
                        }
                    }
                }
            }
        }

        let cost = calculate_cost(
            &model,
            usage.input,
            usage.output,
            usage.cache_write,
            usage.cache_read,
            0,
            Speed::Standard,
        );
        let timestamp = entry.timestamp.unwrap_or_default();

        results.push(ParsedProviderCall {
            provider: "pi".to_string(),
            model,
            input_tokens: usage.input,
            output_tokens: usage.output,
            cache_creation_input_tokens: usage.cache_write,
            cache_read_input_tokens: usage.cache_read,
            cached_input_tokens: usage.cache_read,
            reasoning_tokens: 0,
            web_search_requests: 0,
            cost_usd: cost,
            tools,
            bash_commands,
            timestamp,
            speed: Speed::Standard,
            deduplication_key: dedup_key,
            user_message: std::mem::take(&mut pending_user),
            session_id: session_id.clone(),
            user_message_timestamp: String::new(),
        });
    }

    results
}

impl Provider for PiProvider {
    fn name(&self) -> &str {
        "pi"
    }

    fn discovery_fingerprint(&self) -> Vec<(String, u64)> {
        let dir = get_pi_sessions_dir();
        let mut out: Vec<(String, u64)> = Vec::with_capacity(16);
        out.push((
            dir.to_string_lossy().to_string(),
            crate::discovery_cache::mtime_secs(&dir),
        ));
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                match entry.file_type() {
                    Ok(t) if t.is_dir() => {
                        let p = entry.path();
                        out.push((
                            p.to_string_lossy().to_string(),
                            crate::discovery_cache::mtime_secs(&p),
                        ));
                    }
                    _ => {}
                }
            }
        }
        out
    }

    fn discover_sessions(&self) -> Result<Vec<SessionSource>> {
        let dir = get_pi_sessions_dir();
        let mut sources = Vec::new();
        let project_dirs = match fs::read_dir(&dir) {
            Ok(d) => d,
            Err(_) => return Ok(sources),
        };

        for entry in project_dirs.flatten() {
            let dir_path = entry.path();
            if !dir_path.is_dir() {
                continue;
            }
            let dir_name = entry.file_name().to_string_lossy().to_string();
            let files = match fs::read_dir(&dir_path) {
                Ok(f) => f,
                Err(_) => continue,
            };
            for file_entry in files.flatten() {
                let fp = file_entry.path();
                if !fp.extension().map(|e| e == "jsonl").unwrap_or(false) {
                    continue;
                }
                let first = match read_first_entry(&fp) {
                    Some(e) => e,
                    None => continue,
                };
                if first.entry_type.as_deref() != Some("session") {
                    continue;
                }
                let cwd = first.cwd.unwrap_or_else(|| dir_name.clone());
                let project = basename_str(&cwd);
                sources.push(SessionSource {
                    path: fp.to_string_lossy().to_string(),
                    project,
                    provider: "pi".to_string(),
                });
            }
        }
        Ok(sources)
    }

    fn parse_session(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<String>,
    ) -> Result<Vec<ParsedProviderCall>> {
        Ok(parse_pi_file(Path::new(&source.path), seen_keys))
    }

    fn parse_session_filtered(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<String>,
        since: Option<SystemTime>,
        _date_start: Option<&str>,
        _date_end: Option<&str>,
    ) -> Result<Vec<ParsedProviderCall>> {
        if let Some(since) = since {
            if let Ok(meta) = fs::metadata(&source.path) {
                if let Ok(mtime) = meta.modified() {
                    if mtime < since {
                        return Ok(Vec::new());
                    }
                }
            }
        }
        self.parse_session(source, seen_keys)
    }

    fn parse_session_status(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<u64>,
        bounds: &StatusBounds,
    ) -> Result<(StatusAggregate, HashMap<String, (f64, u64)>)> {
        let calls = parse_pi_file(Path::new(&source.path), &DashSet::new());
        let mut agg = StatusAggregate::default();
        let mut by_day: HashMap<String, (f64, u64)> = HashMap::new();

        for call in &calls {
            let mut h = DefaultHasher::new();
            call.deduplication_key.hash(&mut h);
            let key = h.finish();
            if seen_keys.contains(&key) {
                continue;
            }
            seen_keys.insert(key);

            if call.timestamp.len() >= 19 {
                let ts = &call.timestamp[..19];
                let day = &call.timestamp[..10];
                let e = by_day.entry(day.to_string()).or_insert((0.0, 0));
                e.0 += call.cost_usd;
                e.1 += 1;

                if ts >= bounds.today_start.as_str() && ts <= bounds.today_end.as_str() {
                    agg.today_cost += call.cost_usd;
                    agg.today_calls += 1;
                }
                if ts >= bounds.week_start.as_str() && ts <= bounds.month_end.as_str() {
                    agg.week_cost += call.cost_usd;
                    agg.week_calls += 1;
                }
                if ts >= bounds.month_start.as_str() && ts <= bounds.month_end.as_str() {
                    agg.month_cost += call.cost_usd;
                    agg.month_calls += 1;
                }
            }
        }
        Ok((agg, by_day))
    }
}

