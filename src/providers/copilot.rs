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

use crate::models::calculate_cost;
use crate::providers::Provider;
use crate::types::{ParsedProviderCall, SessionSource, Speed, StatusAggregate, StatusBounds};

pub struct CopilotProvider;

static TOOL_NAME_MAP: &[(&str, &str)] = &[
    ("bash", "Bash"),
    ("read_file", "Read"),
    ("write_file", "Edit"),
    ("edit_file", "Edit"),
    ("create_file", "Write"),
    ("delete_file", "Delete"),
    ("search_files", "Grep"),
    ("find_files", "Glob"),
    ("list_directory", "LS"),
    ("web_search", "WebSearch"),
    ("fetch_webpage", "WebFetch"),
    ("github_repo", "GitHub"),
];

fn tool_name(raw: &str) -> String {
    crate::providers::common::lookup_tool(TOOL_NAME_MAP, raw)
}

pub fn get_copilot_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join(".copilot/session-state")
}

#[derive(Deserialize)]
struct CopilotEvent {
    #[serde(rename = "type")]
    event_type: Option<String>,
    timestamp: Option<String>,
    data: Option<serde_json::Value>,
}

fn parse_cwd_from_yaml(yaml: &str) -> Option<String> {
    for line in yaml.lines() {
        let line = line.trim_end();
        if let Some(rest) = line.strip_prefix("cwd:") {
            let rest = rest.trim();
            // strip trailing comment
            let rest = rest.split('#').next().unwrap_or(rest).trim();
            // strip surrounding quotes
            let rest = rest.trim_matches(|c: char| c == '\'' || c == '"');
            if !rest.is_empty() {
                return Some(rest.to_string());
            }
        }
    }
    None
}

fn parse_copilot_file(
    path: &Path,
    seen_keys: &DashSet<String>,
) -> Vec<ParsedProviderCall> {
    let content = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    let session_id = path
        .parent()
        .and_then(|p| p.file_name())
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();

    let mut results = Vec::new();
    let mut current_model = String::new();
    let mut pending_user = String::new();

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let event: CopilotEvent = match serde_json::from_str(line) {
            Ok(e) => e,
            Err(_) => continue,
        };
        let et = event.event_type.as_deref().unwrap_or("");
        let data = match &event.data {
            Some(d) => d,
            None => continue,
        };

        if et == "session.model_change" {
            if let Some(m) = data.get("newModel").and_then(|v| v.as_str()) {
                current_model = m.to_string();
            }
            continue;
        }
        if et == "user.message" {
            if let Some(c) = data.get("content").and_then(|v| v.as_str()) {
                pending_user = c.to_string();
            }
            continue;
        }
        if et != "assistant.message" {
            continue;
        }
        let output_tokens = data
            .get("outputTokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if output_tokens == 0 || current_model.is_empty() {
            continue;
        }
        let message_id = data
            .get("messageId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let dedup_key = format!("copilot:{}:{}", session_id, message_id);
        if seen_keys.contains(&dedup_key) {
            continue;
        }
        seen_keys.insert(dedup_key.clone());

        let mut tools: Vec<String> = Vec::new();
        if let Some(reqs) = data.get("toolRequests").and_then(|v| v.as_array()) {
            for r in reqs {
                if let Some(name) = r.get("name").and_then(|v| v.as_str()) {
                    if !name.is_empty() {
                        tools.push(tool_name(name));
                    }
                }
            }
        }

        let cost = calculate_cost(&current_model, 0, output_tokens, 0, 0, 0, Speed::Standard);

        results.push(ParsedProviderCall {
            provider: "copilot".to_string(),
            model: current_model.clone(),
            input_tokens: 0,
            output_tokens,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            cached_input_tokens: 0,
            reasoning_tokens: 0,
            web_search_requests: 0,
            cost_usd: cost,
            tools,
            bash_commands: Vec::new(),
            timestamp: event.timestamp.unwrap_or_default(),
            speed: Speed::Standard,
            deduplication_key: dedup_key,
            user_message: std::mem::take(&mut pending_user),
            session_id: session_id.clone(),
            user_message_timestamp: String::new(),
        });
    }
    results
}

impl Provider for CopilotProvider {
    fn name(&self) -> &str {
        "copilot"
    }

    fn discovery_fingerprint(&self) -> Vec<(String, u64)> {
        let dir = get_copilot_dir();
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
        let dir = get_copilot_dir();
        let mut sources = Vec::new();
        let sessions = match fs::read_dir(&dir) {
            Ok(s) => s,
            Err(_) => return Ok(sources),
        };
        for entry in sessions.flatten() {
            let session_dir = entry.path();
            if !session_dir.is_dir() {
                continue;
            }
            let session_id = entry.file_name().to_string_lossy().to_string();
            let events_path = session_dir.join("events.jsonl");
            if !events_path.is_file() {
                continue;
            }
            let mut project = session_id.clone();
            if let Ok(yaml) = fs::read_to_string(session_dir.join("workspace.yaml")) {
                if let Some(cwd) = parse_cwd_from_yaml(&yaml) {
                    project = Path::new(&cwd)
                        .file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or(cwd);
                }
            }
            sources.push(SessionSource {
                path: events_path.to_string_lossy().to_string(),
                project,
                provider: "copilot".to_string(),
            });
        }
        Ok(sources)
    }

    fn parse_session(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<String>,
    ) -> Result<Vec<ParsedProviderCall>> {
        Ok(parse_copilot_file(Path::new(&source.path), seen_keys))
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
        let calls = parse_copilot_file(Path::new(&source.path), &DashSet::new());
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
