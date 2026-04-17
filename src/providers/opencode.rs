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
use dashmap::{DashMap, DashSet};
use rayon::prelude::*;
use rusqlite::{Connection, OpenFlags};
use serde::Deserialize;

/// Per-DB `(mtime, size)` memoization. All sessions sharing a DB otherwise
/// fan out 100+ redundant stats in `partition_cache`.
static DB_META_CACHE: std::sync::LazyLock<DashMap<String, (u64, u64)>> =
    std::sync::LazyLock::new(DashMap::new);

use crate::bash_utils::extract_bash_commands;
use crate::models::calculate_cost;
use crate::providers::Provider;
use crate::types::{ParsedProviderCall, SessionSource, Speed, StatusAggregate};

pub struct OpenCodeProvider;

static TOOL_NAME_MAP: &[(&str, &str)] = &[
    ("bash", "Bash"),
    ("read", "Read"),
    ("edit", "Edit"),
    ("write", "Write"),
    ("glob", "Glob"),
    ("grep", "Grep"),
    ("task", "Agent"),
    ("fetch", "WebFetch"),
    ("search", "WebSearch"),
    ("todo", "TodoWrite"),
    ("skill", "Skill"),
    ("patch", "Patch"),
];

fn sanitize(dir: &str) -> String {
    dir.strip_prefix('/').unwrap_or(dir).replace('/', "-")
}

pub fn get_data_dir() -> PathBuf {
    let base = std::env::var("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| dirs::home_dir().unwrap_or_default().join(".local/share"));
    base.join("opencode")
}

fn find_db_files(dir: &Path) -> Vec<PathBuf> {
    match fs::read_dir(dir) {
        Ok(entries) => entries
            .flatten()
            .filter(|e| {
                let name = e.file_name();
                let s = name.to_string_lossy();
                s.starts_with("opencode") && s.ends_with(".db")
            })
            .map(|e| e.path())
            .collect(),
        Err(_) => Vec::new(),
    }
}

fn parse_timestamp(raw: f64) -> String {
    let ms = if raw < 1e12 { raw * 1000.0 } else { raw };
    let secs = (ms / 1000.0) as i64;
    let nanos = ((ms % 1000.0) * 1_000_000.0) as u32;
    chrono::DateTime::from_timestamp(secs, nanos)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default()
}

#[derive(Deserialize)]
struct MessageData {
    role: Option<String>,
    #[serde(rename = "modelID")]
    model_id: Option<String>,
    cost: Option<f64>,
    tokens: Option<TokenData>,
}

#[derive(Deserialize)]
struct TokenData {
    input: Option<u64>,
    output: Option<u64>,
    reasoning: Option<u64>,
    cache: Option<CacheData>,
}

#[derive(Deserialize)]
struct CacheData {
    read: Option<u64>,
    write: Option<u64>,
}

#[derive(Deserialize, Clone)]
struct PartData {
    #[serde(rename = "type")]
    part_type: Option<String>,
    text: Option<String>,
    tool: Option<String>,
    state: Option<PartState>,
}

#[derive(Deserialize, Clone)]
struct PartState {
    input: Option<PartInput>,
}

#[derive(Deserialize, Clone)]
struct PartInput {
    command: Option<String>,
}

fn validate_schema(conn: &Connection) -> bool {
    conn.query_row("SELECT COUNT(*) FROM session LIMIT 1", [], |row| {
        row.get::<_, i64>(0)
    })
    .is_ok()
        && conn
            .query_row("SELECT COUNT(*) FROM message LIMIT 1", [], |row| {
                row.get::<_, i64>(0)
            })
            .is_ok()
}

fn discover_from_db(db_path: &Path) -> Vec<SessionSource> {
    let conn = match Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    let mut stmt = match conn.prepare(
        "SELECT id, directory, title, time_created FROM session WHERE time_archived IS NULL AND parent_id IS NULL ORDER BY time_created DESC",
    ) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    let rows = match stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    }) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };

    let db_path_str = db_path.to_string_lossy();
    rows.flatten()
        .map(|(id, directory, title)| {
            let project = directory
                .filter(|d| !d.is_empty())
                .unwrap_or_else(|| title.unwrap_or_else(|| "unknown".to_string()));
            SessionSource {
                path: format!("{}:{}", db_path_str, id),
                project: sanitize(&project),
                provider: "opencode".to_string(),
            }
        })
        .collect()
}

fn split_opencode_path(path: &str) -> Option<(&str, &str)> {
    let segments: Vec<&str> = path.rsplitn(2, ':').collect();
    if segments.len() < 2 {
        return None;
    }
    Some((segments[1], segments[0]))
}

/// Parse every requested session from a single opencode DB with exactly one
/// connection and one pair of big queries. Returns `(project, calls)`
/// entries matching the shape produced by the default per-source fan-out:
/// `(source_path, project, calls)` per session.
fn parse_opencode_db_batch(
    db_path: &str,
    sources: &[&SessionSource],
    seen_keys: &DashSet<String>,
) -> Vec<(String, String, Vec<ParsedProviderCall>)> {
    let prof = std::env::var_os("CODEBURN_PROFILE").is_some();
    let t_open = std::time::Instant::now();
    let conn = match Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    let _ = conn.busy_timeout(std::time::Duration::from_secs(1));
    // Same tuning as cursor: mmap the DB so reads don't copy through the
    // user-space pager, and bump cache_size so the batch parts/message
    // queries don't thrash pages. ~100 MB covers a typical opencode DB.
    let _ = conn.pragma_update(None, "mmap_size", 268_435_456i64);
    let _ = conn.pragma_update(None, "cache_size", -32768i64); // 32 MiB
    let _ = conn.pragma_update(None, "temp_store", 2i64);
    if !validate_schema(&conn) {
        return Vec::new();
    }
    if prof {
        eprintln!("[prof] opencode open+validate {:>6.1} ms", t_open.elapsed().as_secs_f64() * 1000.0);
    }

    // Build the set of session_ids we care about, plus a session_id → project map.
    let mut session_to_project: HashMap<String, String> = HashMap::new();
    for s in sources {
        if let Some((_, sid)) = split_opencode_path(&s.path) {
            session_to_project.insert(sid.to_string(), s.project.clone());
        }
    }
    if session_to_project.is_empty() {
        return Vec::new();
    }

    // Pull all messages for the requested sessions in one go. We filter to
    // just this batch's sessions so we don't pay for archived/unrelated ones.
    let placeholders: String = std::iter::repeat("?").take(session_to_project.len())
        .collect::<Vec<_>>().join(",");
    // Same trick as parts: extract the 7 fields we actually read out of
    // `data` at SQL level so we skip a serde_json round-trip per message.
    let msg_sql = format!(
        "SELECT session_id, id, time_created, \
            json_extract(data, '$.role'), \
            json_extract(data, '$.modelID'), \
            json_extract(data, '$.cost'), \
            json_extract(data, '$.tokens.input'), \
            json_extract(data, '$.tokens.output'), \
            json_extract(data, '$.tokens.reasoning'), \
            json_extract(data, '$.tokens.cache.read'), \
            json_extract(data, '$.tokens.cache.write') \
         FROM message \
         WHERE session_id IN ({}) \
         ORDER BY session_id, time_created ASC",
        placeholders
    );
    // Project at SQL level — the full `data` blob is up to 40 MB total for a
    // typical workspace (mostly tool output we never look at). Pulling only
    // the 4 fields we actually read cuts the transferred + decoded data to
    // ~3 MB and runs faster in SQLite too (measured 197 ms → 80 ms at CLI).
    let part_sql = format!(
        "SELECT session_id, message_id, \
            json_extract(data, '$.type'), \
            json_extract(data, '$.text'), \
            json_extract(data, '$.tool'), \
            json_extract(data, '$.state.input.command') \
         FROM part \
         WHERE session_id IN ({}) \
           AND json_extract(data, '$.type') IN ('text', 'tool') \
         ORDER BY session_id, message_id, id",
        placeholders
    );

    let sid_params: Vec<&str> = session_to_project.keys().map(|s| s.as_str()).collect();

    let t = std::time::Instant::now();
    let messages: Vec<(String, String, f64, MessageData)> = match conn.prepare(&msg_sql) {
        Ok(mut stmt) => match stmt.query_map(rusqlite::params_from_iter(&sid_params), |row| {
            let session_id: String = row.get(0)?;
            let id: String = row.get(1)?;
            let ts: f64 = row.get(2)?;
            let role: Option<String> = row.get(3)?;
            let model_id: Option<String> = row.get(4)?;
            let cost: Option<f64> = row.get(5)?;
            let input: Option<u64> = row.get(6)?;
            let output: Option<u64> = row.get(7)?;
            let reasoning: Option<u64> = row.get(8)?;
            let cache_read: Option<u64> = row.get(9)?;
            let cache_write: Option<u64> = row.get(10)?;
            let has_tokens = input.is_some() || output.is_some() || reasoning.is_some()
                || cache_read.is_some() || cache_write.is_some();
            let tokens = if has_tokens {
                let cache = if cache_read.is_some() || cache_write.is_some() {
                    Some(CacheData { read: cache_read, write: cache_write })
                } else {
                    None
                };
                Some(TokenData { input, output, reasoning, cache })
            } else {
                None
            };
            let msg = MessageData { role, model_id, cost, tokens };
            Ok((session_id, id, ts, msg))
        }) {
            Ok(rows) => rows.flatten().collect(),
            Err(_) => return Vec::new(),
        },
        Err(_) => return Vec::new(),
    };

    if prof {
        eprintln!("[prof] opencode SELECT msgs   {:>6.1} ms ({} rows)",
            t.elapsed().as_secs_f64() * 1000.0, messages.len());
    }
    let t = std::time::Instant::now();
    // Pull parts as pre-extracted columns — no more serde_json on the Rust
    // side for the part stream. SQLite's `json_extract` is run once per row
    // on-engine and is much cheaper than shipping full JSON + re-parsing.
    let parts: Vec<(String, String, PartData)> = match conn.prepare(&part_sql) {
        Ok(mut stmt) => match stmt.query_map(rusqlite::params_from_iter(&sid_params), |row| {
            let session_id: String = row.get(0)?;
            let message_id: String = row.get(1)?;
            let ptype: Option<String> = row.get(2)?;
            let text: Option<String> = row.get(3)?;
            let tool: Option<String> = row.get(4)?;
            let cmd: Option<String> = row.get(5)?;
            let state = cmd.map(|c| PartState {
                input: Some(PartInput { command: Some(c) }),
            });
            let part = PartData { part_type: ptype, text, tool, state };
            Ok((session_id, message_id, part))
        }) {
            Ok(rows) => rows.flatten().collect(),
            Err(_) => return Vec::new(),
        },
        Err(_) => return Vec::new(),
    };

    if prof {
        eprintln!("[prof] opencode SELECT parts  {:>6.1} ms ({} rows)",
            t.elapsed().as_secs_f64() * 1000.0, parts.len());
    }
    let t = std::time::Instant::now();
    let mut parts_by_session: HashMap<String, HashMap<String, Vec<PartData>>> = HashMap::new();
    for (sid, mid, p) in parts {
        parts_by_session
            .entry(sid)
            .or_default()
            .entry(mid)
            .or_default()
            .push(p);
    }

    if prof {
        eprintln!("[prof] opencode bucket parts  {:>6.1} ms", t.elapsed().as_secs_f64() * 1000.0);
    }
    let t = std::time::Instant::now();
    let mut messages_by_session: HashMap<String, Vec<(String, f64, MessageData)>> = HashMap::new();
    for (sid, id, ts, msg) in messages {
        messages_by_session
            .entry(sid)
            .or_default()
            .push((id, ts, msg));
    }

    if prof {
        eprintln!("[prof] opencode bucket msgs   {:>6.1} ms", t.elapsed().as_secs_f64() * 1000.0);
    }
    let t = std::time::Instant::now();
    // Process sessions in parallel — each session is independent. We keep
    // one entry per session so the disk cache can dedupe per session (its
    // invalidation key is the DB's mtime, so in practice entries hit/miss
    // as a group, but the storage shape stays per-source so the cache
    // layer never loses source→call attribution).
    let empty_parts: HashMap<String, Vec<PartData>> = HashMap::new();
    let session_results: Vec<(String, String, Vec<ParsedProviderCall>)> = session_to_project
        .par_iter()
        .filter_map(|(session_id, project)| {
            let msgs = messages_by_session.get(session_id)?;
            let parts_by_msg = parts_by_session.get(session_id).unwrap_or(&empty_parts);
            let calls = build_session_calls(session_id, msgs, parts_by_msg, seen_keys);
            if calls.is_empty() {
                return None;
            }
            let source_path = format!("{}:{}", db_path, session_id);
            Some((source_path, project.clone(), calls))
        })
        .collect();

    if prof {
        eprintln!("[prof] opencode build calls   {:>6.1} ms", t.elapsed().as_secs_f64() * 1000.0);
    }
    session_results
}

fn build_session_calls(
    session_id: &str,
    messages: &[(String, f64, MessageData)],
    parts_by_msg: &HashMap<String, Vec<PartData>>,
    seen_keys: &DashSet<String>,
) -> Vec<ParsedProviderCall> {
    let mut results = Vec::new();
    let mut current_user_message = String::new();

    for (msg_id, time_created, msg_data) in messages {
        if msg_data.role.as_deref() == Some("user") {
            if let Some(msg_parts) = parts_by_msg.get(msg_id) {
                let text_parts: Vec<&str> = msg_parts
                    .iter()
                    .filter(|p| p.part_type.as_deref() == Some("text"))
                    .filter_map(|p| p.text.as_deref())
                    .filter(|t| !t.is_empty())
                    .collect();
                if !text_parts.is_empty() {
                    current_user_message = text_parts.join(" ");
                }
            }
            continue;
        }

        if msg_data.role.as_deref() != Some("assistant") {
            continue;
        }

        let tokens_input = msg_data.tokens.as_ref().and_then(|t| t.input).unwrap_or(0);
        let tokens_output = msg_data.tokens.as_ref().and_then(|t| t.output).unwrap_or(0);
        let tokens_reasoning = msg_data.tokens.as_ref().and_then(|t| t.reasoning).unwrap_or(0);
        let cache_read = msg_data
            .tokens
            .as_ref()
            .and_then(|t| t.cache.as_ref())
            .and_then(|c| c.read)
            .unwrap_or(0);
        let cache_write = msg_data
            .tokens
            .as_ref()
            .and_then(|t| t.cache.as_ref())
            .and_then(|c| c.write)
            .unwrap_or(0);

        let all_zero = tokens_input == 0
            && tokens_output == 0
            && tokens_reasoning == 0
            && cache_read == 0
            && cache_write == 0;
        if all_zero && msg_data.cost.unwrap_or(0.0) == 0.0 {
            continue;
        }

        let tool_parts: Vec<&PartData> = parts_by_msg
            .get(msg_id)
            .map(|v| v.iter().filter(|p| p.part_type.as_deref() == Some("tool")).collect())
            .unwrap_or_default();

        let tools: Vec<String> = tool_parts
            .iter()
            .filter_map(|p| {
                let raw = p.tool.as_deref()?;
                let mapped = TOOL_NAME_MAP
                    .iter()
                    .find(|(k, _)| *k == raw)
                    .map(|(_, v)| *v)
                    .unwrap_or(raw);
                Some(mapped.to_string())
            })
            .collect();

        let bash_commands: Vec<String> = tool_parts
            .iter()
            .filter(|p| p.tool.as_deref() == Some("bash"))
            .filter_map(|p| p.state.as_ref()?.input.as_ref()?.command.as_deref())
            .flat_map(extract_bash_commands)
            .collect();

        let dedup_key = format!("opencode:{}:{}", session_id, msg_id);
        if seen_keys.contains(&dedup_key) {
            continue;
        }
        seen_keys.insert(dedup_key.clone());

        let model = msg_data.model_id.as_deref().unwrap_or("unknown");
        let mut cost_usd = calculate_cost(
            model,
            tokens_input,
            tokens_output + tokens_reasoning,
            cache_write,
            cache_read,
            0,
            Speed::Standard,
        );

        if cost_usd == 0.0 {
            if let Some(c) = msg_data.cost {
                if c > 0.0 {
                    cost_usd = c;
                }
            }
        }

        results.push(ParsedProviderCall {
            provider: "opencode".to_string(),
            model: model.to_string(),
            input_tokens: tokens_input,
            output_tokens: tokens_output,
            cache_creation_input_tokens: cache_write,
            cache_read_input_tokens: cache_read,
            cached_input_tokens: cache_read,
            reasoning_tokens: tokens_reasoning,
            web_search_requests: 0,
            cost_usd,
            tools,
            bash_commands,
            timestamp: parse_timestamp(*time_created),
            speed: Speed::Standard,
            deduplication_key: dedup_key,
            user_message: current_user_message.clone(),
            session_id: session_id.to_string(),
            user_message_timestamp: String::new(),
        });
    }

    results
}

/// Single-session fallback used by `Provider::parse_session`. The cache-hit
/// and cache-miss paths both go through `parse_sources` → `parse_opencode_db_batch`
/// instead, so this path fires only when `parse_session` is called directly.
/// Structure mirrors the batch path's per-session loop: fetch rows, decode
/// JSON, defer to `build_session_calls` for the message→part→call logic.
fn parse_opencode_session(
    source: &SessionSource,
    seen_keys: &DashSet<String>,
) -> Vec<ParsedProviderCall> {
    let (db_path, session_id) = match split_opencode_path(&source.path) {
        Some(x) => x,
        None => return Vec::new(),
    };

    let conn = match Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    let _ = conn.busy_timeout(std::time::Duration::from_secs(1));
    if !validate_schema(&conn) {
        return Vec::new();
    }

    let messages_raw: Vec<(String, f64, String)> = match conn.prepare(
        "SELECT id, time_created, data FROM message WHERE session_id = ?1 ORDER BY time_created ASC",
    ) {
        Ok(mut stmt) => stmt
            .query_map([session_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, f64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .ok()
            .map(|rows| rows.flatten().collect())
            .unwrap_or_default(),
        Err(_) => return Vec::new(),
    };

    let parts_raw: Vec<(String, String)> = match conn.prepare(
        "SELECT message_id, data FROM part WHERE session_id = ?1 ORDER BY message_id, id",
    ) {
        Ok(mut stmt) => stmt
            .query_map([session_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .ok()
            .map(|rows| rows.flatten().collect())
            .unwrap_or_default(),
        Err(_) => return Vec::new(),
    };

    let messages: Vec<(String, f64, MessageData)> = messages_raw
        .into_iter()
        .filter_map(|(id, ts, data)| {
            serde_json::from_str::<MessageData>(&data).ok().map(|md| (id, ts, md))
        })
        .collect();

    let mut parts_by_msg: HashMap<String, Vec<PartData>> = HashMap::new();
    for (msg_id, data) in &parts_raw {
        if let Ok(parsed) = serde_json::from_str::<PartData>(data) {
            parts_by_msg.entry(msg_id.clone()).or_default().push(parsed);
        }
    }

    build_session_calls(session_id, &messages, &parts_by_msg, seen_keys)
}

impl Provider for OpenCodeProvider {
    fn name(&self) -> &str {
        "opencode"
    }

    fn discovery_fingerprint(&self) -> Vec<(String, u64)> {
        // Fingerprint: the data dir + every `opencode*.db` file's mtime.
        // Session additions/deletions happen inside the DB, but the DB file
        // mtime bumps on every write, so any state change is caught.
        let dir = get_data_dir();
        let mut out: Vec<(String, u64)> = Vec::with_capacity(4);
        out.push((
            dir.to_string_lossy().to_string(),
            crate::discovery_cache::mtime_secs(&dir),
        ));
        for db in find_db_files(&dir) {
            out.push((
                db.to_string_lossy().to_string(),
                crate::discovery_cache::mtime_secs(&db),
            ));
        }
        out
    }

    fn cache_metadata(&self, source: &SessionSource) -> Option<(u64, u64)> {
        // `source.path` is `{dbPath}:{sessionId}`. Invalidation is driven by
        // the DB file's mtime — a single session edit bumps that and
        // correctly invalidates every session's cache entry.
        //
        // Memoize per DB file: all N sessions that share a DB would
        // otherwise stat it N times in parallel during `partition_cache`
        // (N=186 on this machine, ~1.5 ms of redundant work). The cache is
        // process-local and empty at startup; it only helps the per-run
        // fan-out.
        let (db_path, _sid) = split_opencode_path(&source.path)?;
        if let Some(hit) = DB_META_CACHE.get(db_path) {
            return Some(*hit.value());
        }
        let meta = std::fs::metadata(db_path).ok()?;
        let mtime = meta
            .modified()
            .ok()?
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_secs();
        let out = (mtime, meta.len());
        DB_META_CACHE.insert(db_path.to_string(), out);
        Some(out)
    }

    fn discover_sessions(&self) -> Result<Vec<SessionSource>> {
        let dir = get_data_dir();
        let db_paths = find_db_files(&dir);
        let mut sessions = Vec::new();
        for db_path in &db_paths {
            sessions.extend(discover_from_db(db_path));
        }
        Ok(sessions)
    }

    fn parse_session(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<String>,
    ) -> Result<Vec<ParsedProviderCall>> {
        Ok(parse_opencode_session(source, seen_keys))
    }

    fn parse_sources(
        &self,
        sources: &[SessionSource],
        seen_keys: &DashSet<String>,
        _since: Option<std::time::SystemTime>,
        _date_start: Option<&str>,
        _date_end: Option<&str>,
    ) -> Vec<(String, String, Vec<ParsedProviderCall>)> {
        // Group sources by the DB file encoded in the path prefix. Each DB
        // gets exactly one SQLite connection and one pair of queries
        // (messages + parts) — the N× per-session open/prepare overhead
        // that dominated the per-source path is gone.
        let mut by_db: HashMap<String, Vec<&SessionSource>> = HashMap::new();
        for s in sources {
            if let Some((db_path, _sid)) = split_opencode_path(&s.path) {
                by_db.entry(db_path.to_string()).or_default().push(s);
            }
        }
        by_db
            .into_iter()
            .flat_map(|(db_path, db_sources)| {
                parse_opencode_db_batch(&db_path, &db_sources, seen_keys)
            })
            .collect()
    }

    fn parse_session_status(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<u64>,
        bounds: &crate::types::StatusBounds,
    ) -> Result<(StatusAggregate, HashMap<String, (f64, u64)>)> {
        let segments: Vec<&str> = source.path.rsplitn(2, ':').collect();
        if segments.len() < 2 {
            return Ok((StatusAggregate::default(), HashMap::new()));
        }
        let session_id = segments[0];
        let db_path = segments[1];

        let conn = match Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ) {
            Ok(c) => c,
            Err(_) => return Ok((StatusAggregate::default(), HashMap::new())),
        };
        let _ = conn.busy_timeout(std::time::Duration::from_secs(1));

        if !validate_schema(&conn) {
            return Ok((StatusAggregate::default(), HashMap::new()));
        }

        let mut msg_stmt = match conn.prepare(
            "SELECT id, time_created, data FROM message WHERE session_id = ?1 AND data LIKE '%\"assistant\"%' ORDER BY time_created ASC",
        ) {
            Ok(s) => s,
            Err(_) => return Ok((StatusAggregate::default(), HashMap::new())),
        };

        let messages: Vec<(String, f64, String)> = msg_stmt
            .query_map([session_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, f64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .ok()
            .map(|rows| rows.flatten().collect())
            .unwrap_or_default();

        let mut agg = StatusAggregate::default();
        let mut by_day: HashMap<String, (f64, u64)> = HashMap::new();

        for (msg_id, time_created, data) in &messages {
            let msg_data: MessageData = match serde_json::from_str(data) {
                Ok(d) => d,
                Err(_) => continue,
            };

            if msg_data.role.as_deref() != Some("assistant") {
                continue;
            }

            let tokens_input = msg_data.tokens.as_ref().and_then(|t| t.input).unwrap_or(0);
            let tokens_output = msg_data.tokens.as_ref().and_then(|t| t.output).unwrap_or(0);
            let tokens_reasoning = msg_data.tokens.as_ref().and_then(|t| t.reasoning).unwrap_or(0);
            let cache_read = msg_data.tokens.as_ref().and_then(|t| t.cache.as_ref()).and_then(|c| c.read).unwrap_or(0);
            let cache_write = msg_data.tokens.as_ref().and_then(|t| t.cache.as_ref()).and_then(|c| c.write).unwrap_or(0);

            let all_zero = tokens_input == 0 && tokens_output == 0 && tokens_reasoning == 0 && cache_read == 0 && cache_write == 0;
            if all_zero && msg_data.cost.unwrap_or(0.0) == 0.0 {
                continue;
            }

            let dedup_key = format!("opencode:{}:{}", session_id, msg_id);
            let h = {
                let mut hasher = DefaultHasher::new();
                dedup_key.hash(&mut hasher);
                hasher.finish()
            };
            if seen_keys.contains(&h) {
                continue;
            }
            seen_keys.insert(h);

            let model = msg_data.model_id.as_deref().unwrap_or("unknown");
            let mut cost = calculate_cost(
                model, tokens_input, tokens_output + tokens_reasoning, cache_write, cache_read, 0, Speed::Standard,
            );
            if cost == 0.0 {
                if let Some(c) = msg_data.cost {
                    if c > 0.0 {
                        cost = c;
                    }
                }
            }

            let timestamp = parse_timestamp(*time_created);
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

        Ok((agg, by_day))
    }
}
