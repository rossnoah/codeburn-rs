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
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use anyhow::Result;
use dashmap::DashSet;
use rayon::prelude::*;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};

use crate::models::calculate_cost;
use crate::providers::Provider;
use crate::types::{ParsedProviderCall, SessionSource, Speed, StatusAggregate};

#[derive(Serialize, Deserialize)]
struct CursorCache {
    db_mtime: u64,
    calls: Vec<ParsedProviderCall>,
}

fn cursor_cache_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join(".cache/codeburn/cursor-full-cache.json")
}

/// Returns (cached_entry_if_readable, db_mtime_if_readable).
fn load_cursor_cache(db_path: &str) -> (Option<CursorCache>, Option<u64>) {
    let db_mtime = std::fs::metadata(db_path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs());

    let cache_content = std::fs::read_to_string(cursor_cache_path()).ok();
    let cached = cache_content.and_then(|s| serde_json::from_str::<CursorCache>(&s).ok());
    (cached, db_mtime)
}

fn write_cursor_cache(db_mtime: u64, calls: &[ParsedProviderCall]) {
    let path = cursor_cache_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let cached = CursorCache {
        db_mtime,
        calls: calls.to_vec(),
    };
    if let Ok(json) = serde_json::to_string(&cached) {
        let _ = std::fs::write(&path, json);
    }
}

/// Spawn a detached child process that rebuilds the cursor cache and exits.
/// Because it's a separate process, it survives when the parent returns.
/// We deliberately don't track the child — it's fire-and-forget.
fn spawn_background_refresh(_db_path: String, _db_mtime: u64) {
    // Use current_exe to re-invoke ourselves with the hidden subcommand.
    // On macOS/Linux, stdio piped to /dev/null ensures the child is fully
    // decoupled from our stdio even when the parent exits.
    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(_) => return,
    };
    let _ = std::process::Command::new(exe)
        .arg("refresh-cursor-cache")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

/// Entry point for the detached refresh subprocess.
/// Runs the full cursor query and writes the cache, regardless of how long
/// it takes. The parent does not wait on this.
pub fn run_background_refresh() {
    let db_path_buf = get_cursor_db_path();
    if !db_path_buf.exists() {
        return;
    }
    let db_path_str = db_path_buf.to_string_lossy().to_string();
    let db_mtime = match std::fs::metadata(&db_path_buf) {
        Ok(m) => m
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0),
        Err(_) => return,
    };

    // Skip if someone else already populated a fresh cache while we were
    // starting up (e.g. two codeburn invocations in quick succession).
    let (cached_opt, _) = load_cursor_cache(&db_path_str);
    if let Some(cached) = cached_opt {
        if cached.db_mtime == db_mtime {
            return;
        }
    }

    let conn = match Connection::open_with_flags(
        &db_path_buf,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(c) => c,
        Err(_) => return,
    };
    let _ = conn.busy_timeout(std::time::Duration::from_secs(5));
    if !validate_schema(&conn) {
        return;
    }
    let calls = parse_bubbles(&db_path_str, &DashSet::new());
    write_cursor_cache(db_mtime, &calls);
}

pub struct CursorProvider;

const CURSOR_DEFAULT_MODEL: &str = "claude-sonnet-4-5";
const DEFAULT_LOOKBACK_DAYS: u64 = 35;

pub fn get_cursor_db_path() -> PathBuf {
    let home = dirs::home_dir().unwrap_or_default();
    if cfg!(target_os = "macos") {
        home.join("Library/Application Support/Cursor/User/globalStorage/state.vscdb")
    } else if cfg!(target_os = "windows") {
        home.join("AppData/Roaming/Cursor/User/globalStorage/state.vscdb")
    } else {
        home.join(".config/Cursor/User/globalStorage/state.vscdb")
    }
}

fn resolve_model(raw: Option<&str>) -> String {
    match raw {
        None | Some("default") | Some("") => CURSOR_DEFAULT_MODEL.to_string(),
        Some(m) => m.to_string(),
    }
}

fn model_for_display(raw: Option<&str>) -> String {
    match raw {
        None | Some("default") | Some("") => "default".to_string(),
        Some(m) => m.to_string(),
    }
}

fn extract_languages(code_blocks_json: Option<&str>) -> Vec<String> {
    let json = match code_blocks_json {
        Some(j) if !j.is_empty() => j,
        _ => return Vec::new(),
    };
    let blocks: Vec<serde_json::Value> = match serde_json::from_str(json) {
        Ok(b) => b,
        Err(_) => return Vec::new(),
    };
    let mut langs: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for block in &blocks {
        if let Some(lang) = block.get("languageId").and_then(|l| l.as_str()) {
            if lang != "plaintext" && seen.insert(lang.to_string()) {
                langs.push(lang.to_string());
            }
        }
    }
    langs
}

fn validate_schema(conn: &Connection) -> bool {
    conn.query_row(
        "SELECT COUNT(*) FROM cursorDiskKV WHERE key LIKE 'bubbleId:%' LIMIT 1",
        [],
        |row| row.get::<_, i64>(0),
    )
    .is_ok()
}

/// Lightweight struct for simd-json DOM traversal.
/// We only parse the fields we need from each bubble value.
#[derive(Debug, Default)]
struct RawBubble {
    bubble_type: i64,             // 1 = user, 2 = assistant
    input_tokens: u64,
    output_tokens: u64,
    model_name: Option<String>,
    created_at: Option<String>,
    conversation_id: Option<String>,
    text: Option<String>,
    code_blocks_json: Option<String>,
}

fn parse_bubble_json(mut bytes: Vec<u8>) -> Option<RawBubble> {
    // Use serde_json::Value for simpler access. simd-json has a faster
    // parsing path via to_owned_value but simpler API via serde.
    let v: serde_json::Value = simd_json::serde::from_slice(&mut bytes).ok()?;
    let obj = v.as_object()?;
    let mut b = RawBubble::default();

    b.bubble_type = obj.get("type").and_then(|t| t.as_i64()).unwrap_or(0);
    if let Some(tc) = obj.get("tokenCount").and_then(|t| t.as_object()) {
        b.input_tokens = tc.get("inputTokens").and_then(|v| v.as_u64()).unwrap_or(0);
        b.output_tokens = tc.get("outputTokens").and_then(|v| v.as_u64()).unwrap_or(0);
    }
    if let Some(mi) = obj.get("modelInfo").and_then(|v| v.as_object()) {
        b.model_name = mi.get("modelName").and_then(|v| v.as_str()).map(String::from);
    }
    b.created_at = obj.get("createdAt").and_then(|v| v.as_str()).map(String::from);
    b.conversation_id = obj.get("conversationId").and_then(|v| v.as_str()).map(String::from);
    if let Some(t) = obj.get("text").and_then(|v| v.as_str()) {
        let truncated: String = t.chars().take(500).collect();
        b.text = Some(truncated);
    }
    if let Some(cb) = obj.get("codeBlocks") {
        if !cb.is_null() {
            b.code_blocks_json = serde_json::to_string(cb).ok();
        }
    }

    Some(b)
}

/// Hex bucket boundaries for splitting `bubbleId:*`. Each chunk gets its own
/// READ_ONLY connection so the scans run in parallel.
///
/// 8 chunks empirically beats 4 once the per-row work is small (we filter
/// `createdAt` in SQL — the JSON parse only fires on a few hundred
/// surviving rows). At 4 chunks the heaviest single chunk dominates the
/// parallel wall; at 16 the per-chunk fixed cost (open conn + prepare
/// stmt) starts to add up.
const BUBBLE_RANGE_CHUNKS: &[(&str, &str)] = &[
    ("bubbleId:0", "bubbleId:2"),
    ("bubbleId:2", "bubbleId:4"),
    ("bubbleId:4", "bubbleId:6"),
    ("bubbleId:6", "bubbleId:8"),
    ("bubbleId:8", "bubbleId:a"),
    ("bubbleId:a", "bubbleId:c"),
    ("bubbleId:c", "bubbleId:e"),
    ("bubbleId:e", "bubbleId;"),
];

/// Scan + decode one key range. We parse JSON inside the row loop so the big
/// TEXT `value` column doesn't require an intermediate `Vec<Vec<u8>>`. We
/// also skip `row.get::<String>` (which does UTF-8 validation we don't need
/// because simd-json re-parses) in favour of `as_blob().to_vec()`.
fn scan_and_decode_range(
    db_path: &str,
    low: &str,
    high: &str,
    time_floor: &str,
) -> Vec<RawBubble> {
    let conn = match Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    let _ = conn.busy_timeout(std::time::Duration::from_secs(5));
    // mmap the DB pages so SQLite can read straight out of the page cache
    // without a copy. 2 GiB covers the full 1.5 GB vscdb. Cache size bump
    // gives the BTree scan more room before pages get evicted mid-scan.
    let _ = conn.pragma_update(None, "mmap_size", 2_147_483_648i64);
    let _ = conn.pragma_update(None, "cache_size", -65536i64); // 64 MiB
    let _ = conn.pragma_update(None, "temp_store", 2i64);

    // Register a Rust-side scalar `bubble_useful(value, floor)` that
    // returns 1 only when the JSON blob is interesting: it has a
    // `createdAt` later than `floor` AND it's either a user bubble
    // (`"type":1`) or an assistant bubble with non-zero token counts.
    //
    // memchr's SIMD search beats SQLite's byte-wise `instr` by ~3x on
    // multi-KB values; combining what used to be `instr+substr+IN(...)`
    // SQLite functions into a single Rust call also halves the per-row
    // dispatch overhead. Pushing the type/tokens check in here drops the
    // SELECT result set from ~2 300 to ~100 rows, saving most of the
    // simd-json parses on the Rust side.
    use rusqlite::functions::FunctionFlags;
    let create = conn.create_scalar_function(
        "bubble_useful",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            // ValueRef::as_bytes works for both TEXT and BLOB columns —
            // cursorDiskKV.value is TEXT, so as_blob() would always fail.
            let blob = ctx.get_raw(0).as_bytes().unwrap_or_default();
            let floor = ctx.get_raw(1).as_bytes().unwrap_or_default();

            // 1) createdAt > floor.
            let ts_needle = b"\"createdAt\":\"";
            let ts_after = match memchr::memmem::find(blob, ts_needle) {
                Some(p) => p + ts_needle.len(),
                None => return Ok(0i64),
            };
            if ts_after + 20 > blob.len() {
                return Ok(0i64);
            }
            if &blob[ts_after..ts_after + 20] <= floor {
                return Ok(0i64);
            }

            // 2) Either a user bubble OR an assistant with tokens.
            //    "type":1 → user (always useful)
            //    "type":2 → assistant — useful only if input/output tokens > 0
            let type_needle = b"\"type\":";
            let bubble_type = memchr::memmem::find(blob, type_needle).and_then(|p| {
                blob.get(p + type_needle.len()).and_then(|b| match b {
                    b'1' => Some(1u8),
                    b'2' => Some(2u8),
                    _ => None,
                })
            });
            match bubble_type {
                Some(1) => Ok(1i64),
                Some(2) => {
                    // Look for `"inputTokens":N` or `"outputTokens":N` with
                    // any non-zero N. Cheap substring + check next-non-space
                    // byte != '0'.
                    let in_needle = b"\"inputTokens\":";
                    let out_needle = b"\"outputTokens\":";
                    let any_nonzero = |needle: &[u8]| -> bool {
                        match memchr::memmem::find(blob, needle) {
                            Some(p) => {
                                let mut i = p + needle.len();
                                while i < blob.len() && blob[i] == b' ' {
                                    i += 1;
                                }
                                i < blob.len() && blob[i] >= b'1' && blob[i] <= b'9'
                            }
                            None => false,
                        }
                    };
                    Ok(if any_nonzero(in_needle) || any_nonzero(out_needle) {
                        1
                    } else {
                        0
                    })
                }
                _ => Ok(0),
            }
        },
    );
    if create.is_err() {
        return Vec::new();
    }

    let query = "SELECT value FROM cursorDiskKV \
         WHERE key >= ?1 AND key < ?2 \
         AND bubble_useful(value, ?3) = 1";
    let mut stmt = match conn.prepare(query) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let mut out: Vec<RawBubble> = Vec::new();
    let mut rows = match stmt.query([low, high, time_floor]) {
        Ok(r) => r,
        Err(_) => return out,
    };
    while let Ok(Some(row)) = rows.next() {
        let bytes = match row.get_ref(0) {
            Ok(v) => match v.as_bytes() {
                Ok(b) => b.to_vec(),
                Err(_) => continue,
            },
            Err(_) => continue,
        };
        if let Some(b) = parse_bubble_json(bytes) {
            if b.bubble_type == 1 || b.input_tokens > 0 || b.output_tokens > 0 {
                out.push(b);
            }
        }
    }
    out
}

fn parse_bubbles(
    db_path: &str,
    seen_keys: &DashSet<String>,
) -> Vec<ParsedProviderCall> {
    let prof = std::env::var_os("CODEBURN_PROFILE").is_some();
    let time_floor = {
        let now = chrono::Utc::now();
        let lookback = chrono::Duration::days(DEFAULT_LOOKBACK_DAYS as i64);
        (now - lookback).to_rfc3339()
    };

    // Fan out the range scan across independent READ_ONLY connections. Each
    // chunk does scan + simd-json decode inline so the big TEXT `value`s
    // never live in an intermediate `Vec<Vec<u8>>` — peak memory drops and
    // the JSON decode overlaps with other chunks' SQL I/O.
    let t = std::time::Instant::now();
    let bubbles: Vec<RawBubble> = BUBBLE_RANGE_CHUNKS
        .par_iter()
        .flat_map(|(low, high)| scan_and_decode_range(db_path, low, high, &time_floor))
        .collect();
    if prof {
        eprintln!("[prof] cursor scan+decode     {:>6.1} ms ({} kept)",
            t.elapsed().as_secs_f64() * 1000.0, bubbles.len());
    }

    let t = std::time::Instant::now();
    // Build user message map from type=1 bubbles
    let mut user_messages: HashMap<String, Vec<String>> = HashMap::new();
    for b in bubbles.iter().filter(|b| b.bubble_type == 1) {
        if let (Some(conv_id), Some(text)) = (&b.conversation_id, &b.text) {
            if !conv_id.is_empty() && !text.is_empty() {
                user_messages
                    .entry(conv_id.clone())
                    .or_default()
                    .push(text.clone());
            }
        }
    }

    // Build assistant call results from type=2 bubbles with input_tokens > 0
    let mut results = Vec::new();
    let mut assistant_bubbles: Vec<&RawBubble> = bubbles
        .iter()
        .filter(|b| b.bubble_type == 2 && b.input_tokens > 0)
        .collect();
    assistant_bubbles.sort_by(|a, b| a.created_at.cmp(&b.created_at));

    for b in assistant_bubbles {
        let created_at = b.created_at.clone().unwrap_or_default();
        let conversation_id = b.conversation_id.clone().unwrap_or_else(|| "unknown".to_string());
        let dedup_key = format!(
            "cursor:{}:{}:{}:{}",
            conversation_id, created_at, b.input_tokens, b.output_tokens
        );
        if seen_keys.contains(&dedup_key) {
            continue;
        }
        seen_keys.insert(dedup_key.clone());

        let pricing_model = resolve_model(b.model_name.as_deref());
        let display_model = model_for_display(b.model_name.as_deref());

        let cost_usd = calculate_cost(
            &pricing_model, b.input_tokens, b.output_tokens, 0, 0, 0, Speed::Standard,
        );

        let user_question = user_messages
            .get_mut(&conversation_id)
            .and_then(|msgs| if msgs.is_empty() { None } else { Some(msgs.remove(0)) })
            .unwrap_or_default();
        let assistant_text = b.text.clone().unwrap_or_default();
        let user_text = format!("{} {}", user_question, assistant_text).trim().to_string();

        let languages = extract_languages(b.code_blocks_json.as_deref());
        let has_code = !languages.is_empty();
        let mut cursor_tools: Vec<String> = Vec::new();
        if has_code {
            cursor_tools.push("cursor:edit".to_string());
            for lang in &languages {
                cursor_tools.push(format!("lang:{}", lang));
            }
        }

        results.push(ParsedProviderCall {
            provider: "cursor".to_string(),
            model: display_model,
            input_tokens: b.input_tokens,
            output_tokens: b.output_tokens,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            cached_input_tokens: 0,
            reasoning_tokens: 0,
            web_search_requests: 0,
            cost_usd,
            tools: cursor_tools,
            bash_commands: Vec::new(),
            timestamp: created_at,
            speed: Speed::Standard,
            deduplication_key: dedup_key,
            user_message: user_text,
            session_id: conversation_id,
            user_message_timestamp: String::new(),
        });
    }

    if prof {
        eprintln!("[prof] cursor build results   {:>6.1} ms ({} calls)",
            t.elapsed().as_secs_f64() * 1000.0, results.len());
    }
    results
}

impl Provider for CursorProvider {
    fn name(&self) -> &str {
        "cursor"
    }

    fn discovery_fingerprint(&self) -> Vec<(String, u64)> {
        // Cursor's discovery is just "does the state.vscdb exist" — its
        // mtime or a 0 sentinel covers both presence and any change.
        let db_path = get_cursor_db_path();
        vec![(
            db_path.to_string_lossy().to_string(),
            crate::discovery_cache::mtime_secs(&db_path),
        )]
    }

    fn discover_sessions(&self) -> Result<Vec<SessionSource>> {
        let db_path = get_cursor_db_path();
        if !db_path.exists() {
            return Ok(Vec::new());
        }
        Ok(vec![SessionSource {
            path: db_path.to_string_lossy().to_string(),
            project: "cursor".to_string(),
            provider: "cursor".to_string(),
        }])
    }

    fn parse_session(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<String>,
    ) -> Result<Vec<ParsedProviderCall>> {
        // --no-cache: ignore the disk cache entirely and do the full SQLite
        // query synchronously. This blows the usual 250ms budget (1-2s) but
        // is what the user explicitly asked for.
        if crate::parser::is_cache_bypassed() {
            // One cheap schema-validate connection up front so we bail fast
            // on a Cursor install that hasn't written bubbles yet.
            let conn = Connection::open_with_flags(
                &source.path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            conn.busy_timeout(std::time::Duration::from_secs(5))?;
            if !validate_schema(&conn) {
                return Ok(Vec::new());
            }
            drop(conn);
            return Ok(parse_bubbles(&source.path, seen_keys));
        }

        // Cursor's DB is 1.5GB+; a cold query takes 1-2s. To stay under our
        // 250ms report budget we never block on it. Instead we:
        //   - Fresh cache → return it.
        //   - Stale/missing cache → return whatever we have (possibly empty)
        //     and spawn a DETACHED subprocess to rebuild the cache. The
        //     subprocess uses `std::process::Command` so it survives after
        //     the parent exits; the next report will have fresh data.
        let (cached_opt, db_mtime) = load_cursor_cache(&source.path);
        let db_mtime = match db_mtime {
            Some(m) => m,
            None => return Ok(cached_opt.map(|c| c.calls).unwrap_or_default()),
        };

        if let Some(cached) = &cached_opt {
            if cached.db_mtime == db_mtime {
                return Ok(cached.calls.clone());
            }
        }

        spawn_background_refresh(source.path.clone(), db_mtime);
        Ok(cached_opt.map(|c| c.calls).unwrap_or_default())
    }

    fn parse_session_status(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<u64>,
        bounds: &crate::types::StatusBounds,
    ) -> Result<(StatusAggregate, HashMap<String, (f64, u64)>)> {
        // Check mtime-based cache for the Cursor DB (it's 1.5GB+ and queries are slow).
        // --no-cache bypasses this entirely.
        if !crate::parser::is_cache_bypassed() {
            if let Ok(meta) = std::fs::metadata(&source.path) {
                let mtime_secs = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let file_size = meta.len();
                let cache_key = format!("cursor-db:{}", source.path);

                if let Some(cached) =
                    crate::session_cache::lookup_cached(&cache_key, mtime_secs, file_size)
                {
                    if !crate::session_cache::has_boundary_day(&cached, bounds) {
                        let agg = crate::session_cache::aggregate_cached_entry(
                            &cached, bounds,
                        );
                        return Ok((agg, HashMap::new()));
                    }
                }
            }
        }

        let month_prefix = &bounds.month_start[..7];
        let conn = Connection::open_with_flags(
            &source.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(std::time::Duration::from_secs(1))?;

        if !validate_schema(&conn) {
            return Ok((StatusAggregate::default(), HashMap::new()));
        }

        let time_floor = format!("{}-01", month_prefix);
        let query = "
            SELECT
                json_extract(value, '$.tokenCount.inputTokens') as input_tokens,
                json_extract(value, '$.tokenCount.outputTokens') as output_tokens,
                json_extract(value, '$.modelInfo.modelName') as model,
                json_extract(value, '$.createdAt') as created_at,
                json_extract(value, '$.conversationId') as conversation_id
            FROM cursorDiskKV
            WHERE key LIKE 'bubbleId:%'
                AND json_extract(value, '$.tokenCount.inputTokens') > 0
                AND json_extract(value, '$.createdAt') > ?1
            ORDER BY json_extract(value, '$.createdAt') ASC
        ";

        let mut stmt = conn.prepare(query)?;
        let mut agg = StatusAggregate::default();
        let mut by_day: HashMap<String, (f64, u64)> = HashMap::new();

        let rows = stmt.query_map([&time_floor], |row| {
            Ok((
                row.get::<_, Option<i64>>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?;

        for row in rows.flatten() {
            let (input_opt, output_opt, model_opt, created_at_opt, conv_id_opt) = row;
            let input_tokens = input_opt.unwrap_or(0) as u64;
            let output_tokens = output_opt.unwrap_or(0) as u64;
            if input_tokens == 0 && output_tokens == 0 {
                continue;
            }

            let created_at = created_at_opt.unwrap_or_default();
            let conversation_id = conv_id_opt.unwrap_or_else(|| "unknown".to_string());
            let dedup_key = format!(
                "cursor:{}:{}:{}:{}",
                conversation_id, created_at, input_tokens, output_tokens
            );

            let h = {
                let mut hasher = DefaultHasher::new();
                dedup_key.hash(&mut hasher);
                hasher.finish()
            };
            if seen_keys.contains(&h) {
                continue;
            }
            seen_keys.insert(h);

            let pricing_model = resolve_model(model_opt.as_deref());
            let cost = calculate_cost(&pricing_model, input_tokens, output_tokens, 0, 0, 0, Speed::Standard);

            if created_at.len() >= 19 {
                let ts_prefix = &created_at[..19];
                let day = &created_at[..10];
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

        // Cache results keyed by DB path + mtime (even if empty, to avoid re-querying)
        if let Ok(meta) = std::fs::metadata(&source.path) {
            let mtime_secs = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let file_size = meta.len();
            let cache_key = format!("cursor-db:{}", source.path);
            // Store a sentinel "empty" entry so we know this DB was scanned
            if by_day.is_empty() {
                let mut sentinel = HashMap::new();
                sentinel.insert("_empty".to_string(), (0.0, 0u64));
                crate::session_cache::store_batch(&[(cache_key, mtime_secs, file_size, sentinel)]);
            } else {
                crate::session_cache::store_batch(&[(cache_key, mtime_secs, file_size, by_day.clone())]);
            }
        }

        Ok((agg, by_day))
    }
}
