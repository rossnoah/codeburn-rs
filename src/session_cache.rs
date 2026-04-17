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
use std::sync::LazyLock;
use std::sync::Mutex;

use rusqlite::Connection;

use crate::types::{StatusAggregate, StatusBounds};

static CACHE_CONN: LazyLock<Mutex<Option<Connection>>> =
    LazyLock::new(|| Mutex::new(open_cache_db()));

fn get_cache_path() -> std::path::PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join(".cache/codeburn/status-cache.db")
}

fn open_cache_db() -> Option<Connection> {
    let path = get_cache_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let conn = Connection::open(&path).ok()?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA cache_size=-4000;
         CREATE TABLE IF NOT EXISTS file_costs (
             file_path TEXT NOT NULL,
             file_mtime INTEGER NOT NULL,
             file_size INTEGER NOT NULL,
             day TEXT NOT NULL,
             cost_usd REAL NOT NULL,
             call_count INTEGER NOT NULL,
             PRIMARY KEY (file_path, day)
         );
         CREATE INDEX IF NOT EXISTS idx_file_costs_day ON file_costs(day);
         CREATE TABLE IF NOT EXISTS session_summaries (
             file_path TEXT NOT NULL,
             file_mtime INTEGER NOT NULL,
             file_size INTEGER NOT NULL,
             session_id TEXT NOT NULL,
             project TEXT NOT NULL,
             summary_json TEXT NOT NULL,
             PRIMARY KEY (file_path, session_id)
         );
         CREATE INDEX IF NOT EXISTS idx_session_file ON session_summaries(file_path);",
    )
    .ok()?;
    Some(conn)
}

/// Entry for a single file in the cache: mtime, size, and per-day costs.
#[derive(Debug)]
pub struct CachedFileEntry {
    pub mtime_secs: u64,
    pub file_size: u64,
    pub costs_by_day: HashMap<String, (f64, u64)>,
}

/// Look up a single file in the cache (for non-Claude providers like Cursor).
pub fn lookup_cached(
    file_path: &str,
    mtime_secs: u64,
    file_size: u64,
) -> Option<CachedFileEntry> {
    let guard = CACHE_CONN.lock().ok()?;
    let conn = guard.as_ref()?;

    let mut stmt = conn
        .prepare_cached(
            "SELECT day, cost_usd, call_count FROM file_costs
             WHERE file_path = ?1 AND file_mtime = ?2 AND file_size = ?3",
        )
        .ok()?;

    let rows = stmt
        .query_map(
            rusqlite::params![file_path, mtime_secs as i64, file_size as i64],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, f64>(1)?,
                    row.get::<_, i64>(2)? as u64,
                ))
            },
        )
        .ok()?;

    let mut costs_by_day = HashMap::new();
    let mut found_any = false;
    for row in rows.flatten() {
        found_any = true;
        costs_by_day.insert(row.0, (row.1, row.2));
    }

    if found_any {
        Some(CachedFileEntry {
            mtime_secs,
            file_size,
            costs_by_day,
        })
    } else {
        None
    }
}

/// Batch-load ALL cached entries into a HashMap keyed by file_path.
/// This turns N individual queries into a single table scan — much faster
/// than doing one query per file through a Mutex.
pub fn load_all_cached() -> HashMap<String, CachedFileEntry> {
    let guard = match CACHE_CONN.lock() {
        Ok(g) => g,
        Err(_) => return HashMap::new(),
    };
    let conn = match guard.as_ref() {
        Some(c) => c,
        None => return HashMap::new(),
    };

    let mut stmt = match conn.prepare(
        "SELECT file_path, file_mtime, file_size, day, cost_usd, call_count FROM file_costs",
    ) {
        Ok(s) => s,
        Err(_) => return HashMap::new(),
    };

    let rows = match stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)? as u64,
            row.get::<_, i64>(2)? as u64,
            row.get::<_, String>(3)?,
            row.get::<_, f64>(4)?,
            row.get::<_, i64>(5)? as u64,
        ))
    }) {
        Ok(r) => r,
        Err(_) => return HashMap::new(),
    };

    let mut map: HashMap<String, CachedFileEntry> = HashMap::new();
    for row in rows.flatten() {
        let (file_path, mtime_secs, file_size, day, cost_usd, call_count) = row;
        let entry = map.entry(file_path).or_insert_with(|| CachedFileEntry {
            mtime_secs,
            file_size,
            costs_by_day: HashMap::new(),
        });
        entry.costs_by_day.insert(day, (cost_usd, call_count));
    }

    map
}

/// Batch-store multiple files' costs into the cache.
/// Takes a vec of (file_path, mtime, size, per-day costs).
pub fn store_batch(entries: &[(String, u64, u64, HashMap<String, (f64, u64)>)]) {
    if entries.is_empty() {
        return;
    }
    let guard = match CACHE_CONN.lock() {
        Ok(g) => g,
        Err(_) => return,
    };
    let conn = match guard.as_ref() {
        Some(c) => c,
        None => return,
    };

    let _ = conn.execute_batch("BEGIN TRANSACTION;");

    for (file_path, mtime_secs, file_size, costs_by_day) in entries {
        // Delete old entries for this file
        let _ = conn.execute(
            "DELETE FROM file_costs WHERE file_path = ?1",
            rusqlite::params![file_path],
        );

        let mut stmt = match conn.prepare_cached(
            "INSERT INTO file_costs (file_path, file_mtime, file_size, day, cost_usd, call_count)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        ) {
            Ok(s) => s,
            Err(_) => continue,
        };

        for (day, (cost, calls)) in costs_by_day {
            let _ = stmt.execute(rusqlite::params![
                file_path,
                *mtime_secs as i64,
                *file_size as i64,
                day,
                cost,
                *calls as i64,
            ]);
        }
    }

    let _ = conn.execute_batch("COMMIT;");
}

/// Returns true if any of the cached file's UTC days overlap with a local
/// time boundary, meaning we can't accurately bucket that day's entries from
/// the cache alone (some entries fall inside the range, some outside).
pub fn has_boundary_day(entry: &CachedFileEntry, bounds: &StatusBounds) -> bool {
    let boundaries = [
        &bounds.today_start[..10],
        &bounds.today_end[..10],
        &bounds.week_start[..10],
        &bounds.month_start[..10],
    ];
    entry
        .costs_by_day
        .keys()
        .any(|d| d.len() >= 10 && boundaries.contains(&&d[..10]))
}

pub fn aggregate_cached_entry(
    entry: &CachedFileEntry,
    bounds: &StatusBounds,
) -> StatusAggregate {
    let mut agg = StatusAggregate::default();
    // The cache stores costs by UTC day. For UTC days entirely inside a range,
    // their aggregate is correct. For boundary days, `has_boundary_day` should
    // have flagged the file for re-parse, so we don't hit this path for them.
    let today_start_day = &bounds.today_start[..10];
    let today_end_day = &bounds.today_end[..10];
    let week_start_day = &bounds.week_start[..10];
    let month_start_day = &bounds.month_start[..10];
    let month_end_day = &bounds.month_end[..10];

    for (day, (cost, calls)) in &entry.costs_by_day {
        if day.len() < 10 {
            continue;
        }
        let d = day.as_str();
        // Include only UTC days that are strictly inside each range
        // (i.e., not on a boundary day).
        if d > today_start_day && d < today_end_day {
            agg.today_cost += cost;
            agg.today_calls += calls;
        }
        if d > week_start_day && d < month_end_day {
            agg.week_cost += cost;
            agg.week_calls += calls;
        }
        if d > month_start_day && d < month_end_day {
            agg.month_cost += cost;
            agg.month_calls += calls;
        }
    }
    agg
}
