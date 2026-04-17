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

//! Output memoization for the static `report`/`today`/`month` paths.
//!
//! After the static path computes its result, it writes the rendered output
//! plus a fingerprint to `~/.cache/codeburn/output-{period}-{provider}.bin`.
//! Subsequent runs with the same fingerprint skip the entire parse pipeline
//! and `write_all` the cached bytes straight to stdout — typically <1 ms.
//!
//! The fingerprint covers everything that can change the output:
//!   - mtime of `report-cache.bin` (any new parse miss bumps it)
//!   - mtime of `discovery.bin`    (any new/deleted source bumps it)
//!   - session-files signature     (hash of every known session file's
//!                                  mtime + size — catches appends to an
//!                                  existing session jsonl while the user
//!                                  keeps working in Claude / Cursor / etc.
//!                                  between runs, which don't bump the
//!                                  discovery-dir mtimes)
//!   - today's local date          (period windows shift at the day boundary)
//!   - period + provider filter
//!
//! Cache file layout (little endian):
//!   u64 fingerprint
//!   u32 output_len
//!   bytes output  (raw bytes that the static renderer would have printed)

use std::hash::Hasher;
use std::io::Write;
use std::path::PathBuf;

const CACHE_DIR: &str = ".cache/codeburn";

fn cache_dir() -> PathBuf {
    dirs::home_dir().unwrap_or_default().join(CACHE_DIR)
}

/// Format-specific cache key. Each render mode (plain text aggregate vs.
/// the rich ratatui dashboard) gets its own cache file because the bytes
/// they produce are not interchangeable.
fn output_cache_path(period: &str, provider: &str, format: &str) -> PathBuf {
    cache_dir().join(format!("output-{}-{}-{}.bin", format, period, provider))
}

fn report_cache_path() -> PathBuf {
    cache_dir().join("report-cache.bin")
}

fn discovery_cache_path() -> PathBuf {
    cache_dir().join("discovery.bin")
}

/// `(mtime_secs, file_size)` for `path`. Returns `(0, 0)` when missing —
/// callers fold that into the fingerprint so a deleted file invalidates
/// the cache the same way as a modified one.
fn meta(path: &std::path::Path) -> (u64, u64) {
    match std::fs::metadata(path) {
        Ok(m) => {
            let mtime = m
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            (mtime, m.len())
        }
        Err(_) => (0, 0),
    }
}

/// Local-day count since the unix epoch. Used as the day-rollover signal
/// in the fingerprint — bumps once a day even though `report-cache.bin`
/// might not change. Without it, a "today" query at 23:55 would still
/// return yesterday's cached output at 00:05.
fn today_local_packed() -> u64 {
    use chrono::{Datelike, Local};
    let d = Local::now().date_naive();
    // Pack year/month/day into a single u64. Cheaper than `num_days_from_ce`.
    (d.year() as u64) * 512 + (d.month() as u64) * 32 + (d.day() as u64)
}

/// `extra` lets format-specific renderers fold things like terminal size
/// into the fingerprint — for the rich dashboard a different `cols` /
/// `rows` would produce different bytes, so the cache must invalidate.
/// `session_sig` is the `u64` hash of every known session file's
/// `(path, mtime, size)` triple (see `parser::stat_all_sources`) — this
/// is what catches in-place appends to existing jsonl files when the
/// user keeps working in Claude / Cursor / etc. between runs.
fn fingerprint(period: &str, provider: &str, format: &str, extra: u64, session_sig: u64) -> u64 {
    let (rep_mtime, rep_size) = meta(&report_cache_path());
    let (disc_mtime, disc_size) = meta(&discovery_cache_path());
    let day = today_local_packed();
    let mut h = rustc_hash::FxHasher::default();
    h.write_u8(period.len() as u8);
    h.write(period.as_bytes());
    h.write_u8(provider.len() as u8);
    h.write(provider.as_bytes());
    h.write_u8(format.len() as u8);
    h.write(format.as_bytes());
    h.write_u64(extra);
    h.write_u64(day);
    h.write_u64(rep_mtime);
    h.write_u64(rep_size);
    h.write_u64(disc_mtime);
    h.write_u64(disc_size);
    h.write_u64(session_sig);
    // Bake the cache file format version into the fingerprint so a binary
    // with a changed renderer can never serve stale output from before
    // its upgrade.
    h.write_u32(VERSION);
    h.finish()
}

// Bumped to 3: fingerprint now folds in a session-files signature so
// in-place appends to existing jsonl files correctly invalidate.
const VERSION: u32 = 3;

/// Try to serve the report straight from a previous run's cached output.
/// Returns `true` if a hit was found and printed; the caller should exit.
/// `session_sig` is the caller's freshly-computed session-files signature
/// (see `parser::stat_all_sources`) — it's the only piece of the
/// fingerprint that catches in-place appends to a session jsonl.
pub fn try_serve(period: &str, provider: &str, format: &str, extra: u64, session_sig: u64) -> bool {
    let path = output_cache_path(period, provider, format);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(_) => return false,
    };
    if bytes.len() < 12 {
        return false;
    }
    let stored_fp = u64::from_le_bytes(match bytes[..8].try_into() {
        Ok(a) => a,
        Err(_) => return false,
    });
    let len = u32::from_le_bytes(match bytes[8..12].try_into() {
        Ok(a) => a,
        Err(_) => return false,
    }) as usize;
    if 12 + len > bytes.len() {
        return false;
    }
    let want_fp = fingerprint(period, provider, format, extra, session_sig);
    if stored_fp != want_fp {
        return false;
    }
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(&bytes[12..12 + len]).is_ok()
}

/// Persist the rendered output keyed on the current fingerprint. Called
/// after a fresh compute so the next identical run can hit `try_serve`.
pub fn store(period: &str, provider: &str, format: &str, extra: u64, session_sig: u64, output: &[u8]) {
    let fp = fingerprint(period, provider, format, extra, session_sig);
    let path = output_cache_path(period, provider, format);
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let mut buf = Vec::with_capacity(12 + output.len());
    buf.extend_from_slice(&fp.to_le_bytes());
    buf.extend_from_slice(&(output.len() as u32).to_le_bytes());
    buf.extend_from_slice(output);
    let tmp = path.with_extension("bin.tmp");
    if std::fs::write(&tmp, &buf).is_ok() {
        let _ = std::fs::rename(&tmp, &path);
    }
}
