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

//! Filesystem watcher for the TUI — detects new AI session data written to
//! disk while the dashboard is open so we can auto-refresh.

use std::fs;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime};

use crate::providers::{claude, codex, copilot, cursor, opencode, pi};

/// How often to poll the filesystem for changes while the TUI is idle.
/// 5 s feels live enough for "Claude is writing" without stat-storming.
pub const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Tracks the newest mtime seen across every data source. Consumers poll it
/// periodically; [`FsWatcher::poll_changed`] returns `true` only when a new
/// mtime has been observed since the last poll (rate-limited by
/// [`POLL_INTERVAL`]).
///
/// Construction is lazy — [`FsWatcher::new`] does no I/O. The initial
/// baseline mtime is stat'd on the first `poll_changed` call that actually
/// runs (after the rate-limit window elapses), so the one-shot non-TTY
/// render path — which constructs an App but never polls — pays nothing.
pub struct FsWatcher {
    baseline_taken: bool,
    last_mtime: Option<SystemTime>,
    last_poll: Instant,
}

impl FsWatcher {
    pub fn new() -> Self {
        FsWatcher {
            baseline_taken: false,
            last_mtime: None,
            last_poll: Instant::now(),
        }
    }

    /// Refresh the baseline mtime without signaling a change. Call this
    /// after an explicit refresh so the watcher doesn't fire a redundant
    /// reparse on its next poll.
    pub fn resync(&mut self) {
        self.last_mtime = max_activity_mtime();
        self.baseline_taken = true;
    }

    /// Check for fresh session data. Respects the rate limit — returns
    /// `false` and does nothing if `POLL_INTERVAL` hasn't elapsed since the
    /// last poll. Returns `true` only when a newer mtime is observed; on
    /// `true` the internal baseline is already advanced.
    pub fn poll_changed(&mut self) -> bool {
        if self.last_poll.elapsed() < POLL_INTERVAL {
            return false;
        }
        self.last_poll = Instant::now();
        let current = max_activity_mtime();
        if !self.baseline_taken {
            // Lazy initialisation — first real poll seeds the baseline
            // without signaling a change. Subsequent polls can compare.
            self.last_mtime = current;
            self.baseline_taken = true;
            return false;
        }
        if current != self.last_mtime && current.is_some() {
            self.last_mtime = current;
            true
        } else {
            false
        }
    }
}

/// Scan every location we might read session data from and return the newest
/// mtime observed. Pure stats — no writes, no file contents read. Takes
/// ~3 ms on a typical ~1000-file install.
fn max_activity_mtime() -> Option<SystemTime> {
    fn stat_mtime(path: &Path) -> Option<SystemTime> {
        fs::metadata(path).ok()?.modified().ok()
    }

    fn walk_jsonl(dir: &Path, out: &mut Option<SystemTime>) {
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let ft = match entry.file_type() {
                Ok(ft) => ft,
                Err(_) => continue,
            };
            if ft.is_file() && path.extension().map(|e| e == "jsonl").unwrap_or(false) {
                if let Some(mt) = stat_mtime(&path) {
                    *out = Some(out.map_or(mt, |prev| prev.max(mt)));
                }
            } else if ft.is_dir() {
                walk_jsonl(&path, out);
            }
        }
    }

    let mut max = None;
    // Claude (projects + subagents, recursive — honors CLAUDE_CONFIG_DIR).
    walk_jsonl(&claude::get_projects_dir(), &mut max);
    // Claude Desktop (local-agent-mode-sessions, platform-specific).
    walk_jsonl(&claude::get_desktop_sessions_dir(), &mut max);
    // Codex sessions (date-bucketed subdirs — honors CODEX_HOME).
    walk_jsonl(&codex::get_codex_dir().join("sessions"), &mut max);
    walk_jsonl(&pi::get_pi_sessions_dir(), &mut max);
    walk_jsonl(&copilot::get_copilot_dir(), &mut max);

    // Cursor DB (single file).
    if let Some(mt) = stat_mtime(&cursor::get_cursor_db_path()) {
        max = Some(max.map_or(mt, |prev| prev.max(mt)));
    }

    // OpenCode DBs (honors XDG_DATA_HOME).
    if let Ok(entries) = fs::read_dir(opencode::get_data_dir()) {
        for e in entries.flatten() {
            let p = e.path();
            let name = e.file_name();
            let s = name.to_string_lossy();
            if s.starts_with("opencode") && s.ends_with(".db") {
                if let Some(mt) = stat_mtime(&p) {
                    max = Some(max.map_or(mt, |prev| prev.max(mt)));
                }
            }
        }
    }

    max
}
