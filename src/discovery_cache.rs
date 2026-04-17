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

//! Discovery-result cache.
//!
//! Full filesystem discovery across providers costs ~7-10 ms even when
//! everything is in the OS page cache (it does hundreds of `readdir` +
//! `stat` calls). That's more than the remaining work in the cached-hit
//! path, so we short-circuit it: before walking, each provider stats a
//! small set of "root directories" and hands back a fingerprint pair
//! `(path, mtime)`. If every pair matches what was fingerprinted on the
//! previous run, we trust the cached `Vec<SessionSource>` instead of
//! re-walking.
//!
//! Directory mtimes bump on file creation (and `rename`, `unlink`) but
//! NOT on file append, which is exactly the invalidation rule we want:
//! new session files are detected (dir mtime changes) while ongoing
//! writes to existing files don't force a full re-walk (per-file stat
//! in `partition_cache` already catches those).

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::types::SessionSource;

#[derive(Serialize, Deserialize, Default)]
pub struct DiscoveryCache {
    /// Provider name → fingerprint of (dir_path, mtime) pairs. Flat so
    /// bincode stays zero-ceremony.
    pub fingerprint: HashMap<String, Vec<(String, u64)>>,
    /// Provider name → list of sources discovered last time.
    pub sources: HashMap<String, Vec<SessionSource>>,
}

fn cache_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join(".cache/codeburn/discovery.bin")
}

/// Read the cache from disk. Returns `None` on any read/decode failure — the
/// caller falls back to a full walk.
pub fn load() -> Option<DiscoveryCache> {
    let bytes = fs::read(cache_path()).ok()?;
    bincode::deserialize(&bytes).ok()
}

/// Persist the cache atomically. Called from a detached thread after a full
/// walk so the main flow never waits on disk.
pub fn persist(cache: &DiscoveryCache) {
    let bytes = match bincode::serialize(cache) {
        Ok(b) => b,
        Err(_) => return,
    };
    let path = cache_path();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let tmp = path.with_extension("bin.tmp");
    if fs::write(&tmp, &bytes).is_ok() {
        let _ = fs::rename(&tmp, &path);
    }
}

/// Stat one path and return its mtime in whole seconds, or `0` if the path
/// doesn't exist. Using `0` as the "missing" sentinel folds into fingerprint
/// equality — a deleted dir will mismatch the previously-recorded mtime and
/// correctly force a re-walk.
pub fn mtime_secs(path: &std::path::Path) -> u64 {
    fs::metadata(path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
