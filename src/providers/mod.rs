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

pub mod claude;
pub mod codex;
pub mod common;
pub mod copilot;
pub mod cursor;
pub mod opencode;
pub mod pi;

use std::collections::HashMap;
use std::time::SystemTime;

use anyhow::Result;
use dashmap::DashSet;
use rayon::prelude::*;

use crate::types::{ParsedProviderCall, SessionSource, StatusAggregate, StatusBounds};

pub trait Provider: Send + Sync {
    fn name(&self) -> &str;
    fn discover_sessions(&self) -> Result<Vec<SessionSource>>;

    /// Discovery with a hint table: `known[source_path] = project` for every
    /// source already present in the disk cache. Providers whose discovery
    /// is expensive per-file (e.g. codex reads + json-parses the first line
    /// of each rollout just to extract `cwd`) can use the hint to skip that
    /// work for known paths. Default implementation ignores the hint.
    fn discover_sessions_with_hints(
        &self,
        _known: Option<&std::collections::HashMap<String, String>>,
    ) -> Result<Vec<SessionSource>> {
        self.discover_sessions()
    }

    /// Return a set of `(dir_path, mtime_secs)` pairs whose stability
    /// implies the provider's discovery result is unchanged. Directory
    /// mtimes bump on file creation/deletion but not on file append, which
    /// matches what we need (new session files force a re-walk, ongoing
    /// writes don't — per-file stat in `partition_cache` catches those).
    ///
    /// Default: empty fingerprint, which means discovery is always re-walked
    /// for this provider.
    fn discovery_fingerprint(&self) -> Vec<(String, u64)> {
        Vec::new()
    }

    /// `(mtime_secs, size_bytes)` for the backing file of this source, used
    /// as the disk-cache invalidation key. Default: stat `source.path`
    /// directly. Providers whose `SessionSource.path` isn't a regular file
    /// (opencode's `{db_path}:{session_id}` composite) override this.
    /// Returning `None` disables caching for that source.
    fn cache_metadata(&self, source: &SessionSource) -> Option<(u64, u64)> {
        let meta = std::fs::metadata(&source.path).ok()?;
        let mtime = meta
            .modified()
            .ok()?
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_secs();
        Some((mtime, meta.len()))
    }
    fn parse_session(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<String>,
    ) -> Result<Vec<ParsedProviderCall>>;

    /// Parse session with mtime + date range filtering.
    /// `since` filters files by modification time (skip files older than date range).
    /// `date_start`/`date_end` are "YYYY-MM-DDThh:mm:ss" strings for pre-filtering lines.
    fn parse_session_filtered(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<String>,
        _since: Option<SystemTime>,
        _date_start: Option<&str>,
        _date_end: Option<&str>,
    ) -> Result<Vec<ParsedProviderCall>> {
        // Default: fall back to unfiltered parse
        self.parse_session(source, seen_keys)
    }

    /// Fast path for status command: returns only aggregate cost/call data.
    /// Returns (StatusAggregate, per-day costs for caching).
    /// `bounds` contains UTC timestamp boundaries for today/week/month.
    fn parse_session_status(
        &self,
        source: &SessionSource,
        seen_keys: &DashSet<u64>,
        bounds: &StatusBounds,
    ) -> Result<(StatusAggregate, HashMap<String, (f64, u64)>)>;

    /// Batch entry point used by `parse_all_sessions`. Returns one entry per
    /// logical source that produced any calls: `(source_path, project,
    /// calls)`. `source_path` MUST match the `SessionSource.path` the call
    /// came from — it's the cache-key primitive downstream.
    ///
    /// Default implementation fans out to `parse_session_filtered` per
    /// source via rayon. Providers that can amortise setup (e.g. one SQLite
    /// connection across many sessions) should override this but preserve
    /// the per-source attribution in the output.
    fn parse_sources(
        &self,
        sources: &[SessionSource],
        seen_keys: &DashSet<String>,
        since: Option<SystemTime>,
        date_start: Option<&str>,
        date_end: Option<&str>,
    ) -> Vec<(String, String, Vec<ParsedProviderCall>)> {
        sources
            .par_iter()
            .filter_map(|source| {
                let calls = self
                    .parse_session_filtered(source, seen_keys, since, date_start, date_end)
                    .unwrap_or_default();
                if calls.is_empty() {
                    None
                } else {
                    Some((source.path.clone(), source.project.clone(), calls))
                }
            })
            .collect()
    }
}

pub fn get_all_providers() -> Vec<Box<dyn Provider>> {
    vec![
        Box::new(claude::ClaudeProvider),
        Box::new(codex::CodexProvider),
        // Cursor provider is disabled: Cursor no longer writes per-call token
        // counts to the local state.vscdb.
        // `tokenCount.inputTokens`/`outputTokens` = 0, so parsing it would
        // always report $0. Parser code in `providers::cursor` is kept for
        // reference and in case the data layout is restored upstream.
        // Box::new(cursor::CursorProvider),
        Box::new(opencode::OpenCodeProvider),
        Box::new(pi::PiProvider),
        Box::new(copilot::CopilotProvider),
    ]
}

/// Discovers sessions across all providers, optionally scoped by
/// `provider_filter` (`"all"` or a provider `name()`). Callers may pass a
/// `known` table of `source_path → project`: providers whose discovery does
/// expensive per-file validation (e.g. codex reads + parses the first line of
/// each rollout to pick up its `cwd`) skip that work for any path already in
/// the table. Default implementation of
/// `Provider::discover_sessions_with_hints` ignores the hint.
///
/// Discovery is gated by a separate cache at
/// `~/.cache/codeburn/discovery.bin`. For each provider we compute a
/// fingerprint of `(dir_path, mtime)` pairs via
/// `Provider::discovery_fingerprint`; if it matches the cached fingerprint,
/// we return the cached source list and skip the full filesystem walk. A
/// full walk that happens (first run, or any fingerprint mismatch) writes
/// back a fresh cache from a detached thread.
pub fn discover_all_sessions_with_hints(
    provider_filter: Option<&str>,
    known: Option<&std::collections::HashMap<String, String>>,
) -> Result<Vec<SessionSource>> {
    let prof = std::env::var_os("CODEBURN_PROFILE").is_some();
    let providers = get_all_providers();

    let selected: Vec<&Box<dyn Provider>> = providers
        .iter()
        .filter(|p| match provider_filter {
            Some(f) => f == "all" || f == p.name(),
            None => true,
        })
        .collect();

    // Discovery cache records *which files exist*, not what's inside them.
    // `--no-cache` is about bypassing parsed-content caches so we
    // explicitly include this cache even when bypass is set — the parser
    // still re-reads every file.
    let cache = crate::discovery_cache::load();

    // Per-provider: compute fingerprint, then either use cached sources or
    // run the full walk. Both the fingerprinting + walking happen in
    // parallel across providers — they hit disjoint subtrees.
    let per_provider: Vec<(String, Vec<(String, u64)>, Vec<SessionSource>, bool)> = selected
        .par_iter()
        .map(|p| {
            let t = std::time::Instant::now();
            let fingerprint = p.discovery_fingerprint();
            // Empty fingerprint → provider didn't opt in, always re-walk.
            let cache_hit = !fingerprint.is_empty()
                && cache
                    .as_ref()
                    .and_then(|c| c.fingerprint.get(p.name()))
                    .map(|f| f == &fingerprint)
                    .unwrap_or(false);
            let (sources, hit) = if cache_hit {
                let sources = cache
                    .as_ref()
                    .and_then(|c| c.sources.get(p.name()).cloned())
                    .unwrap_or_default();
                (sources, true)
            } else {
                (
                    p.discover_sessions_with_hints(known).unwrap_or_default(),
                    false,
                )
            };
            if prof {
                eprintln!(
                    "[prof]   discover {:<10} {:>6.1} ms ({} sources){}",
                    p.name(),
                    t.elapsed().as_secs_f64() * 1000.0,
                    sources.len(),
                    if hit { " [cache]" } else { "" }
                );
            }
            (p.name().to_string(), fingerprint, sources, hit)
        })
        .collect();

    // Decide whether to persist a fresh discovery cache. If any provider
    // actually walked, the cache is stale and we write back. Persist runs
    // detached so the hot path doesn't wait on disk.
    let any_walked = per_provider.iter().any(|(_, _, _, hit)| !*hit);
    if any_walked {
        let mut new_cache = crate::discovery_cache::DiscoveryCache::default();
        // Start from the prior cache so providers that *didn't* walk this
        // run still contribute their previously-captured fingerprint.
        if let Some(c) = &cache {
            for (k, v) in &c.fingerprint {
                new_cache.fingerprint.insert(k.clone(), v.clone());
            }
            for (k, v) in &c.sources {
                new_cache.sources.insert(k.clone(), v.clone());
            }
        }
        for (name, fp, sources, hit) in &per_provider {
            if !*hit {
                new_cache.fingerprint.insert(name.clone(), fp.clone());
                new_cache.sources.insert(name.clone(), sources.clone());
            }
        }
        let h = std::thread::spawn(move || crate::discovery_cache::persist(&new_cache));
        crate::parser::PENDING_PERSIST.lock().unwrap().push(h);
    }

    Ok(per_provider
        .into_iter()
        .flat_map(|(_, _, sources, _)| sources)
        .collect())
}
