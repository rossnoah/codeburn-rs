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

//! Shared disk cache for parsed provider calls.
//!
//! The cache is a single mmap'd binary file at
//! `~/.cache/codeburn/report-cache.bin`. Every `parse_all_sessions` run opens
//! it read-only and maps it into every rayon thread — lookups are `&HashMap`
//! hits, and blobs are decoded in parallel via rayon. Writes happen once per
//! run, atomically via `write-to-tmp + rename`.
//!
//! Invalidation is per-entry: each entry is keyed on the source path plus the
//! `(mtime, size)` of its backing file. A row stays valid until the file
//! changes. Providers that back many sources with a single file (opencode,
//! cursor) return that file's metadata for all their sources, so a single
//! mtime bump invalidates every row at once — desired behaviour.
//!
//! File format (little endian throughout), version 3:
//!
//! ```text
//! [ magic 8 bytes = b"CODEBRN1" ]
//! [ version u32  = 3            ]
//! [ n_entries u32               ]
//! per-entry header:
//!     key_len     u16, key  bytes
//!     proj_len    u16, proj bytes
//!     mtime       u64
//!     size        u64
//!     summary_len u32, summary bytes (custom packed format — see SummaryIter)
//!     blob_len    u32, blob    bytes (bincode(Vec<ParsedProviderCall>))
//! ```
//!
//! The summary blob is a tightly packed pre-aggregated view used by the static
//! report path. It avoids the bincode-decode + Vec<String> allocations needed
//! for the full call list — a 10x speedup on the cached path.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use memmap2::Mmap;
use serde::{Deserialize, Serialize};

use crate::types::{ParsedProviderCall, SessionSummary};

const MAGIC: &[u8; 8] = b"CODEBRN1";
// v3: added per-entry summary blob (compact pre-aggregated view) for fast
// static-report path. Old v2 caches are discarded on first run.
// v4: Claude provider stopped pre-filtering by date at parse time — older
// caches hold period-shaped subsets and must be rejected so a wider-period
// query can't get a truncated cache hit.
// v5: same poisoning fix extended to all providers — `parse_misses` now
// passes date filters to providers ONLY in --no-cache mode. Earlier v4
// caches written by the cached path may still hold pi/codex empty entries
// from a "today" run, so they have to be discarded.
// v6: the main `blob` field is now `bincode(Vec<SessionSummary>)` — already
// classified + aggregated — instead of the raw `Vec<ParsedProviderCall>`.
// Cuts the hot path by skipping bincode-deserialise of ~100 MB of per-call
// Strings + classify_turn for ~20k turns on every run. Old v5 caches hold
// raw-call blobs whose shape no longer matches, so we have to reject them.
const VERSION: u32 = 6;

fn cache_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join(".cache/codeburn/report-cache.bin")
}

/// Offsets + metadata for one cached entry. `blob_offset..blob_offset+blob_len`
/// slices into the mmap to produce the bincode bytes.
#[derive(Debug, Clone)]
pub struct EntryHeader {
    pub mtime: u64,
    pub size: u64,
    pub project: String,
    summary_offset: usize,
    summary_len: usize,
    blob_offset: usize,
    blob_len: usize,
}

/// A loaded cache. Holds the mmap alive; lookups are cheap HashMap hits.
/// Cloning is cheap — the mmap/entries live behind an `Arc` so we can pass
/// the snapshot into a detached compose+persist thread without touching the
/// hot path.
#[derive(Clone)]
pub struct CacheSnapshot {
    mmap: Option<Arc<Mmap>>,
    entries: Arc<HashMap<String, EntryHeader>>,
}

impl CacheSnapshot {
    /// Load the cache. Returns an empty snapshot on any kind of trouble
    /// (missing file, bad magic, truncation) — callers just see "no cache".
    pub fn load() -> Self {
        match Self::try_load() {
            Ok(s) => s,
            Err(_) => Self::empty(),
        }
    }

    pub fn empty() -> Self {
        Self {
            mmap: None,
            entries: Arc::new(HashMap::new()),
        }
    }

    fn try_load() -> std::io::Result<Self> {
        let path = cache_path();
        let file = fs::File::open(&path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let entries = parse_index(&mmap).unwrap_or_default();
        Ok(Self {
            mmap: Some(Arc::new(mmap)),
            entries: Arc::new(entries),
        })
    }

    /// Look up a cache hit by source path + (mtime, size). Returns the
    /// entry header only when all three match — any staleness is a miss.
    pub fn lookup(&self, key: &str, mtime: u64, size: u64) -> Option<&EntryHeader> {
        let e = self.entries.get(key)?;
        if e.mtime == mtime && e.size == size {
            Some(e)
        } else {
            None
        }
    }

    /// Slice the blob for an entry back out of the mmap.
    pub fn blob(&self, entry: &EntryHeader) -> &[u8] {
        match &self.mmap {
            Some(m) => &m[entry.blob_offset..entry.blob_offset + entry.blob_len],
            None => &[],
        }
    }

    /// Slice the summary bytes for an entry. Empty when the entry has no
    /// summary (legacy entries without one).
    pub fn summary_bytes(&self, entry: &EntryHeader) -> &[u8] {
        match &self.mmap {
            Some(m) => &m[entry.summary_offset..entry.summary_offset + entry.summary_len],
            None => &[],
        }
    }

    /// Decode the cached session summaries for `entry`. Each source's
    /// `Vec<SessionSummary>` is pre-classified at the miss-parse side, so the
    /// cached-hit path can skip both the raw-call bincode decode and the
    /// `classify_turn` / `build_session_summary` work. Typically called
    /// inside a rayon `par_iter` so many blobs decode simultaneously.
    pub fn decode(&self, entry: &EntryHeader) -> Option<Vec<SessionSummary>> {
        let blob = self.blob(entry);
        bincode::deserialize(blob).ok()
    }

    /// Iterate over every entry (used when rewriting the file — we need to
    /// preserve unrelated rows).
    pub fn iter(&self) -> impl Iterator<Item = (&str, &EntryHeader)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }

}

fn parse_index(buf: &[u8]) -> Option<HashMap<String, EntryHeader>> {
    if buf.len() < 16 {
        return None;
    }
    if &buf[..8] != MAGIC {
        return None;
    }
    let version = u32::from_le_bytes(buf[8..12].try_into().ok()?);
    if version != VERSION {
        return None;
    }
    let n = u32::from_le_bytes(buf[12..16].try_into().ok()?) as usize;
    let mut pos = 16usize;

    let mut entries: HashMap<String, EntryHeader> = HashMap::with_capacity(n);
    for _ in 0..n {
        if pos + 2 > buf.len() {
            return None;
        }
        let key_len = u16::from_le_bytes(buf[pos..pos + 2].try_into().ok()?) as usize;
        pos += 2;
        if pos + key_len > buf.len() {
            return None;
        }
        let key = std::str::from_utf8(&buf[pos..pos + key_len]).ok()?.to_string();
        pos += key_len;

        if pos + 2 > buf.len() {
            return None;
        }
        let proj_len = u16::from_le_bytes(buf[pos..pos + 2].try_into().ok()?) as usize;
        pos += 2;
        if pos + proj_len > buf.len() {
            return None;
        }
        let project = std::str::from_utf8(&buf[pos..pos + proj_len]).ok()?.to_string();
        pos += proj_len;

        if pos + 8 + 8 + 4 > buf.len() {
            return None;
        }
        let mtime = u64::from_le_bytes(buf[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let size = u64::from_le_bytes(buf[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let summary_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if pos + summary_len > buf.len() {
            return None;
        }
        let summary_offset = pos;
        pos += summary_len;

        if pos + 4 > buf.len() {
            return None;
        }
        let blob_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if pos + blob_len > buf.len() {
            return None;
        }
        let blob_offset = pos;
        pos += blob_len;

        entries.insert(
            key,
            EntryHeader {
                mtime,
                size,
                project,
                summary_offset,
                summary_len,
                blob_offset,
                blob_len,
            },
        );
    }
    Some(entries)
}

/// One fresh parse result to write back. `blob` must already be the bincode
/// serialised bytes of `Vec<ParsedProviderCall>` — the caller serialises
/// once so large blobs never cross a lock. `summary` is the compact
/// pre-aggregated representation (see `encode_summary`).
pub struct NewEntry {
    pub key: String,
    pub mtime: u64,
    pub size: u64,
    pub project: String,
    pub summary: Vec<u8>,
    pub blob: Vec<u8>,
}

/// Compose the final cache file bytes directly from an mmap'd snapshot + a
/// list of fresh entries. Skips the intermediate `owned_base` copy — we can
/// stream old blobs straight from the mmap into the output buffer. Runs on
/// the detached persist thread so the memcpy stays off the hot path.
pub fn compose_from_snapshot(snapshot: &CacheSnapshot, fresh: &[NewEntry]) -> Vec<u8> {
    let mut seen: std::collections::HashSet<&str> =
        std::collections::HashSet::with_capacity(fresh.len());
    for e in fresh {
        seen.insert(e.key.as_str());
    }

    // Guess at final size so we don't reallocate during the appends.
    let mut total = 16usize; // magic + version + n_entries
    for e in fresh {
        total += 2 + e.key.len() + 2 + e.project.len() + 8 + 8 + 4 + e.summary.len() + 4 + e.blob.len();
    }
    for (k, h) in snapshot.entries.iter() {
        if seen.contains(k.as_str()) {
            continue;
        }
        total += 2 + k.len() + 2 + h.project.len() + 8 + 8 + 4 + h.summary_len + 4 + h.blob_len;
    }

    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&VERSION.to_le_bytes());
    let n = fresh.len()
        + snapshot
            .entries
            .iter()
            .filter(|(k, _)| !seen.contains(k.as_str()))
            .count();
    buf.extend_from_slice(&(n as u32).to_le_bytes());
    for e in fresh {
        write_entry(&mut buf, &e.key, &e.project, e.mtime, e.size, &e.summary, &e.blob);
    }
    for (k, h) in snapshot.entries.iter() {
        if seen.contains(k.as_str()) {
            continue;
        }
        let summary = snapshot.summary_bytes(h);
        let blob = snapshot.blob(h);
        write_entry(&mut buf, k, &h.project, h.mtime, h.size, summary, blob);
    }
    buf
}

/// Write already-composed bytes to the cache file atomically. Fire from a
/// detached thread so the main flow never waits on disk.
pub fn persist_bytes(bytes: Vec<u8>) -> std::io::Result<()> {
    let path = cache_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("bin.tmp");
    {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(&bytes)?;
    }
    fs::rename(&tmp, &path)?;
    Ok(())
}


fn write_entry(buf: &mut Vec<u8>, key: &str, project: &str, mtime: u64, size: u64, summary: &[u8], blob: &[u8]) {
    buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
    buf.extend_from_slice(key.as_bytes());
    buf.extend_from_slice(&(project.len() as u16).to_le_bytes());
    buf.extend_from_slice(project.as_bytes());
    buf.extend_from_slice(&mtime.to_le_bytes());
    buf.extend_from_slice(&size.to_le_bytes());
    buf.extend_from_slice(&(summary.len() as u32).to_le_bytes());
    buf.extend_from_slice(summary);
    buf.extend_from_slice(&(blob.len() as u32).to_le_bytes());
    buf.extend_from_slice(blob);
}

/// Helper: serialise a `Vec<SessionSummary>` to bincode bytes. Callers
/// store the result into a `NewEntry.blob`. The summaries are the already-
/// classified / aggregated per-session data for one source, so the cached-
/// hit read path skips both `classify_turn` and the full per-call bincode
/// deserialize.
pub fn encode_session_summaries(sessions: &[SessionSummary]) -> Vec<u8> {
    bincode::serialize(sessions).unwrap_or_default()
}


/// Pre-aggregate the calls of one source into a packed summary blob. The
/// static report path iterates these blobs without any String allocation
/// (only unique session IDs end up materialised on the read side).
///
/// Format (little endian):
/// ```text
/// n_buckets u32
/// per bucket:
///     ts_prefix [19 u8]       // "YYYY-MM-DDThh:mm:ss"
///                              //   or 19 NULL bytes for "no timestamp"
///                              //   (special-cased: always included)
///     session_id_len u8
///     session_id [bytes]
///     cost f64
///     calls u32
///     input u64
///     output u64
///     cache_read u64
///     cache_write u64
/// ```
///
/// One bucket per (ts_prefix, session_id) pair. The aggregator's date
/// filter compares `ts_prefix` lexicographically against the run's
/// `[start, end]` 19-char window — the same comparison the original
/// per-call loop did, so output is byte-identical. Calls whose timestamp
/// is shorter than 19 chars get a zero prefix and are always included
/// (mirrors the original `aggregate_static`'s behaviour of falling
/// through the date check for unparseable timestamps).
pub fn encode_summary(calls: &[ParsedProviderCall]) -> Vec<u8> {
    use std::collections::HashMap;
    #[derive(Default)]
    struct B {
        cost: f64,
        calls: u32,
        input: u64,
        output: u64,
        cache_read: u64,
        cache_write: u64,
    }
    // Key: (ts_prefix_19, session_id). ts_prefix is borrowed from the call
    // (or a static all-zero sentinel for untimestamped calls). session_id
    // is borrowed from the call. We only own them on write.
    static ZERO_TS: &str = "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
    let mut buckets: HashMap<(&str, &str), B> = HashMap::new();
    for c in calls {
        let ts_prefix: &str = if c.timestamp.len() >= 19 {
            &c.timestamp[..19]
        } else {
            ZERO_TS
        };
        let sid = c.session_id.as_str();
        let b = buckets.entry((ts_prefix, sid)).or_default();
        b.cost += c.cost_usd;
        b.calls += 1;
        b.input += c.input_tokens;
        b.output += c.output_tokens;
        b.cache_read += c.cache_read_input_tokens;
        b.cache_write += c.cache_creation_input_tokens;
    }

    let mut out = Vec::with_capacity(4 + buckets.len() * 72);
    out.extend_from_slice(&(buckets.len() as u32).to_le_bytes());
    for ((ts_prefix, sid), b) in buckets {
        let ts_bytes = ts_prefix.as_bytes();
        let mut ts_pad = [0u8; 19];
        let n = ts_bytes.len().min(19);
        ts_pad[..n].copy_from_slice(&ts_bytes[..n]);
        out.extend_from_slice(&ts_pad);
        let sid_bytes = sid.as_bytes();
        let sid_len = sid_bytes.len().min(255) as u8;
        out.push(sid_len);
        out.extend_from_slice(&sid_bytes[..sid_len as usize]);
        out.extend_from_slice(&b.cost.to_le_bytes());
        out.extend_from_slice(&b.calls.to_le_bytes());
        out.extend_from_slice(&b.input.to_le_bytes());
        out.extend_from_slice(&b.output.to_le_bytes());
        out.extend_from_slice(&b.cache_read.to_le_bytes());
        out.extend_from_slice(&b.cache_write.to_le_bytes());
    }
    out
}

/// One bucket from a summary blob — borrowed slices into the mmap, so
/// iteration is allocation-free.
#[derive(Debug)]
pub struct SummaryBucket<'a> {
    /// 19-byte ASCII timestamp prefix "YYYY-MM-DDThh:mm:ss", or 19 NULL
    /// bytes for buckets that came from calls without a parseable
    /// timestamp (always include those — see `has_timestamp`).
    pub ts_prefix: &'a [u8],
    pub session_id: &'a [u8], // utf-8 bytes
    pub cost: f64,
    pub calls: u32,
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}

impl<'a> SummaryBucket<'a> {
    /// True if this bucket has a real timestamp. Untimestamped buckets
    /// are always included by the date filter, matching the original
    /// per-call aggregator's behaviour.
    pub fn has_timestamp(&self) -> bool {
        self.ts_prefix[0] != 0
    }
}

/// Walk a packed summary blob without allocating. Stops on truncation /
/// corruption (returns whatever was read so far).
pub struct SummaryIter<'a> {
    buf: &'a [u8],
    pos: usize,
    remaining: u32,
}

impl<'a> SummaryIter<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        if buf.len() < 4 {
            return SummaryIter { buf, pos: 0, remaining: 0 };
        }
        let n = u32::from_le_bytes(buf[..4].try_into().unwrap_or([0; 4]));
        SummaryIter {
            buf,
            pos: 4,
            remaining: n,
        }
    }
}

impl<'a> Iterator for SummaryIter<'a> {
    type Item = SummaryBucket<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let buf = self.buf;
        let mut p = self.pos;
        if p + 19 + 1 > buf.len() {
            self.remaining = 0;
            return None;
        }
        let ts_prefix = &buf[p..p + 19];
        p += 19;
        let sid_len = buf[p] as usize;
        p += 1;
        if p + sid_len + 8 + 4 + 8 + 8 + 8 + 8 > buf.len() {
            self.remaining = 0;
            return None;
        }
        let session_id = &buf[p..p + sid_len];
        p += sid_len;
        let cost = f64::from_le_bytes(buf[p..p + 8].try_into().unwrap());
        p += 8;
        let calls = u32::from_le_bytes(buf[p..p + 4].try_into().unwrap());
        p += 4;
        let input = u64::from_le_bytes(buf[p..p + 8].try_into().unwrap());
        p += 8;
        let output = u64::from_le_bytes(buf[p..p + 8].try_into().unwrap());
        p += 8;
        let cache_read = u64::from_le_bytes(buf[p..p + 8].try_into().unwrap());
        p += 8;
        let cache_write = u64::from_le_bytes(buf[p..p + 8].try_into().unwrap());
        p += 8;
        self.pos = p;
        self.remaining -= 1;
        Some(SummaryBucket {
            ts_prefix,
            session_id,
            cost,
            calls,
            input,
            output,
            cache_read,
            cache_write,
        })
    }
}

// Shut up dead-code warnings on the serde Derive path when nothing else
// imports this module yet.
#[allow(dead_code)]
#[derive(Serialize, Deserialize)]
struct _BincodeSmoke {
    ok: bool,
}
