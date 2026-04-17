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
//! File format (little endian throughout):
//!
//! ```text
//! [ magic 8 bytes = b"CODEBRN1" ]
//! [ version u32  = 1            ]
//! [ n_entries u32               ]
//! per-entry header:
//!     key_len  u16, key  bytes
//!     proj_len u16, proj bytes
//!     mtime    u64
//!     size     u64
//!     blob_len u32
//!     blob     bytes (bincode(Vec<ParsedProviderCall>))
//! ```
//!
//! The linear format keeps the loader trivially fast (~200 µs for 1k
//! entries) and the blobs are contiguous so rayon's parallel decode doesn't
//! fight over the same CPU cache lines.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use memmap2::Mmap;
use serde::{Deserialize, Serialize};

use crate::types::ParsedProviderCall;

const MAGIC: &[u8; 8] = b"CODEBRN1";
// v2: ParsedProviderCall gained `user_message_timestamp` for accurate turn
// grouping (same-text repeat messages were collapsing into one turn).
const VERSION: u32 = 2;

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

    /// Decode the cached calls for `entry`. Typically called inside a rayon
    /// `par_iter` so many blobs decode simultaneously.
    pub fn decode(&self, entry: &EntryHeader) -> Option<Vec<ParsedProviderCall>> {
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
                blob_offset,
                blob_len,
            },
        );
    }
    Some(entries)
}

/// One fresh parse result to write back. `blob` must already be the bincode
/// serialised bytes of `Vec<ParsedProviderCall>` — the caller serialises
/// once so large blobs never cross a lock.
pub struct NewEntry {
    pub key: String,
    pub mtime: u64,
    pub size: u64,
    pub project: String,
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
        total += 2 + e.key.len() + 2 + e.project.len() + 8 + 8 + 4 + e.blob.len();
    }
    for (k, h) in snapshot.entries.iter() {
        if seen.contains(k.as_str()) {
            continue;
        }
        total += 2 + k.len() + 2 + h.project.len() + 8 + 8 + 4 + h.blob_len;
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
        write_entry(&mut buf, &e.key, &e.project, e.mtime, e.size, &e.blob);
    }
    for (k, h) in snapshot.entries.iter() {
        if seen.contains(k.as_str()) {
            continue;
        }
        let blob = snapshot.blob(h);
        write_entry(&mut buf, k, &h.project, h.mtime, h.size, blob);
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


fn write_entry(buf: &mut Vec<u8>, key: &str, project: &str, mtime: u64, size: u64, blob: &[u8]) {
    buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
    buf.extend_from_slice(key.as_bytes());
    buf.extend_from_slice(&(project.len() as u16).to_le_bytes());
    buf.extend_from_slice(project.as_bytes());
    buf.extend_from_slice(&mtime.to_le_bytes());
    buf.extend_from_slice(&size.to_le_bytes());
    buf.extend_from_slice(&(blob.len() as u32).to_le_bytes());
    buf.extend_from_slice(blob);
}

/// Helper: serialise a `Vec<ParsedProviderCall>` to bincode bytes. Callers
/// store the result into a `NewEntry.blob`.
pub fn encode_calls(calls: &[ParsedProviderCall]) -> Vec<u8> {
    bincode::serialize(calls).unwrap_or_default()
}

// Shut up dead-code warnings on the serde Derive path when nothing else
// imports this module yet.
#[allow(dead_code)]
#[derive(Serialize, Deserialize)]
struct _BincodeSmoke {
    ok: bool,
}
