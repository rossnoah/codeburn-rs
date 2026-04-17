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

//! Helpers shared across provider implementations.
//!
//! Anything on the hot parse path (simd-json / memmem / rayon fan-outs / SQL
//! projections) lives in the provider's own module — this file is for
//! one-line duplication cleanup, not for factoring out parse logic.

use std::collections::HashMap;

/// Build an O(1) lookup map from a static `(raw_name, display_name)` slice.
/// Call once at the top of a parse pass and reuse for every row.
/// Used by providers that walk many tool invocations per session (codex,
/// opencode).
pub fn build_tool_map(
    entries: &[(&'static str, &'static str)],
) -> HashMap<&'static str, &'static str> {
    entries.iter().copied().collect()
}

/// Linear lookup for providers with few tool invocations per session
/// (copilot, pi). Avoids building a HashMap when the cost of iterating a
/// ~10-entry slice once per turn beats setting up the map.
pub fn lookup_tool(entries: &[(&str, &str)], raw: &str) -> String {
    for (k, v) in entries {
        if *k == raw {
            return (*v).to_string();
        }
    }
    raw.to_string()
}
