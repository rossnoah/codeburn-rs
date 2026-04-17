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

pub const MIN_WIDE: u16 = 90;
pub const MAX_WIDTH: u16 = 160;

pub fn is_wide(width: u16) -> bool {
    width >= MIN_WIDE
}

pub fn dash_width(term_width: u16) -> u16 {
    term_width.min(MAX_WIDTH).max(80)
}

pub fn bar_width(inner_width: u16) -> u16 {
    (inner_width.saturating_sub(30)).clamp(6, 10)
}
