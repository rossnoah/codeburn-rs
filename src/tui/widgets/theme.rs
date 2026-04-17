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

use ratatui::style::Color;

use crate::types::TaskCategory;

pub const COL_DAILY: Color = Color::Rgb(0x5B, 0x9E, 0xF5);
pub const COL_PROJECT: Color = Color::Rgb(0x5B, 0xF5, 0xA0);
pub const COL_MODEL: Color = Color::Rgb(0xE0, 0x5B, 0xF5);
pub const COL_ACTIVITY: Color = Color::Rgb(0xF5, 0xC8, 0x5B);
pub const COL_TOOLS: Color = Color::Rgb(0x5B, 0xF5, 0xE0);
pub const COL_MCP: Color = Color::Rgb(0xF5, 0x5B, 0xE0);
pub const COL_BASH: Color = Color::Rgb(0xF5, 0xA0, 0x5B);

// Accent used for every cost figure in the dashboard (matches JS gold `#FFD700`).
pub const COL_COST: Color = Color::Rgb(0xFF, 0xD7, 0x00);
// Title orange (matches JS `#FF8C42`).
pub const COL_TITLE: Color = Color::Rgb(0xFF, 0x8C, 0x42);

pub fn category_color(cat: TaskCategory) -> Color {
    match cat {
        TaskCategory::Coding => Color::Rgb(0x5B, 0x9E, 0xF5),
        TaskCategory::Debugging => Color::Rgb(0xF5, 0x5B, 0x5B),
        TaskCategory::Feature => Color::Rgb(0x5B, 0xF5, 0x8C),
        TaskCategory::Refactoring => Color::Rgb(0xF5, 0xE0, 0x5B),
        TaskCategory::Testing => Color::Rgb(0xE0, 0x5B, 0xF5),
        TaskCategory::Exploration => Color::Rgb(0x5B, 0xF5, 0xE0),
        TaskCategory::Planning => Color::Rgb(0x7B, 0x9E, 0xF5),
        TaskCategory::Delegation => Color::Rgb(0xF5, 0xC8, 0x5B),
        TaskCategory::Git => Color::Rgb(0xCC, 0xCC, 0xCC),
        TaskCategory::BuildDeploy => Color::Rgb(0x5B, 0xF5, 0xA0),
        TaskCategory::Conversation => Color::Rgb(0x88, 0x88, 0x88),
        TaskCategory::Brainstorming => Color::Rgb(0xF5, 0x5B, 0xE0),
        TaskCategory::General => Color::Rgb(0x66, 0x66, 0x66),
    }
}

pub fn gradient_color(pct: f64) -> Color {
    fn lerp(a: f64, b: f64, t: f64) -> u8 {
        (a + t * (b - a)).round() as u8
    }
    if pct <= 0.33 {
        let t = pct / 0.33;
        Color::Rgb(lerp(91.0, 245.0, t), lerp(158.0, 200.0, t), lerp(245.0, 91.0, t))
    } else if pct <= 0.66 {
        let t = (pct - 0.33) / 0.33;
        Color::Rgb(lerp(245.0, 255.0, t), lerp(200.0, 140.0, t), lerp(91.0, 66.0, t))
    } else {
        let t = (pct - 0.66) / 0.34;
        Color::Rgb(lerp(255.0, 245.0, t), lerp(140.0, 91.0, t), lerp(66.0, 91.0, t))
    }
}

/// Thousands separator: "10877" -> "10,877".
pub fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}
