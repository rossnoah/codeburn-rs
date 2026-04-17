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

use ratatui::buffer::Buffer;
use ratatui::prelude::*;

use crate::currency::format_cost;
use crate::types::{CategoryStats, TaskCategory};

use super::super::bar::make_bar;
use super::super::panel::column_header;
use super::super::theme::{category_color, format_number, COL_ACTIVITY, COL_COST, COL_TITLE};
use super::bar_list;

pub fn render(
    buf: &mut Buffer,
    area: Rect,
    bw: u16,
    categories: &[(TaskCategory, CategoryStats)],
    cols: usize,
) {
    let max_cost = categories.iter().map(|c| c.1.cost_usd).fold(0.0f64, f64::max);
    let header = column_header(
        bw as usize + 1 + 14,
        &[("cost", 8), ("turns", 6), ("1-shot", 7)],
    );

    bar_list::render(
        buf,
        area,
        "By Activity",
        COL_ACTIVITY,
        categories,
        header,
        |(cat, stats)| {
            let oneshot = if stats.edit_turns > 0 {
                // Match JS `Math.round((oneShot/edit)*100)`: round half up (away
                // from zero for positives). Rust's `{:.0}` uses banker's rounding,
                // which disagrees with JS on x.5 values (e.g. 12.5 → 12 vs 13).
                // Color check uses the displayed integer so 99.5 → "100%" → green,
                // matching the JS string comparison.
                let pct_int = ((stats.one_shot_turns as f64 / stats.edit_turns as f64) * 100.0)
                    .round() as i64;
                let color = if pct_int >= 100 {
                    Color::Rgb(0x5B, 0xF5, 0x8C)
                } else {
                    COL_TITLE
                };
                Span::styled(format!(" {:>5}%", pct_int), Style::default().fg(color))
            } else {
                Span::styled(format!(" {:>6}", "-"), Style::default().fg(Color::DarkGray))
            };

            let mut spans = Vec::new();
            spans.extend(make_bar(stats.cost_usd, max_cost, bw));
            spans.push(Span::raw(" "));
            spans.push(Span::styled(
                format!("{:<14}", cat.label()),
                Style::default().fg(category_color(*cat)).bold(),
            ));
            spans.push(Span::styled(
                format!("{:>8}", format_cost(stats.cost_usd)),
                Style::default().fg(COL_COST).bold(),
            ));
            spans.push(Span::styled(
                format!(" {:>5}", format_number(stats.turns)),
                Style::default().fg(Color::White),
            ));
            spans.push(oneshot);
            Line::from(spans)
        },
        cols,
        "No activity",
    );
}
