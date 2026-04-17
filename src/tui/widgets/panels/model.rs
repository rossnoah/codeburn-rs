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

use super::super::aggregate::ModelAgg;
use super::super::bar::make_bar;
use super::super::panel::column_header;
use super::super::theme::{format_number, COL_COST, COL_MODEL};
use super::bar_list;

pub fn render(
    buf: &mut Buffer,
    area: Rect,
    bw: u16,
    models: &[(String, ModelAgg)],
    cols: usize,
) {
    let shown = &models[..models.len().min(10)];
    let max_cost = shown.iter().map(|m| m.1.cost).fold(0.0f64, f64::max);
    let header = column_header(
        bw as usize + 1 + 14,
        &[("cost", 8), ("cache", 7), ("calls", 7)],
    );

    bar_list::render(
        buf,
        area,
        "By Model",
        COL_MODEL,
        shown,
        header,
        |(name, m)| {
            let display = if name.len() > 14 { &name[..14] } else { name };
            let all_input = m.input + m.cache_read + m.cache_write;
            let cache_label = if all_input > 0 {
                format!("{:.1}%", (m.cache_read as f64 / all_input as f64) * 100.0)
            } else {
                "-".to_string()
            };
            let mut spans = Vec::new();
            spans.extend(make_bar(m.cost, max_cost, bw));
            spans.push(Span::raw(" "));
            spans.push(Span::styled(
                format!("{:<14}", display),
                Style::default().fg(Color::White),
            ));
            spans.push(Span::styled(
                format!("{:>8}", format_cost(m.cost)),
                Style::default().fg(COL_COST).bold(),
            ));
            spans.push(Span::styled(
                format!("{:>7}", cache_label),
                Style::default().fg(Color::White),
            ));
            spans.push(Span::styled(
                format!("{:>7}", format_number(m.calls)),
                Style::default().fg(Color::White),
            ));
            Line::from(spans)
        },
        cols,
        "No models",
    );
}
