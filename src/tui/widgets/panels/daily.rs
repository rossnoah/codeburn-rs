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

use super::super::bar::make_bar;
use super::super::panel::column_header;
use super::super::theme::{format_number, COL_COST, COL_DAILY};
use super::bar_list;

pub fn render(
    buf: &mut Buffer,
    area: Rect,
    bw: u16,
    daily: &[(String, f64, u64)],
    cols: usize,
) {
    let max_cost = daily.iter().map(|d| d.1).fold(0.0f64, f64::max);
    let header = column_header(6 + bw as usize + 1, &[("cost", 8), ("calls", 7)]);

    bar_list::render(
        buf,
        area,
        "Daily Activity",
        COL_DAILY,
        daily,
        header,
        |(label, cost, calls)| {
            let mut spans = vec![Span::styled(
                format!("{:<6}", label),
                Style::default().fg(Color::DarkGray),
            )];
            spans.extend(make_bar(*cost, max_cost, bw));
            spans.push(Span::raw(" "));
            spans.push(Span::styled(
                format!("{:>8}", format_cost(*cost)),
                Style::default().fg(COL_COST),
            ));
            spans.push(Span::styled(
                format!(" {:>6}", format_number(*calls)),
                Style::default().fg(Color::White),
            ));
            Line::from(spans)
        },
        cols,
        "No activity",
    );
}
