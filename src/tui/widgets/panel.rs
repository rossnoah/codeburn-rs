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

use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders};

pub fn panel(title: &str, color: Color) -> Block<'_> {
    Block::default()
        .borders(Borders::ALL)
        .border_type(ratatui::widgets::BorderType::Rounded)
        .border_style(Style::default().fg(color))
        .title(format!(" {} ", title))
        .title_style(Style::default().fg(color).bold())
}

/// One dim header row for a panel. `pad` is the space before the first
/// right-aligned column. Each `(label, width)` pairs a label with the
/// width of the data column below it — so padStart(width) aligns the
/// label's right edge with the data's right edge.
pub fn column_header(pad: usize, cols: &[(&str, usize)]) -> Line<'static> {
    let mut spans = vec![Span::raw(" ".repeat(pad))];
    for (label, width) in cols {
        spans.push(Span::styled(
            format!("{:>width$}", label, width = width),
            Style::default().fg(Color::DarkGray),
        ));
    }
    Line::from(spans)
}
