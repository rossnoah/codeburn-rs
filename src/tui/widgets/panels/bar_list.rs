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
use ratatui::widgets::{Paragraph, Widget};

use super::super::panel::panel;

/// Render a bar-list panel — header line + one row per item, optionally
/// split into `cols` sub-columns (rows flow top-to-bottom within each
/// sub-column, then left-to-right across sub-columns).
///
/// Each caller supplies its own `header` and `row_builder` so the three
/// bar-list panels (Daily, By Activity, By Model) share layout code
/// without sharing row shape.
pub fn render<T>(
    buf: &mut Buffer,
    area: Rect,
    title: &str,
    color: Color,
    items: &[T],
    header: Line<'static>,
    row_builder: impl Fn(&T) -> Line<'static>,
    cols: usize,
    empty_message: &str,
) {
    let cols = cols.max(1);
    let block = panel(title, color);

    if cols == 1 {
        let mut lines = vec![header];
        if items.is_empty() {
            lines.push(Line::styled(
                format!("  {}", empty_message),
                Style::default().fg(Color::DarkGray),
            ));
        } else {
            lines.extend(items.iter().map(&row_builder));
        }
        Paragraph::new(lines).block(block).render(area, buf);
        return;
    }

    // Multi-column: split inner area horizontally, chunk items top-to-bottom.
    let inner = block.inner(area);
    block.render(area, buf);

    let rows_per_col = items.len().div_ceil(cols);
    let col_areas = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(
            (0..cols)
                .map(|_| Constraint::Ratio(1, cols as u32))
                .collect::<Vec<_>>(),
        )
        .split(inner);

    for c in 0..cols {
        let start = c * rows_per_col;
        if start >= items.len() {
            break;
        }
        let end = (start + rows_per_col).min(items.len());
        let mut lines = vec![header.clone()];
        lines.extend(items[start..end].iter().map(&row_builder));
        Paragraph::new(lines).render(col_areas[c], buf);
    }
}
