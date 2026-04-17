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

use super::theme::gradient_color;

pub fn make_bar(value: f64, max_value: f64, width: u16) -> Vec<Span<'static>> {
    if max_value <= 0.0 || width == 0 {
        return vec![Span::raw(" ".repeat(width as usize))];
    }
    let ratio = (value / max_value).min(1.0);
    let filled = (ratio * width as f64).round() as usize;
    let mut spans = Vec::new();
    for i in 0..filled {
        let pct = if filled > 1 {
            i as f64 / (filled - 1) as f64
        } else {
            0.5
        };
        spans.push(Span::styled(
            "\u{2588}",
            Style::default().fg(gradient_color(pct)),
        ));
    }
    if filled < width as usize {
        spans.push(Span::styled(
            "\u{2591}".repeat(width as usize - filled),
            Style::default().fg(Color::DarkGray),
        ));
    }
    spans
}
