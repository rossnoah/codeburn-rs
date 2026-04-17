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

use crate::currency::format_cost;
use crate::types::ProjectSummary;

use super::super::bar::make_bar;
use super::super::panel::{column_header, panel};
use super::super::theme::{COL_COST, COL_PROJECT};

fn shorten_project(path: &str) -> String {
    let home = dirs::home_dir()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_default();
    let stripped = if !home.is_empty() && path.starts_with(&home) {
        &path[home.len()..]
    } else {
        path
    };
    stripped.trim_start_matches('/').to_string()
}

pub fn render(buf: &mut Buffer, area: Rect, bw: u16, projects: &[ProjectSummary]) {
    let max_cost = projects.iter().map(|p| p.total_cost_usd).fold(0.0f64, f64::max);
    let inner = area.width.saturating_sub(4) as usize;
    const COL_COST_W: usize = 8;
    const COL_AVG_W: usize = 7;
    const COL_SESS_W: usize = 6;
    let name_width = inner
        .saturating_sub(bw as usize + 1 + COL_COST_W + COL_AVG_W + COL_SESS_W)
        .max(8);

    let mut lines = vec![column_header(
        bw as usize + 1 + name_width,
        &[("cost", COL_COST_W), ("avg/s", COL_AVG_W), ("sess", COL_SESS_W)],
    )];

    for p in projects.iter().take(8) {
        let name_full = shorten_project(&p.project_path);
        let name = if name_full.len() > name_width {
            &name_full[..name_width]
        } else {
            &name_full
        };
        let avg_cost = if !p.sessions.is_empty() {
            format_cost(p.total_cost_usd / p.sessions.len() as f64)
        } else {
            "-".to_string()
        };
        let mut spans = Vec::new();
        spans.extend(make_bar(p.total_cost_usd, max_cost, bw));
        spans.push(Span::raw(" "));
        spans.push(Span::styled(
            format!("{:<width$}", name, width = name_width),
            Style::default().fg(Color::DarkGray),
        ));
        spans.push(Span::styled(
            format!("{:>width$}", format_cost(p.total_cost_usd), width = COL_COST_W),
            Style::default().fg(COL_COST).bold(),
        ));
        spans.push(Span::styled(
            format!("{:>width$}", avg_cost, width = COL_AVG_W),
            Style::default().fg(COL_COST),
        ));
        spans.push(Span::styled(
            format!("{:>width$}", p.sessions.len(), width = COL_SESS_W),
            Style::default().fg(Color::White),
        ));
        lines.push(Line::from(spans));
    }
    if lines.len() <= 1 {
        lines.push(Line::styled("  No projects", Style::default().fg(Color::DarkGray)));
    }
    Paragraph::new(lines)
        .block(panel("By Project", COL_PROJECT))
        .render(area, buf);
}
