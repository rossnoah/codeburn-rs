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

use super::super::bar::make_bar;
use super::super::panel::{column_header, panel};
use super::super::theme::{format_number, COL_BASH, COL_MCP, COL_TOOLS};

fn render_calls_panel(
    buf: &mut Buffer,
    area: Rect,
    bw: u16,
    items: &[(String, u64)],
    title: &str,
    color: Color,
    empty_message: &str,
) {
    let max_calls = items.iter().map(|t| t.1).max().unwrap_or(0);
    let inner = area.width.saturating_sub(4) as usize;
    let name_width = inner.saturating_sub(bw as usize + 1 + 7 + 1).max(8);
    let mut lines = vec![column_header(bw as usize + 1 + name_width, &[("calls", 7)])];
    for (name, calls) in items.iter().take(10) {
        let display = if name.len() > name_width {
            &name[..name_width]
        } else {
            name
        };
        let mut spans = Vec::new();
        spans.extend(make_bar(*calls as f64, max_calls as f64, bw));
        spans.push(Span::raw(" "));
        spans.push(Span::styled(
            format!("{:<width$}", display, width = name_width),
            Style::default().fg(Color::White),
        ));
        spans.push(Span::styled(
            format!("{:>7}", format_number(*calls)),
            Style::default().fg(Color::White),
        ));
        lines.push(Line::from(spans));
    }
    if lines.len() <= 1 {
        lines.push(Line::styled(
            format!("  {}", empty_message),
            Style::default().fg(Color::DarkGray),
        ));
    }
    Paragraph::new(lines).block(panel(title, color)).render(area, buf);
}

pub fn render_tools(buf: &mut Buffer, area: Rect, bw: u16, tools: &[(String, u64)]) {
    render_calls_panel(buf, area, bw, tools, "Core Tools", COL_TOOLS, "No tools");
}

pub fn render_bash(buf: &mut Buffer, area: Rect, bw: u16, cmds: &[(String, u64)]) {
    render_calls_panel(buf, area, bw, cmds, "Shell Commands", COL_BASH, "No commands");
}

pub fn render_mcp(buf: &mut Buffer, area: Rect, bw: u16, servers: &[(String, u64)]) {
    render_calls_panel(buf, area, bw, servers, "MCP Servers", COL_MCP, "No MCP usage");
}
