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
use ratatui::widgets::{Block, Borders, Paragraph, Widget};

use crate::currency::format_cost;
use crate::format::format_tokens;
use crate::tui::App;

use super::aggregate::DashboardData;
use super::theme::{format_number, COL_COST, COL_TITLE};

pub fn render_tabs(buf: &mut Buffer, area: Rect, app: &App, periods: &[(&str, &str)]) {
    let mut tab_spans = vec![Span::raw("  ")];
    for (i, (_, label)) in periods.iter().enumerate() {
        if i == app.period_idx {
            tab_spans.push(Span::styled(
                format!("[ {} ]", label),
                Style::default().bold().fg(COL_TITLE),
            ));
        } else {
            tab_spans.push(Span::styled(
                format!("  {}  ", label),
                Style::default().fg(Color::DarkGray),
            ));
        }
    }
    if app.detected_providers.len() > 1 {
        tab_spans.push(Span::raw("  | "));
        tab_spans.push(Span::styled(
            format!("[p] {}", app.provider_filter),
            Style::default().fg(COL_TITLE).bold(),
        ));
    }
    Paragraph::new(Line::from(tab_spans)).render(area, buf);
}

pub fn render_status_bar(buf: &mut Buffer, area: Rect) {
    let kb = |key: &str| Span::styled(key.to_string(), Style::default().fg(COL_TITLE).bold());
    let lbl = |text: &str| Span::styled(text.to_string(), Style::default().fg(Color::DarkGray));
    let status = Line::from(vec![
        kb("<>"), lbl(" switch   "),
        kb("q"),  lbl(" quit   "),
        kb("1"),  lbl(" today   "),
        kb("2"),  lbl(" week   "),
        kb("3"),  lbl(" 30 days   "),
        kb("4"),  lbl(" month   "),
        kb("p"),  lbl(" provider"),
    ]);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(ratatui::widgets::BorderType::Rounded)
        .border_style(Style::default().fg(COL_TITLE));
    Paragraph::new(status)
        .alignment(Alignment::Center)
        .block(block)
        .render(area, buf);
}

pub fn render_header(buf: &mut Buffer, area: Rect, data: &DashboardData, period_label: &str) {
    let header = vec![
        Line::from(vec![
            Span::raw("  "),
            Span::styled("CodeBurn", Style::default().fg(COL_TITLE).bold()),
            Span::styled(format!("  {}", period_label), Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(vec![
            Span::raw("  "),
            Span::styled(format_cost(data.total_cost), Style::default().fg(COL_COST).bold()),
            Span::styled(" cost   ", Style::default().fg(Color::DarkGray)),
            Span::styled(format_number(data.total_calls), Style::default().fg(Color::White).bold()),
            Span::styled(" calls   ", Style::default().fg(Color::DarkGray)),
            Span::styled(format_number(data.sessions_len as u64), Style::default().fg(Color::White).bold()),
            Span::styled(" sessions   ", Style::default().fg(Color::DarkGray)),
            Span::styled(format!("{:.1}%", data.cache_pct), Style::default().fg(Color::White).bold()),
            Span::styled(" cache hit", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(vec![
            Span::raw("  "),
            Span::styled(
                format!(
                    "{} in   {} out   {} cached   {} written",
                    format_tokens(data.total_input),
                    format_tokens(data.total_output),
                    format_tokens(data.total_cache_read),
                    format_tokens(data.total_cache_write),
                ),
                Style::default().fg(Color::DarkGray),
            ),
        ]),
    ];
    Paragraph::new(header).render(area, buf);
}

/// Matches the JS dashboard: whenever a load is in flight, show tabs +
/// an orange "CodeBurn" panel with "Loading <Period>..." text + the
/// bordered status bar. Hides the rest of the content to avoid mixing
/// stale data with the "in-progress" indicator.
pub fn render_loading(f: &mut Frame, area: Rect, app: &App, periods: &[(&str, &str)]) {
    render_loading_into(f.buffer_mut(), area, app, periods);
}

pub fn render_loading_into(
    buf: &mut Buffer,
    area: Rect,
    app: &App,
    periods: &[(&str, &str)],
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![
            Constraint::Length(1), // tabs
            Constraint::Length(4), // CodeBurn loading panel
            Constraint::Min(0),    // spacer
            Constraint::Length(3), // status bar
        ])
        .split(area);

    render_tabs(buf, chunks[0], app, periods);

    let period_label = periods.get(app.period_idx).map(|p| p.1).unwrap_or("");
    let loading = vec![
        Line::from(Span::styled(
            "CodeBurn",
            Style::default().fg(COL_TITLE).bold(),
        )),
        Line::from(Span::styled(
            format!("Loading {}...", period_label),
            Style::default().fg(Color::DarkGray),
        )),
    ];
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(ratatui::widgets::BorderType::Rounded)
        .border_style(Style::default().fg(COL_TITLE));
    Paragraph::new(loading).block(block).render(chunks[1], buf);

    render_status_bar(buf, chunks[3]);
}
