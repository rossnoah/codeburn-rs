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

mod aggregate;
mod bar;
mod chrome;
mod dashboard;
mod panel;
mod panels;
mod theme;

use ratatui::buffer::Buffer;
use ratatui::prelude::*;

use crate::tui::layout::{bar_width, dash_width, is_wide};
use crate::types::ProjectSummary;

use super::App;

pub fn render(
    f: &mut Frame,
    projects: &[ProjectSummary],
    app: &App,
    periods: &[(&str, &str)],
) {
    let area = f.area();
    let dw = dash_width(area.width);
    let wide = is_wide(dw);
    let bw = bar_width(if wide { dw / 2 - 4 } else { dw - 4 });

    if app.loading {
        chrome::render_loading(f, area, app, periods);
        return;
    }

    let data = aggregate::build(projects, app.current_period());
    let period_label = periods.get(app.period_idx).map(|p| p.1).unwrap_or("");

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![
            Constraint::Length(1), // period tabs
            Constraint::Length(3), // header (title, stats, tokens)
            Constraint::Min(0),    // content
            Constraint::Length(3), // bordered status bar
        ])
        .split(area);

    chrome::render_tabs(f.buffer_mut(), chunks[0], app, periods);
    chrome::render_header(f.buffer_mut(), chunks[1], &data, period_label);

    // Content: render at natural height. If the terminal is shorter, render
    // into an off-screen buffer and blit a scrolled window into the frame so
    // every panel keeps its full size and the user can scroll vertically.
    let content_area = chunks[2];
    let natural_h = dashboard::natural_height(wide, content_area.width, bw, &data);

    if natural_h <= content_area.height {
        render_content(f.buffer_mut(), content_area, bw, wide, &data);
        app.last_max_scroll.set(0);
    } else {
        let virtual_area = Rect::new(0, 0, content_area.width, natural_h);
        let mut vbuf = Buffer::empty(virtual_area);
        render_content(&mut vbuf, virtual_area, bw, wide, &data);
        blit_scrolled(f.buffer_mut(), content_area, &vbuf, natural_h, app.scroll_offset);
        app.last_max_scroll.set(natural_h - content_area.height);
    }

    chrome::render_status_bar(f.buffer_mut(), chunks[3]);
}

fn render_content(
    buf: &mut Buffer,
    area: Rect,
    bw: u16,
    wide: bool,
    data: &aggregate::DashboardData,
) {
    if wide {
        dashboard::render_wide(buf, area, bw, data);
    } else {
        dashboard::render_narrow(buf, area, bw, data);
    }
}

fn blit_scrolled(
    dst: &mut Buffer,
    dst_area: Rect,
    src: &Buffer,
    src_height: u16,
    scroll_offset: u16,
) {
    let max_scroll = src_height.saturating_sub(dst_area.height);
    let scroll = scroll_offset.min(max_scroll);
    for dy in 0..dst_area.height {
        let src_y = scroll + dy;
        if src_y >= src_height {
            break;
        }
        for dx in 0..dst_area.width {
            if let Some(cell) = src.cell((dx, src_y)).cloned() {
                if let Some(target) = dst.cell_mut((dst_area.x + dx, dst_area.y + dy)) {
                    *target = cell;
                }
            }
        }
    }
}
