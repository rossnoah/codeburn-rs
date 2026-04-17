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

use super::aggregate::DashboardData;
use super::panels::{activity, calls, daily, model, project};

/// Choose how many sub-columns a bar-list panel should split into.
/// Keeps 1 column for short lists; splits once we cross `split_at` rows
/// AND the panel has enough inner width to host another `col_width`-wide
/// sub-column.
pub fn column_count(
    items_len: usize,
    panel_width: u16,
    col_width: u16,
    split_at: usize,
) -> usize {
    if items_len <= split_at {
        return 1;
    }
    let inner = panel_width.saturating_sub(2);
    let max_cols = (inner / col_width.max(1)).max(1) as usize;
    let desired_cols = items_len.div_ceil(split_at);
    desired_cols.min(max_cols).max(1)
}

// Per-panel per-row character widths (bar + label + stat columns).
// Used by `column_count` to decide whether a sub-column fits.
//   label(6) + bar(bw) + space(1) + cost(8) + calls(7) = 22 + bw
const DAILY_COL_WIDTH_EXTRA: u16 = 22;
//   bar(bw) + space(1) + label(14) + cost(8) + turns(6) + 1-shot(7) = 36 + bw
const ACTIVITY_COL_WIDTH_EXTRA: u16 = 36;
//   bar(bw) + space(1) + name(14) + cost(8) + cache(7) + calls(7) = 37 + bw
const MODEL_COL_WIDTH_EXTRA: u16 = 37;

const DAILY_SPLIT_AT: usize = 16;
const ACTIVITY_SPLIT_AT: usize = 7;
const MODEL_SPLIT_AT: usize = 5;

const PROJECTS_CAP: usize = 8;
const CATEGORIES_CAP: usize = 13;
const MODELS_CAP: usize = 10;
const CALLS_CAP: usize = 10;

fn daily_layout(panel_width: u16, bw: u16, data: &DashboardData) -> (usize, u16) {
    let cols = column_count(
        data.daily.len(),
        panel_width,
        DAILY_COL_WIDTH_EXTRA + bw,
        DAILY_SPLIT_AT,
    );
    let rows = data.daily.len().div_ceil(cols.max(1)) as u16;
    (cols, rows)
}

fn activity_layout(panel_width: u16, bw: u16, data: &DashboardData) -> (usize, u16) {
    let shown = data.categories.len().min(CATEGORIES_CAP);
    let cols = column_count(
        shown,
        panel_width,
        ACTIVITY_COL_WIDTH_EXTRA + bw,
        ACTIVITY_SPLIT_AT,
    );
    let rows = shown.div_ceil(cols.max(1)) as u16;
    (cols, rows)
}

fn model_layout(panel_width: u16, bw: u16, data: &DashboardData) -> (usize, u16) {
    let shown = data.models.len().min(MODELS_CAP);
    let cols = column_count(
        shown,
        panel_width,
        MODEL_COL_WIDTH_EXTRA + bw,
        MODEL_SPLIT_AT,
    );
    let rows = shown.div_ceil(cols.max(1)) as u16;
    (cols, rows)
}

/// Natural (uncompressed) height the content panels want. When the terminal
/// is shorter than this, the caller renders into a tall off-screen buffer
/// and blits a scroll window — so every panel keeps its full height.
pub fn natural_height(wide: bool, content_width: u16, bw: u16, data: &DashboardData) -> u16 {
    let panel_w = if wide { content_width / 2 } else { content_width };
    let (_, daily_rows) = daily_layout(panel_w, bw, data);
    let (_, activity_rows) = activity_layout(panel_w, bw, data);
    let (_, model_rows) = model_layout(panel_w, bw, data);

    // Each panel height = data_rows + 1 column header + 2 borders.
    if wide {
        let row1 = daily_rows.max(data.projects.len().min(PROJECTS_CAP) as u16);
        let row2 = activity_rows.max(model_rows);
        let row3 = (data.tools.len().min(CALLS_CAP) as u16)
            .max(data.bash_cmds.len().min(CALLS_CAP) as u16);
        row1 + 3 + row2 + 3 + row3 + 3 + data.mcp_servers.len().min(CALLS_CAP) as u16 + 3
    } else {
        let mut h = daily_rows + 3
            + (data.projects.len().min(PROJECTS_CAP) as u16 + 3)
            + (activity_rows + 3)
            + (model_rows + 3)
            + (data.tools.len().min(CALLS_CAP) as u16 + 3)
            + (data.bash_cmds.len().min(CALLS_CAP) as u16 + 3);
        if !data.mcp_servers.is_empty() {
            h += data.mcp_servers.len().min(CALLS_CAP) as u16 + 3;
        }
        h
    }
}

pub fn render_wide(buf: &mut Buffer, area: Rect, bw: u16, data: &DashboardData) {
    let panel_w = area.width / 2;
    let (daily_cols, daily_rows) = daily_layout(panel_w, bw, data);
    let (activity_cols, activity_rows) = activity_layout(panel_w, bw, data);
    let (model_cols, model_rows) = model_layout(panel_w, bw, data);

    let row1_rows = daily_rows.max(data.projects.len().min(PROJECTS_CAP) as u16);
    let row2_rows = activity_rows.max(model_rows);
    let row3_rows = (data.tools.len().min(CALLS_CAP) as u16)
        .max(data.bash_cmds.len().min(CALLS_CAP) as u16);

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![
            Constraint::Length(row1_rows + 3),
            Constraint::Length(row2_rows + 3),
            Constraint::Length(row3_rows + 3),
            Constraint::Min(0), // mcp
        ])
        .split(area);

    let row1 = split_halves(rows[0]);
    daily::render(buf, row1[0], bw, &data.daily, daily_cols);
    project::render(buf, row1[1], bw, data.projects);

    let row2 = split_halves(rows[1]);
    activity::render(buf, row2[0], bw, &data.categories, activity_cols);
    model::render(buf, row2[1], bw, &data.models, model_cols);

    let row3 = split_halves(rows[2]);
    calls::render_tools(buf, row3[0], bw, &data.tools);
    calls::render_bash(buf, row3[1], bw, &data.bash_cmds);

    calls::render_mcp(buf, rows[3], bw, &data.mcp_servers);
}

pub fn render_narrow(buf: &mut Buffer, area: Rect, bw: u16, data: &DashboardData) {
    let panel_w = area.width;
    let (daily_cols, daily_rows) = daily_layout(panel_w, bw, data);
    let (activity_cols, activity_rows) = activity_layout(panel_w, bw, data);
    let (model_cols, model_rows) = model_layout(panel_w, bw, data);

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![
            Constraint::Length(daily_rows + 3),
            Constraint::Length(data.projects.len().min(PROJECTS_CAP) as u16 + 3),
            Constraint::Length(activity_rows + 3),
            Constraint::Length(model_rows + 3),
            Constraint::Length(data.tools.len().min(CALLS_CAP) as u16 + 3),
            Constraint::Length(data.bash_cmds.len().min(CALLS_CAP) as u16 + 3),
            Constraint::Min(0),
        ])
        .split(area);

    daily::render(buf, rows[0], bw, &data.daily, daily_cols);
    project::render(buf, rows[1], bw, data.projects);
    activity::render(buf, rows[2], bw, &data.categories, activity_cols);
    model::render(buf, rows[3], bw, &data.models, model_cols);
    calls::render_tools(buf, rows[4], bw, &data.tools);
    calls::render_bash(buf, rows[5], bw, &data.bash_cmds);
    if !data.mcp_servers.is_empty() {
        calls::render_mcp(buf, rows[6], bw, &data.mcp_servers);
    }
}

fn split_halves(area: Rect) -> std::rc::Rc<[Rect]> {
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints(vec![Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area)
}
