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

use std::collections::HashMap;

use crate::types::{CategoryStats, ProjectSummary, TaskCategory};

#[derive(Default, Clone, Copy)]
pub struct ModelAgg {
    pub calls: u64,
    pub cost: f64,
    pub input: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}

pub struct DashboardData<'a> {
    pub projects: &'a [ProjectSummary],
    pub total_cost: f64,
    pub total_calls: u64,
    pub total_input: u64,
    pub total_output: u64,
    pub total_cache_read: u64,
    pub total_cache_write: u64,
    pub cache_pct: f64,
    pub sessions_len: usize,
    pub daily: Vec<(String, f64, u64)>,
    pub categories: Vec<(TaskCategory, CategoryStats)>,
    pub models: Vec<(String, ModelAgg)>,
    pub tools: Vec<(String, u64)>,
    pub bash_cmds: Vec<(String, u64)>,
    pub mcp_servers: Vec<(String, u64)>,
}

pub fn build<'a>(projects: &'a [ProjectSummary], period: &str) -> DashboardData<'a> {
    let sessions: Vec<_> = projects.iter().flat_map(|p| &p.sessions).collect();
    let total_cost: f64 = projects.iter().map(|p| p.total_cost_usd).sum();
    let total_calls: u64 = projects.iter().map(|p| p.total_api_calls).sum();
    let total_input: u64 = sessions.iter().map(|s| s.total_input_tokens).sum();
    let total_output: u64 = sessions.iter().map(|s| s.total_output_tokens).sum();
    let total_cache_read: u64 = sessions.iter().map(|s| s.total_cache_read_tokens).sum();
    let total_cache_write: u64 = sessions.iter().map(|s| s.total_cache_write_tokens).sum();

    // Matches the JS dashboard: cache hit % is cache_read / (input + cache_read + cache_write).
    // Omitting cache_write shrinks the denominator and inflates the percentage.
    let all_input = total_input + total_cache_read + total_cache_write;
    let cache_pct = if all_input > 0 {
        (total_cache_read as f64 / all_input as f64) * 100.0
    } else {
        0.0
    };

    // Model breakdown. cache_read + cache_write are needed for the
    // per-model cache-hit column rendered by the model panel.
    let mut model_totals: HashMap<String, ModelAgg> = HashMap::new();
    for sess in &sessions {
        for (model, stats) in &sess.model_breakdown {
            let e = model_totals.entry(model.clone()).or_default();
            e.calls += stats.calls;
            e.cost += stats.cost_usd;
            e.input += stats.tokens.input_tokens;
            e.cache_read += stats.tokens.cache_read_input_tokens;
            e.cache_write += stats.tokens.cache_creation_input_tokens;
        }
    }
    // Sort by cost desc, name asc tiebreak. A stable tiebreak prevents
    // equal-cost items from swapping positions between renders.
    let mut models: Vec<_> = model_totals.into_iter().collect();
    models.sort_by(|a, b| {
        b.1.cost
            .partial_cmp(&a.1.cost)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });

    let mut tool_totals: HashMap<String, u64> = HashMap::new();
    for sess in &sessions {
        for (tool, stats) in &sess.tool_breakdown {
            if !tool.starts_with("mcp__") && !tool.starts_with("lang:") {
                *tool_totals.entry(tool.clone()).or_default() += stats.calls;
            }
        }
    }
    let mut tools: Vec<_> = tool_totals.into_iter().collect();
    tools.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let mut bash_totals: HashMap<String, u64> = HashMap::new();
    for sess in &sessions {
        for (cmd, stats) in &sess.bash_breakdown {
            *bash_totals.entry(cmd.clone()).or_default() += stats.calls;
        }
    }
    let mut bash_cmds: Vec<_> = bash_totals.into_iter().collect();
    bash_cmds.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let mut mcp_totals: HashMap<String, u64> = HashMap::new();
    for sess in &sessions {
        for (server, stats) in &sess.mcp_breakdown {
            *mcp_totals.entry(server.clone()).or_default() += stats.calls;
        }
    }
    let mut mcp_servers: Vec<_> = mcp_totals.into_iter().collect();
    mcp_servers.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let mut cat_totals: HashMap<TaskCategory, CategoryStats> = HashMap::new();
    for sess in &sessions {
        for (cat, stats) in &sess.category_breakdown {
            let e = cat_totals.entry(*cat).or_default();
            e.turns += stats.turns;
            e.cost_usd += stats.cost_usd;
            e.retries += stats.retries;
            e.edit_turns += stats.edit_turns;
            e.one_shot_turns += stats.one_shot_turns;
        }
    }
    let mut categories: Vec<_> = cat_totals.into_iter().collect();
    categories.sort_by(|a, b| {
        b.1.cost_usd
            .partial_cmp(&a.1.cost_usd)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.label().cmp(b.0.label()))
    });

    let num_days = match period {
        "today" | "week" => 14u32,
        _ => 31,
    };
    let mut day_totals: HashMap<String, (f64, u64)> = HashMap::new();
    for sess in &sessions {
        for dc in &sess.daily_costs {
            let e = day_totals.entry(dc.day.clone()).or_default();
            e.0 += dc.cost_usd;
            e.1 += dc.call_count;
        }
    }
    let mut daily: Vec<(String, f64, u64)> = Vec::new();
    let now = chrono::Local::now();
    for i in (0..num_days).rev() {
        let day_label = (now - chrono::Duration::days(i as i64)).format("%m-%d").to_string();
        let day_date = (now - chrono::Duration::days(i as i64))
            .format("%Y-%m-%d")
            .to_string();
        let (cost, calls) = day_totals.get(&day_date).copied().unwrap_or((0.0, 0));
        daily.push((day_label, cost, calls));
    }
    // Skip leading days with zero cost (matches JS behavior — only show days
    // once there's real activity in the window).
    let first_nonzero = daily
        .iter()
        .position(|d| d.1 > 0.0 || d.2 > 0)
        .unwrap_or(daily.len());
    daily.drain(0..first_nonzero);

    let sessions_len = sessions.len();
    DashboardData {
        projects,
        total_cost,
        total_calls,
        total_input,
        total_output,
        total_cache_read,
        total_cache_write,
        cache_pct,
        sessions_len,
        daily,
        categories,
        models,
        tools,
        bash_cmds,
        mcp_servers,
    }
}
