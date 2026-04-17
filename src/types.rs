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
use std::fmt;

use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DailyCostEntry {
    pub day: String,
    pub cost_usd: f64,
    pub call_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub cached_input_tokens: u64,
    pub reasoning_tokens: u64,
    pub web_search_requests: u64,
}

impl TokenUsage {
    pub fn add(&mut self, other: &TokenUsage) {
        self.input_tokens += other.input_tokens;
        self.output_tokens += other.output_tokens;
        self.cache_creation_input_tokens += other.cache_creation_input_tokens;
        self.cache_read_input_tokens += other.cache_read_input_tokens;
        self.cached_input_tokens += other.cached_input_tokens;
        self.reasoning_tokens += other.reasoning_tokens;
        self.web_search_requests += other.web_search_requests;
    }
}

#[derive(Debug, Clone)]
pub struct ParsedApiCall {
    pub model: String,
    pub usage: TokenUsage,
    pub cost_usd: f64,
    pub tools: Vec<String>,
    pub mcp_tools: Vec<String>,
    pub has_agent_spawn: bool,
    pub has_plan_mode: bool,
    pub timestamp: String,
    pub bash_commands: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Speed {
    Standard,
    Fast,
}

impl Default for Speed {
    fn default() -> Self {
        Speed::Standard
    }
}

#[derive(Debug, Clone)]
pub struct ParsedTurn {
    pub user_message: String,
    pub assistant_calls: Vec<ParsedApiCall>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskCategory {
    Coding,
    Debugging,
    Feature,
    Refactoring,
    Testing,
    Exploration,
    Planning,
    Delegation,
    Git,
    #[serde(rename = "build/deploy")]
    BuildDeploy,
    Conversation,
    Brainstorming,
    General,
}

impl fmt::Display for TaskCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

impl TaskCategory {
    pub fn label(&self) -> &'static str {
        match self {
            TaskCategory::Coding => "Coding",
            TaskCategory::Debugging => "Debugging",
            TaskCategory::Feature => "Feature Dev",
            TaskCategory::Refactoring => "Refactoring",
            TaskCategory::Testing => "Testing",
            TaskCategory::Exploration => "Exploration",
            TaskCategory::Planning => "Planning",
            TaskCategory::Delegation => "Delegation",
            TaskCategory::Git => "Git Ops",
            TaskCategory::BuildDeploy => "Build/Deploy",
            TaskCategory::Conversation => "Conversation",
            TaskCategory::Brainstorming => "Brainstorming",
            TaskCategory::General => "General",
        }
    }

}

#[derive(Debug, Clone)]
pub struct ClassifiedTurn {
    pub turn: ParsedTurn,
    pub category: TaskCategory,
    pub retries: u32,
    pub has_edits: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelStats {
    pub calls: u64,
    pub cost_usd: f64,
    pub tokens: TokenUsage,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ToolStats {
    pub calls: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CategoryStats {
    pub turns: u64,
    pub cost_usd: f64,
    pub retries: u64,
    pub edit_turns: u64,
    pub one_shot_turns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub project: String,
    pub first_timestamp: String,
    pub last_timestamp: String,
    pub total_cost_usd: f64,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub total_cache_read_tokens: u64,
    pub total_cache_write_tokens: u64,
    pub api_calls: u64,
    pub daily_costs: Vec<DailyCostEntry>,
    pub model_breakdown: HashMap<String, ModelStats>,
    pub tool_breakdown: HashMap<String, ToolStats>,
    pub mcp_breakdown: HashMap<String, ToolStats>,
    pub bash_breakdown: HashMap<String, ToolStats>,
    pub category_breakdown: HashMap<TaskCategory, CategoryStats>,
}

#[derive(Debug, Clone)]
pub struct ProjectSummary {
    pub project_path: String,
    pub sessions: Vec<SessionSummary>,
    pub total_cost_usd: f64,
    pub total_api_calls: u64,
}

#[derive(Debug, Clone, Default)]
pub struct StatusAggregate {
    pub today_cost: f64,
    pub today_calls: u64,
    pub week_cost: f64,
    pub week_calls: u64,
    pub month_cost: f64,
    pub month_calls: u64,
}

impl StatusAggregate {
    pub fn merge(&mut self, other: &StatusAggregate) {
        self.today_cost += other.today_cost;
        self.today_calls += other.today_calls;
        self.week_cost += other.week_cost;
        self.week_calls += other.week_calls;
        self.month_cost += other.month_cost;
        self.month_calls += other.month_calls;
    }
}

#[derive(Debug, Clone)]
pub struct DateRange {
    pub start: DateTime<Local>,
    pub end: DateTime<Local>,
}

/// UTC timestamp boundaries for the status fast path.
/// All fields are "YYYY-MM-DDThh:mm:ss" strings in UTC, suitable for
/// lexicographic comparison against JSONL timestamps (which are also UTC).
#[derive(Debug, Clone)]
pub struct StatusBounds {
    pub today_start: String,
    pub today_end: String,
    pub week_start: String,
    pub month_start: String,
    pub month_end: String,
}

#[derive(Debug, Clone)]
pub struct ModelCosts {
    pub input_cost_per_token: f64,
    pub output_cost_per_token: f64,
    pub cache_write_cost_per_token: f64,
    pub cache_read_cost_per_token: f64,
    pub web_search_cost_per_request: f64,
    pub fast_multiplier: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedProviderCall {
    pub provider: String,
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub cached_input_tokens: u64,
    pub reasoning_tokens: u64,
    pub web_search_requests: u64,
    pub cost_usd: f64,
    pub tools: Vec<String>,
    pub bash_commands: Vec<String>,
    pub timestamp: String,
    pub speed: Speed,
    pub deduplication_key: String,
    pub user_message: String,
    pub session_id: String,
    /// Timestamp of the user-message entry that triggered this call. Two
    /// calls with the same `user_message` text but different timestamps
    /// belong to different turns — matches JS `groupIntoTurns` which starts
    /// a new turn on every user-entry regardless of content repetition.
    /// Defaults to empty for providers that don't track user-entry timing.
    #[serde(default)]
    pub user_message_timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSource {
    pub path: String,
    pub project: String,
    pub provider: String,
}
