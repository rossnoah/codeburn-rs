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

use std::collections::HashSet;
use std::sync::LazyLock;

use aho_corasick::AhoCorasick;
use regex::Regex;

use crate::types::{ClassifiedTurn, ParsedTurn, TaskCategory};

// Every keyword from every group is packed into a single Aho-Corasick
// automaton — one O(n) DFA pass per user message finds every potential
// match. We then apply a word-boundary check on each hit (AC is literal
// substring matching; JS uses `\bword\b`) and set the corresponding
// group's flag. Beats N separate regex scans and beats RegexSet: AC is a
// specialized DFA for this exact shape, and the boundary filter stays
// branchless per-char.
//
// Patterns with `\s+` / `\s*` in JS are represented here as their
// single-space form. User prose with multi-space sequences between
// keyword tokens is exceedingly rare in practice; worst case a handful
// of turns class differently by one keyword — well within the existing
// 1-turn drift between Rust and JS.
#[derive(Clone, Copy)]
#[repr(u8)]
enum KwGroup {
    Debug,
    Feature,
    Refactor,
    Brainstorm,
    Research,
    Test,
    Git,
    Build,
    Install,
}

static AC_KEYWORDS: LazyLock<(AhoCorasick, Vec<KwGroup>)> = LazyLock::new(|| {
    let entries: &[(&str, KwGroup)] = &[
        // Debug
        ("fix", KwGroup::Debug), ("bug", KwGroup::Debug),
        ("error", KwGroup::Debug), ("broken", KwGroup::Debug),
        ("failing", KwGroup::Debug), ("crash", KwGroup::Debug),
        ("issue", KwGroup::Debug), ("debug", KwGroup::Debug),
        ("traceback", KwGroup::Debug), ("exception", KwGroup::Debug),
        ("stack trace", KwGroup::Debug), ("stacktrace", KwGroup::Debug),
        ("not working", KwGroup::Debug), ("wrong", KwGroup::Debug),
        ("unexpected", KwGroup::Debug), ("status code", KwGroup::Debug),
        ("404", KwGroup::Debug), ("500", KwGroup::Debug),
        ("401", KwGroup::Debug), ("403", KwGroup::Debug),
        // Feature
        ("add", KwGroup::Feature), ("create", KwGroup::Feature),
        ("implement", KwGroup::Feature), ("new", KwGroup::Feature),
        ("build", KwGroup::Feature), ("feature", KwGroup::Feature),
        ("introduce", KwGroup::Feature), ("set up", KwGroup::Feature),
        ("setup", KwGroup::Feature), ("scaffold", KwGroup::Feature),
        ("generate", KwGroup::Feature),
        ("make a", KwGroup::Feature), ("make me", KwGroup::Feature),
        ("make the", KwGroup::Feature),
        ("write a", KwGroup::Feature), ("write me", KwGroup::Feature),
        ("write the", KwGroup::Feature),
        // Refactor
        ("refactor", KwGroup::Refactor), ("clean up", KwGroup::Refactor),
        ("cleanup", KwGroup::Refactor), ("rename", KwGroup::Refactor),
        ("reorganize", KwGroup::Refactor), ("simplify", KwGroup::Refactor),
        ("extract", KwGroup::Refactor), ("restructure", KwGroup::Refactor),
        ("move", KwGroup::Refactor), ("migrate", KwGroup::Refactor),
        ("split", KwGroup::Refactor),
        // Brainstorm
        ("brainstorm", KwGroup::Brainstorm), ("idea", KwGroup::Brainstorm),
        ("what if", KwGroup::Brainstorm), ("explore", KwGroup::Brainstorm),
        ("think about", KwGroup::Brainstorm), ("approach", KwGroup::Brainstorm),
        ("strategy", KwGroup::Brainstorm), ("design", KwGroup::Brainstorm),
        ("consider", KwGroup::Brainstorm), ("how should", KwGroup::Brainstorm),
        ("what would", KwGroup::Brainstorm), ("opinion", KwGroup::Brainstorm),
        ("suggest", KwGroup::Brainstorm), ("recommend", KwGroup::Brainstorm),
        // Research
        ("research", KwGroup::Research), ("investigate", KwGroup::Research),
        ("look into", KwGroup::Research), ("find out", KwGroup::Research),
        ("check", KwGroup::Research), ("search", KwGroup::Research),
        ("analyze", KwGroup::Research), ("review", KwGroup::Research),
        ("understand", KwGroup::Research), ("explain", KwGroup::Research),
        ("how does", KwGroup::Research), ("what is", KwGroup::Research),
        ("show me", KwGroup::Research), ("list", KwGroup::Research),
        ("compare", KwGroup::Research),
        // Test
        ("test", KwGroup::Test), ("pytest", KwGroup::Test),
        ("vitest", KwGroup::Test), ("jest", KwGroup::Test),
        ("mocha", KwGroup::Test), ("spec", KwGroup::Test),
        ("coverage", KwGroup::Test),
        ("npm test", KwGroup::Test),
        ("npx vitest", KwGroup::Test), ("npx jest", KwGroup::Test),
        // Git
        ("git push", KwGroup::Git), ("git pull", KwGroup::Git),
        ("git commit", KwGroup::Git), ("git merge", KwGroup::Git),
        ("git rebase", KwGroup::Git), ("git checkout", KwGroup::Git),
        ("git branch", KwGroup::Git), ("git stash", KwGroup::Git),
        ("git log", KwGroup::Git), ("git diff", KwGroup::Git),
        ("git status", KwGroup::Git), ("git add", KwGroup::Git),
        ("git reset", KwGroup::Git), ("git cherry-pick", KwGroup::Git),
        ("git tag", KwGroup::Git),
        // Build
        ("npm run build", KwGroup::Build), ("npm publish", KwGroup::Build),
        ("pip install", KwGroup::Build), ("docker", KwGroup::Build),
        ("deploy", KwGroup::Build), ("make build", KwGroup::Build),
        ("npm run dev", KwGroup::Build), ("npm start", KwGroup::Build),
        ("pm2", KwGroup::Build), ("systemctl", KwGroup::Build),
        ("brew", KwGroup::Build), ("cargo build", KwGroup::Build),
        // Install (`build/deploy` category in JS, same as Build here)
        ("npm install", KwGroup::Install), ("brew install", KwGroup::Install),
        ("apt install", KwGroup::Install), ("cargo add", KwGroup::Install),
    ];

    let patterns: Vec<&str> = entries.iter().map(|(p, _)| *p).collect();
    let groups: Vec<KwGroup> = entries.iter().map(|(_, g)| *g).collect();
    // Standard match kind supports overlapping iter — necessary because
    // patterns can overlap (e.g. "add" is inside "add a", and "build" is
    // both a Feature and a Build/Deploy keyword). We want all hits so the
    // word-boundary filter decides which ones count.
    let ac = AhoCorasick::builder()
        .ascii_case_insensitive(true)
        .build(&patterns)
        .unwrap();
    (ac, groups)
});

/// Per-message keyword hit flags. Built once per user message via a single
/// Aho-Corasick pass; cheap Copy struct so callers can pass it around
/// without re-scanning.
#[derive(Default, Clone, Copy)]
struct KeywordMatches {
    debug: bool,
    feature: bool,
    refactor: bool,
    brainstorm: bool,
    research: bool,
    test: bool,
    git: bool,
    build: bool,
    install: bool,
}

/// True if `b` is a word character for `\b` purposes: [A-Za-z0-9_].
#[inline]
fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn scan_keywords(msg: &str) -> KeywordMatches {
    let mut m = KeywordMatches::default();
    if msg.is_empty() {
        return m;
    }
    let (ac, groups) = &*AC_KEYWORDS;
    let bytes = msg.as_bytes();
    for mat in ac.find_overlapping_iter(msg) {
        let start = mat.start();
        let end = mat.end();
        // \b before: start of string OR prev char is non-word.
        if start > 0 && is_word_byte(bytes[start - 1]) {
            continue;
        }
        // \b after: end of string OR next char is non-word.
        if end < bytes.len() && is_word_byte(bytes[end]) {
            continue;
        }
        match groups[mat.pattern().as_usize()] {
            KwGroup::Debug => m.debug = true,
            KwGroup::Feature => m.feature = true,
            KwGroup::Refactor => m.refactor = true,
            KwGroup::Brainstorm => m.brainstorm = true,
            KwGroup::Research => m.research = true,
            KwGroup::Test => m.test = true,
            KwGroup::Git => m.git = true,
            KwGroup::Build => m.build = true,
            KwGroup::Install => m.install = true,
        }
        // Early-out once every group is lit.
        if m.debug && m.feature && m.refactor && m.brainstorm
            && m.research && m.test && m.git && m.build && m.install
        {
            break;
        }
    }
    m
}

// Structural patterns kept as individual regexes — they're only queried by
// `classify_conversation` as tie-breakers when no keyword group hits.
static FILE_PATTERNS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\.(py|js|ts|tsx|jsx|json|yaml|yml|toml|sql|sh|go|rs|java|rb|php|css|html|md|csv|xml)\b").unwrap()
});
static SCRIPT_PATTERNS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(run\s+\S+\.\w+|execute|scrip?t|curl|api\s+\S+|endpoint|request\s+url|fetch\s+\S+|query|database|db\s+\S+)\b").unwrap()
});
static URL_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)https?://\S+").unwrap());

static EDIT_TOOLS: LazyLock<HashSet<&str>> = LazyLock::new(|| {
    HashSet::from([
        "Edit",
        "Write",
        "FileEditTool",
        "FileWriteTool",
        "NotebookEdit",
        "cursor:edit",
    ])
});
static READ_TOOLS: LazyLock<HashSet<&str>> = LazyLock::new(|| {
    HashSet::from([
        "Read",
        "Grep",
        "Glob",
        "FileReadTool",
        "GrepTool",
        "GlobTool",
    ])
});
pub static BASH_TOOLS: LazyLock<HashSet<&str>> =
    LazyLock::new(|| HashSet::from(["Bash", "BashTool", "PowerShellTool"]));
static TASK_TOOLS: LazyLock<HashSet<&str>> = LazyLock::new(|| {
    HashSet::from([
        "TaskCreate",
        "TaskUpdate",
        "TaskGet",
        "TaskList",
        "TaskOutput",
        "TaskStop",
        "TodoWrite",
    ])
});
static SEARCH_TOOLS: LazyLock<HashSet<&str>> =
    LazyLock::new(|| HashSet::from(["WebSearch", "WebFetch", "ToolSearch"]));

fn has_edit_tools(tools: &[String]) -> bool {
    tools.iter().any(|t| EDIT_TOOLS.contains(t.as_str()))
}

fn has_read_tools(tools: &[String]) -> bool {
    tools.iter().any(|t| READ_TOOLS.contains(t.as_str()))
}

fn has_bash_tool(tools: &[String]) -> bool {
    tools.iter().any(|t| BASH_TOOLS.contains(t.as_str()))
}

fn has_task_tools(tools: &[String]) -> bool {
    tools.iter().any(|t| TASK_TOOLS.contains(t.as_str()))
}

fn has_search_tools(tools: &[String]) -> bool {
    tools.iter().any(|t| SEARCH_TOOLS.contains(t.as_str()))
}

fn has_mcp_tools(tools: &[String]) -> bool {
    tools.iter().any(|t| t.starts_with("mcp__"))
}

fn has_skill_tool(tools: &[String]) -> bool {
    tools.iter().any(|t| t == "Skill")
}

fn get_all_tools(turn: &ParsedTurn) -> Vec<String> {
    turn.assistant_calls
        .iter()
        .flat_map(|c| c.tools.iter().cloned())
        .collect()
}

fn classify_by_tool_pattern(turn: &ParsedTurn, kw: &KeywordMatches) -> Option<TaskCategory> {
    let tools = get_all_tools(turn);
    if tools.is_empty() {
        return None;
    }

    if turn.assistant_calls.iter().any(|c| c.has_plan_mode) {
        return Some(TaskCategory::Planning);
    }
    if turn.assistant_calls.iter().any(|c| c.has_agent_spawn) {
        return Some(TaskCategory::Delegation);
    }

    let has_edits = has_edit_tools(&tools);
    let has_reads = has_read_tools(&tools);
    let has_bash = has_bash_tool(&tools);
    let has_tasks = has_task_tools(&tools);
    let has_search = has_search_tools(&tools);
    let has_mcp = has_mcp_tools(&tools);
    let has_skill = has_skill_tool(&tools);

    if has_bash && !has_edits {
        if kw.test {
            return Some(TaskCategory::Testing);
        }
        if kw.git {
            return Some(TaskCategory::Git);
        }
        if kw.build {
            return Some(TaskCategory::BuildDeploy);
        }
        if kw.install {
            return Some(TaskCategory::BuildDeploy);
        }
    }

    if has_edits {
        return Some(TaskCategory::Coding);
    }

    if has_bash && has_reads {
        return Some(TaskCategory::Exploration);
    }
    if has_bash {
        return Some(TaskCategory::Coding);
    }

    if has_search || has_mcp {
        return Some(TaskCategory::Exploration);
    }
    if has_reads && !has_edits {
        return Some(TaskCategory::Exploration);
    }
    if has_tasks && !has_edits {
        return Some(TaskCategory::Planning);
    }
    if has_skill {
        return Some(TaskCategory::General);
    }

    None
}

fn refine_by_keywords(category: TaskCategory, kw: &KeywordMatches) -> TaskCategory {
    match category {
        TaskCategory::Coding => {
            if kw.debug {
                return TaskCategory::Debugging;
            }
            if kw.refactor {
                return TaskCategory::Refactoring;
            }
            if kw.feature {
                return TaskCategory::Feature;
            }
            TaskCategory::Coding
        }
        TaskCategory::Exploration => {
            if kw.research {
                return TaskCategory::Exploration;
            }
            if kw.debug {
                return TaskCategory::Debugging;
            }
            TaskCategory::Exploration
        }
        other => other,
    }
}

fn classify_conversation(user_message: &str, kw: &KeywordMatches) -> TaskCategory {
    if kw.brainstorm {
        return TaskCategory::Brainstorming;
    }
    if kw.research {
        return TaskCategory::Exploration;
    }
    if kw.debug {
        return TaskCategory::Debugging;
    }
    if kw.feature {
        return TaskCategory::Feature;
    }
    if FILE_PATTERNS.is_match(user_message) {
        return TaskCategory::Coding;
    }
    if SCRIPT_PATTERNS.is_match(user_message) {
        return TaskCategory::Coding;
    }
    if URL_PATTERN.is_match(user_message) {
        return TaskCategory::Exploration;
    }
    TaskCategory::Conversation
}

fn count_retries(turn: &ParsedTurn) -> u32 {
    let mut saw_edit_before_bash = false;
    let mut saw_bash_after_edit = false;
    let mut retries = 0u32;

    for call in &turn.assistant_calls {
        let has_edit = call.tools.iter().any(|t| EDIT_TOOLS.contains(t.as_str()));
        let has_bash = call.tools.iter().any(|t| BASH_TOOLS.contains(t.as_str()));

        if has_edit {
            if saw_bash_after_edit {
                retries += 1;
            }
            saw_edit_before_bash = true;
            saw_bash_after_edit = false;
        }
        if has_bash && saw_edit_before_bash {
            saw_bash_after_edit = true;
        }
    }

    retries
}

fn turn_has_edits(turn: &ParsedTurn) -> bool {
    turn.assistant_calls
        .iter()
        .any(|c| c.tools.iter().any(|t| EDIT_TOOLS.contains(t.as_str())))
}

pub fn classify_turn(turn: ParsedTurn) -> ClassifiedTurn {
    let tools = get_all_tools(&turn);
    let retries = count_retries(&turn);
    let has_edits = turn_has_edits(&turn);
    // One RegexSet pass per turn, reused by every downstream branch.
    let kw = scan_keywords(&turn.user_message);

    let category = if tools.is_empty() {
        classify_conversation(&turn.user_message, &kw)
    } else {
        let tool_category = classify_by_tool_pattern(&turn, &kw);
        match tool_category {
            Some(cat) => refine_by_keywords(cat, &kw),
            None => classify_conversation(&turn.user_message, &kw),
        }
    };

    ClassifiedTurn {
        turn,
        category,
        retries,
        has_edits,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ParsedApiCall, TokenUsage};

    fn make_turn(user_msg: &str, tools: Vec<Vec<&str>>) -> ParsedTurn {
        ParsedTurn {
            user_message: user_msg.to_string(),
            assistant_calls: tools
                .into_iter()
                .map(|t| ParsedApiCall {
                    model: "claude-opus-4-6".to_string(),
                    usage: TokenUsage::default(),
                    cost_usd: 0.0,
                    tools: t.into_iter().map(String::from).collect(),
                    mcp_tools: vec![],
                    has_agent_spawn: false,
                    has_plan_mode: false,
                    timestamp: String::new(),
                    bash_commands: vec![],
                })
                .collect(),
            timestamp: String::new(),
        }
    }

    #[test]
    fn test_coding_with_edits() {
        let turn = make_turn("update the config", vec![vec!["Edit", "Read"]]);
        let result = classify_turn(turn);
        assert_eq!(result.category, TaskCategory::Coding);
    }

    #[test]
    fn test_debugging() {
        let turn = make_turn("fix the bug in auth", vec![vec!["Edit", "Bash"]]);
        let result = classify_turn(turn);
        assert_eq!(result.category, TaskCategory::Debugging);
    }

    #[test]
    fn test_feature_dev() {
        let turn = make_turn("add a new login page", vec![vec!["Edit", "Write"]]);
        let result = classify_turn(turn);
        assert_eq!(result.category, TaskCategory::Feature);
    }

    #[test]
    fn test_testing() {
        let turn = make_turn("npm test", vec![vec!["Bash"]]);
        let result = classify_turn(turn);
        assert_eq!(result.category, TaskCategory::Testing);
    }

    #[test]
    fn test_git_ops() {
        let turn = make_turn("git push to main", vec![vec!["Bash"]]);
        let result = classify_turn(turn);
        assert_eq!(result.category, TaskCategory::Git);
    }

    #[test]
    fn test_exploration_read_only() {
        let turn = make_turn("look at the code", vec![vec!["Read", "Grep"]]);
        let result = classify_turn(turn);
        assert_eq!(result.category, TaskCategory::Exploration);
    }

    #[test]
    fn test_conversation_no_tools() {
        let turn = make_turn("hello how are you", vec![]);
        let result = classify_turn(turn);
        assert_eq!(result.category, TaskCategory::Conversation);
    }

    #[test]
    fn test_planning() {
        let mut turn = make_turn("plan the refactor", vec![vec!["EnterPlanMode"]]);
        turn.assistant_calls[0].has_plan_mode = true;
        let result = classify_turn(turn);
        assert_eq!(result.category, TaskCategory::Planning);
    }

    #[test]
    fn test_delegation() {
        let mut turn = make_turn("delegate this task", vec![vec!["Agent"]]);
        turn.assistant_calls[0].has_agent_spawn = true;
        let result = classify_turn(turn);
        assert_eq!(result.category, TaskCategory::Delegation);
    }

    #[test]
    fn test_retry_count() {
        let turn = make_turn(
            "fix it",
            vec![
                vec!["Edit"],
                vec!["Bash"],
                vec!["Edit"], // retry
                vec!["Bash"],
                vec!["Edit"], // retry
            ],
        );
        assert_eq!(count_retries(&turn), 2);
    }

    #[test]
    fn test_no_retries() {
        let turn = make_turn("write code", vec![vec!["Edit"], vec!["Bash"]]);
        assert_eq!(count_retries(&turn), 0);
    }

    #[test]
    fn test_brainstorming() {
        let turn = make_turn("what if we redesign the architecture", vec![]);
        let result = classify_turn(turn);
        assert_eq!(result.category, TaskCategory::Brainstorming);
    }

    #[test]
    fn test_build_deploy() {
        let turn = make_turn("npm run build", vec![vec!["Bash"]]);
        let result = classify_turn(turn);
        assert_eq!(result.category, TaskCategory::BuildDeploy);
    }
}
