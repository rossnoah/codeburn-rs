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

pub mod fs_watcher;
pub mod layout;
pub mod widgets;

use std::cell::Cell;
use std::collections::HashMap;
use std::io;
use std::time::{Duration, Instant};

use fs_watcher::FsWatcher;

use anyhow::Result;
use chrono::{Datelike, Local};
use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, KeyModifiers,
    MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::prelude::*;

use tokio::sync::mpsc;

use crate::cli::Period;
use crate::currency::format_cost;
use crate::format::format_tokens;
use crate::models::load_pricing;
use crate::parser::{parse_all_sessions, parse_all_sessions_static, StaticAggregate};
use crate::providers::get_all_providers;
use crate::types::{DateRange, ProjectSummary};

const PERIODS: &[(&str, &str)] = &[
    ("today", "Today"),
    ("week", "7 Days"),
    ("30days", "30 Days"),
    ("month", "This Month"),
];

fn period_to_index(p: &str) -> usize {
    match p {
        "today" => 0,
        "week" => 1,
        "30days" => 2,
        "month" => 3,
        _ => 1,
    }
}

fn period_str(p: Period) -> &'static str {
    match p {
        Period::Today => "today",
        Period::Week => "week",
        Period::ThirtyDays => "30days",
        Period::Month => "month",
    }
}

pub fn get_date_range(period: &str) -> DateRange {
    let now = Local::now();
    let today_start = now
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let today_end = now
        .date_naive()
        .and_hms_milli_opt(23, 59, 59, 999)
        .unwrap();
    let end = today_end.and_local_timezone(Local).unwrap();

    let start = match period {
        "today" => today_start.and_local_timezone(Local).unwrap(),
        "week" => (now - chrono::Duration::days(7))
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_local_timezone(Local)
            .unwrap(),
        "30days" => (now - chrono::Duration::days(30))
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_local_timezone(Local)
            .unwrap(),
        "month" => {
            let d = now.date_naive();
            chrono::NaiveDate::from_ymd_opt(d.year(), d.month(), 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_local_timezone(Local)
                .unwrap()
        }
        _ => (now - chrono::Duration::days(7))
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_local_timezone(Local)
            .unwrap(),
    };

    DateRange { start, end }
}

pub struct App {
    pub period_idx: usize,
    pub projects: Vec<ProjectSummary>,
    pub provider_filter: String,
    pub detected_providers: Vec<String>,
    pub provider_idx: usize,
    pub loading: bool,
    pub should_quit: bool,
    pub last_switch: Instant,
    /// Cache of parsed period data keyed by "period:provider". Lets us show
    /// cached data instantly when the user switches to a period/provider
    /// we've already loaded.
    period_cache: HashMap<String, Vec<ProjectSummary>>,
    /// Keys with at least one background parse currently running.
    /// Prevents spawning duplicate parses for the same key.
    in_flight: std::collections::HashSet<String>,
    /// When set, the UI is waiting for this cache key's result to land.
    /// Any arriving result that matches promotes to the visible view.
    active_key: Option<String>,
    /// Bumped whenever we invalidate the whole cache (e.g. --refresh fires).
    /// Results sent with an older generation are silently dropped so stale
    /// data can't overwrite a freshly-requested load.
    generation: u64,
    /// Polls the filesystem for new session data while the TUI is open,
    /// so auto-refresh can fire when an AI tool writes a new message.
    fs_watcher: FsWatcher,
    /// Vertical scroll offset (rows) into the content region when the
    /// rendered content is taller than the terminal. Clamped at render
    /// time against the actual overflow; reset on period/provider change.
    pub scroll_offset: u16,
    /// Max valid `scroll_offset` for the last-rendered frame. Written by
    /// `widgets::render` so the event loop can clamp downward scrolls
    /// (mouse, Down/PageDown, End) and avoid "phantom scroll" — internal
    /// offset growing past the visible max, then scroll-up having to
    /// burn through the excess before any visible motion.
    pub last_max_scroll: Cell<u16>,
}

/// Result returned by a background load task.
struct LoadResult {
    cache_key: String,
    generation: u64,
    projects: Vec<ProjectSummary>,
}

fn cache_key(period: &str, provider_filter: &str) -> String {
    format!("{}:{}", period, provider_filter)
}

impl App {
    fn new(period: Period, provider: &str) -> Self {
        App {
            period_idx: period_to_index(period_str(period)),
            projects: Vec::new(),
            provider_filter: provider.to_string(),
            detected_providers: vec!["all".to_string()],
            provider_idx: 0,
            loading: true,
            should_quit: false,
            last_switch: Instant::now(),
            period_cache: HashMap::new(),
            in_flight: std::collections::HashSet::new(),
            active_key: None,
            generation: 0,
            fs_watcher: FsWatcher::new(),
            scroll_offset: 0,
            last_max_scroll: Cell::new(0),
        }
    }

    /// Invalidate everything. Bumps the generation counter so any results
    /// from pre-invalidation spawns are discarded when they land. Used by
    /// --refresh and any future manual-refresh keybinding.
    fn invalidate_all(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.period_cache.clear();
        self.in_flight.clear();
        self.active_key = None;
    }

    pub fn current_period(&self) -> &str {
        PERIODS[self.period_idx].0
    }

    /// Switch to the requested period/provider view.
    /// Cache hit → show instantly with no loading flash.
    /// In-flight → set active_key and wait (the pending result will
    ///             promote when it lands).
    /// Miss → spawn a new load and wait.
    fn switch_to(&mut self, tx: &mpsc::UnboundedSender<LoadResult>) {
        self.scroll_offset = 0;
        let key = cache_key(self.current_period(), &self.provider_filter);
        if let Some(cached) = self.period_cache.get(&key) {
            self.projects = cached.clone();
            self.loading = false;
            self.active_key = None;
            return;
        }
        self.active_key = Some(key.clone());
        self.loading = true;
        if !self.in_flight.contains(&key) {
            self.start_parse(self.current_period().to_string(), self.provider_filter.clone(), tx);
        }
    }

    /// Spawn a background parse task. Always pushes the result to the
    /// channel; the result-handler decides whether to promote it to the
    /// visible view (via `active_key`) and/or store it in the cache.
    fn start_parse(
        &mut self,
        period: String,
        provider: String,
        tx: &mpsc::UnboundedSender<LoadResult>,
    ) {
        let key = cache_key(&period, &provider);
        if !self.in_flight.insert(key.clone()) {
            return;
        }
        let date_range = get_date_range(&period);
        let filter = if provider == "all" { None } else { Some(provider) };
        let generation = self.generation;
        let tx = tx.clone();
        tokio::task::spawn_blocking(move || {
            let projects = parse_all_sessions(Some(&date_range), filter.as_deref())
                .unwrap_or_default();
            let _ = tx.send(LoadResult {
                cache_key: key,
                generation,
                projects,
            });
        });
    }

    /// Queue background prefetches for every period we don't have cached
    /// for the current provider. Safe to call repeatedly — `start_parse`
    /// dedupes via `in_flight`.
    fn prefetch_other_periods(&mut self, tx: &mpsc::UnboundedSender<LoadResult>) {
        let provider = self.provider_filter.clone();
        for (period_key, _) in PERIODS {
            let key = cache_key(period_key, &provider);
            if !self.period_cache.contains_key(&key) && !self.in_flight.contains(&key) {
                self.start_parse(period_key.to_string(), provider.clone(), tx);
            }
        }
    }

    async fn detect_providers(&mut self) {
        let mut detected = vec!["all".to_string()];
        for p in get_all_providers() {
            if let Ok(sessions) = p.discover_sessions() {
                if !sessions.is_empty() {
                    detected.push(p.name().to_string());
                }
            }
        }
        self.detected_providers = detected;
        // Keep provider_idx pointing at the current filter if it's still
        // in the list; otherwise fall back to "all".
        self.provider_idx = self
            .detected_providers
            .iter()
            .position(|p| p == &self.provider_filter)
            .unwrap_or(0);
    }
}

/// Drop guard that restores the terminal to its normal state. Runs on
/// panic, early return, or normal shutdown — whatever happens the user's
/// shell won't be left in raw mode with the alt screen active.
struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(io::stdout(), DisableMouseCapture, LeaveAlternateScreen);
        let _ = disable_raw_mode();
    }
}

pub async fn run(period: Period, provider: &str, refresh: Option<u64>) -> Result<()> {
    if !io::stdin().is_terminal() {
        return run_static(period, provider).await;
    }

    load_pricing().await?;

    let mut app = App::new(period, provider);
    app.detect_providers().await;

    // Channel for background load results so tab switches don't block render.
    let (tx, mut rx) = mpsc::unbounded_channel::<LoadResult>();
    // Initial load of the current view (shows loading screen).
    app.switch_to(&tx);

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    // RAII guard — from here on, any panic or early return restores the terminal.
    let _terminal_guard = TerminalGuard;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Clamp to >=1s: --refresh 0 would make `elapsed >= ZERO` always true
    // and reparse every event-poll tick (~20/sec).
    let refresh_interval = refresh.map(|s| Duration::from_secs(s.max(1)));
    let mut last_refresh = Instant::now();

    loop {
        terminal.draw(|f| widgets::render(f, &app.projects, &app, PERIODS))?;

        // Drain completed loads. Every result populates the cache so the
        // next tab switch to that (period, provider) is instant.
        // Stale results (older generation) are discarded silently.
        //
        // A result promotes to the visible view when it matches either:
        //   - active_key (the UI was blocked on this key, loading view shown)
        //   - the current view key (we were showing cached data; silent swap)
        let mut promoted = false;
        let current_key = cache_key(app.current_period(), &app.provider_filter);
        while let Ok(res) = rx.try_recv() {
            if res.generation != app.generation {
                continue;
            }
            app.in_flight.remove(&res.cache_key);
            let is_active_match = app.active_key.as_deref() == Some(res.cache_key.as_str());
            let is_current_view = res.cache_key == current_key;
            app.period_cache.insert(res.cache_key.clone(), res.projects.clone());
            if is_active_match || is_current_view {
                app.projects = res.projects;
                app.loading = false;
                app.active_key = None;
                promoted = true;
            }
        }

        // After the first user-visible load finishes, prefetch the rest of
        // the periods so upcoming switches land on cached data instantly.
        if promoted {
            app.prefetch_other_periods(&tx);
        }

        if app.should_quit {
            break;
        }

        // Short poll so we can keep draining the channel and re-rendering
        // the loading indicator smoothly.
        let timeout = Duration::from_millis(50);
        if event::poll(timeout)? {
            match event::read()? {
                Event::Mouse(m) => match m.kind {
                    MouseEventKind::ScrollUp => {
                        app.scroll_offset = app.scroll_offset.saturating_sub(1);
                    }
                    MouseEventKind::ScrollDown => {
                        app.scroll_offset = app
                            .scroll_offset
                            .saturating_add(1)
                            .min(app.last_max_scroll.get());
                    }
                    _ => {}
                },
                Event::Key(key) => {
                    if key.kind != KeyEventKind::Press {
                        continue;
                    }
                    if key.modifiers.contains(KeyModifiers::CONTROL)
                        && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('d'))
                    {
                        app.should_quit = true;
                        break;
                    }
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            app.should_quit = true;
                        }
                        KeyCode::Char('1') => {
                            if app.period_idx != 0 {
                                app.period_idx = 0;
                                app.switch_to(&tx);
                            }
                        }
                        KeyCode::Char('2') => {
                            if app.period_idx != 1 {
                                app.period_idx = 1;
                                app.switch_to(&tx);
                            }
                        }
                        KeyCode::Char('3') => {
                            if app.period_idx != 2 {
                                app.period_idx = 2;
                                app.switch_to(&tx);
                            }
                        }
                        KeyCode::Char('4') => {
                            if app.period_idx != 3 {
                                app.period_idx = 3;
                                app.switch_to(&tx);
                            }
                        }
                        KeyCode::Left | KeyCode::Char('<') => {
                            if app.last_switch.elapsed() > Duration::from_millis(120) {
                                app.period_idx =
                                    (app.period_idx + PERIODS.len() - 1) % PERIODS.len();
                                app.last_switch = Instant::now();
                                app.switch_to(&tx);
                            }
                        }
                        KeyCode::Right | KeyCode::Char('>') => {
                            if app.last_switch.elapsed() > Duration::from_millis(120) {
                                app.period_idx = (app.period_idx + 1) % PERIODS.len();
                                app.last_switch = Instant::now();
                                app.switch_to(&tx);
                            }
                        }
                        KeyCode::Tab => {
                            app.period_idx = (app.period_idx + 1) % PERIODS.len();
                            app.switch_to(&tx);
                        }
                        KeyCode::Up | KeyCode::Char('k') => {
                            app.scroll_offset = app.scroll_offset.saturating_sub(1);
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            app.scroll_offset = app
                                .scroll_offset
                                .saturating_add(1)
                                .min(app.last_max_scroll.get());
                        }
                        KeyCode::PageUp => {
                            app.scroll_offset = app.scroll_offset.saturating_sub(10);
                        }
                        KeyCode::PageDown => {
                            app.scroll_offset = app
                                .scroll_offset
                                .saturating_add(10)
                                .min(app.last_max_scroll.get());
                        }
                        KeyCode::Home => {
                            app.scroll_offset = 0;
                        }
                        KeyCode::End => {
                            app.scroll_offset = app.last_max_scroll.get();
                        }
                        KeyCode::Char('p') => {
                            if app.detected_providers.len() > 1 {
                                app.provider_idx =
                                    (app.provider_idx + 1) % app.detected_providers.len();
                                app.provider_filter =
                                    app.detected_providers[app.provider_idx].clone();
                                app.switch_to(&tx);
                            }
                        }
                        KeyCode::Char('r') | KeyCode::F(5) => {
                            // Manual refresh: drop everything we have, bump
                            // the generation so pending parses can't overwrite
                            // new data, and kick off a fresh load. Also sync
                            // the watcher/auto-interval bookkeeping so they
                            // don't fire a redundant reparse right after.
                            app.invalidate_all();
                            app.detect_providers().await;
                            app.fs_watcher.resync();
                            last_refresh = Instant::now();
                            app.switch_to(&tx);
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        if let Some(interval) = refresh_interval {
            if last_refresh.elapsed() >= interval {
                // Auto-refresh: invalidate all caches and generation.
                // Pending pre-refresh parses that arrive later are dropped
                // via the generation check in the drain loop. Resync the
                // FS watcher so it doesn't fire a redundant reparse on its
                // next poll.
                app.invalidate_all();
                app.detect_providers().await;
                app.fs_watcher.resync();
                app.switch_to(&tx);
                last_refresh = Instant::now();
            }
        }

        // Filesystem watcher: detect new AI usage written to disk since
        // our last parse. `poll_changed` handles its own rate limiting.
        if app.fs_watcher.poll_changed() {
            // Also reset the --refresh timer: the data is fresh now,
            // no reason to reparse again on the user's schedule.
            last_refresh = Instant::now();
            // Re-detect providers: a brand-new tool might have written its
            // first session file, and we want it in the `p` cycle.
            app.detect_providers().await;
            // Passive refresh: bump generation so pre-change spawns are
            // ignored, clear in_flight (but NOT the cache — the stale data
            // stays on screen until the fresh parse lands so there's no
            // loading flash), then re-parse every period we have cached
            // for the current provider. Each arriving result silently
            // replaces the stale entry and the drain loop promotes
            // whichever one matches the user's current view.
            app.generation = app.generation.wrapping_add(1);
            app.in_flight.clear();
            let provider = app.provider_filter.clone();
            let mut keys_to_refresh: std::collections::HashSet<String> = app
                .period_cache
                .keys()
                .filter(|k| k.ends_with(&format!(":{}", provider)))
                .cloned()
                .collect();
            // Always include the active period even if it wasn't cached yet.
            keys_to_refresh.insert(cache_key(app.current_period(), &provider));
            for key in keys_to_refresh {
                // Parse the period half of the key back out.
                if let Some(period) = key.split(':').next() {
                    app.start_parse(period.to_string(), provider.clone(), &tx);
                }
            }
        }
    }

    // Show cursor before the guard restores other terminal state.
    let _ = terminal.show_cursor();
    // TerminalGuard::drop handles disable_raw_mode + LeaveAlternateScreen.
    Ok(())
}

/// Fully synchronous non-TTY report — skips tokio entirely. Called by
/// `main` when stdin isn't a terminal and the command is a
/// report/today/month with no `--refresh`.
pub fn run_static_sync(period: Period, provider: &str) -> Result<()> {
    let prof = std::env::var_os("CODEBURN_PROFILE").is_some();
    // Pricing is lazy-loaded inside `get_model_costs`; the full-cache-hit
    // path never calls it, so we skip the eager `load_pricing_sync` here.
    let date_range = get_date_range(period_str(period));
    let filter = if provider == "all" { None } else { Some(provider) };
    let t_parse = Instant::now();
    let agg = parse_all_sessions_static(Some(&date_range), filter).unwrap_or_default();
    if prof {
        eprintln!("[prof main] parse_all_sessions    {:>8.2} ms", t_parse.elapsed().as_secs_f64() * 1000.0);
    }
    let t_render = Instant::now();
    render_static_aggregate(period, &agg);
    if prof {
        eprintln!("[prof main] render_static         {:>8.2} ms", t_render.elapsed().as_secs_f64() * 1000.0);
    }
    // Drain any background persist threads (report cache + discovery cache
    // writes). Detached `std::thread::spawn` threads are killed when `main`
    // returns, so without this join the first run after deleting caches
    // never actually writes anything back and every subsequent run looks
    // like a full miss.
    let t_join = Instant::now();
    crate::parser::join_pending_persists();
    if prof {
        eprintln!("[prof main] join persists         {:>8.2} ms", t_join.elapsed().as_secs_f64() * 1000.0);
    }
    Ok(())
}

fn render_static_aggregate(period: Period, agg: &StaticAggregate) {
    let all_input = agg.total_input + agg.total_cache_read + agg.total_cache_write;
    let cache_pct = if all_input > 0 {
        (agg.total_cache_read as f64 / all_input as f64) * 100.0
    } else {
        0.0
    };
    let total_cost: f64 = agg.projects.iter().map(|p| p.total_cost_usd).sum();
    let total_calls: u64 = agg.projects.iter().map(|p| p.total_api_calls).sum();
    let period_label = PERIODS[period_to_index(period_str(period))].1;

    println!();
    println!("  CodeBurn - {}", period_label);
    println!("  ──────────────────────");
    println!(
        "  Cost: {}  Calls: {}  Sessions: {}",
        format_cost(total_cost),
        total_calls,
        agg.total_sessions
    );
    println!(
        "  Input: {}  Output: {}  Cache: {:.0}%",
        format_tokens(agg.total_input),
        format_tokens(agg.total_output),
        cache_pct
    );
    println!();

    if !agg.projects.is_empty() {
        println!("  Projects:");
        for p in agg.projects.iter().take(8) {
            println!("    {} - {}", format_cost(p.total_cost_usd), p.project_path);
        }
        println!();
    }
}

async fn run_static(period: Period, provider: &str) -> Result<()> {
    load_pricing().await?;
    let date_range = get_date_range(period_str(period));
    let filter = if provider == "all" {
        None
    } else {
        Some(provider)
    };
    let projects = parse_all_sessions(Some(&date_range), filter)
        .unwrap_or_default();
    render_static(period, &projects);
    Ok(())
}

fn render_static(period: Period, projects: &[ProjectSummary]) {

    let sessions: Vec<_> = projects.iter().flat_map(|p| &p.sessions).collect();
    let total_cost: f64 = projects.iter().map(|p| p.total_cost_usd).sum();
    let total_calls: u64 = projects.iter().map(|p| p.total_api_calls).sum();
    let total_input: u64 = sessions.iter().map(|s| s.total_input_tokens).sum();
    let total_output: u64 = sessions.iter().map(|s| s.total_output_tokens).sum();
    let total_cache_read: u64 = sessions.iter().map(|s| s.total_cache_read_tokens).sum();
    let total_cache_write: u64 = sessions.iter().map(|s| s.total_cache_write_tokens).sum();

    let all_input = total_input + total_cache_read + total_cache_write;
    let cache_pct = if all_input > 0 {
        (total_cache_read as f64 / all_input as f64) * 100.0
    } else {
        0.0
    };

    let period_label = PERIODS[period_to_index(period_str(period))].1;

    println!();
    println!("  CodeBurn - {}", period_label);
    println!("  ──────────────────────");
    println!(
        "  Cost: {}  Calls: {}  Sessions: {}",
        format_cost(total_cost),
        total_calls,
        sessions.len()
    );
    println!(
        "  Input: {}  Output: {}  Cache: {:.0}%",
        format_tokens(total_input),
        format_tokens(total_output),
        cache_pct
    );
    println!();

    if !projects.is_empty() {
        println!("  Projects:");
        for p in projects.iter().take(8) {
            println!("    {} - {}", format_cost(p.total_cost_usd), p.project_path);
        }
        println!();
    }

    // Debug: print category breakdown matching the TUI dashboard, so we can
    // verify turn counts / 1-shot % against the JS reference.
    let mut cat_totals: std::collections::HashMap<crate::types::TaskCategory, crate::types::CategoryStats> = std::collections::HashMap::new();
    for sess in &sessions {
        for (cat, stats) in &sess.category_breakdown {
            let e = cat_totals.entry(*cat).or_default();
            e.turns += stats.turns;
            e.cost_usd += stats.cost_usd;
            e.edit_turns += stats.edit_turns;
            e.one_shot_turns += stats.one_shot_turns;
        }
    }
    let mut cats: Vec<_> = cat_totals.into_iter().collect();
    cats.sort_by(|a, b| b.1.cost_usd.partial_cmp(&a.1.cost_usd).unwrap_or(std::cmp::Ordering::Equal));
    println!("  By Activity:");
    for (cat, s) in &cats {
        let oneshot = if s.edit_turns > 0 {
            format!("{}%", ((s.one_shot_turns as f64 / s.edit_turns as f64) * 100.0).round() as i64)
        } else {
            "-".to_string()
        };
        println!("    {:<14} {:>9} {:>6} turns  {:>5} 1-shot",
            cat.label(), format_cost(s.cost_usd), s.turns, oneshot);
    }
    println!();
}

use std::io::IsTerminal;
