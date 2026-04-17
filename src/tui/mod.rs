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

use std::sync::mpsc;

use crate::cli::Period;
use crate::currency::format_cost;
use crate::format::format_tokens;
use crate::models::load_pricing_sync;
use crate::parser::{parse_all_sessions, parse_all_sessions_static, StaticAggregate};
use crate::providers::get_all_providers;
use crate::types::{DateRange, ProjectSummary};

const PERIODS: &[(&str, &str)] = &[
    ("today", "Today"),
    ("week", "7 Days"),
    ("30days", "30 Days"),
    ("month", "This Month"),
    ("all", "All Time"),
];

fn period_to_index(p: &str) -> usize {
    match p {
        "today" => 0,
        "week" => 1,
        "30days" => 2,
        "month" => 3,
        "all" => 4,
        _ => 1,
    }
}

fn period_str(p: Period) -> &'static str {
    match p {
        Period::Today => "today",
        Period::Week => "week",
        Period::ThirtyDays => "30days",
        Period::Month => "month",
        Period::All => "all",
    }
}

/// Detect the terminal size for the non-interactive render path, clamped to
/// sane minimums (60×20) so the dashboard layout always fits.
///
/// Prefer `$COLUMNS` / `$LINES` — most shells set these — because
/// `crossterm::terminal::size()` opens `/dev/tty` under the hood, and even
/// its failure path costs ~4 ms on macOS when stdin is non-TTY. Only fall
/// back to crossterm when stdout is a TTY AND the env vars aren't set.
fn detect_terminal_size() -> (u16, u16) {
    let env_cols: Option<u16> = std::env::var("COLUMNS").ok().and_then(|s| s.parse().ok());
    let env_rows: Option<u16> = std::env::var("LINES").ok().and_then(|s| s.parse().ok());
    let (cols, rows) = match (env_cols, env_rows) {
        (Some(c), Some(r)) => (c, r),
        _ => {
            if io::stdout().is_terminal() {
                crossterm::terminal::size().unwrap_or((100, 40))
            } else {
                (env_cols.unwrap_or(100), env_rows.unwrap_or(40))
            }
        }
    };
    (cols.max(60), rows.max(20))
}

/// Memoized "parse → render → write to stdout" flow shared by the rich
/// (`run_render_once`) and static (`run_static_sync`) non-TTY paths.
///
/// 1. Compute a session-files signature so an append to any jsonl / DB
///    flips the output-cache fingerprint. Skipped under `--no-cache` or
///    `--no-output-cache`.
/// 2. `try_serve` — if the fingerprint matches, the previous run's bytes
///    are already on stdout and we return.
/// 3. Call the caller-supplied `compute` closure with the pre-stats map,
///    which does the actual parse + render and returns the rendered
///    bytes. Threading `pre_stats` through means `partition_cache` inside
///    `parse_all_sessions` reuses the stats we just did for the
///    fingerprint instead of re-stat'ing every source.
/// 4. Print the bytes, join persist threads, then `store` the output
///    cache with a *fresh* session signature. The persist thread may
///    have just rewritten `report-cache.bin`, so we re-stat after the
///    join to capture its new mtime.
fn render_with_output_cache<F>(
    period: Period,
    provider: &str,
    format: &str,
    extra: u64,
    compute: F,
) -> Result<()>
where
    F: FnOnce(&HashMap<String, (u64, u64)>) -> Vec<u8>,
{
    use std::io::Write;
    let bypass = crate::parser::is_cache_bypassed();
    let output_cache_bypass = crate::parser::is_output_cache_bypassed();

    // Keep demo and real outputs on separate cache entries by stealing
    // the top bit of `extra` for the demo flag.
    let extra = if crate::parser::is_demo_mode() {
        extra | (1u64 << 63)
    } else {
        extra
    };

    let (pre_stats, session_sig) = if !bypass && !output_cache_bypass {
        crate::parser::stat_all_sources()
    } else {
        (HashMap::new(), 0)
    };

    if !bypass && !output_cache_bypass
        && crate::output_cache::try_serve(period_str(period), provider, format, extra, session_sig)
    {
        return Ok(());
    }

    let bytes = compute(&pre_stats);

    {
        let mut stdout = io::stdout().lock();
        let _ = stdout.write_all(&bytes);
    }

    crate::parser::join_pending_persists();
    if !bypass && !output_cache_bypass {
        let (_, fresh_sig) = crate::parser::stat_all_sources();
        crate::output_cache::store(period_str(period), provider, format, extra, fresh_sig, &bytes);
    }
    Ok(())
}

pub fn get_date_range(period: &str) -> Option<DateRange> {
    if period == "all" {
        return None;
    }
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

    Some(DateRange { start, end })
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
    fn switch_to(&mut self, tx: &mpsc::Sender<LoadResult>) {
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
        tx: &mpsc::Sender<LoadResult>,
    ) {
        let key = cache_key(&period, &provider);
        if !self.in_flight.insert(key.clone()) {
            return;
        }
        let date_range = get_date_range(&period);
        let filter = if provider == "all" { None } else { Some(provider) };
        let generation = self.generation;
        let tx = tx.clone();
        std::thread::spawn(move || {
            let projects = parse_all_sessions(
                date_range.as_ref(),
                filter.as_deref(),
                &HashMap::new(),
            )
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
    fn prefetch_other_periods(&mut self, tx: &mpsc::Sender<LoadResult>) {
        let provider = self.provider_filter.clone();
        for (period_key, _) in PERIODS {
            let key = cache_key(period_key, &provider);
            if !self.period_cache.contains_key(&key) && !self.in_flight.contains(&key) {
                self.start_parse(period_key.to_string(), provider.clone(), tx);
            }
        }
    }

    fn detect_providers(&mut self) {
        // Reuse the on-disk discovery cache instead of walking the
        // filesystem again — `parse_all_sessions` is already going to
        // do the canonical fingerprint+walk, and an out-of-date detection
        // list only affects the `p` keybind cycle (the user can still
        // type any provider name). If the discovery cache is missing,
        // fall back to a parallel walk.
        let cache = crate::discovery_cache::load();
        let mut detected = vec!["all".to_string()];
        if let Some(c) = &cache {
            for p in get_all_providers() {
                let has = c
                    .sources
                    .get(p.name())
                    .map(|srcs| !srcs.is_empty())
                    .unwrap_or(false);
                if has {
                    detected.push(p.name().to_string());
                }
            }
        } else {
            // First-ever run with no cache — fan out a parallel walk so
            // the per-provider discovery costs run concurrently.
            use rayon::prelude::*;
            let providers = get_all_providers();
            let mut walked: Vec<String> = providers
                .par_iter()
                .filter_map(|p| match p.discover_sessions() {
                    Ok(sessions) if !sessions.is_empty() => Some(p.name().to_string()),
                    _ => None,
                })
                .collect();
            detected.append(&mut walked);
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

pub fn run(period: Period, provider: &str, refresh: Option<u64>) -> Result<()> {
    if !io::stdin().is_terminal() {
        // Debug: CODEBURN_FULL_STATIC=1 routes through the full async-style
        // parse (with category breakdown) for verification against the
        // fast aggregate.
        if std::env::var_os("CODEBURN_FULL_STATIC").is_some() {
            return run_static(period, provider);
        }
        // One-shot rich render — matches the npx `codeburn report`
        // behaviour where any non-TTY stdin (`< /dev/null`, piped, file
        // redirect) emits the full ratatui dashboard inline. Terminal
        // size falls back to a sensible default when stdout isn't a TTY
        // either (file/pipe), so the captured ANSI is still parseable.
        return run_render_once(period, provider);
    }

    let prof_file = std::env::var_os("CODEBURN_PROFILE").map(|_| {
        // Profiling appended to /tmp/codeburn-tui-prof.log so it doesn't
        // collide with the alternate screen.
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("/tmp/codeburn-tui-prof.log")
            .ok()
    }).flatten();
    let log_prof = |prof: &Option<std::fs::File>, label: &str, t: Instant| {
        if let Some(mut f) = prof.as_ref().and_then(|f| f.try_clone().ok()) {
            use std::io::Write;
            let _ = writeln!(f, "[tui] {:<28} {:>8.2} ms", label, t.elapsed().as_secs_f64() * 1000.0);
        }
    };
    let t0 = Instant::now();
    if prof_file.is_some() {
        if let Some(mut f) = prof_file.as_ref().and_then(|f| f.try_clone().ok()) {
            use std::io::Write;
            let _ = writeln!(f, "==== tui run start ====");
        }
    }

    let t = Instant::now();
    load_pricing_sync();
    log_prof(&prof_file, "load_pricing", t);

    let mut app = App::new(period, provider);
    let t = Instant::now();
    app.detect_providers();
    log_prof(&prof_file, "detect_providers", t);

    // Channel for background load results so tab switches don't block render.
    let (tx, rx) = mpsc::channel::<LoadResult>();

    // Kick off the initial parse on a background thread immediately,
    // then race a short timeout against it. On a warm cache the parse
    // wins (~25 ms) and we render the fully-populated TUI on the very
    // first frame — no loading flash. On a cold cache the timeout wins
    // and we enter the alt screen with the loading panel showing, so
    // the user gets immediate visual feedback during the slow parse
    // (which can run hundreds of ms).
    let t = Instant::now();
    let init_period = app.current_period().to_string();
    let init_provider = app.provider_filter.clone();
    let init_key = cache_key(&init_period, &init_provider);
    app.in_flight.insert(init_key.clone());
    {
        let key = init_key.clone();
        let date_range = get_date_range(&init_period);
        let filter = if init_provider == "all" {
            None
        } else {
            Some(init_provider.clone())
        };
        let generation = app.generation;
        let tx2 = tx.clone();
        std::thread::spawn(move || {
            let projects = parse_all_sessions(
                date_range.as_ref(),
                filter.as_deref(),
                &HashMap::new(),
            )
            .unwrap_or_default();
            let _ = tx2.send(LoadResult { cache_key: key, generation, projects });
        });
    }
    // Race: wait up to 80 ms for the parse. Faster than the 100 ms human
    // perception threshold, so a hit feels instant; a miss falls through
    // to the loading screen quickly enough that the user never thinks
    // the program hung.
    let mut got_initial = false;
    if let Ok(res) = rx.recv_timeout(Duration::from_millis(80)) {
        if res.cache_key == init_key {
            app.in_flight.remove(&res.cache_key);
            app.period_cache.insert(res.cache_key.clone(), res.projects.clone());
            app.projects = res.projects;
            app.loading = false;
            app.active_key = None;
            got_initial = true;
        }
    }
    if !got_initial {
        // Parse is still running — show the loading view until the
        // background thread sends its result through `rx`.
        app.loading = true;
        app.active_key = Some(init_key.clone());
    }
    log_prof(&prof_file, "initial parse race", t);

    let t = Instant::now();
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    log_prof(&prof_file, "enter alternate screen", t);
    // RAII guard — from here on, any panic or early return restores the terminal.
    let _terminal_guard = TerminalGuard;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    log_prof(&prof_file, "TOTAL pre-loop", t0);
    let mut first_data_logged = got_initial;
    if got_initial {
        log_prof(&prof_file, "TOTAL launch->data", t0);
    }

    // Warm the other periods in the background. Safe regardless of
    // whether the initial parse landed yet — `start_parse` dedupes via
    // `in_flight` so we won't double-spawn the active key.
    app.prefetch_other_periods(&tx);

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
                if !first_data_logged {
                    log_prof(&prof_file, "TOTAL launch->data", t0);
                    first_data_logged = true;
                }
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
                        KeyCode::Char('5') => {
                            if app.period_idx != 4 {
                                app.period_idx = 4;
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
                            app.detect_providers();
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
                app.detect_providers();
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
            app.detect_providers();
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

/// One-shot rich render to stdout (no alt screen, no raw mode).
/// Used when stdin is redirected — `cburn report < /dev/null` from a
/// shell, or piped output like `cburn report | head` — to dump the
/// full ratatui dashboard inline. Matches the npx `codeburn report`
/// behaviour so the two implementations can be diff-compared.
///
/// Renders directly into a `ratatui::buffer::Buffer` (no Terminal /
/// CrosstermBackend, both of which require a live TTY) and walks the
/// buffer cells emitting ANSI to stdout. The result is memoized via
/// `output_cache` keyed on terminal width so repeat invocations at the
/// same size short-circuit the parse + render pipeline.
fn run_render_once(period: Period, provider: &str) -> Result<()> {
    let (cols, rows) = detect_terminal_size();
    let extra = (cols as u64) | ((rows as u64) << 16);

    render_with_output_cache(period, provider, "rich", extra, |pre_stats| {
        // Pricing is lazy-loaded inside `get_model_costs` on first hit —
        // cached calls already carry `cost_usd`, so the eager
        // `load_pricing_sync()` shaved here only matters on a cache miss.
        let mut app = App::new(period, provider);
        // `detect_providers` reads the discovery cache and fills in
        // `app.detected_providers`; without it `render_tabs` hides the
        // "[p] all" provider label.
        app.detect_providers();

        let date_range = get_date_range(period_str(period));
        let filter = if provider == "all" { None } else { Some(provider) };
        app.projects = crate::parser::parse_all_sessions(date_range.as_ref(), filter, pre_stats)
            .unwrap_or_default();
        app.loading = false;

        // Size the off-screen buffer to the dashboard's natural height so
        // the one-shot view never needs to scroll.
        let data = widgets::build_dashboard_data(&app.projects, app.current_period());
        let dw = crate::tui::layout::dash_width(cols);
        let wide = crate::tui::layout::is_wide(dw);
        let bw = crate::tui::layout::bar_width(if wide { dw / 2 - 4 } else { dw - 4 });
        let content_h = widgets::dashboard_natural_height(wide, cols, bw, &data);
        // Tabs + header + content + status bar = 1 + 3 + content + 3.
        let total_h = 1u16
            .saturating_add(3)
            .saturating_add(content_h)
            .saturating_add(3)
            .max(rows);
        let area = ratatui::layout::Rect::new(0, 0, cols, total_h);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        widgets::render_into_buffer(&mut buf, area, &app, PERIODS);

        let mut out: Vec<u8> = Vec::with_capacity((cols as usize) * (total_h as usize) * 4);
        buffer_to_ansi(&buf, area, &mut out);
        out
    })
}

/// Walk a ratatui Buffer and emit ANSI escape sequences + cell contents
/// to `out`. Tracks current style so we only emit color/modifier
/// transitions when they actually change. Each row is terminated with a
/// reset + newline so the captured output stays clean when piped to a
/// pager or file.
fn buffer_to_ansi(buf: &ratatui::buffer::Buffer, area: ratatui::layout::Rect, out: &mut Vec<u8>) {
    use ratatui::style::{Color, Modifier};
    use std::io::Write;

    let mut cur_fg: Option<Color> = None;
    let mut cur_bg: Option<Color> = None;
    let mut cur_mod = Modifier::empty();

    let write_color = |out: &mut Vec<u8>, c: Color, fg: bool| {
        let prefix = if fg { 38 } else { 48 };
        match c {
            Color::Reset => {
                let _ = write!(out, "\x1b[{}9m", prefix / 10);
            }
            Color::Black => { let _ = write!(out, "\x1b[{}m", if fg { 30 } else { 40 }); }
            Color::Red => { let _ = write!(out, "\x1b[{}m", if fg { 31 } else { 41 }); }
            Color::Green => { let _ = write!(out, "\x1b[{}m", if fg { 32 } else { 42 }); }
            Color::Yellow => { let _ = write!(out, "\x1b[{}m", if fg { 33 } else { 43 }); }
            Color::Blue => { let _ = write!(out, "\x1b[{}m", if fg { 34 } else { 44 }); }
            Color::Magenta => { let _ = write!(out, "\x1b[{}m", if fg { 35 } else { 45 }); }
            Color::Cyan => { let _ = write!(out, "\x1b[{}m", if fg { 36 } else { 46 }); }
            Color::Gray => { let _ = write!(out, "\x1b[{}m", if fg { 37 } else { 47 }); }
            Color::DarkGray => { let _ = write!(out, "\x1b[{}m", if fg { 90 } else { 100 }); }
            Color::LightRed => { let _ = write!(out, "\x1b[{}m", if fg { 91 } else { 101 }); }
            Color::LightGreen => { let _ = write!(out, "\x1b[{}m", if fg { 92 } else { 102 }); }
            Color::LightYellow => { let _ = write!(out, "\x1b[{}m", if fg { 93 } else { 103 }); }
            Color::LightBlue => { let _ = write!(out, "\x1b[{}m", if fg { 94 } else { 104 }); }
            Color::LightMagenta => { let _ = write!(out, "\x1b[{}m", if fg { 95 } else { 105 }); }
            Color::LightCyan => { let _ = write!(out, "\x1b[{}m", if fg { 96 } else { 106 }); }
            Color::White => { let _ = write!(out, "\x1b[{}m", if fg { 97 } else { 107 }); }
            Color::Indexed(i) => { let _ = write!(out, "\x1b[{};5;{}m", prefix, i); }
            Color::Rgb(r, g, b) => { let _ = write!(out, "\x1b[{};2;{};{};{}m", prefix, r, g, b); }
        }
    };

    for y in area.top()..area.bottom() {
        for x in area.left()..area.right() {
            let cell = match buf.cell((x, y)) { Some(c) => c, None => continue };
            let fg = cell.fg;
            let bg = cell.bg;
            let modi = cell.modifier;
            // Emit modifier transitions first — bold/italic/underline.
            if modi != cur_mod {
                let _ = out.write_all(b"\x1b[0m");
                cur_fg = None;
                cur_bg = None;
                if modi.contains(Modifier::BOLD) { let _ = out.write_all(b"\x1b[1m"); }
                if modi.contains(Modifier::DIM) { let _ = out.write_all(b"\x1b[2m"); }
                if modi.contains(Modifier::ITALIC) { let _ = out.write_all(b"\x1b[3m"); }
                if modi.contains(Modifier::UNDERLINED) { let _ = out.write_all(b"\x1b[4m"); }
                if modi.contains(Modifier::REVERSED) { let _ = out.write_all(b"\x1b[7m"); }
                cur_mod = modi;
            }
            if Some(fg) != cur_fg {
                write_color(out, fg, true);
                cur_fg = Some(fg);
            }
            if Some(bg) != cur_bg {
                write_color(out, bg, false);
                cur_bg = Some(bg);
            }
            let _ = out.write_all(cell.symbol().as_bytes());
        }
        let _ = out.write_all(b"\x1b[0m\n");
        cur_fg = None;
        cur_bg = None;
        cur_mod = Modifier::empty();
    }
}

/// Fully synchronous non-TTY report — skips tokio entirely. Called by
/// `main` when stdin isn't a terminal and the command is a
/// report/today/month with no `--refresh`. Output is memoized through
/// `render_with_output_cache`, so repeat invocations replay the stored
/// bytes in <1 ms as long as no session file has changed.
pub fn run_static_sync(period: Period, provider: &str) -> Result<()> {
    render_with_output_cache(period, provider, "static", 0, |_pre_stats| {
        // The static path's `parse_all_sessions_static` has its own cache
        // read+write logic today; it doesn't accept pre-stats, so we don't
        // forward the map. Wiring that through is a parser-side refactor
        // orthogonal to this cleanup.
        let date_range = get_date_range(period_str(period));
        let filter = if provider == "all" { None } else { Some(provider) };
        let agg = parse_all_sessions_static(date_range.as_ref(), filter).unwrap_or_default();

        let mut buf: Vec<u8> = Vec::with_capacity(1024);
        render_static_aggregate_into(&mut buf, period, &agg);
        buf
    })
}

fn render_static_aggregate_into<W: std::io::Write>(out: &mut W, period: Period, agg: &StaticAggregate) {
    let all_input = agg.total_input + agg.total_cache_read + agg.total_cache_write;
    let cache_pct = if all_input > 0 {
        (agg.total_cache_read as f64 / all_input as f64) * 100.0
    } else {
        0.0
    };
    let total_cost: f64 = agg.projects.iter().map(|p| p.total_cost_usd).sum();
    let total_calls: u64 = agg.projects.iter().map(|p| p.total_api_calls).sum();
    let period_label = PERIODS[period_to_index(period_str(period))].1;

    let _ = writeln!(out);
    let _ = writeln!(out, "  CodeBurn - {}", period_label);
    let _ = writeln!(out, "  ──────────────────────");
    let _ = writeln!(
        out,
        "  Cost: {}  Calls: {}  Sessions: {}",
        format_cost(total_cost),
        total_calls,
        agg.total_sessions
    );
    let _ = writeln!(
        out,
        "  Input: {}  Output: {}  Cache: {:.0}%",
        format_tokens(agg.total_input),
        format_tokens(agg.total_output),
        cache_pct
    );
    let _ = writeln!(out);

    if !agg.projects.is_empty() {
        let _ = writeln!(out, "  Projects:");
        for p in agg.projects.iter().take(8) {
            let _ = writeln!(out, "    {} - {}", format_cost(p.total_cost_usd), p.project_path);
        }
        let _ = writeln!(out);
    }
}

fn run_static(period: Period, provider: &str) -> Result<()> {
    load_pricing_sync();
    let date_range = get_date_range(period_str(period));
    let filter = if provider == "all" {
        None
    } else {
        Some(provider)
    };
    let projects = parse_all_sessions(date_range.as_ref(), filter, &HashMap::new())
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
