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

mod bash_utils;
mod classifier;
mod cli;
mod config;
mod currency;
mod discovery_cache;
mod export;
mod format;
mod menubar;
mod models;
mod output_cache;
mod parser;
mod providers;
mod report_cache;
mod session_cache;
mod tui;
mod types;

use std::io::IsTerminal;

use anyhow::Result;
use clap::Parser;

use cli::{Cli, Commands, Period};

fn main() -> Result<()> {
    let prof = std::env::var_os("CODEBURN_PROFILE").is_some();
    let t_start = std::time::Instant::now();
    let cli = Cli::parse();
    if prof {
        eprintln!("[prof main] clap parse            {:>8.2} ms", t_start.elapsed().as_secs_f64() * 1000.0);
    }

    parser::set_cache_bypass(cli.no_cache);
    parser::set_output_cache_bypass(cli.no_output_cache);

    let command = cli.command.unwrap_or(Commands::Report {
        period: Period::Week,
        provider: "all".to_string(),
        refresh: None,
    });

    // Status command is fully synchronous — skip tokio runtime entirely
    if let Commands::Status { format, provider } = &command {
        return format::run_status_sync(*format, provider);
    }

    // Internal: background cursor refresh subprocess. Runs to completion,
    // writes the cache, and exits. The spawning parent does not wait.
    if matches!(command, Commands::RefreshCursorCache) {
        providers::cursor::run_background_refresh();
        return Ok(());
    }

    // CODEBURN_STATIC_OUTPUT=1 forces the compact text aggregate even
    // when stdin is non-TTY — used by the hyperfine bench so we measure
    // the cached fast path instead of the rich dashboard render.
    // Debug: CODEBURN_FULL_STATIC=1 routes through the full async-style
    // parse with category breakdown for verification.
    let full_static = std::env::var_os("CODEBURN_FULL_STATIC").is_some();
    let static_only = std::env::var_os("CODEBURN_STATIC_OUTPUT").is_some();
    let stdin_tty = std::io::stdin().is_terminal();
    if !stdin_tty && static_only && !full_static {
        if let Some((period, provider)) = static_report_params(&command) {
            // Output memoization: when neither the report cache nor the
            // discovery cache has changed since the last run, we know the
            // rendered output would be byte-identical. Replay it straight
            // to stdout (~1 ms) and skip the whole parse pipeline.
            // --no-cache and --no-output-cache both bypass this.
            if !cli.no_cache && !cli.no_output_cache {
                let period_str = match period {
                    cli::Period::Today => "today",
                    cli::Period::Week => "week",
                    cli::Period::ThirtyDays => "30days",
                    cli::Period::Month => "month",
                };
                if output_cache::try_serve(period_str, &provider, "static", 0) {
                    if prof {
                        eprintln!(
                            "[prof main] output-cache hit       {:>8.2} ms",
                            t_start.elapsed().as_secs_f64() * 1000.0,
                        );
                    }
                    return Ok(());
                }
            }
            return tui::run_static_sync(period, &provider);
        }
    }

    // The interactive TUI is now fully sync — no tokio runtime startup
    // (~25 ms) and no spawn_blocking overhead per parse. Skip the
    // tokio::runtime::Builder cost for this hot path; the async branches
    // below (export/currency/menubar) still pay it for network I/O.
    match command {
        Commands::Report { period, provider, refresh } => {
            return tui::run(period, &provider, refresh);
        }
        Commands::Today { provider, refresh } => {
            return tui::run(Period::Today, &provider, refresh);
        }
        Commands::Month { provider, refresh } => {
            return tui::run(Period::Month, &provider, refresh);
        }
        Commands::Status { .. } => unreachable!(),
        Commands::RefreshCursorCache => unreachable!(),
        // The remaining variants drop into the tokio runtime below.
        Commands::Export { .. }
        | Commands::Currency { .. }
        | Commands::InstallMenubar
        | Commands::UninstallMenubar => {}
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(async move {
        match command {
            Commands::Report { .. }
            | Commands::Today { .. }
            | Commands::Month { .. }
            | Commands::Status { .. }
            | Commands::RefreshCursorCache => unreachable!(),
            Commands::Export {
                format,
                output,
                provider,
            } => {
                export::run_export(format, output, &provider).await?;
            }
            Commands::Currency {
                code,
                symbol,
                reset,
            } => {
                currency::run_currency(code, symbol, reset).await?;
            }
            Commands::InstallMenubar => {
                menubar::install()?;
            }
            Commands::UninstallMenubar => {
                menubar::uninstall()?;
            }
        }
        Ok(())
    })
}

fn static_report_params(cmd: &Commands) -> Option<(Period, String)> {
    match cmd {
        Commands::Report { period, provider, refresh: None } => Some((*period, provider.clone())),
        Commands::Today { provider, refresh: None } => Some((Period::Today, provider.clone())),
        Commands::Month { provider, refresh: None } => Some((Period::Month, provider.clone())),
        _ => None,
    }
}
