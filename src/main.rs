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

    // Non-TTY `report`/`today`/`month` is a pure sync pipeline — skip the
    // multi-thread tokio runtime entirely. This is the hot path for the
    // published bench (and for any scripted `codeburn report` call); saving
    // ~3-5 ms of runtime startup is the difference between "fast" and
    // "feels instant" when the cache is warm.
    // Debug: CODEBURN_FULL_STATIC=1 routes non-TTY through the full async
    // parse (with category breakdown) instead of the fast aggregate.
    let full_static = std::env::var_os("CODEBURN_FULL_STATIC").is_some();
    if !std::io::stdin().is_terminal() && !full_static {
        if let Some((period, provider)) = static_report_params(&command) {
            return tui::run_static_sync(period, &provider);
        }
    }

    // All other commands need the async runtime
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(async move {
        match command {
            Commands::Report {
                period,
                provider,
                refresh,
            } => {
                tui::run(period, &provider, refresh).await?;
            }
            Commands::Today { provider, refresh } => {
                tui::run(Period::Today, &provider, refresh).await?;
            }
            Commands::Month { provider, refresh } => {
                tui::run(Period::Month, &provider, refresh).await?;
            }
            Commands::Status { .. } => unreachable!(),
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
            Commands::RefreshCursorCache => unreachable!(),
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
