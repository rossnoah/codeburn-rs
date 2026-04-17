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

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(
    name = "codeburn",
    about = "See where your AI coding tokens go - by task, tool, model, and project",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,

    /// Bypass all caches and force a fresh parse of every source.
    /// Useful after manually editing sessions or debugging parser changes.
    #[arg(long, global = true)]
    pub no_cache: bool,

    /// Skip the per-(period, provider) memoized output cache for the
    /// non-TTY static report path. Doesn't affect the underlying
    /// `report-cache.bin` parse cache. Mostly used for benchmarking the
    /// full parse pipeline without the output replay short-circuit.
    #[arg(long, global = true)]
    pub no_output_cache: bool,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Interactive usage dashboard (default when no subcommand given)
    Report {
        /// Starting period: today, week, 30days, month
        #[arg(short, long, default_value = "week")]
        period: Period,

        /// Filter by provider: all, claude, codex, cursor, opencode
        #[arg(long, default_value = "all")]
        provider: String,

        /// Auto-refresh interval in seconds
        #[arg(long)]
        refresh: Option<u64>,
    },

    /// Today's usage dashboard
    Today {
        /// Filter by provider
        #[arg(long, default_value = "all")]
        provider: String,

        /// Auto-refresh interval in seconds
        #[arg(long)]
        refresh: Option<u64>,
    },

    /// This month's usage dashboard
    Month {
        /// Filter by provider
        #[arg(long, default_value = "all")]
        provider: String,

        /// Auto-refresh interval in seconds
        #[arg(long)]
        refresh: Option<u64>,
    },

    /// Compact status output (today + week + month)
    Status {
        /// Output format: terminal, menubar, json
        #[arg(long, default_value = "terminal")]
        format: StatusFormat,

        /// Filter by provider
        #[arg(long, default_value = "all")]
        provider: String,
    },

    /// Export usage data to CSV or JSON
    Export {
        /// Export format: csv, json
        #[arg(short, long, default_value = "csv")]
        format: ExportFormat,

        /// Output file path
        #[arg(short, long)]
        output: Option<String>,

        /// Filter by provider
        #[arg(long, default_value = "all")]
        provider: String,
    },

    /// Set display currency (e.g. codeburn currency GBP)
    Currency {
        /// ISO 4217 currency code
        code: Option<String>,

        /// Override the currency symbol
        #[arg(long)]
        symbol: Option<String>,

        /// Reset to USD
        #[arg(long)]
        reset: bool,
    },

    /// Install macOS menu bar plugin (SwiftBar/xbar)
    InstallMenubar,

    /// Remove macOS menu bar plugin
    UninstallMenubar,

    /// Internal: build the cursor cache in the background. Invoked by a
    /// detached child process so the main report command can return
    /// immediately on cold starts.
    #[command(hide = true)]
    RefreshCursorCache,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Period {
    Today,
    Week,
    #[value(name = "30days")]
    ThirtyDays,
    Month,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum StatusFormat {
    Terminal,
    Menubar,
    Json,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ExportFormat {
    Csv,
    Json,
}
