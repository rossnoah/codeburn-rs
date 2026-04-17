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

use anyhow::Result;

use crate::cli::StatusFormat;
use crate::currency::{format_cost, get_currency, load_currency_sync};
use crate::models::load_pricing_sync;
use crate::parser::parse_status_fast;
use crate::types::StatusAggregate;

pub fn format_tokens(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn render_status_bar(agg: &StatusAggregate) -> String {
    format!(
        "\n  \x1b[1mToday\x1b[0m  \x1b[93m{}\x1b[0m  \x1b[2m{} calls\x1b[0m    \x1b[1mMonth\x1b[0m  \x1b[93m{}\x1b[0m  \x1b[2m{} calls\x1b[0m\n",
        format_cost(agg.today_cost),
        agg.today_calls,
        format_cost(agg.month_cost),
        agg.month_calls,
    )
}

/// Fully synchronous status command — no async runtime needed.
/// Uses disk-cached pricing/currency (never makes network requests).
pub fn run_status_sync(fmt: StatusFormat, provider: &str) -> Result<()> {
    // Load pricing and currency from disk cache (synchronous)
    load_pricing_sync();
    load_currency_sync()?;

    let pf = if provider == "all" { None } else { Some(provider) };

    // Single pass computes today/week/month simultaneously
    let agg = parse_status_fast(pf)?;

    match fmt {
        StatusFormat::Terminal => {
            print!("{}", render_status_bar(&agg));
        }
        StatusFormat::Json => {
            let state = get_currency();
            let json = serde_json::json!({
                "currency": state.code,
                "today": {
                    "cost": (agg.today_cost * state.rate * 100.0).round() / 100.0,
                    "calls": agg.today_calls,
                },
                "month": {
                    "cost": (agg.month_cost * state.rate * 100.0).round() / 100.0,
                    "calls": agg.month_calls,
                },
            });
            println!("{}", serde_json::to_string(&json)?);
        }
    }

    Ok(())
}
