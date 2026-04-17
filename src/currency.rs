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

use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::config::{get_config_path, read_config, save_config};

const FRANKFURTER_URL: &str = "https://api.frankfurter.app/latest?from=USD&to=";
const CACHE_TTL_SECS: u64 = 24 * 60 * 60;

#[derive(Clone)]
pub struct CurrencyState {
    pub code: String,
    pub rate: f64,
    pub symbol: String,
}

impl Default for CurrencyState {
    fn default() -> Self {
        CurrencyState {
            code: "USD".to_string(),
            rate: 1.0,
            symbol: "$".to_string(),
        }
    }
}

static ACTIVE: std::sync::LazyLock<Mutex<CurrencyState>> =
    std::sync::LazyLock::new(|| Mutex::new(CurrencyState::default()));

static SYMBOLS: &[(&str, &str)] = &[
    ("USD", "$"),
    ("EUR", "\u{20ac}"),
    ("GBP", "\u{00a3}"),
    ("JPY", "\u{00a5}"),
    ("CNY", "\u{00a5}"),
    ("KRW", "\u{20a9}"),
    ("INR", "\u{20b9}"),
    ("BRL", "R$"),
    ("CAD", "C$"),
    ("AUD", "A$"),
    ("CHF", "CHF"),
    ("SEK", "kr"),
    ("NOK", "kr"),
    ("DKK", "kr"),
    ("PLN", "z\u{0142}"),
    ("TRY", "\u{20ba}"),
    ("MXN", "MX$"),
];

fn resolve_symbol(code: &str) -> String {
    for (c, s) in SYMBOLS {
        if *c == code {
            return s.to_string();
        }
    }
    code.to_string()
}

pub fn is_valid_currency_code(code: &str) -> bool {
    // ISO 4217: 3 uppercase alpha characters
    code.len() == 3 && code.chars().all(|c| c.is_ascii_uppercase())
}

fn get_rate_cache_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join(".cache/codeburn/exchange-rate.json")
}

#[derive(Serialize, Deserialize)]
struct CachedRate {
    timestamp: u64,
    code: String,
    rate: f64,
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn load_cached_rate(code: &str) -> Option<f64> {
    let path = get_rate_cache_path();
    let content = std::fs::read_to_string(path).ok()?;
    let cached: CachedRate = serde_json::from_str(&content).ok()?;
    if cached.code != code {
        return None;
    }
    if now_secs() - cached.timestamp > CACHE_TTL_SECS {
        return None;
    }
    Some(cached.rate)
}

fn cache_rate(code: &str, rate: f64) {
    let path = get_rate_cache_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let cached = CachedRate {
        timestamp: now_secs(),
        code: code.to_string(),
        rate,
    };
    let _ = std::fs::write(path, serde_json::to_string(&cached).unwrap_or_default());
}

async fn fetch_rate(code: &str) -> Result<f64> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;
    let url = format!("{}{}", FRANKFURTER_URL, code);
    let resp = client.get(&url).send().await?;
    let data: serde_json::Value = resp.json().await?;
    let rate = data
        .get("rates")
        .and_then(|r| r.get(code))
        .and_then(|v| v.as_f64())
        .ok_or_else(|| anyhow::anyhow!("No rate for {}", code))?;
    Ok(rate)
}

async fn get_exchange_rate(code: &str) -> f64 {
    if code == "USD" {
        return 1.0;
    }
    if let Some(cached) = load_cached_rate(code) {
        return cached;
    }
    match fetch_rate(code).await {
        Ok(rate) => {
            cache_rate(code, rate);
            rate
        }
        Err(_) => 1.0,
    }
}

pub async fn load_currency() -> Result<()> {
    let config = read_config()?;
    let currency = match config.currency {
        Some(c) => c,
        None => return Ok(()),
    };

    let code = currency.code.to_uppercase();
    let rate = get_exchange_rate(&code).await;
    let symbol = currency.symbol.unwrap_or_else(|| resolve_symbol(&code));

    let mut active = ACTIVE.lock().unwrap();
    *active = CurrencyState { code, rate, symbol };
    Ok(())
}

/// Synchronous currency loader for the fast status path.
/// Uses disk cache or defaults to 1.0 — never makes network requests.
pub fn load_currency_sync() -> Result<()> {
    let config = read_config()?;
    let currency = match config.currency {
        Some(c) => c,
        None => return Ok(()),
    };

    let code = currency.code.to_uppercase();
    let rate = if code == "USD" {
        1.0
    } else {
        load_cached_rate(&code).unwrap_or(1.0)
    };
    let symbol = currency.symbol.unwrap_or_else(|| resolve_symbol(&code));

    let mut active = ACTIVE.lock().unwrap();
    *active = CurrencyState { code, rate, symbol };
    Ok(())
}

pub fn get_currency() -> CurrencyState {
    ACTIVE.lock().unwrap().clone()
}

pub fn format_cost(cost_usd: f64) -> String {
    let state = get_currency();
    let cost = cost_usd * state.rate;

    if cost >= 1.0 {
        format!("{}{:.2}", state.symbol, cost)
    } else if cost >= 0.01 {
        format!("{}{:.3}", state.symbol, cost)
    } else {
        format!("{}{:.4}", state.symbol, cost)
    }
}

pub fn convert_cost(cost_usd: f64) -> f64 {
    let state = get_currency();
    (cost_usd * state.rate * 100.0).round() / 100.0
}

pub fn get_cost_column_header() -> String {
    let state = get_currency();
    format!("Cost ({})", state.code)
}

pub async fn run_currency(code: Option<String>, symbol: Option<String>, reset: bool) -> Result<()> {
    if reset {
        let mut config = read_config()?;
        config.currency = None;
        save_config(&config)?;
        println!("\n  Currency reset to USD.\n");
        return Ok(());
    }

    if let Some(code) = code {
        let upper = code.to_uppercase();
        if !is_valid_currency_code(&upper) {
            eprintln!("\n  \"{}\" is not a valid ISO 4217 currency code.\n", code);
            std::process::exit(1);
        }

        let mut config = read_config()?;
        config.currency = Some(crate::config::CurrencyConfig {
            code: upper.clone(),
            symbol: symbol.clone(),
        });
        save_config(&config)?;

        load_currency().await?;
        let state = get_currency();

        println!("\n  Currency set to {}.", upper);
        println!("  Symbol: {}", state.symbol);
        println!("  Rate: 1 USD = {} {}", state.rate, upper);
        println!(
            "  Config saved to {}\n",
            get_config_path().display()
        );
    } else {
        let state = get_currency();
        if state.code == "USD" && state.rate == 1.0 {
            println!("\n  Currency: USD (default)");
            println!("  Config: {}\n", get_config_path().display());
        } else {
            println!("\n  Currency: {}", state.code);
            println!("  Symbol: {}", state.symbol);
            println!("  Rate: 1 USD = {} {}", state.rate, state.code);
            println!("  Config: {}\n", get_config_path().display());
        }
    }

    Ok(())
}
