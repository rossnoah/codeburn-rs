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
use std::sync::LazyLock;
use std::sync::OnceLock;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::types::{ModelCosts, Speed};

const LITELLM_URL: &str =
    "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json";
const CACHE_TTL_SECS: u64 = 24 * 60 * 60;
const WEB_SEARCH_COST: f64 = 0.01;

static FALLBACK_PRICING: LazyLock<HashMap<&str, ModelCosts>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    let mc = |i: f64, o: f64, cw: f64, cr: f64, fm: f64| ModelCosts {
        input_cost_per_token: i,
        output_cost_per_token: o,
        cache_write_cost_per_token: cw,
        cache_read_cost_per_token: cr,
        web_search_cost_per_request: WEB_SEARCH_COST,
        fast_multiplier: fm,
    };

    m.insert("claude-opus-4-7", mc(5e-6, 25e-6, 6.25e-6, 0.5e-6, 6.0));
    m.insert("claude-opus-4-6", mc(5e-6, 25e-6, 6.25e-6, 0.5e-6, 6.0));
    m.insert("claude-opus-4-5", mc(5e-6, 25e-6, 6.25e-6, 0.5e-6, 1.0));
    m.insert(
        "claude-opus-4-1",
        mc(15e-6, 75e-6, 18.75e-6, 1.5e-6, 1.0),
    );
    m.insert("claude-opus-4", mc(15e-6, 75e-6, 18.75e-6, 1.5e-6, 1.0));
    m.insert("claude-sonnet-4-6", mc(3e-6, 15e-6, 3.75e-6, 0.3e-6, 1.0));
    m.insert("claude-sonnet-4-5", mc(3e-6, 15e-6, 3.75e-6, 0.3e-6, 1.0));
    m.insert("claude-sonnet-4", mc(3e-6, 15e-6, 3.75e-6, 0.3e-6, 1.0));
    m.insert("claude-3-7-sonnet", mc(3e-6, 15e-6, 3.75e-6, 0.3e-6, 1.0));
    m.insert("claude-3-5-sonnet", mc(3e-6, 15e-6, 3.75e-6, 0.3e-6, 1.0));
    m.insert("claude-haiku-4-5", mc(1e-6, 5e-6, 1.25e-6, 0.1e-6, 1.0));
    m.insert(
        "claude-3-5-haiku",
        mc(0.8e-6, 4e-6, 1.0e-6, 0.08e-6, 1.0),
    );
    m.insert("gpt-4o", mc(2.5e-6, 10e-6, 2.5e-6, 1.25e-6, 1.0));
    m.insert("gpt-4o-mini", mc(0.15e-6, 0.6e-6, 0.15e-6, 0.075e-6, 1.0));
    m.insert(
        "gemini-2.5-pro",
        mc(1.25e-6, 10e-6, 1.25e-6, 0.315e-6, 1.0),
    );
    m.insert("gpt-5.3-codex", mc(2.5e-6, 10e-6, 2.5e-6, 1.25e-6, 1.0));
    m.insert("gpt-5.4", mc(2.5e-6, 10e-6, 2.5e-6, 1.25e-6, 1.0));
    m.insert(
        "gpt-5.4-mini",
        mc(0.4e-6, 1.6e-6, 0.4e-6, 0.2e-6, 1.0),
    );
    m.insert("gpt-5", mc(2.5e-6, 10e-6, 2.5e-6, 1.25e-6, 1.0));
    m
});

static SHORT_NAMES: LazyLock<Vec<(&str, &str)>> = LazyLock::new(|| {
    vec![
        ("claude-opus-4-7", "Opus 4.7"),
        ("claude-opus-4-6", "Opus 4.6"),
        ("claude-opus-4-5", "Opus 4.5"),
        ("claude-opus-4-1", "Opus 4.1"),
        ("claude-opus-4", "Opus 4"),
        ("claude-sonnet-4-6", "Sonnet 4.6"),
        ("claude-sonnet-4-5", "Sonnet 4.5"),
        ("claude-sonnet-4", "Sonnet 4"),
        ("claude-3-7-sonnet", "Sonnet 3.7"),
        ("claude-3-5-sonnet", "Sonnet 3.5"),
        ("claude-haiku-4-5", "Haiku 4.5"),
        ("claude-3-5-haiku", "Haiku 3.5"),
        ("gpt-4o-mini", "GPT-4o Mini"),
        ("gpt-4o", "GPT-4o"),
        ("gpt-5.4-mini", "GPT-5.4 Mini"),
        ("gpt-5.4", "GPT-5.4"),
        ("gpt-5.3-codex", "GPT-5.3 Codex"),
        ("gpt-5", "GPT-5"),
        ("gemini-2.5-pro", "Gemini 2.5 Pro"),
    ]
});

static PRICING_CACHE: OnceLock<HashMap<String, ModelCosts>> = OnceLock::new();

#[derive(Deserialize)]
struct LiteLLMEntry {
    input_cost_per_token: Option<f64>,
    output_cost_per_token: Option<f64>,
    cache_creation_input_token_cost: Option<f64>,
    cache_read_input_token_cost: Option<f64>,
    #[serde(default)]
    provider_specific_entry: Option<ProviderSpecific>,
}

#[derive(Deserialize)]
struct ProviderSpecific {
    fast: Option<f64>,
}

// Cache format matches the JS implementation so both tools can share
// ~/.cache/codeburn/litellm-pricing.json: camelCase fields, timestamp in ms.
#[derive(Serialize, Deserialize)]
struct CachedPricing {
    timestamp: u64,
    data: HashMap<String, CachedModelCosts>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CachedModelCosts {
    input_cost_per_token: f64,
    output_cost_per_token: f64,
    cache_write_cost_per_token: f64,
    cache_read_cost_per_token: f64,
    web_search_cost_per_request: f64,
    fast_multiplier: f64,
}

impl From<&CachedModelCosts> for ModelCosts {
    fn from(c: &CachedModelCosts) -> Self {
        ModelCosts {
            input_cost_per_token: c.input_cost_per_token,
            output_cost_per_token: c.output_cost_per_token,
            cache_write_cost_per_token: c.cache_write_cost_per_token,
            cache_read_cost_per_token: c.cache_read_cost_per_token,
            web_search_cost_per_request: c.web_search_cost_per_request,
            fast_multiplier: c.fast_multiplier,
        }
    }
}

impl From<&ModelCosts> for CachedModelCosts {
    fn from(c: &ModelCosts) -> Self {
        CachedModelCosts {
            input_cost_per_token: c.input_cost_per_token,
            output_cost_per_token: c.output_cost_per_token,
            cache_write_cost_per_token: c.cache_write_cost_per_token,
            cache_read_cost_per_token: c.cache_read_cost_per_token,
            web_search_cost_per_request: c.web_search_cost_per_request,
            fast_multiplier: c.fast_multiplier,
        }
    }
}

fn get_cache_dir() -> std::path::PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join(".cache")
        .join("codeburn")
}

fn get_cache_path() -> std::path::PathBuf {
    get_cache_dir().join("litellm-pricing.json")
}

fn parse_litellm_entry(entry: &LiteLLMEntry) -> Option<ModelCosts> {
    let input = entry.input_cost_per_token?;
    let output = entry.output_cost_per_token?;
    if input == 0.0 && output == 0.0 {
        return None;
    }
    Some(ModelCosts {
        input_cost_per_token: input,
        output_cost_per_token: output,
        cache_write_cost_per_token: entry
            .cache_creation_input_token_cost
            .unwrap_or(input * 1.25),
        cache_read_cost_per_token: entry
            .cache_read_input_token_cost
            .unwrap_or(input * 0.1),
        web_search_cost_per_request: WEB_SEARCH_COST,
        fast_multiplier: entry
            .provider_specific_entry
            .as_ref()
            .and_then(|p| p.fast)
            .unwrap_or(1.0),
    })
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn load_cached_pricing() -> Option<HashMap<String, ModelCosts>> {
    let (cached, _stale) = load_cached_pricing_any_age()?;
    Some(cached)
}

/// Load pricing from disk cache regardless of age.
/// Returns (map, is_stale).
fn load_cached_pricing_any_age() -> Option<(HashMap<String, ModelCosts>, bool)> {
    let path = get_cache_path();
    let content = std::fs::read_to_string(path).ok()?;
    let cached: CachedPricing = serde_json::from_str(&content).ok()?;
    let age_ms = now_millis().saturating_sub(cached.timestamp);
    let is_stale = age_ms > CACHE_TTL_SECS * 1000;
    let map = cached
        .data
        .iter()
        .map(|(k, v)| (k.clone(), ModelCosts::from(v)))
        .collect();
    Some((map, is_stale))
}

async fn fetch_and_cache_pricing() -> Result<HashMap<String, ModelCosts>> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;
    let resp = client.get(LITELLM_URL).send().await?;
    let data: HashMap<String, LiteLLMEntry> = resp.json().await?;
    let mut pricing = HashMap::new();

    for (name, entry) in &data {
        let costs = match parse_litellm_entry(entry) {
            Some(c) => c,
            None => continue,
        };
        pricing.insert(name.clone(), costs.clone());
        // Also index by stripped name so lookups work without provider prefix:
        // 'anthropic/claude-opus-4-6' is also queryable as 'claude-opus-4-6'.
        // First write wins so direct-provider entries take precedence.
        if let Some(slash) = name.find('/') {
            let stripped = &name[slash + 1..];
            if !stripped.is_empty() && !pricing.contains_key(stripped) {
                pricing.insert(stripped.to_string(), costs);
            }
        }
    }

    let cache_dir = get_cache_dir();
    let _ = std::fs::create_dir_all(&cache_dir);
    let cached = CachedPricing {
        timestamp: now_millis(),
        data: pricing.iter().map(|(k, v)| (k.clone(), v.into())).collect(),
    };
    let _ = std::fs::write(get_cache_path(), serde_json::to_string(&cached)?);

    Ok(pricing)
}

pub async fn load_pricing() -> Result<()> {
    if PRICING_CACHE.get().is_some() {
        return Ok(());
    }

    // Stale-while-revalidate: if we have a disk cache at any age, use it
    // immediately and refresh in the background. Only block on network when
    // we have zero pricing data, and even then fall back to the hardcoded
    // table if the fetch fails.
    match load_cached_pricing_any_age() {
        Some((pricing, is_stale)) => {
            let _ = PRICING_CACHE.set(pricing);
            if is_stale {
                std::thread::spawn(|| {
                    let rt = match tokio::runtime::Runtime::new() {
                        Ok(r) => r,
                        Err(_) => return,
                    };
                    let _ = rt.block_on(fetch_and_cache_pricing());
                });
            }
        }
        None => {
            // No cache. Use fallback immediately, fetch in background so the
            // next run has full data.
            let fallback: HashMap<String, ModelCosts> = FALLBACK_PRICING
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect();
            let _ = PRICING_CACHE.set(fallback);
            std::thread::spawn(|| {
                let rt = match tokio::runtime::Runtime::new() {
                    Ok(r) => r,
                    Err(_) => return,
                };
                let _ = rt.block_on(fetch_and_cache_pricing());
            });
        }
    }

    Ok(())
}

/// Synchronous pricing loader for the fast status path.
/// Uses disk cache or hardcoded fallback — never makes network requests.
/// This avoids requiring an async runtime.
pub fn load_pricing_sync() {
    if PRICING_CACHE.get().is_some() {
        return;
    }

    let pricing = if let Some(cached) = load_cached_pricing() {
        cached
    } else {
        // Use fallback pricing rather than making a network request
        FALLBACK_PRICING
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    };

    let _ = PRICING_CACHE.set(pricing);
}

fn get_canonical_name(model: &str) -> String {
    let s = if let Some(idx) = model.find('@') {
        &model[..idx]
    } else {
        model
    };
    // Strip trailing -YYYYMMDD
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len >= 9
        && bytes[len - 9] == b'-'
        && bytes[len - 8..].iter().all(|b| b.is_ascii_digit())
    {
        s[..len - 9].to_string()
    } else {
        s.to_string()
    }
}

pub fn get_model_costs(model: &str) -> Option<ModelCosts> {
    let canonical = get_canonical_name(model);

    // Lazily populate the disk-cached pricing on first lookup. Callers that
    // never hit `get_model_costs` (i.e. the full-cache-hit static report
    // path) skip the ~1.5 ms of disk read + JSON parse entirely.
    let cache = PRICING_CACHE.get_or_init(|| {
        if let Some(cached) = load_cached_pricing() {
            cached
        } else {
            FALLBACK_PRICING
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect()
        }
    });

    // 1. Check runtime cache (exact match)
    if let Some(costs) = cache.get(&canonical) {
        return Some(costs.clone());
    }

    // 2. Check fallback (exact match only)
    if let Some(costs) = FALLBACK_PRICING.get(canonical.as_str()) {
        return Some(costs.clone());
    }

    // 3. Check fallback (key-dash prefix: canonical starts with "key-")
    for (key, costs) in FALLBACK_PRICING.iter() {
        if canonical.starts_with(&format!("{}-", key)) {
            return Some(costs.clone());
        }
    }

    // 4. Check runtime cache (prefix matching both directions)
    for (key, costs) in cache.iter() {
        if canonical.starts_with(key) || key.starts_with(&canonical) {
            return Some(costs.clone());
        }
    }

    // 5. Check fallback (prefix only)
    for (key, costs) in FALLBACK_PRICING.iter() {
        if canonical.starts_with(key) {
            return Some(costs.clone());
        }
    }

    None
}

pub fn calculate_cost(
    model: &str,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_tokens: u64,
    cache_read_tokens: u64,
    web_search_requests: u64,
    speed: Speed,
) -> f64 {
    let costs = match get_model_costs(model) {
        Some(c) => c,
        None => return 0.0,
    };

    let multiplier = match speed {
        Speed::Fast => costs.fast_multiplier,
        Speed::Standard => 1.0,
    };

    multiplier
        * (input_tokens as f64 * costs.input_cost_per_token
            + output_tokens as f64 * costs.output_cost_per_token
            + cache_creation_tokens as f64 * costs.cache_write_cost_per_token
            + cache_read_tokens as f64 * costs.cache_read_cost_per_token
            + web_search_requests as f64 * costs.web_search_cost_per_request)
}

pub fn get_short_model_name(model: &str) -> String {
    let canonical = get_canonical_name(model);
    for (key, name) in SHORT_NAMES.iter() {
        if canonical.starts_with(key) {
            return name.to_string();
        }
    }
    canonical
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_name() {
        assert_eq!(
            get_canonical_name("claude-opus-4-5@20250101"),
            "claude-opus-4-5"
        );
        assert_eq!(
            get_canonical_name("claude-opus-4-5-20250101"),
            "claude-opus-4-5"
        );
        assert_eq!(
            get_canonical_name("claude-opus-4-5"),
            "claude-opus-4-5"
        );
    }

    #[test]
    fn test_short_model_name() {
        assert_eq!(get_short_model_name("claude-opus-4-6"), "Opus 4.6");
        assert_eq!(
            get_short_model_name("claude-sonnet-4-5@20250101"),
            "Sonnet 4.5"
        );
        assert_eq!(get_short_model_name("gpt-4o-mini"), "GPT-4o Mini");
        assert_eq!(
            get_short_model_name("unknown-model"),
            "unknown-model"
        );
    }

    #[test]
    fn test_fallback_pricing_lookup() {
        let costs = get_model_costs("claude-opus-4-6").unwrap();
        assert_eq!(costs.input_cost_per_token, 5e-6);
        assert_eq!(costs.fast_multiplier, 6.0);
    }

    #[test]
    fn test_calculate_cost() {
        // Using fallback pricing for claude-opus-4-6
        let cost = calculate_cost(
            "claude-opus-4-6",
            1000,  // input
            500,   // output
            200,   // cache write
            300,   // cache read
            0,     // web search
            Speed::Standard,
        );
        let expected = 1000.0 * 5e-6
            + 500.0 * 25e-6
            + 200.0 * 6.25e-6
            + 300.0 * 0.5e-6;
        assert!((cost - expected).abs() < 1e-12);
    }

    #[test]
    fn test_calculate_cost_fast_mode() {
        let standard = calculate_cost("claude-opus-4-6", 1000, 500, 0, 0, 0, Speed::Standard);
        let fast = calculate_cost("claude-opus-4-6", 1000, 500, 0, 0, 0, Speed::Fast);
        assert!((fast - standard * 6.0).abs() < 1e-12);
    }

    #[test]
    fn test_unknown_model_zero_cost() {
        let cost = calculate_cost("totally-unknown-model", 1000, 500, 0, 0, 0, Speed::Standard);
        assert_eq!(cost, 0.0);
    }

    #[test]
    fn test_opus_4_7_pricing() {
        // Opus 4.7 was released today (2026-04-16) with same pricing as 4.6.
        // LiteLLM hasn't indexed it yet, so we need a fallback entry.
        let costs = get_model_costs("claude-opus-4-7").unwrap();
        assert_eq!(costs.input_cost_per_token, 5e-6);
        assert_eq!(costs.output_cost_per_token, 25e-6);
        assert_eq!(costs.fast_multiplier, 6.0);
        assert_eq!(get_short_model_name("claude-opus-4-7"), "Opus 4.7");
    }

    #[test]
    fn test_opus_4_7_does_not_fallback_to_opus_4() {
        // Regression: before Opus 4.7 was in the fallback, it matched
        // `claude-opus-4` via startsWith, giving 3x wrong pricing.
        let costs_47 = get_model_costs("claude-opus-4-7").unwrap();
        let costs_4 = get_model_costs("claude-opus-4").unwrap();
        assert_ne!(costs_47.input_cost_per_token, costs_4.input_cost_per_token);
    }
}
