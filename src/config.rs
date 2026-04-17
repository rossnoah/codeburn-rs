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
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CodeburnConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub currency: Option<CurrencyConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrencyConfig {
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}

pub fn get_config_dir() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("~/.config"))
        .join("codeburn")
}

pub fn get_config_path() -> PathBuf {
    get_config_dir().join("config.json")
}

pub fn read_config() -> Result<CodeburnConfig> {
    let path = get_config_path();
    if !path.exists() {
        return Ok(CodeburnConfig::default());
    }
    let content = std::fs::read_to_string(&path)?;
    Ok(serde_json::from_str(&content).unwrap_or_default())
}

pub fn save_config(config: &CodeburnConfig) -> Result<()> {
    let dir = get_config_dir();
    std::fs::create_dir_all(&dir)?;
    let content = serde_json::to_string_pretty(config)?;
    std::fs::write(get_config_path(), content)?;
    Ok(())
}
