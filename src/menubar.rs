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
use std::fs;
use std::path::PathBuf;

fn get_plugin_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join("Library/Application Support/SwiftBar/plugins")
}

fn get_plugin_path() -> PathBuf {
    get_plugin_dir().join("cburn-status.5m.sh")
}

fn find_cburn_binary() -> String {
    // Try to find the cburn binary in PATH
    let output = std::process::Command::new("which")
        .arg("cburn")
        .output();
    match output {
        Ok(o) if o.status.success() => {
            String::from_utf8_lossy(&o.stdout).trim().to_string()
        }
        _ => "cburn".to_string(),
    }
}

#[cfg(target_os = "macos")]
pub fn install() -> Result<()> {
    let plugin_dir = get_plugin_dir();
    if !plugin_dir.exists() {
        anyhow::bail!(
            "SwiftBar plugins directory not found at {}.\n  Install SwiftBar: brew install --cask swiftbar",
            plugin_dir.display()
        );
    }

    let binary = find_cburn_binary();
    let script = format!(
        "#!/bin/bash\n{} status --format menubar\n",
        binary
    );

    let path = get_plugin_path();
    fs::write(&path, &script)?;

    // Make executable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755))?;
    }

    println!(
        "\n  Installed menu bar plugin at {}\n  It will refresh every 5 minutes.\n",
        path.display()
    );
    Ok(())
}

#[cfg(target_os = "macos")]
pub fn uninstall() -> Result<()> {
    let path = get_plugin_path();
    if path.exists() {
        fs::remove_file(&path)?;
        println!("\n  Removed menu bar plugin.\n");
    } else {
        println!("\n  No menu bar plugin found.\n");
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
pub fn install() -> Result<()> {
    anyhow::bail!("Menu bar plugin is only available on macOS.")
}

#[cfg(not(target_os = "macos"))]
pub fn uninstall() -> Result<()> {
    anyhow::bail!("Menu bar plugin is only available on macOS.")
}
