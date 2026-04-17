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

use std::path::Path;

use regex::Regex;
use std::sync::LazyLock;

static QUOTE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#""[^"]*"|'[^']*'"#).unwrap());
static SEPARATOR_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\s*(?:&&|;|\|)\s*").unwrap());

fn strip_quoted_strings(command: &str) -> String {
    QUOTE_RE
        .replace_all(command, |m: &regex::Captures| " ".repeat(m[0].len()))
        .into_owned()
}

pub fn extract_bash_commands(command: &str) -> Vec<String> {
    if command.trim().is_empty() {
        return Vec::new();
    }

    let stripped = strip_quoted_strings(command);

    // Find separator positions in the stripped string
    let mut separators: Vec<(usize, usize)> = Vec::new();
    for m in SEPARATOR_RE.find_iter(&stripped) {
        separators.push((m.start(), m.end()));
    }

    // Build ranges using the original command
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    let mut cursor = 0;
    for (sep_start, sep_end) in &separators {
        ranges.push((cursor, *sep_start));
        cursor = *sep_end;
    }
    ranges.push((cursor, command.len()));

    let mut commands = Vec::new();
    for (start, end) in ranges {
        let segment = command[start..end].trim();
        if segment.is_empty() {
            continue;
        }
        let first_token = segment.split_whitespace().next().unwrap_or("");
        let base = Path::new(first_token)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");

        if !base.is_empty() && base != "cd" {
            commands.push(base.to_string());
        }
    }

    commands
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_command() {
        assert_eq!(extract_bash_commands("ls -la"), vec!["ls"]);
    }

    #[test]
    fn test_chained_commands() {
        assert_eq!(
            extract_bash_commands("npm run build && npm test"),
            vec!["npm", "npm"]
        );
    }

    #[test]
    fn test_piped_commands() {
        assert_eq!(
            extract_bash_commands("cat file.txt | grep pattern"),
            vec!["cat", "grep"]
        );
    }

    #[test]
    fn test_semicolons() {
        assert_eq!(
            extract_bash_commands("echo hello; ls"),
            vec!["echo", "ls"]
        );
    }

    #[test]
    fn test_path_command() {
        assert_eq!(extract_bash_commands("/usr/bin/env node"), vec!["env"]);
    }

    #[test]
    fn test_cd_filtered() {
        assert_eq!(
            extract_bash_commands("cd /tmp && ls"),
            vec!["ls"]
        );
    }

    #[test]
    fn test_quoted_strings_preserved() {
        assert_eq!(
            extract_bash_commands(r#"echo "hello && world""#),
            vec!["echo"]
        );
    }

    #[test]
    fn test_empty() {
        assert!(extract_bash_commands("").is_empty());
        assert!(extract_bash_commands("   ").is_empty());
    }

    #[test]
    fn test_complex_pipeline() {
        assert_eq!(
            extract_bash_commands("git log --oneline | head -5 | wc -l"),
            vec!["git", "head", "wc"]
        );
    }
}
