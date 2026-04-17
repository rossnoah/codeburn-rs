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
use std::path::PathBuf;

use anyhow::Result;

use crate::cli::ExportFormat;
use crate::currency::{convert_cost, get_cost_column_header, load_currency};
use crate::models::load_pricing;
use crate::parser::parse_all_sessions;
use crate::tui::get_date_range;
use crate::types::ProjectSummary;

fn esc_csv(s: &str) -> String {
    let sanitized = if s.starts_with('=')
        || s.starts_with('+')
        || s.starts_with('-')
        || s.starts_with('@')
    {
        format!("'{}", s)
    } else {
        s.to_string()
    };

    if sanitized.contains(',') || sanitized.contains('"') || sanitized.contains('\n') {
        format!("\"{}\"", sanitized.replace('"', "\"\""))
    } else {
        sanitized
    }
}

struct PeriodExport {
    label: String,
    projects: Vec<ProjectSummary>,
}

fn build_summary_row(period: &PeriodExport) -> Vec<(String, String)> {
    let cost: f64 = period.projects.iter().map(|p| p.total_cost_usd).sum();
    let calls: u64 = period.projects.iter().map(|p| p.total_api_calls).sum();
    let sessions: usize = period.projects.iter().map(|p| p.sessions.len()).sum();
    vec![
        ("Period".to_string(), period.label.clone()),
        (get_cost_column_header(), format!("{:.2}", convert_cost(cost))),
        ("API Calls".to_string(), calls.to_string()),
        ("Sessions".to_string(), sessions.to_string()),
    ]
}

fn build_daily_rows(projects: &[ProjectSummary]) -> Vec<Vec<(String, String)>> {
    let mut daily: HashMap<String, (f64, u64, u64, u64, u64, u64)> = HashMap::new();
    for project in projects {
        for session in &project.sessions {
            for dc in &session.daily_costs {
                let entry = daily.entry(dc.day.clone()).or_default();
                entry.0 += dc.cost_usd;
                entry.1 += dc.call_count;
                entry.2 += dc.input_tokens;
                entry.3 += dc.output_tokens;
                entry.4 += dc.cache_read_tokens;
                entry.5 += dc.cache_write_tokens;
            }
        }
    }
    let mut days: Vec<_> = daily.into_iter().collect();
    days.sort_by(|a, b| a.0.cmp(&b.0));

    let header = get_cost_column_header();
    days.into_iter()
        .map(|(date, d)| {
            vec![
                ("Date".to_string(), date),
                (header.clone(), format!("{:.2}", convert_cost(d.0))),
                ("API Calls".to_string(), d.1.to_string()),
                ("Input Tokens".to_string(), d.2.to_string()),
                ("Output Tokens".to_string(), d.3.to_string()),
                ("Cache Read Tokens".to_string(), d.4.to_string()),
                ("Cache Write Tokens".to_string(), d.5.to_string()),
            ]
        })
        .collect()
}

fn build_project_rows(projects: &[ProjectSummary]) -> Vec<Vec<(String, String)>> {
    let header = get_cost_column_header();
    projects
        .iter()
        .map(|p| {
            vec![
                ("Project".to_string(), p.project_path.clone()),
                (header.clone(), format!("{:.2}", convert_cost(p.total_cost_usd))),
                ("API Calls".to_string(), p.total_api_calls.to_string()),
                ("Sessions".to_string(), p.sessions.len().to_string()),
            ]
        })
        .collect()
}

fn rows_to_csv(rows: &[Vec<(String, String)>]) -> String {
    if rows.is_empty() {
        return String::new();
    }
    let headers: Vec<&str> = rows[0].iter().map(|(k, _)| k.as_str()).collect();
    let mut lines = vec![headers.iter().map(|h| esc_csv(h)).collect::<Vec<_>>().join(",")];
    for row in rows {
        lines.push(
            row.iter()
                .map(|(_, v)| esc_csv(v))
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    lines.join("\n")
}

pub async fn run_export(format: ExportFormat, output: Option<String>, provider: &str) -> Result<()> {
    load_pricing().await?;
    load_currency().await?;

    let pf = if provider == "all" {
        None
    } else {
        Some(provider)
    };

    let periods = vec![
        PeriodExport {
            label: "Today".to_string(),
            projects: parse_all_sessions(Some(&get_date_range("today")), pf)?,
        },
        PeriodExport {
            label: "7 Days".to_string(),
            projects: parse_all_sessions(Some(&get_date_range("week")), pf)?,
        },
        PeriodExport {
            label: "30 Days".to_string(),
            projects: parse_all_sessions(Some(&get_date_range("30days")), pf)?,
        },
    ];

    if periods.iter().all(|p| p.projects.is_empty()) {
        println!("\n  No usage data found.\n");
        return Ok(());
    }

    let default_name = format!(
        "codeburn-{}",
        chrono::Local::now().format("%Y-%m-%d")
    );
    let ext = match format {
        ExportFormat::Csv => "csv",
        ExportFormat::Json => "json",
    };
    let output_path = output.unwrap_or_else(|| format!("{}.{}", default_name, ext));
    let full_path = PathBuf::from(&output_path)
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(&output_path));

    match format {
        ExportFormat::Csv => {
            let all_projects = &periods
                .iter()
                .find(|p| p.label == "30 Days")
                .unwrap_or(periods.last().unwrap())
                .projects;

            let mut parts = Vec::new();
            parts.push("# Summary".to_string());
            let summary_rows: Vec<_> = periods.iter().map(build_summary_row).collect();
            parts.push(rows_to_csv(&summary_rows));
            parts.push(String::new());

            for period in &periods {
                parts.push(format!("# Daily - {}", period.label));
                parts.push(rows_to_csv(&build_daily_rows(&period.projects)));
                parts.push(String::new());
            }

            parts.push("# Projects - All".to_string());
            parts.push(rows_to_csv(&build_project_rows(all_projects)));
            parts.push(String::new());

            std::fs::write(&output_path, parts.join("\n"))?;
        }
        ExportFormat::Json => {
            let all_projects = &periods
                .iter()
                .find(|p| p.label == "30 Days")
                .unwrap_or(periods.last().unwrap())
                .projects;

            let mut period_data = serde_json::Map::new();
            for period in &periods {
                let summary = build_summary_row(period);
                let summary_obj: serde_json::Map<String, serde_json::Value> = summary
                    .into_iter()
                    .map(|(k, v)| (k, serde_json::Value::String(v)))
                    .collect();
                period_data.insert(
                    period.label.clone(),
                    serde_json::json!({
                        "summary": summary_obj,
                    }),
                );
            }

            let data = serde_json::json!({
                "generated": chrono::Utc::now().to_rfc3339(),
                "periods": period_data,
                "projects": build_project_rows(all_projects)
                    .iter()
                    .map(|row| {
                        let obj: serde_json::Map<String, serde_json::Value> = row
                            .iter()
                            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                            .collect();
                        serde_json::Value::Object(obj)
                    })
                    .collect::<Vec<_>>(),
            });

            std::fs::write(&output_path, serde_json::to_string_pretty(&data)?)?;
        }
    }

    println!(
        "\n  Exported (Today + 7 Days + 30 Days) to: {}\n",
        full_path.display()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_esc_csv_formula_injection() {
        assert_eq!(esc_csv("=SUM(A1)"), "'=SUM(A1)");
        assert_eq!(esc_csv("+cmd"), "'+cmd");
        assert_eq!(esc_csv("-exec"), "'-exec");
        assert_eq!(esc_csv("@import"), "'@import");
    }

    #[test]
    fn test_esc_csv_normal() {
        assert_eq!(esc_csv("hello"), "hello");
        assert_eq!(esc_csv("hello,world"), "\"hello,world\"");
        assert_eq!(esc_csv("say \"hi\""), "\"say \"\"hi\"\"\"");
    }
}
