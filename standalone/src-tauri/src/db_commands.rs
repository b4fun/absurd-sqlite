use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use rusqlite::{types::Value as SqlValue, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tauri::{AppHandle, State};

use crate::db::DatabaseHandle;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OverviewMetrics {
    pub active_queues: i64,
    pub messages_processed: i64,
    pub messages_in_queue: i64,
    pub visible_now: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueMetric {
    pub name: String,
    pub in_queue: i64,
    pub visible: i64,
    pub newest_age: String,
    pub oldest_age: String,
    pub total_seen: i64,
    pub scraped_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Running,
    Failed,
    Completed,
    Sleeping,
    Pending,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskRun {
    pub id: String,
    pub name: String,
    pub queue: String,
    pub status: TaskStatus,
    pub attempt: String,
    pub attempt_number: i64,
    pub run_id: String,
    pub age: String,
    pub started_at: String,
    pub updated_at: String,
    pub created_ago: String,
    pub updated_ago: String,
    pub params_summary: String,
    pub params_json: String,
    pub final_state_json: Option<String>,
    pub worker: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueSummaryStat {
    pub label: String,
    pub value: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueSummary {
    pub name: String,
    pub created_at: String,
    pub age: String,
    pub stats: Vec<QueueSummaryStat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventFilterDefaults {
    pub event_name_placeholder: String,
    pub queue_label: String,
    pub queue_options: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventEntry {
    pub id: String,
    pub name: String,
    pub queue: String,
    pub created_at: String,
    pub payload_preview: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventFilters {
    pub queue_name: Option<String>,
    pub event_name: Option<String>,
}

pub struct TauriDataProvider<'a> {
    conn: &'a Connection,
    now_ms: i64,
}

impl<'a> TauriDataProvider<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self {
            conn,
            now_ms: current_time_ms(),
        }
    }

    pub fn with_now(conn: &'a Connection, now_ms: i64) -> Self {
        Self { conn, now_ms }
    }

    pub fn get_overview_metrics(&self) -> Result<OverviewMetrics> {
        let active_queues: i64 = self
            .conn
            .query_row(
                "select count(distinct queue_name) from absurd_tasks where state in ('pending', 'running', 'sleeping')",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);

        let messages_processed: i64 = self
            .conn
            .query_row(
                "select count(*) from absurd_tasks where state in ('completed', 'failed', 'cancelled')",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);

        let messages_in_queue: i64 = self
            .conn
            .query_row(
                "select count(*) from absurd_tasks where state = 'pending'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);

        let visible_now: i64 = self
            .conn
            .query_row(
                "select count(*) from absurd_runs where state = 'pending' and available_at <= ?",
                [self.now_ms],
                |row| row.get(0),
            )
            .unwrap_or(0);

        Ok(OverviewMetrics {
            active_queues,
            messages_processed,
            messages_in_queue,
            visible_now,
        })
    }

    pub fn get_queue_names(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("select queue_name from absurd_queues order by queue_name")?;
        let names = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<String>, _>>()?;
        Ok(names)
    }

    pub fn get_queue_metrics(&self) -> Result<Vec<QueueMetric>> {
        let queue_names = self.get_queue_names()?;
        let mut metrics = Vec::with_capacity(queue_names.len());

        for queue in queue_names {
            let in_queue: i64 = self
                .conn
                .query_row(
                    "select count(*) from absurd_tasks where queue_name = ? and state = 'pending'",
                    [&queue],
                    |row| row.get(0),
                )
                .unwrap_or(0);

            let visible: i64 = self
                .conn
                .query_row(
                    "select count(*) from absurd_runs where queue_name = ? and state = 'pending' and available_at <= ?",
                    rusqlite::params![queue, self.now_ms],
                    |row| row.get(0),
                )
                .unwrap_or(0);

            let total_seen: i64 = self
                .conn
                .query_row(
                    "select count(*) from absurd_runs where queue_name = ?",
                    [&queue],
                    |row| row.get(0),
                )
                .unwrap_or(0);

            let mut stmt = self
                .conn
                .prepare(
                    "select min(enqueue_at), max(enqueue_at) from absurd_tasks where queue_name = ?",
                )?;
            let (oldest, newest): (Option<i64>, Option<i64>) = stmt
                .query_row([&queue], |row| Ok((row.get(0)?, row.get(1)?)))?;

            let (oldest_age, newest_age) = match (oldest, newest) {
                (Some(oldest), Some(newest)) => (
                    format_age_short(self.now_ms - oldest),
                    format_age_short(self.now_ms - newest),
                ),
                _ => ("--".to_string(), "--".to_string()),
            };

            metrics.push(QueueMetric {
                name: queue,
                in_queue,
                visible,
                newest_age,
                oldest_age,
                total_seen,
                scraped_at: format_datetime_with_seconds(self.now_ms),
            });
        }

        Ok(metrics)
    }

    pub fn get_task_runs(&self) -> Result<Vec<TaskRun>> {
        self.fetch_task_runs(None)
    }

    pub fn get_task_runs_for_queue(&self, queue_name: &str) -> Result<Vec<TaskRun>> {
        self.fetch_task_runs(Some(queue_name))
    }

    pub fn get_task_history(&self, task_id: &str) -> Result<Vec<TaskRun>> {
        let mut stmt = self.conn.prepare(
            "select
                r.queue_name,
                t.task_id,
                t.task_name,
                r.state,
                r.attempt,
                r.run_id,
                r.started_at,
                r.created_at,
                r.completed_at,
                r.failed_at,
                t.params,
                t.max_attempts,
                r.claimed_by,
                r.result,
                r.failure_reason
             from absurd_runs r
             join absurd_tasks t
               on t.queue_name = r.queue_name and t.task_id = r.task_id
             where t.task_id = ?
             order by r.created_at desc",
        )?;

        let rows = stmt.query_map([task_id], |row| map_task_run_row(row, self.now_ms))?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn get_queue_summaries(&self) -> Result<Vec<QueueSummary>> {
        let mut stmt = self.conn.prepare(
            "select queue_name, created_at from absurd_queues order by queue_name",
        )?;
        let queue_rows = stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)))?;

        let mut summaries = Vec::new();

        for row in queue_rows {
            let (queue_name, created_at) = row?;
            let stats = self.get_queue_stats(&queue_name)?;

            summaries.push(QueueSummary {
                name: queue_name,
                created_at: format!("Created {}", format_datetime(created_at)),
                age: format_age_ago(self.now_ms - created_at),
                stats,
            });
        }

        Ok(summaries)
    }

    pub fn get_event_filter_defaults(&self, queue_name: Option<&str>) -> Result<EventFilterDefaults> {
        let placeholder: Option<String> = self
            .conn
            .query_row(
                "select event_name from absurd_events order by emitted_at desc limit 1",
                [],
                |row| row.get(0),
            )
            .optional()?;

        let queue_options = {
            let mut options = Vec::new();
            options.push("All queues".to_string());
            options.extend(self.get_queue_names()?);
            options
        };

        Ok(EventFilterDefaults {
            event_name_placeholder: placeholder.unwrap_or_else(|| "payment.completed".to_string()),
            queue_label: queue_name.unwrap_or("All queues").to_string(),
            queue_options,
        })
    }

    pub fn get_events(&self) -> Result<Vec<EventEntry>> {
        self.get_filtered_events(None)
    }

    pub fn get_filtered_events(&self, filters: Option<EventFilters>) -> Result<Vec<EventEntry>> {
        let queue_filter = filters
            .as_ref()
            .and_then(|filters| filters.queue_name.clone())
            .filter(|value| !value.is_empty() && value.to_lowercase() != "all queues");
        let event_filter = filters
            .as_ref()
            .and_then(|filters| filters.event_name.clone())
            .map(|value| value.trim().to_lowercase())
            .filter(|value| !value.is_empty());

        let mut stmt = self.conn.prepare(
            "select queue_name, event_name, emitted_at, payload
             from absurd_events
             order by emitted_at desc",
        )?;

        let rows = stmt.query_map([], |row| {
            let queue: String = row.get(0)?;
            let name: String = row.get(1)?;
            let emitted_at: i64 = row.get(2)?;
            let payload: SqlValue = row.get(3)?;

            Ok((queue, name, emitted_at, payload))
        })?;

        let mut entries = Vec::new();

        for row in rows {
            let (queue, name, emitted_at, payload) = row?;

            if let Some(filter_queue) = &queue_filter {
                if queue.to_lowercase() != filter_queue.to_lowercase() {
                    continue;
                }
            }

            if let Some(filter_event) = &event_filter {
                if !name.to_lowercase().contains(filter_event) {
                    continue;
                }
            }

            let payload_preview = payload_preview_from_value(json_from_sql_value(payload));

            entries.push(EventEntry {
                id: format!("{}_{}", queue, name),
                name,
                queue,
                created_at: format_datetime(emitted_at),
                payload_preview,
            });
        }

        Ok(entries)
    }

    fn get_queue_stats(&self, queue_name: &str) -> Result<Vec<QueueSummaryStat>> {
        let mut stmt = self.conn.prepare(
            "select state, count(*) from absurd_tasks where queue_name = ? group by state",
        )?;
        let rows = stmt.query_map([queue_name], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)))?;

        let mut counts = std::collections::HashMap::new();
        for row in rows {
            let (state, count) = row?;
            counts.insert(state, count);
        }

        let labels = [
            ("pending", "Pending"),
            ("running", "Running"),
            ("sleeping", "Sleeping"),
            ("completed", "Completed"),
            ("failed", "Failed"),
            ("cancelled", "Cancelled"),
        ];

        Ok(labels
            .iter()
            .map(|(state, label)| QueueSummaryStat {
                label: (*label).to_string(),
                value: *counts.get(*state).unwrap_or(&0),
            })
            .collect())
    }

    fn fetch_task_runs(&self, queue_name: Option<&str>) -> Result<Vec<TaskRun>> {
        if let Some(queue) = queue_name {
            let mut stmt = self.conn.prepare(
                r#"select
                    r.queue_name,
                    t.task_id,
                    t.task_name,
                    r.state,
                    r.attempt,
                    r.run_id,
                    r.started_at,
                    r.created_at,
                    r.completed_at,
                    r.failed_at,
                    t.params,
                    t.max_attempts,
                    r.claimed_by,
                    r.result,
                    r.failure_reason
                 from absurd_runs r
                 join absurd_tasks t
                   on t.queue_name = r.queue_name and t.task_id = r.task_id
                 where r.queue_name = ?
                 order by r.created_at desc"#,
            )?;
            let rows =
                stmt.query_map(rusqlite::params![queue], |row| map_task_run_row(row, self.now_ms))?;
            return Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?);
        }

        let sql = r#"select
                r.queue_name,
                t.task_id,
                t.task_name,
                r.state,
                r.attempt,
                r.run_id,
                r.started_at,
                r.created_at,
                r.completed_at,
                r.failed_at,
                t.params,
                t.max_attempts,
                r.claimed_by,
                r.result,
                r.failure_reason
             from absurd_runs r
             join absurd_tasks t
               on t.queue_name = r.queue_name and t.task_id = r.task_id
             order by r.created_at desc"#;

        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map([], |row| map_task_run_row(row, self.now_ms))?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }
}

fn map_task_run_row(row: &rusqlite::Row<'_>, now_ms: i64) -> rusqlite::Result<TaskRun> {
    let queue: String = row.get(0)?;
    let task_id: String = row.get(1)?;
    let task_name: String = row.get(2)?;
    let state: String = row.get(3)?;
    let attempt_number: i64 = row.get(4)?;
    let run_id: String = row.get(5)?;
    let started_at: Option<i64> = row.get(6)?;
    let created_at: i64 = row.get(7)?;
    let completed_at: Option<i64> = row.get(8)?;
    let failed_at: Option<i64> = row.get(9)?;
    let params_value = json_from_sql_value(row.get::<_, SqlValue>(10)?).unwrap_or(Value::Null);
    let max_attempts: Option<i64> = row.get(11)?;
    let claimed_by: Option<String> = row.get(12)?;
    let result_value = json_from_sql_value(row.get::<_, SqlValue>(13)?);
    let failure_value = json_from_sql_value(row.get::<_, SqlValue>(14)?);
    let params_summary = build_params_summary(&params_value);
    let params_json = format_json_pretty(&params_value);

    let final_state_json = match state.as_str() {
        "failed" => failure_value.as_ref().map(format_json_pretty),
        "completed" => result_value.as_ref().map(format_json_pretty),
        _ => None,
    };

    let updated_at = completed_at.or(failed_at).or(started_at).unwrap_or(created_at);
    let started_at = started_at.unwrap_or(created_at);

    Ok(TaskRun {
        id: task_id,
        name: task_name,
        queue,
        status: map_task_status(&state),
        attempt: format_attempt(attempt_number, max_attempts),
        attempt_number,
        run_id,
        age: format_age_short(now_ms - created_at),
        started_at: format_datetime(started_at),
        updated_at: format_datetime(updated_at),
        created_ago: format_age_ago(now_ms - created_at),
        updated_ago: format_age_ago(now_ms - updated_at),
        params_summary,
        params_json,
        final_state_json,
        worker: claimed_by.unwrap_or_else(|| "--".to_string()),
    })
}

fn map_task_status(state: &str) -> TaskStatus {
    match state {
        "running" => TaskStatus::Running,
        "failed" => TaskStatus::Failed,
        "completed" => TaskStatus::Completed,
        "sleeping" => TaskStatus::Sleeping,
        "pending" => TaskStatus::Pending,
        "cancelled" => TaskStatus::Cancelled,
        _ => TaskStatus::Pending,
    }
}

fn format_attempt(attempt: i64, max_attempts: Option<i64>) -> String {
    match max_attempts {
        Some(max) => format!("{} / {}", attempt, max),
        None => format!("{} / inf", attempt),
    }
}

fn format_json_pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn build_params_summary(value: &Value) -> String {
    if let Some(obj) = value.as_object() {
        let mut parts = Vec::new();
        for (key, val) in obj.iter().take(2) {
            let formatted = serde_json::to_string(val).unwrap_or_else(|_| "null".to_string());
            parts.push(format!("\"{}\": {}", key, formatted));
        }
        return format!("{{ {} }}", parts.join(", "));
    }

    value.to_string()
}

fn payload_preview_from_value(value: Option<Value>) -> String {
    let Some(value) = value else { return String::new(); };
    let compact = serde_json::to_string(&value).unwrap_or_else(|_| value.to_string());
    truncate_string(&compact, 120)
}

fn json_from_sql_value(value: SqlValue) -> Option<Value> {
    let text = match value {
        SqlValue::Null => return None,
        SqlValue::Text(text) => text,
        SqlValue::Blob(blob) => String::from_utf8(blob).ok()?,
        SqlValue::Integer(value) => value.to_string(),
        SqlValue::Real(value) => value.to_string(),
    };

    serde_json::from_str(&text).ok()
}

fn truncate_string(value: &str, max_len: usize) -> String {
    if value.len() <= max_len {
        return value.to_string();
    }

    let truncated = &value[..max_len.saturating_sub(3)];
    format!("{}...", truncated)
}

fn format_datetime(ms: i64) -> String {
    let fallback = Utc.timestamp_millis_opt(0).single().unwrap();
    let dt = Utc.timestamp_millis_opt(ms).single().unwrap_or(fallback);
    dt.format("%b %-d, %Y, %-I:%M %p").to_string()
}

fn format_datetime_with_seconds(ms: i64) -> String {
    let fallback = Utc.timestamp_millis_opt(0).single().unwrap();
    let dt = Utc.timestamp_millis_opt(ms).single().unwrap_or(fallback);
    dt.format("%b %-d, %Y, %-I:%M:%S %p").to_string()
}

fn format_age_short(delta_ms: i64) -> String {
    let mut delta = delta_ms;
    if delta < 0 {
        delta = 0;
    }

    let seconds = delta / 1000;
    let days = seconds / 86_400;
    if days > 0 {
        return format!("{}d", days);
    }

    let hours = seconds / 3_600;
    if hours > 0 {
        return format!("{}h", hours);
    }

    let minutes = seconds / 60;
    if minutes > 0 {
        return format!("{}m", minutes);
    }

    format!("{}s", seconds)
}

fn format_age_ago(delta_ms: i64) -> String {
    format!("{} ago", format_age_short(delta_ms))
}

fn current_time_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    now.as_millis() as i64
}

#[tauri::command]
pub fn get_overview_metrics(
    app_handle: AppHandle,
    db_handle: State<DatabaseHandle>,
) -> Result<OverviewMetrics, String> {
    with_provider(&app_handle, &db_handle, |provider| provider.get_overview_metrics())
}

#[tauri::command]
pub fn get_queue_metrics(
    app_handle: AppHandle,
    db_handle: State<DatabaseHandle>,
) -> Result<Vec<QueueMetric>, String> {
    with_provider(&app_handle, &db_handle, |provider| provider.get_queue_metrics())
}

#[tauri::command]
pub fn get_task_runs(
    app_handle: AppHandle,
    db_handle: State<DatabaseHandle>,
) -> Result<Vec<TaskRun>, String> {
    with_provider(&app_handle, &db_handle, |provider| provider.get_task_runs())
}

#[tauri::command]
pub fn get_task_runs_for_queue(
    queue_name: String,
    app_handle: AppHandle,
    db_handle: State<DatabaseHandle>,
) -> Result<Vec<TaskRun>, String> {
    with_provider(&app_handle, &db_handle, |provider| provider.get_task_runs_for_queue(&queue_name))
}

#[tauri::command]
pub fn get_task_history(
    task_id: String,
    app_handle: AppHandle,
    db_handle: State<DatabaseHandle>,
) -> Result<Vec<TaskRun>, String> {
    with_provider(&app_handle, &db_handle, |provider| provider.get_task_history(&task_id))
}

#[tauri::command]
pub fn get_queue_names(
    app_handle: AppHandle,
    db_handle: State<DatabaseHandle>,
) -> Result<Vec<String>, String> {
    with_provider(&app_handle, &db_handle, |provider| provider.get_queue_names())
}

#[tauri::command]
pub fn get_queue_summaries(
    app_handle: AppHandle,
    db_handle: State<DatabaseHandle>,
) -> Result<Vec<QueueSummary>, String> {
    with_provider(&app_handle, &db_handle, |provider| provider.get_queue_summaries())
}

#[tauri::command]
pub fn get_event_filter_defaults(
    queue_name: Option<String>,
    app_handle: AppHandle,
    db_handle: State<DatabaseHandle>,
) -> Result<EventFilterDefaults, String> {
    with_provider(&app_handle, &db_handle, |provider| {
        provider.get_event_filter_defaults(queue_name.as_deref())
    })
}

#[tauri::command]
pub fn get_events(
    app_handle: AppHandle,
    db_handle: State<DatabaseHandle>,
) -> Result<Vec<EventEntry>, String> {
    with_provider(&app_handle, &db_handle, |provider| provider.get_events())
}

#[tauri::command]
pub fn get_filtered_events(
    filters: EventFilters,
    app_handle: AppHandle,
    db_handle: State<DatabaseHandle>,
) -> Result<Vec<EventEntry>, String> {
    with_provider(&app_handle, &db_handle, |provider| provider.get_filtered_events(Some(filters)))
}

fn with_provider<T>(
    app_handle: &AppHandle,
    db_handle: &DatabaseHandle,
    f: impl FnOnce(&TauriDataProvider<'_>) -> Result<T>,
) -> Result<T, String> {
    let conn = db_handle
        .connect(app_handle)
        .context("failed to connect to database")
        .map_err(|err| err.to_string())?;
    let provider = TauriDataProvider::new(&conn);
    f(&provider).map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_connection() -> Connection {
        let conn = Connection::open_in_memory().expect("open in-memory");
        conn.execute_batch(include_str!(
            "../../../absurd-sqlite-extension/migrations/1.sql"
        ))
        .expect("apply schema");
        conn
    }

    fn seed_data(conn: &Connection, now_ms: i64) {
        conn.execute(
            "insert into absurd_queues (queue_name, created_at) values (?, ?)",
            rusqlite::params!["default", now_ms - 1000],
        )
        .expect("insert queue");

        let params = serde_json::json!({"tenant": "absurd", "retries": 2}).to_string();

        conn.execute(
            "insert into absurd_tasks (queue_name, task_id, task_name, params, enqueue_at, state, attempts)
             values (?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                "default",
                "task_pending",
                "pending",
                params,
                now_ms - 10_000,
                "pending",
                0
            ],
        )
        .expect("insert task pending");

        conn.execute(
            "insert into absurd_tasks (queue_name, task_id, task_name, params, enqueue_at, state, attempts)
             values (?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                "default",
                "task_running",
                "running",
                params,
                now_ms - 20_000,
                "running",
                1
            ],
        )
        .expect("insert task running");

        conn.execute(
            "insert into absurd_tasks (queue_name, task_id, task_name, params, enqueue_at, state, attempts)
             values (?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                "default",
                "task_completed",
                "completed",
                params,
                now_ms - 30_000,
                "completed",
                1
            ],
        )
        .expect("insert task completed");

        conn.execute(
            "insert into absurd_tasks (queue_name, task_id, task_name, params, enqueue_at, state, attempts)
             values (?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                "default",
                "task_cancelled",
                "cancelled",
                params,
                now_ms - 15_000,
                "cancelled",
                1
            ],
        )
        .expect("insert task cancelled");

        conn.execute(
            "insert into absurd_runs (queue_name, run_id, task_id, attempt, state, available_at, created_at, started_at, claimed_by)
             values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                "default",
                "run_running",
                "task_running",
                1,
                "running",
                now_ms - 5_000,
                now_ms - 20_000,
                now_ms - 15_000,
                "worker-1"
            ],
        )
        .expect("insert run running");

        conn.execute(
            "insert into absurd_runs (queue_name, run_id, task_id, attempt, state, available_at, created_at, completed_at, result)
             values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                "default",
                "run_completed",
                "task_completed",
                1,
                "completed",
                now_ms - 25_000,
                now_ms - 25_000,
                now_ms - 5_000,
                serde_json::json!({"ok": true}).to_string()
            ],
        )
        .expect("insert run completed");

        conn.execute(
            "insert into absurd_runs (queue_name, run_id, task_id, attempt, state, available_at, created_at)
             values (?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                "default",
                "run_pending",
                "task_pending",
                1,
                "pending",
                now_ms - 1_000,
                now_ms - 9_000
            ],
        )
        .expect("insert run pending");

        conn.execute(
            "insert into absurd_runs (queue_name, run_id, task_id, attempt, state, available_at, created_at)
             values (?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                "default",
                "run_cancelled",
                "task_cancelled",
                1,
                "cancelled",
                now_ms - 1_000,
                now_ms - 8_000
            ],
        )
        .expect("insert run cancelled");

        conn.execute(
            "insert into absurd_events (queue_name, event_name, payload, emitted_at)
             values (?, ?, ?, ?)",
            rusqlite::params![
                "default",
                "task.completed",
                serde_json::json!({"taskId": "task_completed"}).to_string(),
                now_ms - 2_000
            ],
        )
        .expect("insert event");
    }

    #[test]
    fn overview_metrics_counts() {
        let now_ms = 1_700_000_000_000;
        let conn = setup_connection();
        seed_data(&conn, now_ms);

        let provider = TauriDataProvider::with_now(&conn, now_ms);
        let metrics = provider.get_overview_metrics().expect("metrics");

        assert_eq!(metrics.active_queues, 1);
        assert_eq!(metrics.messages_processed, 2);
        assert_eq!(metrics.messages_in_queue, 1);
        assert_eq!(metrics.visible_now, 1);
    }

    #[test]
    fn queue_metrics_show_ages_and_counts() {
        let now_ms = 1_700_000_000_000;
        let conn = setup_connection();
        seed_data(&conn, now_ms);

        let provider = TauriDataProvider::with_now(&conn, now_ms);
        let metrics = provider.get_queue_metrics().expect("metrics");

        assert_eq!(metrics.len(), 1);
        let metric = &metrics[0];
        assert_eq!(metric.name, "default");
        assert_eq!(metric.in_queue, 1);
        assert_eq!(metric.visible, 1);
        assert_eq!(metric.total_seen, 4);
        assert_eq!(metric.newest_age, format_age_short(10_000));
        assert_eq!(metric.oldest_age, format_age_short(30_000));
        assert_eq!(
            metric.scraped_at,
            format_datetime_with_seconds(now_ms)
        );
    }

    #[test]
    fn task_runs_include_status_and_attempts() {
        let now_ms = 1_700_000_000_000;
        let conn = setup_connection();
        seed_data(&conn, now_ms);

        let provider = TauriDataProvider::with_now(&conn, now_ms);
        let runs = provider.get_task_runs().expect("runs");

        assert_eq!(runs.len(), 4);
        let running = runs
            .iter()
            .find(|run| run.run_id == "run_running")
            .expect("running run");

        assert_eq!(running.status, TaskStatus::Running);
        assert_eq!(running.attempt, "1 / inf");
        assert_eq!(running.worker, "worker-1");

        let cancelled = runs
            .iter()
            .find(|run| run.run_id == "run_cancelled")
            .expect("cancelled run");
        assert_eq!(cancelled.status, TaskStatus::Cancelled);
    }

    #[test]
    fn queue_summaries_include_state_counts() {
        let now_ms = 1_700_000_000_000;
        let conn = setup_connection();
        seed_data(&conn, now_ms);

        let provider = TauriDataProvider::with_now(&conn, now_ms);
        let summaries = provider.get_queue_summaries().expect("summaries");

        assert_eq!(summaries.len(), 1);
        let summary = &summaries[0];
        assert_eq!(summary.name, "default");
        assert_eq!(summary.age, format_age_ago(1000));
        assert_eq!(summary.stats.len(), 6);
        assert_eq!(summary.stats[0].value, 1); // Pending
        assert_eq!(summary.stats[5].value, 1); // Cancelled
    }

    #[test]
    fn events_and_filters_work() {
        let now_ms = 1_700_000_000_000;
        let conn = setup_connection();
        seed_data(&conn, now_ms);

        let provider = TauriDataProvider::with_now(&conn, now_ms);
        let defaults = provider
            .get_event_filter_defaults(None)
            .expect("defaults");
        assert_eq!(defaults.event_name_placeholder, "task.completed");
        assert_eq!(defaults.queue_label, "All queues");
        assert_eq!(defaults.queue_options, vec!["All queues", "default"]);

        let events = provider.get_events().expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].queue, "default");

        let filtered = provider
            .get_filtered_events(Some(EventFilters {
                queue_name: Some("default".to_string()),
                event_name: Some("completed".to_string()),
            }))
            .expect("filtered");
        assert_eq!(filtered.len(), 1);
    }
}
