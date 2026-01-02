use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tauri::{AppHandle, Manager};
use tauri_plugin_shell::process::{CommandChild, CommandEvent};
use tauri_plugin_shell::ShellExt;
use tauri_plugin_store::StoreExt;

use crate::db::{extension_path, DatabaseHandle};

const WORKER_STORE_PATH: &str = "worker.json";
const WORKER_PATH_KEY: &str = "worker_binary_path";
const CRASH_WINDOW_MS: i64 = 60_000;
const CRASH_THRESHOLD: usize = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerStatus {
    pub configured_path: Option<String>,
    pub running: bool,
    pub pid: Option<u32>,
    pub crashing: bool,
}

struct CrashTracker {
    window_ms: i64,
    threshold: usize,
    history: VecDeque<i64>,
}

impl CrashTracker {
    fn new(window_ms: i64, threshold: usize) -> Self {
        Self {
            window_ms,
            threshold,
            history: VecDeque::new(),
        }
    }

    fn clear(&mut self) {
        self.history.clear();
    }

    fn record_exit(&mut self, now_ms: i64) {
        self.history.push_back(now_ms);
        self.prune(now_ms);
    }

    fn is_crashing(&mut self, now_ms: i64) -> bool {
        self.prune(now_ms);
        self.history.len() >= self.threshold
    }

    fn prune(&mut self, now_ms: i64) {
        let cutoff = now_ms - self.window_ms;
        while self
            .history
            .front()
            .map(|timestamp| *timestamp < cutoff)
            .unwrap_or(false)
        {
            self.history.pop_front();
        }
    }
}

struct RunningWorker {
    pid: u32,
    child: CommandChild,
}

pub struct WorkerState {
    binary_path: Mutex<Option<String>>,
    running: Mutex<Option<RunningWorker>>,
    crash_tracker: Mutex<CrashTracker>,
    stop_requested: Mutex<bool>,
}

impl WorkerState {
    pub fn new(binary_path: Option<String>) -> Self {
        Self {
            binary_path: Mutex::new(binary_path),
            running: Mutex::new(None),
            crash_tracker: Mutex::new(CrashTracker::new(CRASH_WINDOW_MS, CRASH_THRESHOLD)),
            stop_requested: Mutex::new(false),
        }
    }

    fn status(&self) -> WorkerStatus {
        let configured_path = self.binary_path.lock().unwrap().clone();
        let running_guard = self.running.lock().unwrap();
        let running = running_guard.is_some();
        let pid = running_guard.as_ref().map(|worker| worker.pid);
        let crashing = {
            let mut tracker = self.crash_tracker.lock().unwrap();
            tracker.is_crashing(current_time_ms())
        };
        WorkerStatus {
            configured_path,
            running,
            pid,
            crashing,
        }
    }
}

pub fn load_worker_binary_path(app_handle: &AppHandle) -> Option<String> {
    let store = app_handle.store(WORKER_STORE_PATH).ok()?;
    if store.reload().is_err() {
        return None;
    }
    let stored = store.get(WORKER_PATH_KEY);
    stored
        .as_ref()
        .and_then(|value| parse_worker_binary_path(Some(value)))
}

fn persist_worker_binary_path(app_handle: &AppHandle, path: Option<&str>) -> Result<(), String> {
    let store = app_handle
        .store(WORKER_STORE_PATH)
        .map_err(|err| err.to_string())?;
    let value = path
        .map(|text| Value::String(text.to_string()))
        .unwrap_or(Value::Null);
    store.set(WORKER_PATH_KEY, value);
    store.save().map_err(|err| err.to_string())
}

pub fn parse_worker_binary_path(value: Option<&serde_json::Value>) -> Option<String> {
    match value {
        Some(Value::String(text)) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Some(Value::Null) => None,
        Some(_) => None,
        None => None,
    }
}

#[tauri::command]
pub fn get_worker_status(app_handle: AppHandle) -> Result<WorkerStatus, String> {
    get_worker_status_inner(&app_handle)
}

pub fn get_worker_status_inner(app_handle: &AppHandle) -> Result<WorkerStatus, String> {
    let state = app_handle.state::<WorkerState>();
    Ok(state.status())
}

#[tauri::command(async)]
pub async fn set_worker_binary_path(
    app_handle: AppHandle,
    path: String,
) -> Result<WorkerStatus, String> {
    set_worker_binary_path_inner(&app_handle, &path)
}

pub fn set_worker_binary_path_inner(
    app_handle: &AppHandle,
    path: &str,
) -> Result<WorkerStatus, String> {
    let normalized = path.trim();
    let next = if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_string())
    };

    let state = app_handle.state::<WorkerState>();
    let (changed, was_running) = {
        let stored = state.binary_path.lock().unwrap();
        (*stored != next, state.running.lock().unwrap().is_some())
    };

    if changed && was_running {
        stop_worker_inner(app_handle)?;
    }

    persist_worker_binary_path(app_handle, next.as_deref())?;

    {
        let mut stored = state.binary_path.lock().unwrap();
        if *stored != next {
            let mut tracker = state.crash_tracker.lock().unwrap();
            tracker.clear();
        }
        *stored = next;
    }

    if changed && was_running {
        return start_worker_inner(app_handle);
    }

    Ok(state.status())
}

#[tauri::command]
pub fn start_worker(app_handle: AppHandle) -> Result<WorkerStatus, String> {
    start_worker_inner(&app_handle)
}

pub fn start_worker_inner(app_handle: &AppHandle) -> Result<WorkerStatus, String> {
    let state = app_handle.state::<WorkerState>();
    if state.running.lock().unwrap().is_some() {
        return Ok(state.status());
    }

    let configured = state.binary_path.lock().unwrap().clone();
    let command = configured.ok_or_else(|| "Worker command is not configured".to_string())?;
    let (program, args) = parse_command(&command)?;

    let db_handle = app_handle.state::<DatabaseHandle>();
    let db_path = db_handle
        .db_path(app_handle)
        .map_err(|err| err.to_string())?;
    let extension = extension_path(app_handle).map_err(|err| err.to_string())?;

    let shell = app_handle.shell();
    let (rx, child) = shell
        .command(&program)
        .args(args)
        .env("ABSURD_DATABASE_PATH", db_path)
        .env("ABSURD_DATABASE_EXTENSION_PATH", extension)
        .spawn()
        .map_err(|err| err.to_string())?;
    let pid = child.pid();

    *state.stop_requested.lock().unwrap() = false;
    *state.running.lock().unwrap() = Some(RunningWorker { pid, child });

    let app_handle = app_handle.clone();
    tauri::async_runtime::spawn(async move {
        let mut rx = rx;
        while let Some(event) = rx.recv().await {
            match event {
                CommandEvent::Terminated(payload) => {
                    log::info!(
                        "Worker exited with code {:?}, signal {:?}",
                        payload.code,
                        payload.signal
                    );
                    handle_worker_exit(&app_handle);
                    break;
                }
                CommandEvent::Error(err) => {
                    log::warn!("Worker process error: {}", err);
                }
                CommandEvent::Stderr(line) => {
                    if let Ok(message) = String::from_utf8(line) {
                        log::info!("Worker stderr: {}", message.trim());
                    }
                }
                CommandEvent::Stdout(line) => {
                    if let Ok(message) = String::from_utf8(line) {
                        log::info!("Worker stdout: {}", message.trim());
                    }
                }
                _ => {}
            }
        }
    });

    Ok(state.status())
}

#[tauri::command]
pub fn stop_worker(app_handle: AppHandle) -> Result<WorkerStatus, String> {
    stop_worker_inner(&app_handle)
}

pub fn stop_worker_inner(app_handle: &AppHandle) -> Result<WorkerStatus, String> {
    let state = app_handle.state::<WorkerState>();
    let worker = state.running.lock().unwrap().take();
    let Some(worker) = worker else {
        return Ok(state.status());
    };

    *state.stop_requested.lock().unwrap() = true;
    let pid = worker.pid;
    let stopped = stop_worker_process(worker, pid);
    if let Err(err) = stopped {
        *state.stop_requested.lock().unwrap() = false;
        return Err(err);
    }

    Ok(state.status())
}

fn stop_worker_process(worker: RunningWorker, pid: u32) -> Result<(), String> {
    #[cfg(unix)]
    {
        let result = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
        if result != 0 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        let _child = worker.child;
        drop(_child);
        Ok(())
    }

    #[cfg(windows)]
    {
        worker.child.kill().map_err(|err| err.to_string())?;
        Ok(())
    }

    #[cfg(not(any(unix, windows)))]
    {
        worker.child.kill().map_err(|err| err.to_string())?;
        Ok(())
    }
}

fn handle_worker_exit(app_handle: &AppHandle) {
    let state = app_handle.state::<WorkerState>();
    let stopped = {
        let mut flag = state.stop_requested.lock().unwrap();
        let was_stopping = *flag;
        *flag = false;
        was_stopping
    };

    *state.running.lock().unwrap() = None;

    if !stopped {
        let mut tracker = state.crash_tracker.lock().unwrap();
        tracker.record_exit(current_time_ms());
    }
}

fn parse_command(command: &str) -> Result<(String, Vec<String>), String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut chars = command.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;

    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            }
            '\'' if !in_double => {
                in_single = !in_single;
            }
            '"' if !in_single => {
                in_double = !in_double;
            }
            c if c.is_whitespace() && !in_single && !in_double => {
                if !current.is_empty() {
                    args.push(current);
                    current = String::new();
                }
            }
            _ => current.push(ch),
        }
    }

    if in_single || in_double {
        return Err("Worker command has unterminated quote".to_string());
    }

    if !current.is_empty() {
        args.push(current);
    }

    let program = args
        .first()
        .cloned()
        .ok_or_else(|| "Worker command is empty".to_string())?;
    let remaining = if args.len() > 1 {
        args[1..].to_vec()
    } else {
        Vec::new()
    };
    Ok((program, remaining))
}

fn current_time_ms() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    now.as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::{parse_command, parse_worker_binary_path, CrashTracker};
    use serde_json::json;

    #[test]
    fn parses_worker_binary_path() {
        let value = json!(" /usr/local/bin/worker ");
        assert_eq!(
            parse_worker_binary_path(Some(&value)),
            Some("/usr/local/bin/worker".to_string())
        );
        let empty = json!(" ");
        assert_eq!(parse_worker_binary_path(Some(&empty)), None);
        let null_value = json!(null);
        assert_eq!(parse_worker_binary_path(Some(&null_value)), None);
        assert_eq!(parse_worker_binary_path(None), None);
    }

    #[test]
    fn crash_tracker_flags_recent_exits() {
        let mut tracker = CrashTracker::new(60_000, 3);
        tracker.record_exit(1_000);
        assert!(!tracker.is_crashing(1_000));
        tracker.record_exit(2_000);
        assert!(!tracker.is_crashing(2_000));
        tracker.record_exit(3_000);
        assert!(tracker.is_crashing(3_000));
        assert!(!tracker.is_crashing(65_000));
    }

    #[test]
    fn parses_worker_command_line() {
        let (program, args) = parse_command("npx absurd-worker --db foo").unwrap();
        assert_eq!(program, "npx");
        assert_eq!(args, vec!["absurd-worker", "--db", "foo"]);

        let (program, args) = parse_command("uvx \"absurd worker\" --flag").unwrap();
        assert_eq!(program, "uvx");
        assert_eq!(args, vec!["absurd worker", "--flag"]);

        let (program, args) = parse_command("  /usr/bin/worker  ").unwrap();
        assert_eq!(program, "/usr/bin/worker");
        assert!(args.is_empty());
    }

    #[test]
    fn rejects_unterminated_quotes() {
        let err = parse_command("npx \"absurd-worker").unwrap_err();
        assert!(err.contains("unterminated quote"));
    }
}
