use axum::{
    extract::{Path, Query, State},
    routing::post,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Mutex;
use tauri::{AppHandle, Manager};
use tauri_plugin_store::StoreExt;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tower_http::cors::CorsLayer;

use crate::db::DatabaseHandle;
use crate::db_commands::{
    CleanupQueueOptions, CleanupTarget, EventFilters, TaskRunFilters, TauriDataProvider,
};
use crate::worker;

const DEV_API_PORT_DEFAULT: u16 = 11223;
const DEV_API_PORT_ATTEMPTS: u16 = 10;
const DEV_API_STORE_PATH: &str = "dev-api.json";
const DEV_API_ENABLED_KEY: &str = "dev_api_enabled";

#[derive(Clone)]
struct DevApiContext {
    app_handle: AppHandle,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrpcRequest {
    id: Option<Value>,
    json: Option<Value>,
    input: Option<Value>,
}

#[derive(Deserialize)]
struct TrpcQuery {
    input: Option<String>,
    json: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkerPathInput {
    path: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DevApiStatus {
    pub enabled: bool,
    pub running: bool,
    pub port: Option<u16>,
    pub desired_port: u16,
}

struct RunningDevApi {
    port: u16,
    shutdown: oneshot::Sender<()>,
    handle: JoinHandle<()>,
}

pub struct DevApiState {
    enabled: Mutex<bool>,
    desired_port: Mutex<u16>,
    running: Mutex<Option<RunningDevApi>>,
}

impl DevApiState {
    pub fn new(enabled: bool, desired_port: Option<u16>) -> Self {
        Self {
            enabled: Mutex::new(enabled),
            desired_port: Mutex::new(desired_port.unwrap_or(DEV_API_PORT_DEFAULT)),
            running: Mutex::new(None),
        }
    }

    pub fn status(&self) -> DevApiStatus {
        let enabled = *self.enabled.lock().unwrap();
        let desired_port = *self.desired_port.lock().unwrap();
        let running_guard = self.running.lock().unwrap();
        let running = running_guard.is_some();
        let port = running_guard.as_ref().map(|server| server.port);
        DevApiStatus {
            enabled,
            running,
            port,
            desired_port,
        }
    }
}

#[tauri::command]
pub async fn get_dev_api_status(app_handle: AppHandle) -> Result<DevApiStatus, String> {
    let state = app_handle.state::<DevApiState>();
    Ok(state.status())
}

#[tauri::command]
pub async fn set_dev_api_enabled(
    app_handle: AppHandle,
    enabled: bool,
) -> Result<DevApiStatus, String> {
    set_enabled(&app_handle, enabled).await
}

pub fn load_dev_api_enabled(app_handle: &AppHandle) -> Option<bool> {
    let store = app_handle.store(DEV_API_STORE_PATH).ok()?;
    if store.reload().is_err() {
        return None;
    }
    let stored = store.get(DEV_API_ENABLED_KEY);
    stored
        .as_ref()
        .map(|value| parse_dev_api_enabled(Some(value)))
}

fn persist_dev_api_enabled(app_handle: &AppHandle, enabled: bool) -> Result<(), String> {
    let store = app_handle
        .store(DEV_API_STORE_PATH)
        .map_err(|err| err.to_string())?;
    store.set(DEV_API_ENABLED_KEY, Value::Bool(enabled));
    store.save().map_err(|err| err.to_string())
}

pub fn parse_dev_api_enabled(value: Option<&serde_json::Value>) -> bool {
    match value {
        Some(Value::Bool(flag)) => *flag,
        Some(Value::String(text)) => text.parse::<bool>().unwrap_or(false),
        Some(Value::Null) => true,
        Some(_) => true,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::parse_dev_api_enabled;
    use serde_json::json;

    #[test]
    fn parses_dev_api_enabled_true() {
        let value = json!(true);
        assert!(parse_dev_api_enabled(Some(&value)));
    }

    #[test]
    fn parses_dev_api_enabled_string() {
        let value = json!("true");
        assert!(parse_dev_api_enabled(Some(&value)));
    }

    #[test]
    fn parses_dev_api_enabled_null() {
        let value = json!(null);
        assert!(parse_dev_api_enabled(Some(&value)));
    }

    #[test]
    fn parses_dev_api_enabled_missing() {
        assert!(!parse_dev_api_enabled(None));
    }
}

pub async fn set_enabled(app_handle: &AppHandle, enabled: bool) -> Result<DevApiStatus, String> {
    let state = app_handle.state::<DevApiState>();
    {
        let mut flag = state.enabled.lock().unwrap();
        *flag = enabled;
    }

    if enabled {
        if let Err(err) = ensure_running(app_handle).await {
            let mut flag = state.enabled.lock().unwrap();
            *flag = false;
            let _ = persist_dev_api_enabled(app_handle, false);
            return Err(err);
        }
    } else {
        stop_running(app_handle).await?;
    }

    persist_dev_api_enabled(app_handle, enabled)?;

    Ok(state.status())
}

pub async fn ensure_running(app_handle: &AppHandle) -> Result<(), String> {
    let state = app_handle.state::<DevApiState>();
    {
        if state.running.lock().unwrap().is_some() {
            return Ok(());
        }
    }

    let desired_port = *state.desired_port.lock().unwrap();
    let running = start_server(app_handle.clone(), desired_port).await?;
    let mut guard = state.running.lock().unwrap();
    *guard = Some(running);
    Ok(())
}

pub async fn stop_running(app_handle: &AppHandle) -> Result<(), String> {
    let state = app_handle.state::<DevApiState>();
    let running = {
        let mut guard = state.running.lock().unwrap();
        guard.take()
    };

    if let Some(server) = running {
        let _ = server.shutdown.send(());
        let _ = server.handle.await;
    }

    Ok(())
}

async fn start_server(app_handle: AppHandle, desired_port: u16) -> Result<RunningDevApi, String> {
    let (listener, port) = bind_with_fallback(desired_port).await?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let context = DevApiContext { app_handle };
    let router = Router::new()
        .route(
            "/absurd-data/:procedure",
            post(trpc_post_handler).get(trpc_get_handler),
        )
        .with_state(context)
        .layer(CorsLayer::permissive());

    let handle = tokio::spawn(async move {
        let server = axum::serve(listener, router).with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
        });
        if let Err(err) = server.await {
            log::error!("dev api server failed: {}", err);
        }
    });

    Ok(RunningDevApi {
        port,
        shutdown: shutdown_tx,
        handle,
    })
}

async fn bind_with_fallback(start_port: u16) -> Result<(tokio::net::TcpListener, u16), String> {
    for offset in 0..DEV_API_PORT_ATTEMPTS {
        let port = start_port.saturating_add(offset);
        match tokio::net::TcpListener::bind(("127.0.0.1", port)).await {
            Ok(listener) => return Ok((listener, port)),
            Err(_) => continue,
        }
    }

    Err("Failed to bind dev API server port after 10 attempts".to_string())
}

async fn trpc_post_handler(
    Path(procedure): Path<String>,
    State(context): State<DevApiContext>,
    Json(payload): Json<TrpcRequest>,
) -> Json<Value> {
    let id = payload.id.unwrap_or(Value::Null);
    let input = payload.json.or(payload.input).unwrap_or(Value::Null);
    execute_trpc(&context, &procedure, id, input).await
}

async fn trpc_get_handler(
    Path(procedure): Path<String>,
    State(context): State<DevApiContext>,
    Query(query): Query<TrpcQuery>,
) -> Json<Value> {
    let input = query
        .input
        .or(query.json)
        .and_then(|value| serde_json::from_str::<Value>(&value).ok())
        .unwrap_or(Value::Null);
    execute_trpc(&context, &procedure, Value::Null, input).await
}

async fn execute_trpc(
    context: &DevApiContext,
    procedure: &str,
    id: Value,
    input: Value,
) -> Json<Value> {
    let app_handle = context.app_handle.clone();
    let procedure_name = procedure.to_string();

    let result =
        tokio::task::spawn_blocking(move || handle_procedure(&app_handle, &procedure_name, input))
            .await
            .map_err(|err| err.to_string())
            .and_then(|result| result);

    let body = match result {
        Ok(data) => json!({ "id": id, "result": { "data": data } }),
        Err(message) => json!({
            "id": id,
            "error": {
                "message": message,
                "code": -32603,
                "data": { "code": "INTERNAL_SERVER_ERROR", "httpStatus": 500 }
            }
        }),
    };

    Json(body)
}

fn handle_procedure(
    app_handle: &AppHandle,
    procedure: &str,
    input: Value,
) -> Result<Value, String> {
    match procedure {
        "health" => Ok(json!({ "ok": true })),
        "getOverviewMetrics" => with_provider(app_handle, |provider| {
            let metrics = provider.get_overview_metrics()?;
            Ok(serde_json::to_value(metrics)?)
        }),
        "getQueueMetrics" => with_provider(app_handle, |provider| {
            let metrics = provider.get_queue_metrics()?;
            Ok(serde_json::to_value(metrics)?)
        }),
        "getTaskRuns" => with_provider(app_handle, |provider| {
            let runs = provider.get_task_runs()?;
            Ok(serde_json::to_value(runs)?)
        }),
        "getTaskRunsForQueue" => {
            let payload: QueueNameInput = parse_input(input)?;
            with_provider(app_handle, |provider| {
                let runs = provider.get_task_runs_for_queue(&payload.queue_name)?;
                Ok(serde_json::to_value(runs)?)
            })
        }
        "getTaskRunsPage" => {
            let payload: TaskRunFilters = parse_input(input)?;
            with_provider(app_handle, |provider| {
                let page = provider.get_task_runs_page(payload)?;
                Ok(serde_json::to_value(page)?)
            })
        }
        "getTaskHistory" => {
            let payload: TaskIdInput = parse_input(input)?;
            with_provider(app_handle, |provider| {
                let runs = provider.get_task_history(&payload.task_id)?;
                Ok(serde_json::to_value(runs)?)
            })
        }
        "getTaskInfo" => {
            let payload: TaskIdInput = parse_input(input)?;
            with_provider(app_handle, |provider| {
                let info = provider.get_task_info(&payload.task_id)?;
                Ok(serde_json::to_value(info)?)
            })
        }
        "getQueueNames" => with_provider(app_handle, |provider| {
            let names = provider.get_queue_names()?;
            Ok(serde_json::to_value(names)?)
        }),
        "getQueueSummaries" => with_provider(app_handle, |provider| {
            let summaries = provider.get_queue_summaries()?;
            Ok(serde_json::to_value(summaries)?)
        }),
        "getTaskNameOptions" => {
            let payload: OptionalQueueNameInput = parse_optional_input(input)?;
            with_provider(app_handle, |provider| {
                let names = provider.get_task_name_options(payload.queue_name.as_deref())?;
                Ok(serde_json::to_value(names)?)
            })
        }
        "createQueue" => {
            let payload: QueueNameInput = parse_input(input)?;
            with_provider(app_handle, |provider| {
                provider.create_queue(&payload.queue_name)?;
                Ok(Value::Null)
            })
        }
        "cleanupQueue" => {
            let payload: CleanupQueueInput = parse_input(input)?;
            with_provider(app_handle, |provider| {
                let result = provider.cleanup_queue(payload.into())?;
                Ok(serde_json::to_value(result)?)
            })
        }
        "getEventFilterDefaults" => {
            let payload: OptionalQueueNameInput = parse_optional_input(input)?;
            with_provider(app_handle, |provider| {
                let defaults = provider.get_event_filter_defaults(payload.queue_name.as_deref())?;
                Ok(serde_json::to_value(defaults)?)
            })
        }
        "getEvents" => with_provider(app_handle, |provider| {
            let events = provider.get_events()?;
            Ok(serde_json::to_value(events)?)
        }),
        "getFilteredEvents" => {
            let payload: EventFilters = parse_input(input)?;
            with_provider(app_handle, |provider| {
                let events = provider.get_filtered_events(Some(payload))?;
                Ok(serde_json::to_value(events)?)
            })
        }
        "getSettingsInfo" => {
            let db_handle = app_handle.state::<DatabaseHandle>();
            let db_path = db_handle
                .db_path(app_handle)
                .map_err(|err| err.to_string())?;
            with_provider(app_handle, |provider| {
                let info = provider.get_settings_info(db_path)?;
                Ok(serde_json::to_value(info)?)
            })
        }
        "getWorkerStatus" => {
            let status = worker::get_worker_status_inner(app_handle)?;
            Ok(serde_json::to_value(status).map_err(|err| err.to_string())?)
        }
        "getWorkerLogs" => {
            let logs = worker::get_worker_logs(app_handle.clone())?;
            Ok(serde_json::to_value(logs).map_err(|err| err.to_string())?)
        }
        "setWorkerBinaryPath" => {
            let payload: WorkerPathInput = parse_input(input)?;
            let status = worker::set_worker_binary_path_inner(app_handle, &payload.path)?;
            Ok(serde_json::to_value(status).map_err(|err| err.to_string())?)
        }
        "startWorker" => {
            let status = worker::start_worker_inner(app_handle)?;
            Ok(serde_json::to_value(status).map_err(|err| err.to_string())?)
        }
        "stopWorker" => {
            let status = worker::stop_worker_inner(app_handle)?;
            Ok(serde_json::to_value(status).map_err(|err| err.to_string())?)
        }
        "getMigrations" => with_provider(app_handle, |provider| {
            let migrations = provider.get_migrations()?;
            Ok(serde_json::to_value(migrations)?)
        }),
        "applyMigrationsAll" => with_provider(app_handle, |provider| {
            let applied = provider.apply_migrations_all()?;
            Ok(serde_json::to_value(applied)?)
        }),
        "applyMigration" => {
            let payload: MigrationInput = parse_input(input)?;
            with_provider(app_handle, |provider| {
                let applied = provider.apply_migration(payload.migration_id)?;
                Ok(serde_json::to_value(applied)?)
            })
        }
        _ => Err(format!("Unknown procedure: {}", procedure)),
    }
}

fn with_provider<T>(
    app_handle: &AppHandle,
    f: impl FnOnce(&TauriDataProvider) -> anyhow::Result<T>,
) -> Result<T, String> {
    let db_handle = app_handle.state::<DatabaseHandle>();
    let conn = db_handle
        .connect(app_handle)
        .map_err(|err| err.to_string())?;
    let provider = TauriDataProvider::new(&conn);
    f(&provider).map_err(|err| err.to_string())
}

fn parse_input<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, String> {
    serde_json::from_value(value).map_err(|err| err.to_string())
}

fn parse_optional_input<T: for<'de> Deserialize<'de> + Default>(value: Value) -> Result<T, String> {
    if value.is_null() {
        return Ok(T::default());
    }
    serde_json::from_value(value).map_err(|err| err.to_string())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueueNameInput {
    queue_name: String,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OptionalQueueNameInput {
    queue_name: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskIdInput {
    task_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MigrationInput {
    migration_id: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CleanupQueueInput {
    queue_name: String,
    target: CleanupTarget,
    ttl_seconds: i64,
}

impl From<CleanupQueueInput> for CleanupQueueOptions {
    fn from(value: CleanupQueueInput) -> Self {
        Self {
            queue_name: value.queue_name,
            target: value.target,
            ttl_seconds: value.ttl_seconds,
        }
    }
}
