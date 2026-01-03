use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tauri::image::Image;
use tauri::menu::{Menu, MenuItem, PredefinedMenuItem};
use tauri::tray::TrayIconBuilder;
use tauri::{async_runtime, AppHandle, Manager, Result, Wry};
use tokio::time::interval;

use crate::db::DatabaseHandle;
use crate::worker;

const ICON: &[u8] = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/icons/tray-icon.png"));
const ONE_HOUR_MS: i64 = 60 * 60 * 1000;
const MAX_RECENT_TASKS: usize = 5;
const TRAY_ID: &str = "main_tray";
const TRAY_UPDATE_INTERVAL: Duration = Duration::from_secs(30);

pub const TRAY_SHOW_ID: &str = "tray_show";
pub const TRAY_QUIT_ID: &str = "tray_quit";

struct TrayMenuState {
    menu: Menu<Wry>,
    tasks_item: MenuItem<Wry>,
    recent_items: Vec<MenuItem<Wry>>,
    worker_item: MenuItem<Wry>,
    worker_separator: PredefinedMenuItem<Wry>,
    worker_visible: Mutex<bool>,
}

pub fn setup(app: &tauri::App) -> Result<()> {
    let app_handle = app.handle();
    let tasks_label = tasks_last_hour_label(app_handle);
    let tasks_item = MenuItem::with_id(
        app_handle,
        "tasks_last_hour",
        tasks_label,
        false,
        None::<&str>,
    )?;
    let recent_header = MenuItem::with_id(
        app_handle,
        "tasks_processed_header",
        "Last 5 tasks",
        false,
        None::<&str>,
    )?;
    let mut recent_items = Vec::with_capacity(MAX_RECENT_TASKS);
    for idx in 0..MAX_RECENT_TASKS {
        recent_items.push(MenuItem::with_id(
            app_handle,
            format!("tasks_processed_recent_{}", idx),
            "--",
            false,
            None::<&str>,
        )?);
    }
    let recent_separator = PredefinedMenuItem::separator(app_handle)?;
    let worker_label = worker_status_label(app_handle);
    let worker_item = MenuItem::with_id(
        app_handle,
        "worker_status",
        worker_label
            .clone()
            .unwrap_or_else(|| "Worker status: --".to_string()),
        false,
        None::<&str>,
    )?;
    let worker_separator = PredefinedMenuItem::separator(app_handle)?;
    let show_item = MenuItem::with_id(app_handle, TRAY_SHOW_ID, "Show", true, None::<&str>)?;
    let quit_item = MenuItem::with_id(app_handle, TRAY_QUIT_ID, "Quit", true, None::<&str>)?;

    let mut menu_items: Vec<&dyn tauri::menu::IsMenuItem<Wry>> = vec![&tasks_item, &recent_header];
    for item in &recent_items {
        menu_items.push(item);
    }
    menu_items.push(&recent_separator);
    if worker_label.is_some() {
        menu_items.push(&worker_item);
        menu_items.push(&worker_separator);
    }
    menu_items.push(&show_item);
    menu_items.push(&quit_item);

    let menu = Menu::with_items(app_handle, &menu_items)?;

    let icon = Image::from_bytes(ICON)?;

    let _tray = TrayIconBuilder::with_id(TRAY_ID)
        .icon(icon)
        .menu(&menu)
        .show_menu_on_left_click(true)
        .build(app)?;

    app.manage(TrayMenuState {
        menu: menu.clone(),
        tasks_item,
        recent_items,
        worker_item,
        worker_separator,
        worker_visible: Mutex::new(worker_label.is_some()),
    });

    Ok(())
}

pub fn start_updates(app_handle: AppHandle) {
    async_runtime::spawn(async move {
        let mut ticker = interval(TRAY_UPDATE_INTERVAL);
        loop {
            ticker.tick().await;
            if let Err(err) = update_menu_state(&app_handle) {
                log::warn!("Failed to update tray menu: {}", err);
            }
        }
    });
}

pub fn refresh_now(app_handle: &AppHandle) -> Result<()> {
    update_menu_state(app_handle)
}

fn update_menu_state(app_handle: &AppHandle) -> Result<()> {
    let state = app_handle.state::<TrayMenuState>();
    let tasks_label = tasks_last_hour_label(app_handle);
    state.tasks_item.set_text(tasks_label)?;

    let recent = fetch_recent_tasks(app_handle)?;
    for (idx, item) in state.recent_items.iter().enumerate() {
        let label = match recent.get(idx) {
            Some((name, status)) => {
                format!("{} {} - {}", status_indicator(status), name, status)
            }
            None => "--".to_string(),
        };
        item.set_text(label)?;
    }

    let worker_label = worker_status_label(app_handle);
    let mut worker_visible = state.worker_visible.lock().unwrap();
    match (worker_label, *worker_visible) {
        (Some(label), false) => {
            let insert_pos = 1 + 1 + MAX_RECENT_TASKS + 1;
            state.menu.insert(&state.worker_item, insert_pos)?;
            state.menu.insert(&state.worker_separator, insert_pos + 1)?;
            state.worker_item.set_text(label)?;
            *worker_visible = true;
        }
        (Some(label), true) => {
            state.worker_item.set_text(label)?;
        }
        (None, true) => {
            state.menu.remove(&state.worker_item)?;
            state.menu.remove(&state.worker_separator)?;
            *worker_visible = false;
        }
        (None, false) => {}
    }

    Ok(())
}

fn tasks_last_hour_label(app_handle: &tauri::AppHandle) -> String {
    let db_handle = app_handle.state::<DatabaseHandle>();
    let conn = match db_handle.connect(app_handle) {
        Ok(conn) => conn,
        Err(err) => {
            log::warn!("Failed to open database for tray metrics: {}", err);
            return "Tasks last hour: --".to_string();
        }
    };
    let now_ms = current_time_ms();
    let cutoff = now_ms.saturating_sub(ONE_HOUR_MS);
    let count: i64 = match conn.query_row(
        "select count(*) from absurd_tasks where enqueue_at >= ?1",
        [cutoff],
        |row| row.get(0),
    ) {
        Ok(count) => count,
        Err(err) => {
            log::warn!("Failed to query tasks for tray metrics: {}", err);
            return "Tasks last hour: --".to_string();
        }
    };
    format!("Tasks last hour: {}", count)
}

fn fetch_recent_tasks(app_handle: &tauri::AppHandle) -> Result<Vec<(String, String)>> {
    let db_handle = app_handle.state::<DatabaseHandle>();
    let conn = match db_handle.connect(app_handle) {
        Ok(conn) => conn,
        Err(err) => {
            log::warn!("Failed to open database for tray metrics: {}", err);
            return Ok(Vec::new());
        }
    };
    let mut stmt = match conn.prepare(
        "select t.task_name, r.state
         from absurd_runs r
         join absurd_tasks t
           on t.queue_name = r.queue_name and t.task_id = r.task_id
         where r.state in ('completed', 'failed', 'cancelled')
         order by r.created_at desc
         limit 5",
    ) {
        Ok(stmt) => stmt,
        Err(err) => {
            log::warn!("Failed to prepare tray task query: {}", err);
            return Ok(Vec::new());
        }
    };
    let rows = match stmt.query_map([], |row| {
        let name: String = row.get(0)?;
        let state: String = row.get(1)?;
        Ok((name, state))
    }) {
        Ok(rows) => rows,
        Err(err) => {
            log::warn!("Failed to query tasks for tray metrics: {}", err);
            return Ok(Vec::new());
        }
    };
    Ok(rows.filter_map(|row| row.ok()).collect())
}

fn status_indicator(state: &str) -> &'static str {
    match state {
        "completed" => "✅",
        "failed" => "❌",
        "cancelled" => "🚫",
        _ => "•",
    }
}

fn worker_status_label(app_handle: &tauri::AppHandle) -> Option<String> {
    let status = match worker::get_worker_status_inner(app_handle) {
        Ok(status) => status,
        Err(err) => {
            log::warn!("Failed to fetch worker status for tray: {}", err);
            return None;
        }
    };
    status.configured_path.as_ref()?;
    let label = if status.running {
        match status.pid {
            Some(pid) => format!("Worker status: running (pid {})", pid),
            None => "Worker status: running".to_string(),
        }
    } else {
        "Worker status: stopped".to_string()
    };
    Some(label)
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
