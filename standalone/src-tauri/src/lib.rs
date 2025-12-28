use log;
use tauri::{async_runtime, Manager, State};
use tauri_plugin_cli::CliExt;

use crate::{db::DatabaseHandle, worker::spawn_worker};

mod db;
mod worker;

// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
#[tauri::command]
fn greet(
    name: &str,
    app_handle: State<tauri::AppHandle>,
    db_handle: State<DatabaseHandle>,
) -> String {
    let conn = db_handle
        .connect(&app_handle)
        .expect("failed to connect to database");

    conn.execute("select 1", [])
        .expect("failed to execute query");

    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_fs::init())
        .plugin(tauri_plugin_cli::init())
        .plugin(
            tauri_plugin_log::Builder::new()
                .level(tauri_plugin_log::log::LevelFilter::Info)
                .target(tauri_plugin_log::Target::new(
                    tauri_plugin_log::TargetKind::Stderr,
                ))
                .build(),
        )
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![greet])
        .setup(move |app| {
            let app_handle = app.handle().clone();

            match app.cli().matches() {
                Ok(matches) => {
                    let db_handle =
                        DatabaseHandle::from_cli_arg(&app_handle, matches.args.get("db"))?;
                    app_handle.manage(db_handle);
                }
                Err(_) => {}
            }

            async_runtime::spawn(async move { spawn_worker(&app_handle).await });

            log::info!("setup");

            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("create tauri app failed");
}
