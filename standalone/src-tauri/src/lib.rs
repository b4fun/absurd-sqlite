use log;
use tauri::{async_runtime, Manager};
use tauri::menu::{MenuBuilder, MenuItemBuilder, SubmenuBuilder};
use tauri_plugin_cli::CliExt;

use crate::{db::DatabaseHandle, worker::spawn_worker};

mod db;
mod db_commands;
mod worker;
use crate::db_commands::{
    apply_migration, apply_migrations_all, get_event_filter_defaults, get_events,
    get_filtered_events, get_migrations, get_overview_metrics, get_queue_metrics, get_queue_names,
    get_queue_summaries, get_settings_info, get_task_history, get_task_runs, get_task_runs_for_queue,
};

const DEVTOOLS_MENU_ID: &str = "open_devtools";

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
        .invoke_handler(tauri::generate_handler![
            get_overview_metrics,
            get_queue_metrics,
            get_task_runs,
            get_task_runs_for_queue,
            get_task_history,
            get_queue_names,
            get_queue_summaries,
            get_event_filter_defaults,
            get_events,
            get_filtered_events,
            get_settings_info,
            get_migrations,
            apply_migrations_all,
            apply_migration
        ])
        .on_menu_event(|app, event| {
            if event.id() == DEVTOOLS_MENU_ID {
                if let Some(window) = app.get_webview_window("main") {
                    window.open_devtools();
                }
            }
        })
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

            #[cfg(not(any(target_os = "android", target_os = "ios")))]
            {
                let devtools = MenuItemBuilder::with_id(DEVTOOLS_MENU_ID, "Open DevTools")
                    .accelerator("CmdOrCtrl+Alt+I")
                    .build(&app_handle)?;
                let developer_menu = SubmenuBuilder::new(&app_handle, "Developer")
                    .item(&devtools)
                    .build()?;
                let menu = MenuBuilder::new(&app_handle)
                    .item(&developer_menu)
                    .build()?;
                app.set_menu(menu)?;
            }

            async_runtime::spawn(async move { spawn_worker(&app_handle).await });

            log::info!("setup");

            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("create tauri app failed");
}
