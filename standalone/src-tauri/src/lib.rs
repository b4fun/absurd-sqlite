use tauri::menu::{MenuBuilder, MenuItemBuilder, PredefinedMenuItem, SubmenuBuilder};
use tauri::{async_runtime, Manager};
use tauri_plugin_cli::CliExt;

use crate::dev_api::{load_dev_api_enabled, DevApiState};
use crate::{db::DatabaseHandle, worker::spawn_worker};

mod db;
mod db_commands;
mod dev_api;
mod worker;

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
        .plugin(tauri_plugin_store::Builder::default().build())
        .invoke_handler(tauri::generate_handler![
            db_commands::get_overview_metrics,
            db_commands::get_queue_metrics,
            db_commands::get_task_runs,
            db_commands::get_task_runs_for_queue,
            db_commands::get_task_runs_page,
            db_commands::get_task_history,
            db_commands::get_task_info,
            db_commands::get_queue_names,
            db_commands::get_queue_summaries,
            db_commands::create_queue,
            db_commands::get_task_name_options,
            db_commands::get_event_filter_defaults,
            db_commands::get_events,
            db_commands::get_filtered_events,
            db_commands::get_settings_info,
            db_commands::get_migrations,
            db_commands::apply_migrations_all,
            db_commands::apply_migration,
            dev_api::get_dev_api_status,
            dev_api::set_dev_api_enabled
        ])
        .on_menu_event(|app, event| {
            // DevTools is only available in debug builds
            #[cfg(debug_assertions)]
            if event.id() == DEVTOOLS_MENU_ID {
                if let Some(window) = app.get_webview_window("main") {
                    window.open_devtools();
                }
            }
        })
        .setup(move |app| {
            let app_handle = app.handle().clone();

            let enable_dev_api = load_dev_api_enabled(&app_handle).unwrap_or(false);
            let mut db_handle = None;

            if let Ok(matches) = app.cli().matches() {
                db_handle = Some(DatabaseHandle::from_cli_arg(
                    &app_handle,
                    matches.args.get("db"),
                )?);
            }

            let db_handle = match db_handle {
                Some(handle) => handle,
                None => DatabaseHandle::use_app_data(&app_handle)?,
            };

            app_handle.manage(db_handle);
            app_handle.manage(DevApiState::new(enable_dev_api, None));
            if enable_dev_api {
                let app_handle = app_handle.clone();
                async_runtime::spawn(async move {
                    if let Err(err) = dev_api::ensure_running(&app_handle).await {
                        log::error!("Failed to start dev api server: {}", err);
                    }
                });
            }

            #[cfg(not(any(target_os = "android", target_os = "ios")))]
            {
                let devtools = MenuItemBuilder::with_id(DEVTOOLS_MENU_ID, "Open DevTools")
                    .accelerator("CmdOrCtrl+Alt+I")
                    .build(&app_handle)?;
                let edit_menu = SubmenuBuilder::new(&app_handle, "Edit")
                    .item(&PredefinedMenuItem::cut(&app_handle, None)?)
                    .item(&PredefinedMenuItem::copy(&app_handle, None)?)
                    .item(&PredefinedMenuItem::paste(&app_handle, None)?)
                    .item(&PredefinedMenuItem::select_all(&app_handle, None)?)
                    .build()?;
                let developer_menu = SubmenuBuilder::new(&app_handle, "Developer")
                    .item(&devtools)
                    .build()?;
                let menu = MenuBuilder::new(&app_handle)
                    .item(&edit_menu)
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
