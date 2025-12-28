use anyhow::Result;
use log;
use rusqlite::Connection;
use serde_json::Value;
use tauri::{AppHandle, Manager};
use tauri_plugin_cli::ArgData;

enum Source {
    Path(String),
    AppData,
}

pub struct DatabaseHandle {
    source: Source,
}

impl DatabaseHandle {
    fn new(source: Source) -> Self {
        Self { source }
    }

    pub fn from_path(path: &str) -> Result<Self> {
        Ok(Self::new(Source::Path(path.to_string())))
    }

    pub fn use_app_data(app_handle: &AppHandle) -> Result<Self> {
        let app_data_dir = app_handle.path().app_local_data_dir()?;
        if !app_data_dir.exists() {
            std::fs::create_dir_all(&app_data_dir)?;
        }

        let db_path = app_data_dir.join("absurd-sqlite.db");
        log::info!("Using database path: {:?}", db_path);

        Ok(Self::new(Source::AppData))
    }

    pub fn from_cli_arg(app_handle: &AppHandle, path: Option<&ArgData>) -> Result<Self> {
        if let Some(arg_data) = path {
            if let Value::String(db_path) = &arg_data.value {
                log::info!("Opening database from CLI arg: {}", db_path);
                return Self::from_path(&db_path);
            }
        }

        Self::use_app_data(app_handle)
    }

    fn db_path(&self, app_handle: &AppHandle) -> Result<String> {
        match &self.source {
            Source::Path(path) => Ok(path.clone()),
            Source::AppData => {
                let app_data_dir = app_handle.path().app_local_data_dir()?;
                if !app_data_dir.exists() {
                    std::fs::create_dir_all(&app_data_dir)?;
                }

                let db_path = app_data_dir.join("absurd-sqlite.db");
                Ok(db_path.to_string_lossy().to_string())
            }
        }
    }

    pub fn connect(&self, app_handle: &AppHandle) -> Result<Connection> {
        let db_path = self.db_path(app_handle)?;
        let conn = Connection::open(db_path)?;

        // TODO: load extension

        Ok(conn)
    }
}
