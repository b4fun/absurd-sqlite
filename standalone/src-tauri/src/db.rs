use anyhow::{Context, Result};
use log;
use rusqlite::Connection;
use serde_json::Value;
use std::path::{Path, PathBuf};
use tauri::{path::BaseDirectory, AppHandle, Manager};
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

        let extension_path = resolve_extension_path(app_handle);
        if extension_path.is_none() {
            // fail early if no extension found
            return Err(anyhow::anyhow!("SQLite extension not found"));
        }

        log::debug!("Loading SQLite extension from {:?}", extension_path);
        // Safety: extension from own build
        unsafe {
            conn.load_extension_enable()
                .context("enable extension loading")?;
            conn.load_extension(
                extension_path.unwrap().to_string_lossy().as_ref(),
                None::<&str>,
            )
            .context("load SQLite extension")?;
            conn.load_extension_disable()
                .context("disable extension loading")?;
        }
        log::debug!("SQLite extension loaded successfully");

        Ok(conn)
    }
}

fn resolve_extension_path(app_handle: &AppHandle) -> Option<PathBuf> {
    let triple = std::env::var("TAURI_TARGET_TRIPLE").unwrap_or_else(|_| "unknown".to_string());
    let bin_name = format!(
        "absurd-extension-{}{}",
        triple,
        if cfg!(target_os = "windows") {
            ".exe"
        } else {
            ""
        }
    );

    if let Ok(path) = app_handle
        .path()
        .resolve(Path::new("bin").join(&bin_name), BaseDirectory::Resource)
    {
        if path.exists() {
            return Some(path);
        }
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .unwrap_or(&manifest_dir);
    let target_dir = workspace_root.join("target");
    let lib_name = extension_lib_name();
    let debug_path = target_dir.join("debug").join(&lib_name);
    if debug_path.exists() {
        return Some(debug_path);
    }
    let release_path = target_dir.join("release").join(&lib_name);
    if release_path.exists() {
        return Some(release_path);
    }

    None
}

fn extension_lib_name() -> String {
    if cfg!(target_os = "windows") {
        "absurd.dll".to_string()
    } else if cfg!(target_os = "macos") {
        "libabsurd.dylib".to_string()
    } else {
        "libabsurd.so".to_string()
    }
}
