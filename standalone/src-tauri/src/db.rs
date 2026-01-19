use anyhow::{Context, Result};
use reqwest::blocking::Client;
use rusqlite::Connection;
use serde::Deserialize;
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
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
                return Self::from_path(db_path);
            }
        }

        Self::use_app_data(app_handle)
    }

    pub fn db_path(&self, app_handle: &AppHandle) -> Result<String> {
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

        log::info!("using SQLite version: {}", rusqlite::version());

        let extension_path = resolve_extension_path(app_handle);
        if extension_path.is_none() {
            // fail early if no extension found
            log::error!("SQLite extension path could not be resolved");
            return Err(anyhow::anyhow!("SQLite extension not found"));
        }

        log::debug!("Loading SQLite extension from {:?}", extension_path);
        // Safety: extension from own build
        unsafe {
            if let Err(err) = conn
                .load_extension_enable()
                .context("enable extension loading")
            {
                log::error!("Failed to enable SQLite extension loading: {:#}", err);
                return Err(err);
            }
            // remove the extension part from the path
            let extension_path_no_ext = extension_path.unwrap().with_extension("");
            if let Err(err) = conn
                .load_extension(
                    extension_path_no_ext.to_string_lossy().as_ref(),
                    Some("sqlite3_absurd_init"),
                )
                .context("load SQLite extension")
            {
                log::error!("Failed to load SQLite extension: {:#}", err);
                return Err(err);
            }
            if let Err(err) = conn
                .load_extension_disable()
                .context("disable extension loading")
            {
                log::error!("Failed to disable SQLite extension loading: {:#}", err);
                return Err(err);
            }
        }
        log::debug!("SQLite extension loaded successfully");

        Ok(conn)
    }
}

pub fn extension_path(app_handle: &AppHandle) -> Result<String> {
    resolve_extension_path(app_handle)
        .map(|path| path.to_string_lossy().to_string())
        .ok_or_else(|| anyhow::anyhow!("SQLite extension not found"))
}

fn resolve_extension_path(app_handle: &AppHandle) -> Option<PathBuf> {
    let lib_name = extension_lib_name();
    if let Ok(path) = env::var("ABSURD_SQLITE_EXTENSION_PATH") {
        let path = PathBuf::from(path);
        if path.exists() {
            log::info!("Using SQLite extension from env at {}", path.display());
            return Some(path);
        }
        log::warn!(
            "SQLite extension path from env does not exist: {}",
            path.display()
        );
    }
    #[cfg(debug_assertions)]
    {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace_root = manifest_dir
            .parent()
            .and_then(Path::parent)
            .unwrap_or(&manifest_dir);
        let target_dir = workspace_root.join("target");
        let candidates = [
            ("debug build", target_dir.join("debug").join(&lib_name)),
            ("release build", target_dir.join("release").join(&lib_name)),
        ];

        for (label, path) in candidates {
            log::debug!("Checking {} SQLite extension at {}", label, path.display());
            if path.exists() {
                log::info!("Using {} SQLite extension at {}", label, path.display());
                return Some(path);
            }
        }
    }

    match download_extension(app_handle) {
        Ok(path) => return Some(path),
        Err(err) => log::warn!("Failed to download SQLite extension: {:#}", err),
    }

    #[cfg(debug_assertions)]
    {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace_root = manifest_dir
            .parent()
            .and_then(Path::parent)
            .unwrap_or(&manifest_dir);
        let target_dir = workspace_root.join("target");
        log::warn!(
            "SQLite extension not found. Checked bundled resources and build outputs in {}",
            target_dir.display()
        );
    }

    #[cfg(not(debug_assertions))]
    {
        log::warn!("SQLite extension not found in bundled resources.");
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

struct PlatformInfo {
    os: &'static str,
    arch: &'static str,
    ext: &'static str,
}

#[derive(Deserialize)]
struct ReleaseInfo {
    tag_name: String,
    draft: bool,
}

fn download_extension(app_handle: &AppHandle) -> Result<PathBuf> {
    let owner = "b4fun";
    let repo = "absurd-sqlite";

    let client = Client::builder()
        .user_agent("absurd-sqlite-standalone")
        .build()
        .context("build GitHub HTTP client")?;

    let version = fetch_latest_version(&client, owner, repo)?;
    let platform = platform_info()?;
    let asset_name = asset_name(&version, &platform);
    let tag = format!("absurd-sqlite-extension/{}", version);

    let cache_dir = app_handle
        .path()
        .app_cache_dir()
        .context("resolve app cache dir")?
        .join("absurd-sqlite")
        .join("extensions")
        .join(&version);
    fs::create_dir_all(&cache_dir).context("create extension cache dir")?;

    let cached_path = cache_dir.join(extension_lib_name());
    if cached_path.exists() {
        log::info!("Using cached SQLite extension at {}", cached_path.display());
        return Ok(cached_path);
    }

    let url = format!("https://github.com/{owner}/{repo}/releases/download/{tag}/{asset_name}");
    log::info!("Downloading SQLite extension from {}", url);
    let response = client
        .get(url)
        .send()
        .context("request extension asset")?
        .error_for_status()
        .context("download extension asset")?;
    let bytes = response.bytes().context("read extension bytes")?;
    fs::write(&cached_path, bytes).context("write extension to cache")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = fs::Permissions::from_mode(0o755);
        fs::set_permissions(&cached_path, permissions).context("chmod extension")?;
    }

    Ok(cached_path)
}

fn fetch_latest_version(client: &Client, owner: &str, repo: &str) -> Result<String> {
    let url = format!("https://api.github.com/repos/{owner}/{repo}/releases");
    let releases = client
        .get(url)
        .send()
        .context("fetch releases")?
        .error_for_status()
        .context("download releases")?
        .json::<Vec<ReleaseInfo>>()
        .context("parse releases")?;

    for release in releases {
        if !release.draft && release.tag_name.starts_with("absurd-sqlite-extension/") {
            return Ok(release
                .tag_name
                .trim_start_matches("absurd-sqlite-extension/")
                .to_string());
        }
    }

    Err(anyhow::anyhow!("No extension releases found"))
}

fn platform_info() -> Result<PlatformInfo> {
    let (os, ext) = if cfg!(target_os = "macos") {
        ("macOS", "dylib")
    } else if cfg!(target_os = "linux") {
        ("Linux", "so")
    } else if cfg!(target_os = "windows") {
        ("Windows", "dll")
    } else {
        return Err(anyhow::anyhow!("Unsupported platform"));
    };

    let arch = match env::consts::ARCH {
        "x86_64" => "X64",
        "aarch64" => "ARM64",
        other => return Err(anyhow::anyhow!("Unsupported architecture: {}", other)),
    };

    Ok(PlatformInfo { os, arch, ext })
}

fn asset_name(version: &str, platform: &PlatformInfo) -> String {
    format!(
        "absurd-absurd-sqlite-extension-{}-{}-{}.{}",
        version, platform.os, platform.arch, platform.ext
    )
}
