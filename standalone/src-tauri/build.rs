use std::{env, fs, path::PathBuf};

fn main() {
    prepare_sqlite_extension();
    tauri_build::build()
}

fn prepare_sqlite_extension() {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root");

    let target_triple = env::var("TAURI_ENV_TARGET_TRIPLE")
        .or_else(|_| env::var("TARGET"))
        .unwrap_or_else(|_| "x86_64-unknown-linux-gnu".to_string());
    let profile = env::var("PROFILE").unwrap_or_else(|_| "debug".to_string());

    let target_dir = env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace_root.join("target"));
    let dylib_name = match target_triple.as_str() {
        triple if triple.contains("windows") => "absurd.dll",
        triple if triple.contains("apple") => "libabsurd.dylib",
        _ => "libabsurd.so",
    };

    let candidates = [
        target_dir
            .join(&target_triple)
            .join(&profile)
            .join(dylib_name),
        target_dir.join(&profile).join(dylib_name),
    ];

    let extension_path = candidates.into_iter().find(|path| path.exists()).unwrap_or_else(|| {
        panic!(
            "absurd_sqlite_extension artifact not found. Build it with `cargo build -p absurd_sqlite_extension --target {target_triple} {}` first.",
            if profile == "debug" {
                "".to_string()
            } else {
                format!("--profile {profile}")
            }
        );
    });

    let bin_dir = manifest_dir.join("bin");
    fs::create_dir_all(&bin_dir).expect("create bin directory");
    let dest = bin_dir.join("absurd-extension");
    fs::copy(&extension_path, &dest).expect("copy SQLite extension into resources");
    println!(
        "cargo:info=Copying absurd_sqlite_extension from {}",
        extension_path.display()
    );
}
