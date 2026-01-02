use std::{env, fs, path::PathBuf, process::Command};

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

    if candidates.iter().all(|path| !path.exists()) {
        let mut cmd = Command::new("cargo");
        cmd.current_dir(workspace_root)
            .args(["build", "-p", "absurd-sqlite-extension"])
            .arg("--target")
            .arg(&target_triple);
        if profile != "debug" {
            cmd.arg("--profile").arg(&profile);
        }
        let status = cmd
            .status()
            .expect("failed to invoke cargo build for absurd-sqlite-extension");
        if !status.success() {
            panic!("cargo build -p absurd-sqlite-extension failed");
        }
    }

    let extension_path = candidates
        .into_iter()
        .find(|path| path.exists())
        .unwrap_or_else(|| {
            panic!("absurd-sqlite-extension artifact not found after build");
        });

    let resources_dir = manifest_dir.join("resources");
    fs::create_dir_all(&resources_dir).expect("create resources directory");
    let dest = resources_dir.join(dylib_name);
    fs::copy(&extension_path, &dest).expect("copy SQLite extension into resources");
    println!(
        "cargo:info=Copying absurd-sqlite-extension from {} to {}",
        extension_path.display(),
        dest.display()
    );
}
