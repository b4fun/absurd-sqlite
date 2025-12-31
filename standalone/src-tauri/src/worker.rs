use anyhow::Result;
use tauri::AppHandle;
use tauri_plugin_shell::{process::CommandEvent, ShellExt};

pub async fn spawn_worker(handle: &AppHandle) -> Result<()> {
    let shell = handle.shell();

    log::info!("Spawning worker.....");

    let (mut rx, _child) = shell
        .command("node")
        .args(["./worker.js"])
        .spawn()
        .expect("failed to spawn worker");

    while let Some(event) = rx.recv().await {
        match event {
            CommandEvent::Terminated(code) => {
                log::info!("Worker terminated with code: {:?}", code);
                break;
            }
            CommandEvent::Stderr(line) => {
                log::info!("Worker stderr: {}", String::from_utf8(line).unwrap());
            }
            CommandEvent::Stdout(line) => {
                log::info!("Worker stdout: {}", String::from_utf8(line).unwrap());
            }
            _ => {}
        }
    }

    Ok(())
}
