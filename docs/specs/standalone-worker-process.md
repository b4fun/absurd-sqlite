# Standalone Worker Process Control

## Summary

Add support for starting/stopping a user-specified worker executable from the Standalone Settings page. The GUI will spawn the worker with `ABSURD_DATABASE_PATH` (current database path) and `ABSURD_DATABASE_EXTENSION_PATH` (bundled extension path) set, show the running PID, and surface a crash indicator when the worker repeatedly exits unexpectedly.

## Goals

- Let users configure a full path to a worker executable in Settings.
- Allow start/stop control from Settings.
- Display worker PID while running.
- Display a crash indicator if the worker is crashing (rapid, repeated exits).
- Pass required environment variables when starting the worker.

## Non-Goals

- Auto-starting the worker on app launch.
- Managing multiple worker processes.
- Implementing worker stdout/stderr log streaming in the UI.
- Bundling a worker binary with the app.

## User Experience

- Settings page shows a "Worker" card with:
  - Text input for full executable path.
  - Status line: "Running (PID ####)", "Stopped", or "Crashing".
  - Start/Stop button (disabled if no valid path is set).
- Crash indicator appears if the worker exits unexpectedly multiple times within a short window (e.g., 3 exits within 60 seconds).

### UI Layout (Settings)

```
Settings
------------------------------------------------------------
[Version Card]          [Database Card]

[Migrations Card]

[Worker Card]
------------------------------------------------------------
Worker
Run a local worker process for this database.

Executable path  [ /full/path/to/worker............... ]
Status           [ Running (PID 12345) | Stopped | Crashing ]

[ Start ]  [ Stop ]
```

## Data Model & Persistence

- Use `tauri_plugin_store` to persist worker configuration in a JSON store (e.g. `worker.json`).
- Keys:
  - `worker_binary_path` (string, full path to executable).

## Backend Design (Tauri)

### New State

- `WorkerState` managed in `AppHandle`:
  - `binary_path: Mutex<Option<String>>`
  - `running: Mutex<Option<RunningWorker>>`
  - `crash_history: Mutex<VecDeque<Instant>>`
- `RunningWorker`:
  - `pid: u32`
  - `child: tauri_plugin_shell::process::Child`
  - `rx: CommandEvent` receiver task handle

### New Commands

- `get_worker_status` -> `{ configuredPath, running, pid, crashing }`
- `set_worker_binary_path(path: String)` -> updated status
- `start_worker()` -> updated status
- `stop_worker()` -> updated status

### Spawn Behavior

- Validate configured path exists and is executable (best-effort cross-platform checks).
- Use `DatabaseHandle` to resolve the current `ABSURD_DATABASE_PATH`.
- Expose a helper to resolve the bundled extension path from `db.rs` for `ABSURD_DATABASE_EXTENSION_PATH`.
- Spawn using `tauri_plugin_shell`:
  - Command = configured executable path.
  - Env:
    - `ABSURD_DATABASE_PATH=<db_path>`
    - `ABSURD_DATABASE_EXTENSION_PATH=<extension_path>`
- Track process exit:
  - If terminated while `start_worker` initiated and not explicitly stopped, record crash time.
  - Crash indicator = N exits within rolling window (e.g., 3 in 60s).

### Stop Behavior

- If running, terminate the child process.
- Clear `running` state.
- Do not mark crash on user-initiated stop.

## Frontend Design (Svelte)

- Extend `SettingsInfo` or add a new API payload for worker status.
- New UI section in `standalone/src/routes/settings/+page.svelte`:
  - Input bound to worker path.
  - Start/Stop button invoking Tauri commands.
  - Status badge:
    - Running with PID.
    - Stopped.
    - Crashing (if `crashing === true`).
- Refresh status on mount and after any start/stop/path changes.

## Error Handling

- Surface start/stop errors to the UI (e.g., toast or inline message).
- If extension path cannot be resolved, block start and show error.

## Testing

- Backend unit tests for:
  - Parsing/persistence of stored worker path.
  - Crash indicator threshold logic.
- Manual smoke test:
  - Set a valid worker path, start, verify PID.
  - Stop and verify state.
  - Use a dummy executable that exits immediately to trigger crash indicator.
