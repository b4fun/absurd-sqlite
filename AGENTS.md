# Absurd-SQLite

Absurd-SQLite is a SQLite based durable execution engine designed for personal / homelab / edge computing environments.

This project is based on the PostgreSQL implementation from [absurd](https://github.com/earendil-works/absurd).

## Key Concepts

A task is subdivided into steps that act as checkpoints. Tasks can suspend (for sleep or events) and resume without data loss.
State data is stored in the SQLite tables below:

- Queues: `absurd_queues` scopes all durable execution by `queue_name`.
- Tasks: `absurd_tasks` stores task definitions, parameters, headers, retry/cancel info, idempotency keys, and task state.
- Runs: `absurd_runs` represents per-attempt execution for a task with claim/lease timing, availability, and results/failures.
- Checkpoints: `absurd_checkpoints` tracks named task checkpoints with optional state and run ownership.
- Events: `absurd_events` stores emitted events with JSON payloads scoped by queue and event name.
- Waits: `absurd_waits` records task steps waiting on events or timeouts.

To interact with the system, a SQLite extension is provided and exposed various SQL functions.

## Repository Structure

- `absurd-sqlite-extension`: Core Rust crate providing the SQLite extension, engine logic, and schema migrations.
- `standalone`: Tauri + SvelteKit desktop app.
- `sdks`: Client SDKs for different languages.
- `samples`: Example applications demonstrating usage.

## Development

Please follow the sub-project README.md / AGENTS.md for detailed instructions.
