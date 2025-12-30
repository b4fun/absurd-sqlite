# Standalone

Standalone folder hosts the Tauri v2 based desktop application, pairing a Rust backend (`src-tauri`) with a SvelteKit + Typescript frontend (`src`) to provide a local UI around the Absurd-SQLite engine.

## Development

A development environment can be started with:

```bash
$ bun tauri dev
```

This will launch the Tauri application in development mode. The SvelteKit frontend will be served with hot-reloading, and the Rust backend will be compiled and run by Tauri.

The frontend runs on `http://localhost:1420` by default. In development mode, the Tauri application
exposes a dev API server (tRPC based) on `http://localhost:11223` for debugging.

### Debugging via Playwright MCP

In development mode, please use playwright MCP to connect to the frontend app for debugging.

### Code Linting and Formatting

Please make sure to lint / format code. Use the following commands to validate:

**Frontend**

```
$ bun check
```

**Backend**

```
$ cargo fmt -p AbsurdSQLite -- --check
$ cargo clippy -p AbsurdSQLite -- -D warnings
```

## SQLite Extension

This project depends on the [`absurd-sqlite-extension`](../absurd-sqlite-extension/) for the core SQLite extension logic.
Please refer to that sub-project for development instructions around the extension itself. We can copy the built extension
from repo's Rust build target folder.
