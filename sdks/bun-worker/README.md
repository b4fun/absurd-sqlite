# Absurd Sqlite Bun Worker

Utilities for running Absurd-Sqlite workers in Bun.

## Usage

Set the database path and extension path via environment variables:

- `ABSURD_DATABASE_PATH` (required): path to the SQLite database file.
- `ABSURD_DATABASE_EXTENSION_PATH` (required): path to the Absurd-SQLite extension (`libabsurd.*`).

Example:

```ts
import run from "@absurd-sqlite/bun-worker";

await run((absurd) => {
  absurd.registerTask({ name: "hello" }, async (params) => {
    return { ok: true };
  });
});
```
