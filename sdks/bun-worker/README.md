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

## CLI Flags

The worker supports the following CLI flags to configure its behavior:

- `--concurrency` (or `-c`): Number of tasks to process concurrently (default: 10)
- `--poll-interval`: Polling interval in seconds (default: 5)
- `--worker-id`: Worker identifier (default: auto-generated)
- `--claim-timeout`: Claim timeout in seconds (default: 60)
- `--batch-size`: Number of tasks to claim per batch (default: matches concurrency)
- `--fatal-on-lease-timeout`: Exit process if lease timeout occurs (default: false)

Example with CLI flags:

```bash
bun run worker.ts --concurrency 5 --poll-interval 10
```

## Programmatic Configuration

You can also configure worker options programmatically:

```ts
import run from "@absurd-sqlite/bun-worker";

await run(
  (absurd) => {
    absurd.registerTask({ name: "hello" }, async (params) => {
      return { ok: true };
    });
  },
  {
    workerOptions: {
      concurrency: 5,
      pollInterval: 10,
    },
  }
);
```

To disable CLI flag parsing:

```ts
await run(setupFunction, { parseCliFlags: false });
```
