# Bun Worker Sample

This sample demonstrates how to use the `@absurd-sqlite/bun-worker` SDK to create a worker that processes tasks.

## Prerequisites

1. Build the Absurd-SQLite extension:
   ```bash
   cd ../../
   cargo build -p absurd-sqlite-extension --release
   ```

2. Install dependencies:
   ```bash
   bun install
   ```

## Running the Sample

The worker can be run with default settings:

```bash
ABSURD_DATABASE_PATH=./test.db \
ABSURD_DATABASE_EXTENSION_PATH=../../target/release/libabsurd.dylib \
bun run src/index.ts
```

Or with custom CLI flags to configure worker behavior:

```bash
ABSURD_DATABASE_PATH=./test.db \
ABSURD_DATABASE_EXTENSION_PATH=../../target/release/libabsurd.dylib \
bun run src/index.ts --concurrency 5 --poll-interval 10
```

## Available CLI Flags

- `--concurrency` (or `-c`): Number of tasks to process concurrently (default: 10)
- `--poll-interval`: Polling interval in seconds (default: 5)
- `--worker-id`: Worker identifier (default: auto-generated)
- `--claim-timeout`: Claim timeout in seconds (default: 60)
- `--batch-size`: Number of tasks to claim per batch (default: matches concurrency)
- `--fatal-on-lease-timeout`: Exit process if lease timeout occurs (default: false)

## Example with Multiple Workers

You can run multiple workers with different configurations:

```bash
# Worker 1 with high concurrency
ABSURD_DATABASE_PATH=./test.db \
ABSURD_DATABASE_EXTENSION_PATH=../../target/release/libabsurd.dylib \
bun run src/index.ts --concurrency 20 --worker-id worker-1 &

# Worker 2 with lower concurrency
ABSURD_DATABASE_PATH=./test.db \
ABSURD_DATABASE_EXTENSION_PATH=../../target/release/libabsurd.dylib \
bun run src/index.ts --concurrency 5 --worker-id worker-2 &
```
