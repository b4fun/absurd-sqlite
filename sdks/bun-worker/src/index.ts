import { Absurd, type WorkerOptions } from "absurd-sdk";
import { Database } from "bun:sqlite";
import type { AbsurdClient } from "@absurd-sqlite/sdk";
import { cac } from "cac";

import { BunSqliteConnection } from "./sqlite";

export type { AbsurdClient } from "@absurd-sqlite/sdk";
export type { WorkerOptions } from "absurd-sdk";

/**
 * Register tasks and perform any one-time setup before the worker starts.
 */
export type SetupFunction = (absurd: AbsurdClient) => void | Promise<void>;

/**
 * Configuration options for the worker runtime.
 */
export interface RunOptions {
  /**
   * Worker configuration options.
   * If not provided, CLI flags and environment variables will be used.
   */
  workerOptions?: WorkerOptions;
  /**
   * Whether to parse CLI flags for worker configuration.
   * Defaults to true.
   */
  parseCliFlags?: boolean;
}

/**
 * Boots a worker using Bun's SQLite driver and Absurd's task engine.
 *
 * CLI flags (when parseCliFlags is true):
 * - --concurrency, -c: Number of tasks to process concurrently (default: 10)
 * - --database, -d: SQLite database file path (overrides ABSURD_DATABASE_PATH)
 * - --extension, -e: Absurd-SQLite extension path (overrides ABSURD_DATABASE_EXTENSION_PATH)
 */
export default async function run(
  setupFunction: SetupFunction,
  options?: RunOptions
): Promise<void> {
  const parseFlags = options?.parseCliFlags !== false;
  
  let dbPath = process.env.ABSURD_DATABASE_PATH;
  let extensionPath = process.env.ABSURD_DATABASE_EXTENSION_PATH;
  let concurrency: number | undefined;

  if (parseFlags) {
    const cli = cac("bun-worker");
    
    cli
      .option("-c, --concurrency <number>", "Number of tasks to process concurrently", {
        default: 10,
      })
      .option("-d, --database <path>", "SQLite database file path")
      .option("-e, --extension <path>", "Absurd-SQLite extension path")
      .help();

    const parsed = cli.parse(process.argv, { run: false });
    
    if (parsed.options.database) {
      dbPath = parsed.options.database;
    }
    if (parsed.options.extension) {
      extensionPath = parsed.options.extension;
    }
    if (parsed.options.concurrency) {
      const value = parseInt(parsed.options.concurrency, 10);
      if (!isNaN(value) && value > 0) {
        concurrency = value;
      } else {
        console.warn(`Invalid value for --concurrency: "${parsed.options.concurrency}" (must be a positive integer)`);
      }
    }
  }

  if (!dbPath) {
    throw new Error("Database path is required. Set ABSURD_DATABASE_PATH environment variable or use --database flag.");
  }
  if (!extensionPath) {
    throw new Error("Extension path is required. Set ABSURD_DATABASE_EXTENSION_PATH environment variable or use --extension flag.");
  }

  const db = new Database(dbPath);
  (db as unknown as { loadExtension(path: string): void }).loadExtension(
    extensionPath
  );

  const conn = new BunSqliteConnection(db);
  const absurd = new Absurd({ db: conn });

  await setupFunction(absurd);

  // Merge worker options from multiple sources:
  // 1. Default options
  // 2. CLI flags (if parseCliFlags is true)
  // 3. Explicit options passed to run()
  const workerOptions: WorkerOptions = {
    ...getDefaultWorkerOptions(),
    ...(concurrency !== undefined ? { concurrency } : {}),
    ...options?.workerOptions,
  };

  const worker = await absurd.startWorker(workerOptions);

  let shuttingDown = false;
  const shutdown = async (signal: string) => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    try {
      await worker.close();
    } catch (err) {
      console.error(`Failed to close worker on ${signal}`, err);
    } finally {
      db.close();
    }
  };

  process.once("SIGINT", () => {
    void shutdown("SIGINT");
  });
  process.once("SIGTERM", () => {
    void shutdown("SIGTERM");
  });
}

/**
 * Returns default worker options.
 */
function getDefaultWorkerOptions(): WorkerOptions {
  return {
    concurrency: 10,
    pollInterval: 5,
    claimTimeout: 60,
  };
}
