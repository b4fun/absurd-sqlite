import { Database } from "bun:sqlite";
import {
  Absurd,
  type AbsurdClient,
  type WorkerOptions,
} from "@absurd-sqlite/sdk";
import { cac } from "cac";

import { BunSqliteConnection } from "./sqlite";

export type { AbsurdClient } from "@absurd-sqlite/sdk";
export type { WorkerOptions } from "@absurd-sqlite/sdk";

export {
  downloadExtension,
  type DownloadExtensionOptions,
  Temporal,
} from "@absurd-sqlite/sdk";

/**
 * Register tasks and perform any one-time setup before the worker starts.
 */
export type SetupFunction = (absurd: AbsurdClient) => void | Promise<void>;

/**
 * Parsed CLI options.
 */
interface ParsedOptions {
  dbPath?: string;
  extensionPath?: string;
  concurrency?: number;
}

/**
 * Parses CLI arguments and returns parsed options.
 */
function parseCliOptions(): ParsedOptions {
  const cli = cac("bun-worker");

  cli
    .option(
      "-c, --concurrency <number>",
      "Number of tasks to process concurrently",
      {
        default: 10,
      }
    )
    .option("--database-path <path>", "SQLite database file path")
    .option("--extension-path <path>", "Absurd-SQLite extension path")
    .help();

  const parsed = cli.parse(process.argv, { run: false });

  // If help was requested, cac will output it and we should exit gracefully
  if (parsed.options.help) {
    process.exit(0);
  }

  const options: ParsedOptions = {};

  if (parsed.options.databasePath) {
    options.dbPath = parsed.options.databasePath;
  }
  if (parsed.options.extensionPath) {
    options.extensionPath = parsed.options.extensionPath;
  }
  if (parsed.options.concurrency) {
    const value = parseInt(parsed.options.concurrency, 10);
    if (!isNaN(value) && value > 0) {
      options.concurrency = value;
    } else {
      console.warn(
        `Invalid value for --concurrency: "${parsed.options.concurrency}" (must be a positive integer)`
      );
    }
  }

  return options;
}

/**
 * Boots a worker using Bun's SQLite driver and Absurd's task engine.
 *
 * CLI flags:
 * - --concurrency, -c: Number of tasks to process concurrently (default: 10)
 * - --database-path: SQLite database file path (overrides ABSURD_DATABASE_PATH)
 * - --extension-path: Absurd-SQLite extension path (overrides ABSURD_DATABASE_EXTENSION_PATH)
 */
export default async function run(setupFunction: SetupFunction): Promise<void> {
  const cliOptions = parseCliOptions();

  const dbPath = cliOptions.dbPath || process.env.ABSURD_DATABASE_PATH;
  const extensionPath =
    cliOptions.extensionPath || process.env.ABSURD_DATABASE_EXTENSION_PATH;

  if (!dbPath) {
    throw new Error(
      "Database path is required. Set ABSURD_DATABASE_PATH environment variable or use --database-path flag."
    );
  }
  if (!extensionPath) {
    throw new Error(
      "Extension path is required. Set ABSURD_DATABASE_EXTENSION_PATH environment variable or use --extension-path flag."
    );
  }

  const db = new Database(dbPath);
  db.loadExtension(extensionPath);

  const conn = new BunSqliteConnection(db);
  const absurd = new Absurd(conn);

  await setupFunction(absurd);

  // Merge worker options from multiple sources:
  // 1. Default options
  // 2. CLI flags
  const workerOptions: WorkerOptions = {
    ...getDefaultWorkerOptions(),
    ...(cliOptions.concurrency !== undefined
      ? { concurrency: cliOptions.concurrency }
      : {}),
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
