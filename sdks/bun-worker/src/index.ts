import { Absurd, type WorkerOptions } from "absurd-sdk";
import { Database } from "bun:sqlite";
import type { AbsurdClient } from "@absurd-sqlite/sdk";
import { parseArgs } from "node:util";

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
 * Requires:
 * - ABSURD_DATABASE_PATH: SQLite database file path.
 * - ABSURD_DATABASE_EXTENSION_PATH: Absurd-SQLite extension path (libabsurd.*).
 *
 * CLI flags (when parseCliFlags is true):
 * - --concurrency: Number of tasks to process concurrently (default: 10)
 * - --poll-interval: Polling interval in seconds (default: 5)
 * - --worker-id: Worker identifier (default: hostname)
 * - --claim-timeout: Claim timeout in seconds (default: 60)
 * - --batch-size: Number of tasks to claim per batch (default: matches concurrency)
 * - --fatal-on-lease-timeout: Exit process if lease timeout occurs
 */
export default async function run(
  setupFunction: SetupFunction,
  options?: RunOptions
): Promise<void> {
  const dbPath = process.env.ABSURD_DATABASE_PATH;
  const extensionPath = process.env.ABSURD_DATABASE_EXTENSION_PATH;

  if (!dbPath) {
    throw new Error("ABSURD_DATABASE_PATH is required");
  }
  if (!extensionPath) {
    throw new Error("ABSURD_DATABASE_EXTENSION_PATH is required");
  }

  const db = new Database(dbPath);
  (db as unknown as { loadExtension(path: string): void }).loadExtension(
    extensionPath
  );

  const conn = new BunSqliteConnection(db);
  const absurd = new Absurd({ db: conn });

  await setupFunction(absurd);

  // Merge worker options from multiple sources:
  // 1. Explicit options passed to run()
  // 2. CLI flags (if parseCliFlags is true)
  // 3. Defaults
  const parseFlags = options?.parseCliFlags !== false;
  const workerOptions: WorkerOptions = {
    ...getDefaultWorkerOptions(),
    ...(parseFlags ? parseCliArgs() : {}),
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

/**
 * Parses CLI arguments and returns worker options.
 */
function parseCliArgs(): Partial<WorkerOptions> {
  try {
    const { values } = parseArgs({
      options: {
        concurrency: {
          type: "string",
          short: "c",
        },
        "poll-interval": {
          type: "string",
        },
        "worker-id": {
          type: "string",
        },
        "claim-timeout": {
          type: "string",
        },
        "batch-size": {
          type: "string",
        },
        "fatal-on-lease-timeout": {
          type: "boolean",
          default: false,
        },
      },
      strict: false,
      allowPositionals: true,
    });

    const options: Partial<WorkerOptions> = {};

    if (values.concurrency && typeof values.concurrency === "string") {
      const concurrency = parseInt(values.concurrency, 10);
      if (!isNaN(concurrency) && concurrency > 0) {
        options.concurrency = concurrency;
      }
    }

    if (values["poll-interval"] && typeof values["poll-interval"] === "string") {
      const pollInterval = parseFloat(values["poll-interval"]);
      if (!isNaN(pollInterval) && pollInterval > 0) {
        options.pollInterval = pollInterval;
      }
    }

    if (values["worker-id"] && typeof values["worker-id"] === "string") {
      options.workerId = values["worker-id"];
    }

    if (values["claim-timeout"] && typeof values["claim-timeout"] === "string") {
      const claimTimeout = parseInt(values["claim-timeout"], 10);
      if (!isNaN(claimTimeout) && claimTimeout > 0) {
        options.claimTimeout = claimTimeout;
      }
    }

    if (values["batch-size"] && typeof values["batch-size"] === "string") {
      const batchSize = parseInt(values["batch-size"], 10);
      if (!isNaN(batchSize) && batchSize > 0) {
        options.batchSize = batchSize;
      }
    }

    if (values["fatal-on-lease-timeout"]) {
      options.fatalOnLeaseTimeout = true;
    }

    return options;
  } catch (err) {
    // If parseArgs fails, return empty options
    console.error("Failed to parse CLI arguments:", err);
    return {};
  }
}
