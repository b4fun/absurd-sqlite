import { Absurd } from "absurd-sdk";
import { Database } from "bun:sqlite";
import type { AbsurdClient } from "@absurd-sqlite/sdk";

import { BunSqliteConnection } from "./sqlite";
import {
  resolveExtensionPath,
  type DownloadExtensionOptions,
} from "./extension-downloader";

export type { AbsurdClient } from "@absurd-sqlite/sdk";
export {
  downloadExtension,
  resolveExtensionPath,
  type DownloadExtensionOptions,
} from "./extension-downloader";

/**
 * Register tasks and perform any one-time setup before the worker starts.
 */
export type SetupFunction = (absurd: AbsurdClient) => void | Promise<void>;

export interface WorkerOptions {
  /**
   * SQLite database file path.
   * If not provided, reads from ABSURD_DATABASE_PATH environment variable.
   */
  dbPath?: string;

  /**
   * Absurd-SQLite extension path.
   * If not provided, reads from ABSURD_DATABASE_EXTENSION_PATH environment variable,
   * or downloads from GitHub releases if ABSURD_DOWNLOAD_EXTENSION=true.
   */
  extensionPath?: string;

  /**
   * Options for downloading the extension from GitHub releases.
   * Only used if extensionPath is not provided and ABSURD_DOWNLOAD_EXTENSION=true.
   */
  downloadOptions?: DownloadExtensionOptions;
}

/**
 * Boots a worker using Bun's SQLite driver and Absurd's task engine.
 *
 * Environment variables:
 * - ABSURD_DATABASE_PATH: SQLite database file path (required if not in options)
 * - ABSURD_DATABASE_EXTENSION_PATH: Extension path (optional if downloading)
 * - ABSURD_DOWNLOAD_EXTENSION: Set to "true" to enable automatic download
 *
 * @example
 * ```typescript
 * // With environment variables
 * export default run(async (absurd) => {
 *   absurd.registerTask("myTask", async (ctx) => {
 *     // task implementation
 *   });
 * });
 *
 * // With automatic download
 * export default run(async (absurd) => {
 *   absurd.registerTask("myTask", async (ctx) => {
 *     // task implementation
 *   });
 * }, { downloadOptions: { version: "latest" } });
 * ```
 */
export default async function run(
  setupFunction: SetupFunction,
  options: WorkerOptions = {}
): Promise<void> {
  const dbPath = options.dbPath ?? process.env.ABSURD_DATABASE_PATH;

  if (!dbPath) {
    throw new Error("ABSURD_DATABASE_PATH is required");
  }

  // Resolve extension path
  const shouldDownload =
    process.env.ABSURD_DOWNLOAD_EXTENSION === "true" ||
    options.downloadOptions !== undefined;
  let extensionPath = options.extensionPath;

  if (!extensionPath) {
    if (shouldDownload || !process.env.ABSURD_DATABASE_EXTENSION_PATH) {
      // Use resolveExtensionPath which will try env var first, then download
      extensionPath = await resolveExtensionPath(
        undefined,
        options.downloadOptions
      );
    } else {
      extensionPath = process.env.ABSURD_DATABASE_EXTENSION_PATH;
    }
  }

  if (!extensionPath) {
    throw new Error(
      "ABSURD_DATABASE_EXTENSION_PATH is required or set ABSURD_DOWNLOAD_EXTENSION=true"
    );
  }

  const db = new Database(dbPath);
  (db as unknown as { loadExtension(path: string): void }).loadExtension(
    extensionPath
  );

  const conn = new BunSqliteConnection(db);
  const absurd = new Absurd({ db: conn });

  await setupFunction(absurd);
  const worker = await absurd.startWorker();

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
