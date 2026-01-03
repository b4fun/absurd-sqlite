import { Absurd } from "absurd-sdk";
import { Database } from "bun:sqlite";

import { BunSqliteConnection } from "./sqlite";

/**
 * Register tasks and perform any one-time setup before the worker starts.
 */
export type SetupFunction = (absurd: Absurd) => void | Promise<void>;

/**
 * Boots a worker using Bun's SQLite driver and Absurd's task engine.
 *
 * Requires:
 * - ABSURD_DATABASE_PATH: SQLite database file path.
 * - ABSURD_DATABASE_EXTENSION_PATH: Absurd-SQLite extension path (libabsurd.*).
 */
export default async function run(
  setupFunction: SetupFunction
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
  
  // Enable WAL mode for better concurrency and performance
  db.exec("PRAGMA journal_mode=WAL");
  
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
