import { Absurd as AbsurdBase } from "absurd-sdk";

import type { AbsurdClient } from "./absurd-types";
import type { SQLiteDatabase } from "./sqlite-types";
import { SqliteConnection } from "./sqlite";

export type { AbsurdClient, Queryable, Worker } from "./absurd-types";
export {
  downloadExtension,
  resolveExtensionPath,
  type DownloadExtensionOptions,
} from "./extension-downloader";
export type {
  AbsurdHooks,
  AbsurdOptions,
  CancellationPolicy,
  ClaimedTask,
  JsonObject,
  JsonValue,
  RetryStrategy,
  SpawnOptions,
  SpawnResult,
  TaskContext,
  TaskHandler,
  TaskRegistrationOptions,
  WorkerOptions,
} from "absurd-sdk";
export type {
  SQLiteBindParams,
  SQLiteBindValue,
  SQLiteColumnDefinition,
  SQLiteDatabase,
  SQLiteRestBindParams,
  SQLiteStatement,
  SQLiteVerboseLog,
} from "./sqlite-types";

export class Absurd extends AbsurdBase implements AbsurdClient {
  private db: SQLiteDatabase;

  constructor(db: SQLiteDatabase, extensionPath: string) {
    db.loadExtension(extensionPath);
    const queryable = new SqliteConnection(db);
    super(queryable);
    this.db = db;
  }

  /**
   * Creates a new Absurd instance with automatic extension resolution.
   * If no extension path is provided, attempts to use the ABSURD_SQLITE_EXTENSION_PATH
   * environment variable or downloads the extension from GitHub releases.
   *
   * @param db - SQLite database instance
   * @param options - Optional extension path or download options
   * @returns Promise resolving to Absurd instance
   *
   * @example
   * ```typescript
   * import Database from "better-sqlite3";
   * import { Absurd } from "@absurd-sqlite/sdk";
   *
   * const db = new Database("mydb.db");
   *
   * // Automatic: downloads latest version if needed
   * const absurd = await Absurd.create(db);
   *
   * // With specific extension path
   * const absurd = await Absurd.create(db, { extensionPath: "/path/to/extension.so" });
   *
   * // Download specific version
   * const absurd = await Absurd.create(db, { downloadOptions: { version: "v0.1.0-alpha.3" } });
   * ```
   */
  static async create(
    db: SQLiteDatabase,
    options?: {
      extensionPath?: string;
      downloadOptions?: import("./extension-downloader").DownloadExtensionOptions;
    }
  ): Promise<Absurd> {
    const { resolveExtensionPath } = await import("./extension-downloader");
    const extensionPath = await resolveExtensionPath(
      options?.extensionPath,
      options?.downloadOptions
    );
    return new Absurd(db, extensionPath);
  }

  async close(): Promise<void> {
    this.db.close();
  }
}
