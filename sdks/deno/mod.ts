import { Absurd as AbsurdBase } from "absurd-sdk";

import type { AbsurdClient } from "./absurd-types.ts";
import type { SQLiteDatabase } from "./sqlite-types.ts";
import { SqliteConnection } from "./sqlite.ts";

export type { AbsurdClient, Queryable, Worker } from "./absurd-types.ts";
export { Database } from "@db/sqlite";
export type { DatabaseOpenOptions } from "@db/sqlite";
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
} from "./sqlite-types.ts";
export {
  createAbsurdWithDenoSqlite,
  DenoSqliteDatabase,
  DenoSqliteStatement,
  openDenoDatabase,
  wrapDenoDatabase,
} from "./driver.ts";

export class Absurd extends AbsurdBase implements AbsurdClient {
  private db: SQLiteDatabase;

  constructor(db: SQLiteDatabase, extensionPath: string) {
    db.loadExtension(extensionPath);
    const queryable = new SqliteConnection(db);
    super(queryable);
    this.db = db;
  }

  override close(): Promise<void> {
    this.db.close();
    return Promise.resolve();
  }
}
