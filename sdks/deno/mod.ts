import type {
  AbsurdClient,
  AbsurdHooks,
  AbsurdOptions,
  CancellationPolicy,
  ClaimedTask,
  JsonObject,
  JsonValue,
  Queryable,
  RetryStrategy,
  SpawnOptions,
  SpawnResult,
  TaskHandler,
  TaskRegistrationOptions,
  Worker,
  WorkerOptions,
} from "./absurd-types.ts";

import type { SQLiteDatabase } from "./sqlite-types.ts";
import {
  AbsurdBase,
  CancelledTask,
  SuspendTask,
  TaskContext,
  TimeoutError,
} from "./absurd-base.ts";
import { SqliteConnection } from "./sqlite.ts";

export type {
  AbsurdClient,
  AbsurdHooks,
  AbsurdOptions,
  CancellationPolicy,
  ClaimedTask,
  JsonObject,
  JsonValue,
  Queryable,
  RetryStrategy,
  SpawnOptions,
  SpawnResult,
  TaskHandler,
  TaskRegistrationOptions,
  Worker,
  WorkerOptions,
};
export type {
  SQLiteBindParams,
  SQLiteBindValue,
  SQLiteColumnDefinition,
  SQLiteDatabase,
  SQLiteRestBindParams,
  SQLiteStatement,
  SQLiteVerboseLog,
} from "./sqlite-types.ts";

export { AbsurdBase, CancelledTask, SuspendTask, TaskContext, TimeoutError };

export { Database } from "@db/sqlite";
export type { DatabaseOpenOptions } from "@db/sqlite";
export {
  createAbsurdWithDenoSqlite,
  DenoSqliteDatabase,
  DenoSqliteStatement,
  openDenoDatabase,
  wrapDenoDatabase,
} from "./driver.ts";

export class Absurd extends AbsurdBase implements AbsurdClient {
  private db: SQLiteDatabase;

  constructor(
    db: SQLiteDatabase,
    extensionPath: string,
    options?: Omit<AbsurdOptions, "db">,
  ) {
    db.loadExtension(extensionPath);
    const queryable = new SqliteConnection(db);
    super({ ...(options ?? {}), db: queryable });
    this.db = db;
  }

  override async close(): Promise<void> {
    await super.close();
    this.db.close();
  }
}
