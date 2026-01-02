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
} from "@absurd-sqlite/sdk-types";

import type { SQLiteDatabase } from "./sqlite-types";
import {
  AbsurdBase,
  CancelledTask,
  SuspendTask,
  TaskContext,
  TimeoutError,
} from "./absurd-base";
import { SqliteConnection } from "./sqlite";

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
} from "./sqlite-types";

export { AbsurdBase, CancelledTask, SuspendTask, TaskContext, TimeoutError };

export class Absurd extends AbsurdBase implements AbsurdClient {
  private db: SQLiteDatabase;

  constructor(
    db: SQLiteDatabase,
    extensionPath: string,
    options?: Omit<AbsurdOptions, "db">
  ) {
    db.loadExtension(extensionPath);
    const queryable = new SqliteConnection(db);
    super({ ...(options ?? {}), db: queryable });
    this.db = db;
  }

  async close(): Promise<void> {
    await super.close();
    this.db.close();
  }
}
