import { Absurd as AbsurdBase } from "absurd-sdk";

import type { AbsurdClient } from "./absurd-types";
import type { SQLiteDatabase } from "./sqlite-types";
import { SqliteConnection } from "./sqlite";

export type { AbsurdClient, Queryable, Worker } from "./absurd-types";
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

  async close(): Promise<void> {
    this.db.close();
  }
}
