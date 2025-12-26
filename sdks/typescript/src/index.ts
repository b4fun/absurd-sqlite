import { Absurd as AbsurdBase } from "absurd-sdk";
import sqlite from "better-sqlite3";

import { AbsurdClient } from "./absurd";
import { SqliteConnection } from "./sqlite";

export type { AbsurdClient, Queryable, Worker } from "./absurd";
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

export class Absurd extends AbsurdBase implements AbsurdClient {
  private db: sqlite.Database;

  constructor(
    extensionPath: string,
    filename?: string,
    options?: sqlite.Options
  ) {
    const db = new sqlite(filename, options);
    db.loadExtension(extensionPath);
    const queryable = new SqliteConnection(db);
    super(queryable);
    this.db = db;
  }

  async close(): Promise<void> {
    this.db.close();
  }
}
