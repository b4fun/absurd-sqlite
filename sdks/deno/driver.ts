import { Database, type DatabaseOpenOptions, type Statement } from "@db/sqlite";

import { Absurd } from "./mod.ts";
import type {
  SQLiteBindParams,
  SQLiteColumnDefinition,
  SQLiteDatabase,
  SQLiteStatement,
} from "./sqlite-types.ts";

export class DenoSqliteStatement<
  Result extends object = Record<string, unknown>,
> implements SQLiteStatement<Result> {
  private readonly statement: Statement<Result>;

  constructor(statement: Statement<Result>) {
    this.statement = statement;
    this.statement.enableInt64();
  }

  get readonly(): boolean {
    return this.statement.readonly;
  }

  columns(): SQLiteColumnDefinition[] {
    return this.statement.columnNames().map((name) => ({
      name,
      column: null,
      table: null,
      database: null,
      type: null,
    }));
  }

  all(params?: SQLiteBindParams): Result[] {
    return params === undefined
      ? this.statement.all()
      : this.statement.all(params);
  }

  run(params?: SQLiteBindParams): number {
    return params === undefined
      ? this.statement.run()
      : this.statement.run(params);
  }
}

export class DenoSqliteDatabase implements SQLiteDatabase {
  readonly raw: Database;

  constructor(db: Database) {
    this.raw = db;
  }

  prepare<Result extends object = Record<string, unknown>>(
    sql: string,
  ): SQLiteStatement<Result> {
    return new DenoSqliteStatement(this.raw.prepare<Result>(sql));
  }

  close(): void {
    this.raw.close();
  }

  loadExtension(path: string): void {
    this.raw.loadExtension(path);
  }
}

export function wrapDenoDatabase(db: Database): DenoSqliteDatabase {
  return new DenoSqliteDatabase(db);
}

export function openDenoDatabase(
  path: string | URL,
  options: DatabaseOpenOptions = {},
): DenoSqliteDatabase {
  const resolvedOptions = { ...options };
  if (resolvedOptions.enableLoadExtension === undefined) {
    resolvedOptions.enableLoadExtension = true;
  }
  const db = new Database(path, resolvedOptions);
  return new DenoSqliteDatabase(db);
}

export function createAbsurdWithDenoSqlite(
  path: string | URL,
  extensionPath: string,
  options: DatabaseOpenOptions = {},
): { absurd: Absurd; db: DenoSqliteDatabase } {
  const db = openDenoDatabase(path, options);
  const absurd = new Absurd(db, extensionPath);
  return { absurd, db };
}
